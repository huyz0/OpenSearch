/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Whether {@code .opensearch-index-mappings} has gone away after having been there.
 *
 * <p>T48. {@link IndexBackedMappingStore#read} answers null for a missing mapping index, and that answer is
 * a judgement rather than an observation: it is right when nothing has ever been written, because then no
 * index can have a stored mapping, and it is wrong in exactly the opposite way when the index is deleted
 * under a live cluster. There the mappings existed, they are gone, and reporting that as "this index
 * declares no fields" is the silent loss this area keeps producing -- the caller merges onto empty and the
 * write that follows succeeds, because {@code ensureIndexExists} recreates the index and the swap has
 * nothing left to lose to.
 *
 * <p><b>Neither the exception nor cluster state can tell the two apart, which is why this exists.</b> The
 * get fails the same way in both cases, and a cluster state that lacks the index looks the same whether it
 * never had it or lost it. What distinguishes them is a transition, and a transition has to be watched for
 * rather than asked about. So this listens: once the index is seen, its later absence is a deletion, and a
 * deletion is remembered.
 *
 * <p><b>The signal is the index's UUID, not its presence, and the first version got that wrong.</b>
 * Latching on disappearance and clearing on reappearance looks right and is undone by the next write: a
 * creation calls {@code compareAndSwap}, which finds its cached "the index exists" flag still true, skips
 * the create, and lets auto-creation make the index again -- empty. The watcher then reports "not deleted",
 * the store reports absence as truth again, and the next inference merges onto empty and succeeds, because
 * the document it would have lost to is gone. The defect returns within seconds of being detected. A
 * different UUID is what says the contents are gone, and it stays said.
 *
 * <p><b>So the latch does not clear.</b> Once this node has seen the store lost, absence can never again be
 * trusted for any index that existed before, and nothing here can tell those apart from indices created
 * after. Refusing is the direction that fails loudly, and the alternative is quietly rewriting mappings.
 * Restoring the index from a snapshot and restarting the node is the recovery; that is a worse operation
 * than a restart and a better one than silent loss.
 *
 * <p><b>What this does not cover, stated because the limit is the point.</b> The latch is in memory on each
 * node, and the node it has to be set on is whichever is the elected cluster manager, since every
 * put-mapping and every dynamic inference lands in {@code MetadataMappingService.putMapping} there. Delete
 * the index, then restart or fail over, and the new cluster manager has never seen it: the guard is off for
 * the whole cluster. Nothing durable records that the store was lost, and recording it durably means
 * recording it somewhere that is not the store. There is also a window of one read: cluster state is
 * visible before listeners run, so a read that is already in flight when the delete lands gets the old
 * answer, and {@code updateMapping} retries on exception rather than on null.
 */
public final class MappingIndexWatcher implements ClusterStateListener, BooleanSupplier {

    private static final Logger logger = LogManager.getLogger(MappingIndexWatcher.class);

    private final java.util.concurrent.atomic.AtomicReference<String> seenUuid = new java.util.concurrent.atomic.AtomicReference<>();
    private final AtomicBoolean lost = new AtomicBoolean();

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // A second observation, taken here rather than from a listener of its own.
        //
        // DescriptorBackedIndexLifecycle.recordChange runs on every node applying a cluster state diff, and
        // until it could tell a manager from a follower it wrote the same descriptor blob from all of them
        // -- N nodes compare-and-swapping one key for one metadata change. Deciding that needs one boolean
        // off a cluster state, and this listener is already registered on precisely the same condition the
        // descriptor plane is (see ServerlessStoragePlugin's install block, where the two are adjacent), so
        // it is the observation point that costs nothing to keep in step. A dedicated listener would be a
        // second registration, a second lifetime, and a second thing to forget to add.
        //
        // Deliberately unconditional and first: the mapping-index question below returns early in the
        // common case, and a flag that stopped being updated whenever the mapping index was absent would be
        // stale in exactly the cluster that has no gated mappings yet.
        DescriptorGate.observeElectedClusterManager(event.state().nodes().isLocalNodeElectedClusterManager());
        var metadata = event.state().metadata().index(IndexBackedMappingStore.MAPPING_INDEX);
        if (metadata == null) {
            if (seenUuid.get() != null) {
                report("was deleted");
            }
            return;
        }
        String uuid = metadata.getIndexUUID();
        String previous = seenUuid.getAndSet(uuid);
        if (previous != null && previous.equals(uuid) == false) {
            // A different index wearing the same name. Whatever was in the old one is gone, and an empty
            // index answers "this gated index has no fields" for every index that had some.
            report("was replaced by a new index of the same name");
        }
    }

    private void report(String what) {
        if (lost.compareAndSet(false, true)) {
            logger.warn(
                "[{}] {} while this node was running. Mappings for gated indices are held there, so reads "
                    + "now fail rather than reporting those indices as having no fields. Restore it and "
                    + "restart this node.",
                IndexBackedMappingStore.MAPPING_INDEX,
                what
            );
        }
    }

    /** True once the mapping index's contents have been lost, and not cleared afterwards. */
    @Override
    public boolean getAsBoolean() {
        return lost.get();
    }
}
