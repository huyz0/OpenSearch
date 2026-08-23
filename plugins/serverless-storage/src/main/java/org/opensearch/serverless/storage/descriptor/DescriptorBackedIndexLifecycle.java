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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.metadata.ClaimedIndexLifecycle;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.core.action.ActionListener;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Everything this plugin holds for a deleted index, removed as the one operation core asks for.
 *
 * <p><b>What this is made of.</b> Two things core used to run itself, in that order: the durable tombstone
 * write it deferred the acknowledgement on ({@code DurableTombstones}, a static seam this class retires),
 * and the stored-mapping prune it issued afterwards ({@code MetadataDeleteIndexService#removeStoredMappings},
 * whose body is below, javadoc and all). Both are moved rather than rewritten: the same writes in the same
 * order, with the same failure policy for each half. What changed is who sequences them. Core no longer
 * knows there are two.
 *
 * <p><b>Installed-or-not is read live, not captured.</b> The plugin returns this from {@code
 * ClusterPlugin#getClaimedIndexLifecycle()} unconditionally, exactly as it returns {@code
 * SupplierBackedIndexCatalog} -- the object exists for the node's lifetime while {@link DescriptorGate}
 * fills and empties underneath it. With the gate uninstalled there is no store to write a tombstone to and
 * no mapping store to prune from, and this answers with an already-completed stage: identical to what an
 * unregistered {@code DurableTombstones.Writer} answered, which is what makes deletion on a node without
 * the feature cost exactly what it always cost.
 *
 * @see ClaimedIndexLifecycle
 */
public final class DescriptorBackedIndexLifecycle implements ClaimedIndexLifecycle {

    private static final Logger logger = LogManager.getLogger(DescriptorBackedIndexLifecycle.class);

    /**
     * Writes a tombstone for every deleted index, completing when the last one is durable, and prunes their
     * stored mappings afterwards.
     *
     * <p>The publisher {@link DescriptorGate} registers is fire-and-forget by necessity: it runs inside
     * cluster state construction, where blocking deadlocks. That is fine for a creation, whose index is in
     * cluster state anyway, and not fine for a tombstone. For a gated index there is no cluster state entry
     * and no graveyard entry standing behind the tombstone, so losing it means a node holding that shard's
     * data can adopt it again on rejoin -- the resurrection {@code IndexGraveyard} exists to prevent,
     * reintroduced.
     *
     * <p>This runs after the state is committed and before the client is told the delete succeeded, which is
     * the one window where a write can be both off the cluster state thread and ahead of the
     * acknowledgement. Nothing blocks; the acknowledgement is deferred, not waited on.
     *
     * <p><b>The stage is completed before the prune is started, and that order is required of an
     * implementation rather than incidental to this one</b> -- see {@link ClaimedIndexLifecycle}'s own
     * javadoc for the two reasons. Completing it inline, as the first statement of the completion handler,
     * is what makes core's acknowledgement land on the same thread at the same point it always did.
     */
    @Override
    public CompletionStage<Void> removeIndices(Collection<IndexMetadata> indices) {
        final DescriptorBackend store = DescriptorGate.installedStore();
        if (store == null || indices.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        final CompletableFuture<Void> durable = new CompletableFuture<>();
        // Grouped so the acknowledgement waits for all of them and reports the first failure. A delete
        // naming several indices is not durable until the last tombstone is.
        ActionListener<Void> perTombstone = new GroupedActionListener<>(ActionListener.wrap(ignored -> {
            // Completed first, then the prune. Core's acknowledgement is a dependent of this future, so it
            // runs inside this call -- before the round trips below, exactly as it did when this ordering
            // was written out in MetadataDeleteIndexService.
            durable.complete(null);
            removeStoredMappings(indices);
        }, durable::completeExceptionally), indices.size());
        for (IndexMetadata metadata : indices) {
            IndexDescriptor tombstone = IndexDescriptor.from(metadata).tombstoned(System.currentTimeMillis());
            // The change log entry for a deletion, which had no writer at all until this was registered, and
            // its absence was the worst of the five.
            //
            // The publisher has an exists()==false branch that records a DELETED, and nothing reaches it:
            // publish() is only ever called with metadata for an index that is in cluster state, and an
            // all-gated delete does not go through a cluster state update in the first place --
            // MetadataDeleteIndexService.deleteGatedIndices records the removal and acknowledges. So no node
            // was ever told that a gated index had been deleted. Every other node went on resolving it from
            // cache as live for the whole freshness window, a minute by default, and *accepted acknowledged
            // writes against a deleted index* for that long. It also left DescriptorChangeTailer's
            // delete-driven shard release unreachable in production, so the only thing closing those shards
            // was the periodic sweep.
            //
            // Recorded after the write is durable rather than beside it, because an entry for a tombstone
            // that never landed would have other nodes drop a live index's shards.
            store.putTombstoneAsync(tombstone, ActionListener.wrap(ignored -> {
                DescriptorGate.recordWrite(tombstone);
                perTombstone.onResponse(null);
            }, perTombstone::onFailure));
        }
        return durable;
    }

    /**
     * Removes the mappings of indices that have just been deleted.
     *
     * <p>Nothing ever removed one before this: {@code MappingGenerationStore.Store} had no delete at all, so
     * {@code .opensearch-index-mappings} grew with every index that had ever existed rather than with the
     * live population. That is the residency problem this area exists to remove, reproduced one level down,
     * and churn is what makes it bite -- a tenant that creates and drops an index a day leaves a document a
     * day behind forever.
     *
     * <p><b>After the tombstone is durable, and that order is the safety argument.</b> The tombstone is what
     * makes the deletion real. Removing the mapping first would mean a failure between the two left an index
     * that still exists and whose declared fields are gone, which is a silent loss; this way the same
     * failure leaves a document nobody references, which is exactly the status quo before this existed.
     *
     * <p><b>A failure here does not fail the deletion.</b> The index is gone either way, and reporting the
     * delete as failed would invite a retry of something that already happened. A stranded document is the
     * lesser outcome, and it is logged rather than swallowed.
     *
     * <p><b>After the acknowledgement, not before it.</b> Each prune is a blocking round trip, and a request
     * naming five hundred gated indices would otherwise make the client wait for five hundred of them --
     * including for every index that never declared a mapping, since removing an absent document still costs
     * a round trip -- before hearing about a deletion that had already happened. With an unassigned primary
     * on the mapping index each one waits out the replication timeout instead. Making the caller wait for
     * work whose result is then discarded is the worst of both.
     *
     * <p>Catching {@link Throwable} rather than {@link Exception}, which is deliberate and narrow. A
     * blocking client call from the wrong thread raises an {@code AssertionError}, and an {@code Error}
     * escaping here would leave the completion chain broken on a path whose entire purpose is best-effort
     * cleanup after the client has already been answered.
     */
    private static void removeStoredMappings(Collection<IndexMetadata> deleted) {
        // Read once, here, rather than through a static entry point per index. MappingGenerationStore's own
        // deleteMapping wrapper existed only so core could reach a store it never otherwise touched on this
        // path, and it went with the protocol that called it.
        MappingGenerationStore.Store mappingStore = DescriptorGate.installedMappingStore();
        if (mappingStore == null) {
            return;
        }
        for (IndexMetadata metadata : deleted) {
            try {
                mappingStore.delete(metadata.getIndexUUID());
            } catch (Throwable e) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "{} was deleted but its stored mapping could not be removed, leaving a document nobody references",
                        metadata.getIndex()
                    ),
                    e
                );
            }
        }
    }
}
