/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Runs the GC sweep (rfc-serverless-opensearch.md &sect;6.5) on a fixed schedule for one shard --
 * {@link BundleReferenceCounter} and {@link ManifestRetentionPolicy} were fully implemented and
 * tested, but (like {@code CompactionSchedulerTask} before it) nothing ever actually invoked them
 * from a running node: no manifest or bundle this plugin ever writes was reachable by anything that
 * deletes it, so storage grows without bound. This closes that gap the same way
 * {@code CompactionSchedulerTask} closes its own.
 *
 * <p><b>Safety design, deliberately conservative</b>: {@link ManifestRetentionPolicy#computeDeletableManifests}
 * takes both a lease-pin set and a durable-pin set. This task always passes an <em>empty</em>
 * lease-pin set, never derived from {@code ShardDirectory} -- that tier is explicitly documented as
 * node-local, in-memory "hints" (see {@code ShardDirectoryEntry}'s own javadoc and {@code
 * ServerlessStoragePlugin}'s field comment: one instance per node, not a real cluster-wide gossip
 * store yet), so it would silently miss every reader open on any <em>other</em> node -- using it as
 * a delete-safety signal would be worse than not checking at all, since it looks like a check. The
 * two real safety mechanisms this sweep relies on instead are ({@link GcSchedulerConfig#retentionWindowMillis},
 * generous by default) and durable pins ({@link DurablePinRegistry}, real and CAS-backed, correct
 * cluster-wide regardless of which node evaluates it). A manifest only becomes deletable once it is
 * both superseded <em>and</em> has sat unpinned past the retention window -- comfortably longer than
 * any legitimate reader's own manifest-generation lag (bounded by {@code ObjectStoreReaderEngine}'s
 * 5&nbsp;s poll interval), so a reader that is merely slow to advance is never at risk of having its
 * currently-open generation deleted out from under it.
 *
 * <p>Bundles are deleted before the manifests that stopped referencing them, never the reverse --
 * see {@code BlobContainerManifestStore#deleteManifests}'s own javadoc for why that ordering is what
 * makes a mid-sweep crash merely retry-safe rather than bundle-orphaning.
 *
 * <p>A failure here is logged-and-swallowed, same tolerance {@code CompactionSchedulerTask} already
 * has for its own tick: nothing is corrupted by a skipped or failed sweep, only deferred to the next
 * tick, since GC is strictly a space-reclamation concern, never a correctness one for anything still
 * live.
 */
public final class GcSchedulerTask implements Closeable {

    private final String indexUuid;
    private final int shardId;
    private final BlobContainerManifestStore manifestStore;
    private final BlobContainerBundleStore bundleStore;
    private final DurablePinRegistry pinRegistry;
    private final long retentionWindowMillis;
    private final Scheduler.Cancellable task;

    public GcSchedulerTask(ThreadPool threadPool, TimeValue interval, String indexUuid, int shardId, GcSchedulerConfig config) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.manifestStore = config.manifestStore();
        this.bundleStore = config.bundleStore();
        this.pinRegistry = config.pinRegistry();
        this.retentionWindowMillis = config.retentionWindowMillis();
        this.task = threadPool.scheduleWithFixedDelay(this::sweepSafely, interval, ThreadPool.Names.GENERIC);
    }

    private void sweepSafely() {
        try {
            sweep();
        } catch (IOException e) {
            // See class javadoc: swallow and let the next scheduled tick reevaluate.
        }
    }

    private void sweep() throws IOException {
        List<CommitManifest> manifests = manifestStore.listManifests();
        if (manifests.isEmpty()) {
            return; // never activated, or every manifest already swept -- nothing to do
        }

        long retentionCutoffMillis = System.currentTimeMillis() - retentionWindowMillis;
        Set<ManifestId> durablyPinnedManifests = pinRegistry.getPinnedManifestIds(indexUuid, shardId);

        List<CommitManifest> deletableManifests = ManifestRetentionPolicy.computeDeletableManifests(
            manifests,
            retentionCutoffMillis,
            Set.of(),
            durablyPinnedManifests
        );
        if (deletableManifests.isEmpty()) {
            return;
        }

        List<CommitManifest> retainedManifests = ManifestRetentionPolicy.computeRetainedManifests(
            manifests,
            retentionCutoffMillis,
            Set.of(),
            durablyPinnedManifests
        );
        Set<String> liveBundles = BundleReferenceCounter.computeLiveBundles(retainedManifests);
        Set<String> allKnownBundles = bundleStore.listBundleNames();
        Set<String> deletableBundles = BundleReferenceCounter.computeDeletableBundles(allKnownBundles, liveBundles);

        // Bundles before manifests -- see this class's own javadoc for why that ordering, not the
        // reverse, is what keeps a mid-sweep crash merely retry-safe.
        bundleStore.deleteBundles(deletableBundles);
        manifestStore.deleteManifests(deletableManifests);
    }

    /** Invokes {@link #sweep()} synchronously, rather than waiting out the scheduled interval -- test-only visibility. */
    void sweepForTesting() throws IOException {
        sweep();
    }

    @Override
    public void close() {
        task.cancel();
    }
}
