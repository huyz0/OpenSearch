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
import org.opensearch.serverless.storage.scheduling.JitteredScheduling;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

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
 * <p>Bundles get their own, separate safety window (as required by {@link
 * BundleReferenceCounter#computeDeletableBundles}'s own javadoc): the manifest list and the bundle
 * list are two independent snapshots taken at different instants during {@link #sweep()}, so a
 * bundle written and referenced by a manifest published in between can look orphaned this tick even
 * though it is not. Rather than deleting a bundle the first tick it looks unreferenced, this task
 * tracks, per bundle name, when it was <em>first</em> continuously observed as unreferenced
 * ({@link #firstObservedOrphanedAtMillis}) and only deletes it once that has held for a full {@code
 * retentionWindowMillis} -- any manifest publish racing a sweep is picked up as soon as the very next
 * tick, which is always far inside that window given the poll/sweep cadence this class already
 * assumes elsewhere in this javadoc, removing the bundle from consideration before it can ever reach
 * the threshold.
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
    private final LongSupplier clock;
    private final Scheduler.Cancellable task;

    // Node-local, in-memory, reset on restart -- same "best-effort hint, not a correctness
    // dependency" status as the lease-pin tier this class's own javadoc already disclaims.
    // A bundle only becomes deletable once it has been continuously observed as unreferenced
    // (by every sweep in between) for at least retentionWindowMillis: see the safety-window note
    // added below.
    private final Map<String, Long> firstObservedOrphanedAtMillis = new HashMap<>();

    // Node-local, in-memory, reset on restart. Manifests are immutable and write-once (see
    // BlobContainerManifestStore#writeManifest's own javadoc), so once this task has read a given
    // manifest's body, it never needs to re-fetch it on a later tick -- only whether it still
    // exists needs to be current, which listManifests(Map) still checks fresh via a real listing
    // every tick regardless of this cache. Without this, every tick pays one readBlob call per
    // manifest the shard has EVER retained, not per manifest newly written since the last tick --
    // a real, unbounded-with-history S3 GET cost this cache turns into a bounded one.
    private final Map<String, CommitManifest> manifestReadCache = new HashMap<>();

    /**
     * Schedules a recurring GC sweep for one shard.
     *
     * @param threadPool the node's thread pool, used to schedule the recurring sweep.
     * @param interval how often to run the sweep -- jittered once per instance (see {@link
     *                 JitteredScheduling}) before being handed to the scheduler, so many shards'
     *                 sweeps starting around the same moment (e.g. after a coordinated outage
     *                 recovery, rfc-serverless-opensearch.md &sect;13) don't tick in lockstep forever.
     * @param indexUuid the UUID of the index the shard belongs to.
     * @param shardId the shard's numeric id within the index.
     * @param config the shard's manifest/bundle stores, pin registry, and retention window.
     */
    public GcSchedulerTask(ThreadPool threadPool, TimeValue interval, String indexUuid, int shardId, GcSchedulerConfig config) {
        this(threadPool, interval, indexUuid, shardId, config, System::currentTimeMillis);
    }

    /** Test-only visibility: lets tests control time instead of waiting out a real retention window. */
    GcSchedulerTask(
        ThreadPool threadPool,
        TimeValue interval,
        String indexUuid,
        int shardId,
        GcSchedulerConfig config,
        LongSupplier clock
    ) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.manifestStore = config.manifestStore();
        this.bundleStore = config.bundleStore();
        this.pinRegistry = config.pinRegistry();
        this.retentionWindowMillis = config.retentionWindowMillis();
        this.clock = clock;
        this.task = threadPool.scheduleWithFixedDelay(this::sweepSafely, JitteredScheduling.jitter(interval), ThreadPool.Names.GENERIC);
    }

    private void sweepSafely() {
        try {
            sweep();
        } catch (IOException e) {
            // See class javadoc: swallow and let the next scheduled tick reevaluate.
        }
    }

    private void sweep() throws IOException {
        List<CommitManifest> manifests = manifestStore.listManifests(manifestReadCache);
        if (manifests.isEmpty()) {
            return; // never activated, or every manifest already swept -- nothing to do
        }

        long retentionCutoffMillis = clock.getAsLong() - retentionWindowMillis;
        Set<ManifestId> durablyPinnedManifests = pinRegistry.getPinnedManifestIds(indexUuid, shardId);

        List<CommitManifest> deletableManifests = ManifestRetentionPolicy.computeDeletableManifests(
            manifests,
            retentionCutoffMillis,
            Set.of(),
            durablyPinnedManifests
        );

        // Deliberately NOT an early return when deletableManifests is empty: a bundle already
        // mid-observation from an earlier tick (see below) can become deletable on a tick that has
        // no new manifest to delete, and must still be revisited -- an early return here would
        // starve that bundle of ever getting its second look.
        List<CommitManifest> retainedManifests = ManifestRetentionPolicy.computeRetainedManifests(
            manifests,
            retentionCutoffMillis,
            Set.of(),
            durablyPinnedManifests
        );
        Set<String> liveBundles = BundleReferenceCounter.computeLiveBundles(retainedManifests);
        Set<String> allKnownBundles = bundleStore.listBundleNames();
        Set<String> orphanCandidateBundles = BundleReferenceCounter.computeDeletableBundles(allKnownBundles, liveBundles);

        // BundleReferenceCounter#computeDeletableBundles's own javadoc requires a safety delay before
        // actually deleting: a bundle can be written and referenced by a manifest published after this
        // sweep's manifest-list snapshot (taken above) but before this sweep's bundle-list snapshot
        // (taken above too) -- that bundle looks orphaned this tick even though a commit that references
        // it is landing concurrently. Requiring a bundle to be observed as an orphan candidate on every
        // sweep for a full retentionWindowMillis before it is actually deleted closes that window: any
        // manifest publish racing a sweep is picked up by the very next tick (well inside the window),
        // which removes the bundle from firstObservedOrphanedAtMillis before it ever reaches the
        // sustained-orphan threshold.
        long now = clock.getAsLong();
        firstObservedOrphanedAtMillis.keySet().retainAll(orphanCandidateBundles);
        Set<String> deletableBundles = new HashSet<>();
        for (String bundle : orphanCandidateBundles) {
            Long firstObserved = firstObservedOrphanedAtMillis.get(bundle);
            if (firstObserved == null) {
                firstObservedOrphanedAtMillis.put(bundle, now);
            } else if (now - firstObserved >= retentionWindowMillis) {
                deletableBundles.add(bundle);
            }
        }

        // Re-check pins immediately before the actual delete, not just once near the top of this
        // method: a snapshot/PITR pin request landing on this shard concurrently, after the
        // durablyPinnedManifests read above but before this point (the blob-store listing and
        // bookkeeping loops in between leave a real, if narrow, window), would otherwise never be
        // seen by this sweep and get deleted anyway. This is a cheap second read on the same
        // register blob, not a lock, so it doesn't fully close the window against a pin landing in
        // the few remaining lines between this read and the delete call itself -- but it shrinks
        // that window from "the whole sweep method" to "a few lines," which is the best available
        // without introducing real cross-writer coordination this shard's GC has never needed.
        Set<ManifestId> pinnedImmediatelyBeforeDelete = pinRegistry.getPinnedManifestIds(indexUuid, shardId);
        List<CommitManifest> finalDeletableManifests = deletableManifests;
        if (pinnedImmediatelyBeforeDelete.isEmpty() == false) {
            finalDeletableManifests = new java.util.ArrayList<>(deletableManifests.size());
            for (CommitManifest manifest : deletableManifests) {
                if (pinnedImmediatelyBeforeDelete.contains(ManifestId.of(manifest)) == false) {
                    finalDeletableManifests.add(manifest);
                }
            }
        }

        // The same late-pin race applies to bundles, not just manifests: deletableBundles above was
        // derived from liveBundles, which was computed from the *early* durablyPinnedManifests read.
        // A pin landing in the window between that early read and pinnedImmediatelyBeforeDelete above
        // is correctly kept out of finalDeletableManifests -- but without this second filter, its
        // now-surviving manifest's exclusive bundle would still be deleted here anyway, leaving a
        // pinned manifest pointing at a bundle that no longer exists. Recompute live bundles from the
        // late pin snapshot and drop anything newly-live from this tick's delete set; a bundle
        // excluded this way simply falls out of orphanCandidateBundles on the next tick once the late
        // pin is reflected in the early read too, same as any other manifest publish racing a sweep.
        if (pinnedImmediatelyBeforeDelete.isEmpty() == false) {
            List<CommitManifest> retainedManifestsAtDelete = ManifestRetentionPolicy.computeRetainedManifests(
                manifests,
                retentionCutoffMillis,
                Set.of(),
                pinnedImmediatelyBeforeDelete
            );
            Set<String> liveBundlesAtDelete = BundleReferenceCounter.computeLiveBundles(retainedManifestsAtDelete);
            deletableBundles.removeAll(liveBundlesAtDelete);
        }

        // Bundles before manifests -- see this class's own javadoc for why that ordering, not the
        // reverse, is what keeps a mid-sweep crash merely retry-safe.
        bundleStore.deleteBundles(deletableBundles);
        manifestStore.deleteManifests(finalDeletableManifests);
        firstObservedOrphanedAtMillis.keySet().removeAll(deletableBundles);
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
