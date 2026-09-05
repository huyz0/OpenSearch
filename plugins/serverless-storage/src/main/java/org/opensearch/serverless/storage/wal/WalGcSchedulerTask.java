/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.scheduling.JitteredScheduling;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * The node-level counterpart of {@code GcSchedulerTask}, for WAL chunks instead of manifests/bundles
 * (rfc-serverless-opensearch.md &sect;6.4's own status note on this gap). Runs on a fixed schedule,
 * deleting every chunk sequence at or below the minimum {@link WalPosition#offset()} covered by the
 * latest published manifest of every shard {@link WalShardRegistry} currently knows about.
 *
 * <p><b>What makes a chunk deletable, precisely.</b> A shard replays WAL chunks only in the half-open
 * range {@code (itsLatestManifest.walPosition().offset(), activationWalPosition)} -- strictly
 * <em>forward</em> from the position its own last published manifest already covers (see {@code
 * WalReplayRecovery#replayOperations}, which computes {@code fromChunkSequenceInclusive = offset + 1}).
 * So chunk sequence {@code s} is needed by shard S if and only if {@code s > S.offset}. Taking the
 * minimum of {@code offset} across <em>every</em> shard registered against this container therefore
 * gives a sequence at or below which no registered shard can ever ask to replay again, and that is
 * exactly the bound deleted at. Three properties make the bound trustworthy rather than merely
 * plausible:
 * <ul>
 *   <li><b>Every shard that can write a chunk is registered before it writes one.</b> {@code
 *       ObjectStoreWriterEngine} registers in {@code WalShardRegistry} as a hard precondition of
 *       activation -- bounded synchronous retry, and it refuses to activate at all if registration
 *       never succeeds -- so a writer whose chunks would be invisible to this bound never comes up.
 *       (It used to be best-effort and retried on a ten-second lease tick while the engine wrote
 *       chunks regardless, which is precisely the hole that made enabling this sweep unsafe.)</li>
 *   <li><b>A manifest's {@code WalPosition} is snapshotted <em>before</em> its commit, never after.</b>
 *       {@code ObjectStoreWriterEngine#commitIndexWriter} reads the watermark ahead of the Lucene
 *       commit, so the position it records is always a safe lower bound on what that commit contains
 *       -- occasionally conservative, never inflated. An inflated position is the one thing that
 *       could make this sweep delete a chunk holding an acknowledged operation.</li>
 *   <li><b>An offset only ever moves forward.</b> A per-shard read that is momentarily stale can
 *       only under-report, so a bound computed from several reads taken at slightly different
 *       instants is at or below the true current minimum. The sweep is conservative under
 *       concurrency by construction, with no locking.</li>
 * </ul>
 *
 * <p><b>Why the cadence cannot delete a chunk a replay still needs.</b> The bound above is derived
 * entirely from published, durable state -- registry membership and manifest positions -- and never
 * from elapsed time. Sweeping more often does not lower the bound and sweeping less often does not
 * raise it; the cadence changes only <em>how much</em> already-deletable garbage is still lying
 * around when a sweep runs, never <em>what</em> is deletable. That is why this task, unlike {@code
 * GcSchedulerTask}, needs no separate time-based retention window: a time window would only be
 * protecting against a registry that might be wrong, and {@link WalShardRegistry} is designed not to
 * be (grow-only, losing an entry only on a real index deletion -- see that class's own javadoc).
 * Cadence is therefore purely a cost decision, which matters because chunk production is bounded by
 * {@code serverless_storage.wal_flush.interval} (200&nbsp;ms by default, i.e. up to five chunks per
 * second per node) and, with WAL mirroring and group-commit batching both on by default, is no
 * longer hypothetical: with the sweep disabled, chunks accumulate for the life of the cluster.
 *
 * <p><b>Deliberately conservative when incomplete information would make the sweep unsafe</b>: if
 * any registered shard has never published a manifest at all, or its latest manifest carries no
 * real {@link WalPosition} (WAL mirroring wasn't active for that particular commit -- see {@code
 * ObjectStoreWriterEngine#currentWalPosition}'s own placeholder-shape note), this sweep skips
 * entirely for that tick rather than guessing a bound that could delete something still needed.
 * That bail-out is container-wide, not per-shard, and it has no timeout: one stuck shard blocks WAL
 * GC for every shard sharing the container, indefinitely. That is the correct trade (deleting a
 * chunk someone still needs is unrecoverable; retaining chunks costs storage), but it must not be
 * <em>silent</em> now that the sweep runs by default -- so a bail-out that keeps recurring escalates
 * from INFO to WARN, naming the shard responsible, after {@link #STALL_WARN_AFTER_CONSECUTIVE_SKIPS}
 * consecutive ticks. The realistic way to get there is a best-effort {@code deregister} that failed
 * during an index deletion, leaving a marker for a shard whose container no longer answers.
 *
 * <p><b>{@code clusterService} ({@code null} to always sweep, non-{@code null} to run only on
 * the elected cluster-manager)</b>: {@link WalShardRegistry} is one durable registry shared by the
 * whole cluster (a listing of per-shard marker blobs, plus whatever a pre-marker build left in the
 * legacy register -- see that class's own javadoc), so every node's sweep of a shared, cluster-wide {@code
 * walBlobContainer} would compute the exact same {@code minCoveredSequence} bound from the exact
 * same registered-shard list -- running it on every node multiplies the same object-store read
 * traffic (one {@link ShardStateStore} read and one manifest read per registered shard, every
 * tick) by the node count for no benefit, the same "don't multiply identical cluster-wide work by
 * node count" reasoning {@code ScaleToZeroCandidatesSchedulerTask} already applies. Pass a
 * non-{@code null} {@link ClusterService}, checked fresh on every tick (never cached, since the
 * elected node can change), for that case. This does <b>not</b> apply to a shard's own <em>
 * dedicated</em>, single-shard-scoped WAL container ({@code DedicatedWalGcConfig}): exactly one
 * node ever hosts a given shard's writer engine at a time, so there is no redundant peer to
 * eliminate there, and gating that sweep on cluster-manager election would instead wrongly stop
 * it entirely on every node that isn't currently the elected cluster-manager -- pass {@code null}
 * for that case, which is exactly what the narrower constructor below does.
 */
public final class WalGcSchedulerTask implements Closeable {

    private static final Logger logger = LogManager.getLogger(WalGcSchedulerTask.class);

    /**
     * How many consecutive incomplete-information bail-outs are tolerated quietly before the skip is
     * logged at WARN instead of INFO. A handful of ticks is the ordinary case (a freshly created
     * shard that has not committed yet blocks the sweep until its first publish); dozens in a row is
     * a shard that will never publish -- most plausibly a marker left behind by an index deletion
     * whose best-effort {@code deregister} failed -- and means WAL GC has silently stopped.
     */
    static final int STALL_WARN_AFTER_CONSECUTIVE_SKIPS = 20;

    private final BlobContainer walBlobContainer;
    private final WalShardRegistry registry;
    private final BiFunction<String, Integer, BlobContainer> shardContainerResolver;
    private final ClusterService clusterService;
    private final Scheduler.Cancellable task;

    /**
     * The highest bound a completed sweep has already deleted at, or {@code -1} before the first one.
     * If a later sweep computes the same bound, every chunk at or below it is already gone, so the
     * container listing and the delete call are both pure cost -- skipped. This matters because the
     * listing is the one part of a sweep whose cost grows with how many chunks are retained, and at
     * the default {@code wal_flush.interval} a cluster produces up to five per second per node.
     *
     * <p>Purely an optimisation, never a safety input: the bound itself is recomputed from live
     * published state on every tick, and this value only ever suppresses work that would provably
     * find nothing new.
     */
    private long lastSweptCoveredSequence = -1L;

    /**
     * Caches each shard's latest manifest {@code WalPosition} against the exact {@code (primaryTerm,
     * generation)} it came from, so a shard that has not published since the previous sweep costs one
     * head read rather than a head read plus a manifest read. Safe without any invalidation because
     * manifests are immutable and write-once (see {@code BlobContainerManifestStore#writeManifest}):
     * a given {@code (indexUuid, shardId, primaryTerm, generation)} can never describe different
     * content later. Bounded by the registered-shard count -- one entry per shard, replaced when that
     * shard publishes -- and entries for shards no longer registered are evicted at the end of each
     * sweep that completes. A sweep that bails out on incomplete information leaves the entries it
     * had already gathered in place, which is why the bound is "one per shard ever registered in
     * this process" rather than "one per currently registered shard"; both are small, and neither
     * affects the sweep's result.
     *
     * <p>The per-sweep read cost is {@code O(registered shards)} and independent of the cadence, so
     * halving it is what makes a one-minute cadence affordable on a cluster-manager that is also
     * doing everything else.
     */
    private final Map<ManifestKey, WalPosition> cachedWalPositions = new HashMap<>();

    /** Consecutive sweeps that bailed out on incomplete information -- see {@link #STALL_WARN_AFTER_CONSECUTIVE_SKIPS}. */
    private int consecutiveIncompleteInformationSkips = 0;

    /** Identity of one immutable published manifest, the cache key for {@link #cachedWalPositions}. */
    private record ManifestKey(String indexUuid, int shardId, long primaryTerm, long generation) {
    }

    /**
     * Starts the scheduled sweep, running on every node -- for a shard's own dedicated,
     * single-shard-scoped WAL container, where there is no cross-node redundancy to eliminate
     * (see this class's own javadoc).
     *
     * @param threadPool schedules {@link #sweep()} on a fixed delay.
     * @param interval how often to sweep.
     * @param walBlobContainer the WAL container chunks are deleted from.
     * @param registry every shard known to use {@code walBlobContainer}.
     * @param shardContainerResolver given a shard's {@code (indexUuid, shardId)}, returns that shard's own {@link BlobContainer}.
     */
    public WalGcSchedulerTask(
        ThreadPool threadPool,
        TimeValue interval,
        BlobContainer walBlobContainer,
        WalShardRegistry registry,
        BiFunction<String, Integer, BlobContainer> shardContainerResolver
    ) {
        this(threadPool, interval, walBlobContainer, registry, shardContainerResolver, null);
    }

    /**
     * Starts the scheduled sweep.
     *
     * @param threadPool schedules {@link #sweep()} on a fixed delay.
     * @param interval how often to sweep.
     * @param walBlobContainer the WAL container chunks are deleted from.
     * @param registry every shard known to use {@code walBlobContainer}.
     * @param shardContainerResolver given a shard's {@code (indexUuid, shardId)}, returns that shard's own {@link BlobContainer}.
     * @param clusterService {@code null} to sweep on every node unconditionally (a shard's own
     *                       dedicated WAL container); non-{@code null} to sweep only when this
     *                       node is currently the elected cluster-manager (the shared, cluster-wide
     *                       WAL container, where every node would otherwise compute and delete the
     *                       identical bound) -- see this class's own javadoc.
     */
    public WalGcSchedulerTask(
        ThreadPool threadPool,
        TimeValue interval,
        BlobContainer walBlobContainer,
        WalShardRegistry registry,
        BiFunction<String, Integer, BlobContainer> shardContainerResolver,
        ClusterService clusterService
    ) {
        this.walBlobContainer = walBlobContainer;
        this.registry = registry;
        this.shardContainerResolver = shardContainerResolver;
        this.clusterService = clusterService;
        // Jittered once per instance (rfc-serverless-opensearch.md §13's own "recovery stampede"
        // requirement) -- see JitteredScheduling's own javadoc for why a one-time offset is what
        // actually prevents lockstep ticking after a coordinated outage recovery.
        this.task = threadPool.scheduleWithFixedDelay(this::sweepSafely, JitteredScheduling.jitter(interval), ThreadPool.Names.GENERIC);
    }

    private void sweepSafely() {
        try {
            sweep();
        } catch (IOException e) {
            // Swallowed and retried next tick, same tolerance GcSchedulerTask already has for its
            // own sweep: nothing is corrupted by a skipped tick, only deferred.
        } catch (Throwable t) {
            // Deliberately Throwable, not just IOException: ClusterService#state() throws an
            // AssertionError (not an Exception) if called before the node's initial cluster state
            // is applied -- a real window this task's very first tick or two can land in, since
            // threadPool.scheduleWithFixedDelay starts ticking as soon as this task is constructed,
            // which can be well before the node finishes starting up. Same tolerance as the
            // IOException case above: nothing is corrupted by a skipped tick, only deferred.
            logger.warn("WAL GC sweep failed, will retry next tick", t);
        }
    }

    // Synchronized because this now carries state across ticks (the bound memo, the manifest cache,
    // the stall counter). scheduleWithFixedDelay never overlaps a task with itself, but successive
    // ticks can land on different GENERIC threads, and sweepForTesting can be called from a test
    // thread while a scheduled tick is in flight -- neither of which the previous, entirely
    // stateless sweep had to care about.
    synchronized void sweep() throws IOException {
        if (clusterService != null && clusterService.state().nodes().isLocalNodeElectedClusterManager() == false) {
            return; // not our turn -- see class javadoc for why only the cluster-manager sweeps the shared container
        }
        Set<RegisteredShard> shards = registry.registeredShards();
        if (shards.isEmpty()) {
            return; // nothing known to use this container yet -- nothing provably safe to delete
        }

        long minCoveredSequence = Long.MAX_VALUE;
        Set<ManifestKey> observedThisSweep = new HashSet<>();
        for (RegisteredShard shard : shards) {
            BlobContainer shardContainer = shardContainerResolver.apply(shard.indexUuid(), shard.shardId());
            ShardStateStore shardStateStore = new BlobContainerShardStateStore(shardContainer);
            Optional<VersionedShardHead> head = shardStateStore.get(shard.indexUuid(), shard.shardId());
            if (head.isEmpty() || head.get().head().latestManifestGeneration() == 0) {
                // Conservative bail-out, see class javadoc -- this blocks WAL GC for every shard
                // sharing this container, not just this one, on every tick until this shard
                // publishes (a brand-new shard that hasn't committed yet is the common, expected
                // case; a shard that's stuck this way indefinitely is not, and worth an operator
                // noticing rather than silently accumulating unbounded WAL chunk storage).
                reportIncompleteInformation(
                    "shard {}/{} is registered but has never published a manifest",
                    shard.indexUuid(),
                    shard.shardId()
                );
                return;
            }
            ShardHead shardHead = head.get().head();
            ManifestKey key = new ManifestKey(
                shard.indexUuid(),
                shard.shardId(),
                shardHead.primaryTerm(),
                shardHead.latestManifestGeneration()
            );
            observedThisSweep.add(key);
            // Manifests are immutable and write-once, so a hit here is the same bytes a re-read
            // would return -- see cachedWalPositions' own javadoc. containsKey, not a null check:
            // a null WalPosition is a real, cacheable answer (it is the bail-out below), and
            // re-reading the manifest every tick to rediscover it would be the most expensive
            // possible way to stay blocked.
            WalPosition position;
            if (cachedWalPositions.containsKey(key)) {
                position = cachedWalPositions.get(key);
            } else {
                CommitManifest manifest = new BlobContainerManifestStore(shardContainer).readManifest(
                    shardHead.primaryTerm(),
                    shardHead.latestManifestGeneration()
                );
                position = manifest.walPosition();
                cachedWalPositions.put(key, position);
            }
            if (position == null) {
                // Same container-wide bail-out as above, see that branch's comment.
                reportIncompleteInformation(
                    "shard {}/{}'s latest manifest carries no real WAL position",
                    shard.indexUuid(),
                    shard.shardId()
                );
                return;
            }
            minCoveredSequence = Math.min(minCoveredSequence, position.offset());
        }
        // Every registered shard was accounted for, so the bound is trustworthy and the stall
        // counter resets. Reached only past every `return` above, which is the point: the counter
        // measures consecutive *blocked* sweeps, not consecutive sweeps that found nothing to do.
        consecutiveIncompleteInformationSkips = 0;
        cachedWalPositions.keySet().retainAll(observedThisSweep);

        // Nothing new became deletable since the last completed sweep, so the listing below would
        // return only chunks a previous sweep already deleted. A shard that has published nothing
        // yet reports offset -1 (WalMirroringTranslog#lastFlushedWalChunkSequence's "none yet"
        // sentinel, wrapped unconditionally by currentWalPosition), which this also covers: the
        // initial value of lastSweptCoveredSequence is -1, so a bound of -1 never reaches the store.
        if (minCoveredSequence <= lastSweptCoveredSequence) {
            return;
        }

        List<String> deletableBlobNames = new ArrayList<>();
        for (String blobName : walBlobContainer.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).keySet()) {
            if (WalChunkNaming.parseChunkSequence(blobName) <= minCoveredSequence) {
                deletableBlobNames.add(blobName);
            }
        }
        if (deletableBlobNames.isEmpty() == false) {
            walBlobContainer.deleteBlobsIgnoringIfNotExists(deletableBlobNames);
        }
        // Advanced only after the delete actually returned. A failed delete throws out of here
        // without touching this, so the next sweep repeats the same range rather than skipping it --
        // the deletes are idempotent (deleteBlobsIgnoringIfNotExists), so repeating costs one call,
        // whereas advancing optimistically would leak the chunks in that range permanently.
        lastSweptCoveredSequence = minCoveredSequence;
    }

    /**
     * Logs a container-wide bail-out, escalating from INFO to WARN once it has recurred
     * {@link #STALL_WARN_AFTER_CONSECUTIVE_SKIPS} times running. Blocking is the safe answer, but a
     * permanently blocked sweep means WAL chunks accumulate for the life of the cluster, which on a
     * deployment where WAL mirroring is on by default is a disk-exhaustion path, not a tidiness
     * issue -- so it stops being quiet.
     */
    private void reportIncompleteInformation(String reason, String indexUuid, int shardId) {
        consecutiveIncompleteInformationSkips++;
        if (consecutiveIncompleteInformationSkips < STALL_WARN_AFTER_CONSECUTIVE_SKIPS) {
            logger.info("WAL GC sweep skipped this tick for [" + indexUuid + "][" + shardId + "]: " + reason);
            return;
        }
        logger.warn(
            "WAL GC has now been blocked for "
                + consecutiveIncompleteInformationSkips
                + " consecutive sweeps on ["
                + indexUuid
                + "]["
                + shardId
                + "]: "
                + reason
                + ". No WAL chunk has been deleted in that time and none will be until this is resolved; "
                + "if this shard's index was deleted, its WalShardRegistry entry was left behind by a failed "
                + "deregister and must be removed before WAL GC can resume."
        );
    }

    /** Consecutive incomplete-information bail-outs so far -- test-only visibility, see {@link #STALL_WARN_AFTER_CONSECUTIVE_SKIPS}. */
    synchronized int consecutiveIncompleteInformationSkipsForTesting() {
        return consecutiveIncompleteInformationSkips;
    }

    /** Invokes {@link #sweep()} synchronously, rather than waiting out the scheduled interval -- test-only visibility. */
    void sweepForTesting() throws IOException {
        sweep();
    }

    /** Cancels the scheduled sweep; does not touch any already-written chunk or the registry itself. */
    @Override
    public void close() {
        task.cancel();
    }
}
