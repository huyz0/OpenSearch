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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * The node-level counterpart of {@code GcSchedulerTask}, for WAL chunks instead of manifests/bundles
 * (rfc-serverless-opensearch.md &sect;6.4's own status note on this gap). Runs on a fixed schedule,
 * deleting every chunk sequence at or below the minimum {@link WalPosition#offset()} covered by the
 * latest published manifest of every shard {@link WalShardRegistry} currently knows about.
 *
 * <p><b>Safety rests entirely on {@link WalShardRegistry} being accurate</b>, not on a time-based
 * retention window the way {@code GcSchedulerTask} additionally uses one: once every known shard's
 * latest manifest has published a {@code WalPosition} covering a chunk sequence, nothing will ever
 * legitimately need to replay from at or below that sequence again -- replay only ever reads
 * forward from a shard's own last-covered position (see {@code WalReplayRecovery}). A time window
 * on top of that bound would only protect against a registry that might be wrong, which is exactly
 * why {@link WalShardRegistry} is designed to be grow-only and only ever loses an entry on a real,
 * independently-verified index deletion (see that class's own javadoc) rather than trying to be
 * "probably right most of the time."
 *
 * <p><b>Deliberately conservative when incomplete information would make the sweep unsafe</b>: if
 * any registered shard has never published a manifest at all, or its latest manifest carries no
 * real {@link WalPosition} (WAL mirroring wasn't active for that particular commit -- see {@code
 * ObjectStoreWriterEngine#currentWalPosition}'s own placeholder-shape note), this sweep skips
 * entirely for that tick rather than guessing a bound that could delete something still needed.
 *
 * <p><b>{@code clusterService} ({@code null} to always sweep, non-{@code null} to run only on
 * the elected cluster-manager)</b>: {@link WalShardRegistry} is a single durable, CAS-backed
 * register shared by the whole cluster, so every node's sweep of a shared, cluster-wide {@code
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

    private final BlobContainer walBlobContainer;
    private final WalShardRegistry registry;
    private final BiFunction<String, Integer, BlobContainer> shardContainerResolver;
    private final ClusterService clusterService;
    private final Scheduler.Cancellable task;

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

    void sweep() throws IOException {
        if (clusterService != null && clusterService.state().nodes().isLocalNodeElectedClusterManager() == false) {
            return; // not our turn -- see class javadoc for why only the cluster-manager sweeps the shared container
        }
        Set<RegisteredShard> shards = registry.registeredShards();
        if (shards.isEmpty()) {
            return; // nothing known to use this container yet -- nothing provably safe to delete
        }

        long minCoveredSequence = Long.MAX_VALUE;
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
                logger.info(
                    "WAL GC sweep skipped this tick: shard {}/{} is registered but has never published a manifest",
                    shard.indexUuid(),
                    shard.shardId()
                );
                return;
            }
            ShardHead shardHead = head.get().head();
            CommitManifest manifest = new BlobContainerManifestStore(shardContainer).readManifest(
                shardHead.primaryTerm(),
                shardHead.latestManifestGeneration()
            );
            WalPosition position = manifest.walPosition();
            if (position == null) {
                // Same container-wide bail-out as above, see that branch's comment.
                logger.info(
                    "WAL GC sweep skipped this tick: shard {}/{}'s latest manifest carries no real WAL position",
                    shard.indexUuid(),
                    shard.shardId()
                );
                return;
            }
            minCoveredSequence = Math.min(minCoveredSequence, position.offset());
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
