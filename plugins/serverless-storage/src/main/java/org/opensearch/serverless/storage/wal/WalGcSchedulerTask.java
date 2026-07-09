/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.WalPosition;
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
 */
public final class WalGcSchedulerTask implements Closeable {

    private final BlobContainer walBlobContainer;
    private final WalShardRegistry registry;
    private final BiFunction<String, Integer, BlobContainer> shardContainerResolver;
    private final Scheduler.Cancellable task;

    /**
     * Starts the scheduled sweep.
     *
     * @param threadPool schedules {@link #sweep()} on a fixed delay.
     * @param interval how often to sweep.
     * @param walBlobContainer the shared WAL container chunks are deleted from.
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
        this.walBlobContainer = walBlobContainer;
        this.registry = registry;
        this.shardContainerResolver = shardContainerResolver;
        this.task = threadPool.scheduleWithFixedDelay(this::sweepSafely, interval, ThreadPool.Names.GENERIC);
    }

    private void sweepSafely() {
        try {
            sweep();
        } catch (IOException e) {
            // Swallowed and retried next tick, same tolerance GcSchedulerTask already has for its
            // own sweep: nothing is corrupted by a skipped tick, only deferred.
        }
    }

    void sweep() throws IOException {
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
                return; // a known shard has never published -- conservative bail-out, see class javadoc
            }
            ShardHead shardHead = head.get().head();
            CommitManifest manifest = new BlobContainerManifestStore(shardContainer).readManifest(
                shardHead.primaryTerm(),
                shardHead.latestManifestGeneration()
            );
            WalPosition position = manifest.walPosition();
            if (position == null) {
                return; // this shard's latest commit carries no real WAL coverage -- conservative bail-out
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
