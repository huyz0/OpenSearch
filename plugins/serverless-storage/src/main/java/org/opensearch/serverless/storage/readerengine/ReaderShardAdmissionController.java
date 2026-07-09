/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.remote.filecache.FileCache;

import java.util.concurrent.Semaphore;

/**
 * A node-shared cap on how many reader shards may be open at once, now also weighed against the
 * node's actual lazy-directory block cache usage where one is configured
 * (rfc-serverless-opensearch.md &sect;18 risk #3, "reader heap under many shards" -- segment
 * metadata, terms index, and points index per open reader are the expensive parts, and nothing
 * today stops shard density from growing until the node runs out of heap).
 *
 * <p><b>Still not &sect;7's full target design</b>: &sect;7 describes per-*refresh* admission
 * control, deferring and retrying a refresh that would exceed budget while the shard keeps serving
 * slightly stale data. This class only gates shard *open*, not each subsequent refresh, and a
 * rejected open fails outright rather than degrading to stale-but-serving. What has changed since
 * this class's first cut is the *count-only* limitation: with {@link
 * org.opensearch.serverless.storage.readerengine.lazydirectory.LazyBundleDirectory} now wired in
 * (see {@code ServerlessStorageLazyDirectoryFactory}), a real per-node {@link FileCache} byte
 * budget exists to check against, so an open is now also rejected once that cache's usage crosses
 * a configured fraction of its capacity -- not just once the raw shard *count* crosses a fixed
 * number. When no {@link FileCache} is supplied (e.g. the lazy directory feature is off node-wide),
 * this degrades back to the original count-only behavior.
 */
public final class ReaderShardAdmissionController {

    private final int maxConcurrentReaderShards;
    private final Semaphore permits;
    private final FileCache fileCache;
    private final double maxFileCacheUsageRatio;

    public ReaderShardAdmissionController(int maxConcurrentReaderShards) {
        this(maxConcurrentReaderShards, null, 1.0);
    }

    /**
     * @param fileCache {@code null} to disable the byte-budget check entirely (count-only, as
     *                  before); otherwise the node's shared lazy-directory block cache to check
     *                  usage against on every {@link #acquire}.
     * @param maxFileCacheUsageRatio the fraction of {@code fileCache}'s capacity, in (0, 1], above
     *                               which a new reader shard is refused admission even if the
     *                               count cap has headroom. Ignored when {@code fileCache} is
     *                               {@code null}.
     */
    public ReaderShardAdmissionController(int maxConcurrentReaderShards, FileCache fileCache, double maxFileCacheUsageRatio) {
        if (maxConcurrentReaderShards <= 0) {
            throw new IllegalArgumentException("maxConcurrentReaderShards must be > 0, got " + maxConcurrentReaderShards);
        }
        if (maxFileCacheUsageRatio <= 0.0 || maxFileCacheUsageRatio > 1.0) {
            throw new IllegalArgumentException("maxFileCacheUsageRatio must be in (0, 1], got " + maxFileCacheUsageRatio);
        }
        this.maxConcurrentReaderShards = maxConcurrentReaderShards;
        this.permits = new Semaphore(maxConcurrentReaderShards);
        this.fileCache = fileCache;
        this.maxFileCacheUsageRatio = maxFileCacheUsageRatio;
    }

    /**
     * Admits one more open reader shard, or throws if this node has already reached its cap --
     * either the fixed shard-count cap, or (when a {@link FileCache} was supplied) the configured
     * fraction of that cache's byte capacity. Every successful call must be matched by exactly one
     * {@link #release()} once that shard's engine closes -- {@link ObjectStoreReaderEngine} owns
     * that pairing, not callers of this method directly.
     */
    public void acquire(ShardId shardId) {
        if (permits.tryAcquire() == false) {
            throw new IllegalStateException(
                "node has reached its reader-shard admission limit ("
                    + maxConcurrentReaderShards
                    + " concurrently open reader shards); cannot open reader shard "
                    + shardId
                    + " until another reader shard on this node closes"
            );
        }
        if (fileCache != null && isFileCacheOverBudget()) {
            permits.release();
            throw new IllegalStateException(
                "node's lazy-directory block cache usage ("
                    + fileCache.usage()
                    + " bytes) has reached its admission budget ("
                    + Math.round(fileCache.capacity() * maxFileCacheUsageRatio)
                    + " bytes, "
                    + maxFileCacheUsageRatio
                    + " of "
                    + fileCache.capacity()
                    + " byte capacity); cannot open reader shard "
                    + shardId
                    + " until cache usage drops, e.g. another reader shard on this node closes"
            );
        }
    }

    private boolean isFileCacheOverBudget() {
        long capacity = fileCache.capacity();
        if (capacity <= 0) {
            return false;
        }
        return fileCache.usage() >= Math.round(capacity * maxFileCacheUsageRatio);
    }

    public void release() {
        permits.release();
    }

    /** Exposed for tests/metrics, not part of the admission contract itself. */
    int availablePermits() {
        return permits.availablePermits();
    }
}
