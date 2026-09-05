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
 *
 * <p><b>The byte budget is measured against {@link FileCache#activeUsage()}, not {@link
 * FileCache#usage()}, and that distinction was the difference between a working node and a wedged
 * one.</b> The cache is core's own concurrent LRU, whose contract is "once file cache reaches its
 * capacity, it starts evictions" -- so an LRU that is <em>doing its job</em> sits at essentially
 * 100% of capacity for as long as the working set exceeds it. {@code usage()} counts every entry,
 * including every freely evictable one; it is not a pressure signal, it is a "the cache is warm"
 * signal. Checking it against a 0.9 ratio meant that once a node's cache first filled -- which is
 * the steady state, not an incident -- two things became permanently true: every subsequent reader
 * shard open on that node threw, so the shard failed allocation, was retried, and failed again
 * forever; and every refresh on every already-open reader was deferred, so no reader on the node
 * ever advanced a generation again. Lag then grew without bound, which is both a consistency-model
 * violation and an autoscaling signal pointing the wrong way (more reader nodes, each hitting the
 * same wall), and the frozen generation eventually aged past the GC retention window and was
 * deleted out from under the reader. {@code activeUsage()} counts bytes currently
 * <em>referenced</em>, i.e. genuinely unevictable, which is the quantity a budget question is
 * actually about.
 */
public final class ReaderShardAdmissionController {

    private final int maxConcurrentReaderShards;
    private final Semaphore permits;
    private final FileCache fileCache;
    private final double maxFileCacheUsageRatio;
    /**
     * Estimated heap currently held by this node's open reader shards, or {@code null} when nothing
     * supplies it.
     *
     * <p>The quantity the design actually names as expensive per open reader is "segment metadata,
     * terms index, points index" -- heap held by open {@code SegmentReader}s -- and this class
     * measured neither of the two things it had: it counted shards, and it counted the block cache's
     * <em>disk</em> bytes. Nothing in the plugin bounded reader heap at all, which is what the
     * "reader heap under many shards" risk is about. A supplier rather than a hard dependency
     * because summing {@code ramBytesUsed()} across every open searcher's leaves is the caller's
     * knowledge, not this class's, and because a deployment with no way to measure it should get
     * exactly the behaviour it had before rather than a fabricated number.
     */
    private final java.util.function.LongSupplier readerHeapBytesSupplier;
    /** Bytes of estimated reader heap above which a new open is refused; {@code <= 0} disables the check. */
    private final long maxReaderHeapBytes;

    /**
     * Creates a count-only admission controller, with no file-cache byte-budget check.
     *
     * @param maxConcurrentReaderShards the maximum number of reader shards this node will admit at once
     */
    public ReaderShardAdmissionController(int maxConcurrentReaderShards) {
        this(maxConcurrentReaderShards, null, 1.0);
    }

    /**
     * Creates an admission controller with an optional file-cache byte-budget check in addition to
     * the fixed shard-count cap.
     *
     * @param maxConcurrentReaderShards the maximum number of reader shards this node will admit at once
     * @param fileCache {@code null} to disable the byte-budget check entirely (count-only, as
     *                  before); otherwise the node's shared lazy-directory block cache to check
     *                  usage against on every {@link #acquire}.
     * @param maxFileCacheUsageRatio the fraction of {@code fileCache}'s capacity, in (0, 1], above
     *                               which a new reader shard is refused admission even if the
     *                               count cap has headroom. Ignored when {@code fileCache} is
     *                               {@code null}.
     */
    public ReaderShardAdmissionController(int maxConcurrentReaderShards, FileCache fileCache, double maxFileCacheUsageRatio) {
        this(maxConcurrentReaderShards, fileCache, maxFileCacheUsageRatio, null, 0L);
    }

    /**
     * Creates an admission controller that also refuses an open once this node's open reader shards
     * are estimated to hold more than {@code maxReaderHeapBytes} of heap.
     *
     * @param maxConcurrentReaderShards the maximum number of reader shards this node will admit at once
     * @param fileCache {@code null} to disable the block-cache byte-budget check entirely.
     * @param maxFileCacheUsageRatio the fraction of {@code fileCache}'s capacity, in (0, 1], above
     *                               which a new reader shard is refused admission.
     * @param readerHeapBytesSupplier estimated heap currently held by this node's open reader
     *                                shards, or {@code null} to disable the heap check -- see the
     *                                field's own javadoc for why this is supplied rather than measured here.
     * @param maxReaderHeapBytes the estimated-heap ceiling; {@code <= 0} disables the heap check.
     */
    public ReaderShardAdmissionController(
        int maxConcurrentReaderShards,
        FileCache fileCache,
        double maxFileCacheUsageRatio,
        java.util.function.LongSupplier readerHeapBytesSupplier,
        long maxReaderHeapBytes
    ) {
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
        this.readerHeapBytesSupplier = readerHeapBytesSupplier;
        this.maxReaderHeapBytes = maxReaderHeapBytes;
    }

    /**
     * Admits one more open reader shard, or throws if this node has already reached its cap --
     * either the fixed shard-count cap, or (when a {@link FileCache} was supplied) the configured
     * fraction of that cache's byte capacity. Every successful call must be matched by exactly one
     * {@link #release()} once that shard's engine closes -- {@link ObjectStoreReaderEngine} owns
     * that pairing, not callers of this method directly.
     *
     * @param shardId the shard being opened, used only to build the rejection message
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
        if (isReaderHeapOverBudget()) {
            permits.release();
            throw new IllegalStateException(
                "node's estimated reader-shard heap ("
                    + readerHeapBytesSupplier.getAsLong()
                    + " bytes) has reached its admission budget ("
                    + maxReaderHeapBytes
                    + " bytes); cannot open reader shard "
                    + shardId
                    + " until another reader shard on this node closes"
            );
        }
        if (fileCache != null && isFileCacheOverBudget()) {
            permits.release();
            throw new IllegalStateException(
                "node's lazy-directory block cache active usage ("
                    + fileCache.activeUsage()
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
        // activeUsage(), not usage() -- see this class's own javadoc for why the difference between
        // "warm" and "under pressure" is the whole finding here.
        return fileCache.activeUsage() >= Math.round(capacity * maxFileCacheUsageRatio);
    }

    /**
     * Re-asks the budget question after giving the cache a chance to release what it can.
     *
     * <p>{@link FileCache#prune()} drops every entry that is currently unreferenced. Calling it
     * before concluding "no headroom" matters because reading the budget and reclaiming space are
     * otherwise decoupled: a node can be over its active-usage budget purely because nothing has
     * recently asked the cache to let go of anything. Used only on the per-refresh path, never on
     * {@link #acquire}: pruning a node's whole block cache to admit one more shard would trade a
     * bounded staleness cost for an unbounded latency one.
     *
     * @return whether the node is still over budget after a prune.
     */
    public boolean pruneAndRecheckOverBudgetForRefresh() {
        if (fileCache == null) {
            return false;
        }
        fileCache.prune();
        return isFileCacheOverBudget();
    }

    /**
     * Non-throwing budget check for an already-open shard's periodic refresh, not a new shard
     * open: an over-budget refresh should defer and keep serving the shard's current, slightly
     * stale generation (rfc-serverless-opensearch.md &sect;18 risk #3's per-refresh target), not
     * fail the shard the way {@link #acquire} fails a rejected open. Never consults the
     * shard-count permits -- an already-open shard doesn't need one to keep refreshing. Always
     * {@code false} when no {@link FileCache} was supplied.
     *
     * @return whether the node's lazy-directory block cache is currently over its admission budget.
     */
    public boolean isOverBudgetForRefresh() {
        return fileCache != null && isFileCacheOverBudget();
    }

    /**
     * Whether this node's open reader shards already hold more heap than the configured ceiling.
     * Always {@code false} when no supplier or no ceiling was configured, so a deployment that
     * cannot measure reader heap behaves exactly as it did before this check existed.
     */
    private boolean isReaderHeapOverBudget() {
        if (readerHeapBytesSupplier == null || maxReaderHeapBytes <= 0) {
            return false;
        }
        return readerHeapBytesSupplier.getAsLong() >= maxReaderHeapBytes;
    }

    /** Releases the permit acquired by a matching {@link #acquire}, once that reader shard's engine closes. */
    public void release() {
        permits.release();
    }

    /** Exposed for tests/metrics, not part of the admission contract itself. */
    int availablePermits() {
        return permits.availablePermits();
    }
}
