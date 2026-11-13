/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.apache.lucene.store.IOContext;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.MemorySizeValue;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * One node's budget of memory for the segment files its writers have just written, and the count of every byte those
 * writers put on local disk.
 *
 * <p><b>Why memory first.</b> Local disk on a writer holds nothing durable: the log holds a write before it is
 * acknowledged, and the object store holds a segment before its manifest names it. A freshly flushed segment is written
 * to local disk only to be read straight back, by the publisher that uploads it, by the reader refreshed onto it, and by
 * the merge that soon folds it into a bigger one, after which it is deleted. For a node of small tenants, most flushed
 * bytes live a few seconds. Held in memory, they are uploaded, searched and merged from there, and the ones deleted while
 * still in memory never touch the disk at all.
 *
 * <p><b>Bounded, and spilled rather than refused.</b> One budget for the whole node, not one per shard: a node of
 * thousands of shards cannot give each a share worth having, and most are idle at any moment. A file being written whose
 * bytes would take the node past the budget, or that grows past {@link #maxFileBytes}, moves to disk where it is and
 * carries on there. A merge writes to disk from the start: its output is large, long-lived, and written off the write
 * path. And when the files held reach the high watermark, the oldest are written to disk in the background until they are
 * back under the low one. So a byte goes to disk at most once and only if it outlived the room memory had for it, and the
 * memory used is the budget plus the few blocks in flight.
 *
 * <p><b>Nothing here is ever the durable copy</b>, which is why a crash losing what memory held loses nothing: a shard
 * opens from its published commit, after dropping every local commit point (see {@code ShardOpening#dropLocalFilesNamedBy}),
 * and replays the log for the rest. A commit point is never moved to disk on its own, so a local commit can never name a
 * file that went with the memory.
 *
 * <p>On by default ({@link #ENABLED_SETTING}); the byte counts are kept either way, so the disk writes a node makes without
 * it can be measured with it off.
 */
public final class LocalSegmentTier {

    /** Whether writers hold freshly written segment files in memory. */
    public static final String ENABLED_SETTING = "serverless.local_segments.memory.enabled";
    /** The default {@link #ENABLED_SETTING}. */
    public static final boolean DEFAULT_ENABLED = true;
    /** The node's budget for them, in bytes or as a share of the heap. */
    public static final String MAX_BYTES_SETTING = "serverless.local_segments.memory.max_bytes";
    /** The default {@link #MAX_BYTES_SETTING}. */
    public static final String DEFAULT_MAX_BYTES = "5%";
    /** The largest file held in memory; a bigger one moves to disk as it passes this. */
    public static final String MAX_FILE_BYTES_SETTING = "serverless.local_segments.memory.max_file_bytes";
    /** The default {@link #MAX_FILE_BYTES_SETTING}. */
    public static final String DEFAULT_MAX_FILE_BYTES = "16mb";

    /** Memory is taken in steps of this much, so a writer is not charged on every byte. */
    static final int RESERVE_STEP = 64 * 1024;

    /** What a file was written for: what Lucene's context says, plus the files a writer makes for itself. */
    public enum Purpose {
        /** A segment written by a flush (refresh), or a commit point. */
        FLUSH,
        /** A segment written by a merge. */
        MERGE,
        /** Anything else: temporary files, lock files, files written outside indexing. */
        OTHER;

        static Purpose of(IOContext context) {
            return switch (context.context()) {
                case FLUSH -> FLUSH;
                case MERGE -> MERGE;
                default -> OTHER;
            };
        }
    }

    private final boolean enabled;
    private final long maxBytes;
    private final long maxFileBytes;
    private final Supplier<Executor> evictionExecutor;

    private final AtomicLong reserved = new AtomicLong();
    private final AtomicLong peakReserved = new AtomicLong();
    private final AtomicLong heldFiles = new AtomicLong();
    private final ConcurrentLinkedQueue<TieredSegmentDirectory.RamFile> oldestFirst = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean evicting = new AtomicBoolean();
    private final AtomicLong queued = new AtomicLong();
    private final AtomicBoolean sweeping = new AtomicBoolean();

    private final LongAdder[] diskBytes = adders();
    private final LongAdder[] memoryBytes = adders();
    private final LongAdder spilledWhileWriting = new LongAdder();
    private final LongAdder evictedBytes = new LongAdder();
    private final LongAdder evictedFiles = new LongAdder();
    private final LongAdder deletedInMemoryBytes = new LongAdder();
    private final LongAdder memoryReads = new LongAdder();

    /**
     * @param enabled whether files are held in memory at all
     * @param maxBytes the node's budget
     * @param maxFileBytes the largest file held
     * @param evictionExecutor where the oldest files are written out when the budget runs low; null runs it on the caller
     */
    public LocalSegmentTier(boolean enabled, long maxBytes, long maxFileBytes, Supplier<Executor> evictionExecutor) {
        this.enabled = enabled && maxBytes > 0;
        this.maxBytes = Math.max(0, maxBytes);
        this.maxFileBytes = Math.max(0, Math.min(maxFileBytes, this.maxBytes));
        this.evictionExecutor = evictionExecutor;
    }

    /**
     * Builds the tier a node's settings ask for.
     *
     * @param settings the node settings
     * @param evictionExecutor where eviction runs
     * @return the tier
     */
    public static LocalSegmentTier fromSettings(Settings settings, Supplier<Executor> evictionExecutor) {
        final long max = MemorySizeValue.parseBytesSizeValueOrHeapRatio(
            settings.get(MAX_BYTES_SETTING, DEFAULT_MAX_BYTES),
            MAX_BYTES_SETTING
        ).getBytes();
        final long maxFile = settings.getAsBytesSize(
            MAX_FILE_BYTES_SETTING,
            org.opensearch.core.common.unit.ByteSizeValue.parseBytesSizeValue(DEFAULT_MAX_FILE_BYTES, MAX_FILE_BYTES_SETTING)
        ).getBytes();
        return new LocalSegmentTier(settings.getAsBoolean(ENABLED_SETTING, DEFAULT_ENABLED), max, maxFile, evictionExecutor);
    }

    private static LongAdder[] adders() {
        final LongAdder[] out = new LongAdder[Purpose.values().length];
        for (int i = 0; i < out.length; i++) {
            out[i] = new LongAdder();
        }
        return out;
    }

    /** @return whether files are held in memory */
    public boolean enabled() {
        return enabled;
    }

    /** @return the node's budget in bytes */
    public long maxBytes() {
        return maxBytes;
    }

    long maxFileBytes() {
        return maxFileBytes;
    }

    /** Whether a file written in this context starts in memory: not a merge's, and not one expected to be too big. */
    boolean startsInMemory(IOContext context) {
        if (enabled == false || context.context() == IOContext.Context.MERGE) {
            return false;
        }
        final var flush = context.flushInfo();
        // A segment's estimate covers all its files; a segment expected to be bigger than the budget's largest file
        // would mostly spill anyway, after first being copied.
        return flush == null || flush.estimatedSegmentSize() <= maxFileBytes * 4;
    }

    /**
     * Takes more memory for a file being written.
     *
     * @return false if the node has no room, and the file should move to disk
     */
    boolean reserve(long bytes) {
        while (true) {
            final long now = reserved.get();
            if (now + bytes > maxBytes) {
                maybeEvict();
                return false;
            }
            if (reserved.compareAndSet(now, now + bytes)) {
                peakReserved.accumulateAndGet(now + bytes, Math::max);
                if (now + bytes > highWatermark()) {
                    maybeEvict();
                }
                return true;
            }
        }
    }

    /**
     * Charges memory already in use, room or not: a file's last block, found when it closes. Called holding a directory's
     * lock, so it never evicts; the caller runs {@link #evictIfHigh} once it lets go.
     */
    void charge(long bytes) {
        peakReserved.accumulateAndGet(reserved.addAndGet(bytes), Math::max);
    }

    void evictIfHigh() {
        if (reserved.get() > highWatermark()) {
            maybeEvict();
        }
    }

    void release(long bytes) {
        reserved.addAndGet(-bytes);
    }

    private long highWatermark() {
        return maxBytes / 10 * 9;
    }

    private long lowWatermark() {
        return maxBytes / 4 * 3;
    }

    void held(TieredSegmentDirectory.RamFile file) {
        final long held = heldFiles.incrementAndGet();
        oldestFirst.add(file);
        // Files deleted in memory leave an empty entry behind until eviction reaches it, which a node under no pressure
        // never does; swept once they outnumber the files held.
        if (queued.incrementAndGet() > 2 * held + 1024 && sweeping.compareAndSet(false, true)) {
            try {
                oldestFirst.removeIf(entry -> entry.isLive() == false);
                queued.set(oldestFirst.size());
            } finally {
                sweeping.set(false);
            }
        }
    }

    void dropped(TieredSegmentDirectory.RamFile file) {
        heldFiles.decrementAndGet();
    }

    /** Starts writing the oldest files out, unless that is already running. */
    private void maybeEvict() {
        if (evicting.compareAndSet(false, true) == false) {
            return;
        }
        final Executor executor = evictionExecutor == null ? null : evictionExecutor.get();
        if (executor == null) {
            evictNow();
            return;
        }
        try {
            executor.execute(this::evictNow);
        } catch (RuntimeException rejected) {
            evicting.set(false);
        }
    }

    private void evictNow() {
        try {
            // Each entry looked at once a pass at most, so a queue of nothing but pinned files ends the pass.
            for (int left = oldestFirst.size(); left > 0 && reserved.get() > lowWatermark(); left--) {
                final TieredSegmentDirectory.RamFile oldest = oldestFirst.poll();
                if (oldest == null) {
                    break;
                }
                queued.decrementAndGet();
                if (oldest.owner().evict(oldest) == false && oldest.isLive()) {
                    // Pinned (a commit point); it goes when its shard writes the next one.
                    oldestFirst.add(oldest);
                    queued.incrementAndGet();
                }
            }
        } finally {
            evicting.set(false);
        }
    }

    void countDisk(Purpose purpose, long bytes) {
        diskBytes[purpose.ordinal()].add(bytes);
    }

    void countMemory(Purpose purpose, long bytes) {
        memoryBytes[purpose.ordinal()].add(bytes);
    }

    void countSpilledWhileWriting(long bytes) {
        spilledWhileWriting.add(bytes);
    }

    void countEvicted(long bytes) {
        evictedBytes.add(bytes);
        evictedFiles.increment();
    }

    void countDeletedInMemory(long bytes) {
        deletedInMemoryBytes.add(bytes);
    }

    void countMemoryRead() {
        memoryReads.increment();
    }

    /**
     * What writers have written so far.
     *
     * @param diskFlush bytes written to local disk by flushes and commit points, including those that spilled or were evicted
     * @param diskMerge bytes written to local disk by merges
     * @param diskOther every other byte written to local disk
     * @param memoryFlush bytes of flushed files that were written to memory
     * @param memoryOther bytes of other files written to memory
     * @param spilledWhileWriting bytes of files that started in memory and moved to disk while being written
     * @param evicted bytes of held files written to disk later to make room
     * @param evictedFiles how many files were
     * @param deletedInMemory bytes of held files deleted before they ever reached disk
     * @param heldBytes memory held or reserved now
     * @param heldFiles files held now
     * @param peakBytes the most memory ever held or reserved at once
     * @param memoryOpens files opened from memory
     */
    public record Stats(long diskFlush, long diskMerge, long diskOther, long memoryFlush, long memoryOther, long spilledWhileWriting,
        long evicted, long evictedFiles, long deletedInMemory, long heldBytes, long heldFiles, long peakBytes, long memoryOpens) {
        /** @return every byte written to local disk */
        public long diskTotal() {
            return diskFlush + diskMerge + diskOther;
        }

        /**
         * The change from an earlier snapshot, for counters; the held and peak figures are this snapshot's.
         *
         * @param earlier the earlier snapshot
         * @return the difference
         */
        public Stats since(Stats earlier) {
            return new Stats(
                diskFlush - earlier.diskFlush,
                diskMerge - earlier.diskMerge,
                diskOther - earlier.diskOther,
                memoryFlush - earlier.memoryFlush,
                memoryOther - earlier.memoryOther,
                spilledWhileWriting - earlier.spilledWhileWriting,
                evicted - earlier.evicted,
                evictedFiles - earlier.evictedFiles,
                deletedInMemory - earlier.deletedInMemory,
                heldBytes,
                heldFiles,
                peakBytes,
                memoryOpens - earlier.memoryOpens
            );
        }
    }

    /** @return the counts so far */
    public Stats stats() {
        return new Stats(
            diskBytes[Purpose.FLUSH.ordinal()].sum(),
            diskBytes[Purpose.MERGE.ordinal()].sum(),
            diskBytes[Purpose.OTHER.ordinal()].sum(),
            memoryBytes[Purpose.FLUSH.ordinal()].sum(),
            memoryBytes[Purpose.MERGE.ordinal()].sum() + memoryBytes[Purpose.OTHER.ordinal()].sum(),
            spilledWhileWriting.sum(),
            evictedBytes.sum(),
            evictedFiles.sum(),
            deletedInMemoryBytes.sum(),
            reserved.get(),
            heldFiles.get(),
            peakReserved.get(),
            memoryReads.sum()
        );
    }

    /** Resets the peak to what is held now, so a measurement can report the peak of its own window. */
    public void resetPeak() {
        peakReserved.set(reserved.get());
    }
}
