/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteArrayDataInput;
import org.opensearch.common.blobstore.BlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fetches, concurrently, the blocks Lucene reads when it opens a commit, before it asks for them.
 *
 * <p>Opening a commit is a chain of small reads Lucene makes one after another: {@code segments_N}, then per
 * segment its {@code .si}, its {@code .cfe}, and the header and footer of every file it opens, each sub-file
 * of a compound file included -- a header to check the codec, a footer to check the checksum is there. Each
 * read that misses the block cache is a round trip to the object store, so a shard of twenty segments
 * waited for something like a hundred round trips in a row before it could answer. Every one of those
 * blocks is known from the manifest, the file lengths and the compound entries, without Lucene's help.
 *
 * <p>So the open fetches them first, in two waves: the head and tail block of every file (all of a file of
 * two blocks or less), then the head and tail of every compound sub-file, located from the {@code .cfe} the
 * first wave fetched. Adjacent blocks of one file go in one ranged read. The blocks are the ones Lucene
 * would have read anyway, so the request count stays about the same and the wait becomes a couple of round
 * trips. The caller fetches too, so a saturated executor costs width, never progress.
 *
 * <p>A hint, never a dependency. A block it fails to fetch is fetched by Lucene's own read as before, and a
 * compound entries file it cannot parse only loses the second wave. A cache too small to keep what it would
 * fetch until Lucene reads it is left alone: the prefetch would evict its own blocks.
 */
final class OpenPrefetch {

    private static final Logger logger = LogManager.getLogger(OpenPrefetch.class);

    /** Concurrent reads per open, the caller included. */
    static final int WIDTH = 16;

    /** The most blocks one ranged read covers. */
    static final int MAX_RANGE_BLOCKS = 16;

    /** How long an open waits for its prefetch before letting Lucene read for itself. */
    private static final long WAIT_SECONDS = 30;

    /** A file whose blocks may be fetched. */
    record Target(BlobContainer container, String blobName, String keyPrefix, long length, long blobOffset) {
        String key(long block) {
            return keyPrefix + "#" + block;
        }
    }

    /** Consecutive blocks of one file, fetched with one ranged read. */
    private record Range(Target target, long first, long last) {
    }

    private OpenPrefetch() {}

    /**
     * Fetches the blocks opening this commit will read.
     *
     * @param files the commit's files by name
     * @param cache the cache the blocks go into
     * @param executor where the helper reads run
     */
    static void run(Map<String, Target> files, BlockCache cache, Executor executor) {
        final int blockSize = cache.blockSize();
        final List<Range> first = new ArrayList<>();
        long blocks = 0;
        for (Target file : files.values()) {
            final TreeSet<Long> wanted = new TreeSet<>();
            addFile(wanted, 0, file.length(), blockSize);
            blocks += wanted.size();
            first.addAll(ranges(file, wanted));
        }
        if (first.isEmpty() || blocks > cache.maxBlocks() / 4) {
            return;
        }
        fetchAll(first, cache, executor);

        final List<Range> second = new ArrayList<>();
        for (Map.Entry<String, Target> entry : files.entrySet()) {
            if (entry.getKey().endsWith(".cfe") == false) {
                continue;
            }
            final Target data = files.get(entry.getKey().substring(0, entry.getKey().length() - 4) + ".cfs");
            if (data == null) {
                continue;
            }
            final List<long[]> subFiles = compoundEntries(entry.getValue(), cache);
            if (subFiles == null) {
                continue;
            }
            final TreeSet<Long> wanted = new TreeSet<>();
            for (long[] subFile : subFiles) {
                addFile(wanted, subFile[0], subFile[1], blockSize);
            }
            blocks += wanted.size();
            second.addAll(ranges(data, wanted));
        }
        if (second.isEmpty() == false && blocks <= cache.maxBlocks() / 4) {
            fetchAll(second, cache, executor);
        }
    }

    /** The blocks a reader of the byte span {@code [offset, offset + length)} touches first: its head and tail. */
    private static void addFile(TreeSet<Long> wanted, long offset, long length, int blockSize) {
        if (length <= 0) {
            return;
        }
        final long head = offset / blockSize;
        final long tail = (offset + length - 1) / blockSize;
        wanted.add(head);
        wanted.add(tail);
        if (tail - head <= 1) {
            // Small enough to be read whole, and a meta file of this size is.
            for (long block = head; block <= tail; block++) {
                wanted.add(block);
            }
        }
    }

    private static List<Range> ranges(Target file, TreeSet<Long> blocks) {
        final List<Range> ranges = new ArrayList<>();
        long start = -1;
        long end = -1;
        for (long block : blocks) {
            if (start >= 0 && block == end + 1 && block - start < MAX_RANGE_BLOCKS) {
                end = block;
                continue;
            }
            if (start >= 0) {
                ranges.add(new Range(file, start, end));
            }
            start = block;
            end = block;
        }
        if (start >= 0) {
            ranges.add(new Range(file, start, end));
        }
        return ranges;
    }

    /**
     * The {@code [offset, length]} of every sub-file a compound entries file lists, read from the blocks the
     * first wave left in the cache; null if any is missing or the file is not in the format this knows.
     */
    private static List<long[]> compoundEntries(Target entries, BlockCache cache) {
        final int blockSize = cache.blockSize();
        if (entries.length() > (long) MAX_RANGE_BLOCKS * blockSize) {
            return null;
        }
        final byte[] bytes = new byte[(int) entries.length()];
        for (long block = 0; block * blockSize < entries.length(); block++) {
            final byte[] resident = cache.peek(entries.key(block));
            if (resident == null) {
                return null;
            }
            final int offset = (int) (block * blockSize);
            System.arraycopy(resident, 0, bytes, offset, Math.min(resident.length, bytes.length - offset));
        }
        try {
            final ByteArrayDataInput in = new ByteArrayDataInput(bytes);
            // The index header of Lucene90CompoundFormat's entries file, the segment id read past rather than
            // checked: the open that follows checks it against the segment, and a mismatch here only means
            // the wrong blocks were warmed.
            CodecUtil.checkHeader(in, "Lucene90CompoundEntries", 0, 0);
            in.skipBytes(org.apache.lucene.util.StringHelper.ID_LENGTH);
            in.skipBytes(in.readByte() & 0xFF);
            final int count = in.readVInt();
            final List<long[]> subFiles = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                in.readString();
                subFiles.add(new long[] { in.readLong(), in.readLong() });
            }
            return subFiles;
        } catch (IOException | RuntimeException e) {
            logger.debug("not warming the compound sub-files of {}: {}", entries.blobName(), e.toString());
            return null;
        }
    }

    private static void fetchAll(List<Range> ranges, BlockCache cache, Executor executor) {
        final AtomicInteger next = new AtomicInteger();
        final CountDownLatch done = new CountDownLatch(ranges.size());
        final Runnable drain = () -> {
            for (int i = next.getAndIncrement(); i < ranges.size(); i = next.getAndIncrement()) {
                try {
                    fetch(ranges.get(i), cache);
                } catch (IOException | RuntimeException e) {
                    logger.debug("prefetch of {} failed; the open reads it itself: {}", ranges.get(i).target().blobName(), e.toString());
                } finally {
                    done.countDown();
                }
            }
        };
        for (int helper = 0; helper < Math.min(WIDTH, ranges.size()) - 1; helper++) {
            try {
                executor.execute(drain);
            } catch (RuntimeException rejected) {
                break;
            }
        }
        drain.run();
        try {
            done.await(WAIT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void fetch(Range range, BlockCache cache) throws IOException {
        final Target file = range.target();
        long first = range.first();
        while (first <= range.last() && cache.peek(file.key(first)) != null) {
            first++;
        }
        if (first > range.last()) {
            return;
        }
        final long from = first;
        final int blockSize = cache.blockSize();
        cache.fetch(file.key(from), () -> {
            final long start = from * blockSize;
            final int size = (int) Math.min((range.last() - from + 1) * blockSize, file.length() - start);
            final byte[] span = new byte[size];
            try (InputStream in = file.container().readBlob(file.blobName(), file.blobOffset() + start, size)) {
                int read = 0;
                while (read < size) {
                    final int n = in.read(span, read, size - read);
                    if (n < 0) {
                        throw new IOException("short read of " + file.blobName() + " at " + start + ": wanted " + size + ", got " + read);
                    }
                    read += n;
                }
            }
            if (size <= blockSize) {
                return span;
            }
            for (int offset = blockSize; offset < size; offset += blockSize) {
                final byte[] block = new byte[Math.min(blockSize, size - offset)];
                System.arraycopy(span, offset, block, 0, block.length);
                cache.put(file.key(from + offset / blockSize), block);
            }
            final byte[] head = new byte[blockSize];
            System.arraycopy(span, 0, head, 0, blockSize);
            return head;
        });
    }
}
