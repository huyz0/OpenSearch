/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;

/**
 * One shard's local directory with the node's {@link LocalSegmentTier} in front of it: freshly flushed files are written
 * to memory, while the node has room, and read from there by whatever reads them next -- the publisher uploading them, a
 * refreshed reader, a merge, a commit pushed to a search node. A merge's output, a file too big, and a file whose bytes
 * found the node's budget spent go to disk. Every byte written to disk is counted by what wrote it, whether or not the
 * tier holds anything in memory.
 *
 * <p>To everything above it this is one directory: a name lives in memory or on disk, never both, and
 * {@link #listAll}, {@link #fileLength}, {@link #openInput}, {@link #rename} and {@link #deleteFile} answer for either.
 * A file moved to disk later is written whole before its memory copy is let go, under the same lock a delete or rename
 * takes, so nothing ever sees it missing or sees two copies; a reader already open on the memory copy keeps it.
 *
 * <p>{@link #sync} has nothing to do for a file in memory: nothing local is the durable copy (see {@link LocalSegmentTier}).
 */
public final class TieredSegmentDirectory extends FilterDirectory {

    /** Files under this are copied into one exact array when they close. */
    private static final int SMALL_FILE = 1 << 12;

    private final LocalSegmentTier tier;
    private final Map<String, RamFile> held = new ConcurrentHashMap<>();
    private final Map<String, TieredOutput> writing = new ConcurrentHashMap<>();
    private volatile boolean closed;
    private long tempCounter;

    /**
     * @param local the shard's local directory
     * @param tier the node's tier
     */
    public TieredSegmentDirectory(Directory local, LocalSegmentTier tier) {
        super(local);
        this.tier = tier;
    }

    /** A file held in memory: its bytes until it is deleted, renamed away or moved to disk. */
    static final class RamFile {
        private final TieredSegmentDirectory owner;
        private final LocalSegmentTier.Purpose purpose;
        private final long length;
        private final long reserved;
        private volatile String name;
        private volatile List<ByteBuffer> buffers;

        RamFile(
            TieredSegmentDirectory owner,
            String name,
            LocalSegmentTier.Purpose purpose,
            List<ByteBuffer> buffers,
            long length,
            long reserved
        ) {
            this.owner = owner;
            this.name = name;
            this.purpose = purpose;
            this.buffers = buffers;
            this.length = length;
            this.reserved = reserved;
        }

        TieredSegmentDirectory owner() {
            return owner;
        }

        boolean isLive() {
            return buffers != null;
        }

        /** A commit point stays in memory: on disk alone, it could name files that went with the memory. */
        boolean pinned() {
            return isCommitPoint(name);
        }
    }

    static boolean isCommitPoint(String name) {
        return name.startsWith("segments_") || name.startsWith("pending_segments_");
    }

    /** @return the files this directory holds in memory now */
    public Collection<String> heldInMemory() {
        return new TreeSet<>(held.keySet());
    }

    private LocalSegmentTier.Purpose purposeOf(String name, IOContext context) {
        return isCommitPoint(name) ? LocalSegmentTier.Purpose.FLUSH : LocalSegmentTier.Purpose.of(context);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureNotClosed();
        final LocalSegmentTier.Purpose purpose = purposeOf(name, context);
        if (tier.startsInMemory(context) == false) {
            return new CountingOutput(in.createOutput(name, context), purpose);
        }
        synchronized (this) {
            if (held.containsKey(name) || writing.containsKey(name)) {
                throw new FileAlreadyExistsException(name);
            }
            final TieredOutput output = new TieredOutput(name, context, purpose);
            writing.put(name, output);
            return output;
        }
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        ensureNotClosed();
        if (tier.startsInMemory(context) == false) {
            return new CountingOutput(in.createTempOutput(prefix, suffix, context), LocalSegmentTier.Purpose.of(context));
        }
        // Lucene's stored fields keep two of these open while a segment fills and read them back once when it flushes; the
        // name is unique on its own (a counter with a mark the disk directory's names never carry), and checked anyway.
        synchronized (this) {
            while (true) {
                final String name = org.apache.lucene.index.IndexFileNames.segmentFileName(
                    prefix,
                    suffix + "_m" + Long.toString(tempCounter++, Character.MAX_RADIX),
                    "tmp"
                );
                if (held.containsKey(name) || writing.containsKey(name) || exists(name)) {
                    continue;
                }
                final TieredOutput output = new TieredOutput(name, context, LocalSegmentTier.Purpose.of(context));
                writing.put(name, output);
                return output;
            }
        }
    }

    private boolean exists(String name) throws IOException {
        try {
            in.fileLength(name);
            return true;
        } catch (NoSuchFileException | java.io.FileNotFoundException e) {
            return false;
        }
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        final RamFile file = held.get(name);
        if (file != null) {
            final List<ByteBuffer> buffers = file.buffers;
            if (buffers != null) {
                tier.countMemoryRead();
                return memoryInput(name, buffers);
            }
            // Moved to disk since the lookup: read on.
        }
        return in.openInput(name, context);
    }

    @Override
    public ChecksumIndexInput openChecksumInput(String name) throws IOException {
        return new BufferedChecksumIndexInput(openInput(name, IOContext.READONCE));
    }

    private static IndexInput memoryInput(String name, List<ByteBuffer> buffers) {
        final List<ByteBuffer> views = new ArrayList<>(buffers.size());
        for (ByteBuffer buffer : buffers) {
            views.add(buffer.duplicate());
        }
        return new ByteBuffersIndexInput(new ByteBuffersDataInput(views), "memory(" + name + ")");
    }

    @Override
    public long fileLength(String name) throws IOException {
        final RamFile file = held.get(name);
        if (file != null && file.isLive()) {
            return file.length;
        }
        final TieredOutput open = writing.get(name);
        if (open != null) {
            return open.getFilePointer();
        }
        return in.fileLength(name);
    }

    @Override
    public String[] listAll() throws IOException {
        final TreeSet<String> all = new TreeSet<>(Arrays.asList(in.listAll()));
        all.addAll(held.keySet());
        all.addAll(writing.keySet());
        return all.toArray(new String[0]);
    }

    @Override
    public synchronized void deleteFile(String name) throws IOException {
        final RamFile file = held.remove(name);
        if (file != null) {
            let(file);
            tier.countDeletedInMemory(file.length);
            return;
        }
        final TieredOutput open = writing.get(name);
        if (open != null && open.onDisk() == false) {
            open.deletedWhileOpen = true;
            writing.remove(name);
            return;
        }
        in.deleteFile(name);
    }

    @Override
    public synchronized void rename(String source, String dest) throws IOException {
        final RamFile file = held.get(source);
        if (file == null) {
            in.rename(source, dest);
            return;
        }
        if (held.containsKey(dest)) {
            throw new FileAlreadyExistsException(dest);
        }
        held.remove(source);
        file.name = dest;
        held.put(dest, file);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        final List<String> onDisk = new ArrayList<>(names.size());
        for (String name : names) {
            if (held.containsKey(name) == false) {
                onDisk.add(name);
            }
        }
        if (onDisk.isEmpty() == false) {
            in.sync(onDisk);
        }
    }

    @Override
    public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
        try (IndexInput input = from.openInput(src, context); IndexOutput output = createOutput(dest, context)) {
            output.copyBytes(input, input.length());
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        for (RamFile file : held.values()) {
            let(file);
        }
        held.clear();
        super.close();
    }

    private void ensureNotClosed() {
        if (closed) {
            throw new org.apache.lucene.store.AlreadyClosedException("this directory is closed");
        }
    }

    /** Gives a held file's memory back. Called holding this directory's lock. */
    private void let(RamFile file) {
        if (file.buffers != null) {
            file.buffers = null;
            tier.release(file.reserved);
            tier.dropped(file);
        }
    }

    /**
     * Writes a held file to disk and lets its memory go, unless it is pinned, gone, or this directory is closed.
     *
     * @return whether it was moved
     */
    synchronized boolean evict(RamFile file) {
        final List<ByteBuffer> buffers = file.buffers;
        if (closed || buffers == null || held.get(file.name) != file || file.pinned()) {
            return false;
        }
        try {
            try (IndexOutput output = in.createOutput(file.name, IOContext.DEFAULT)) {
                copy(buffers, output);
            }
        } catch (IOException | RuntimeException e) {
            try {
                in.deleteFile(file.name);
            } catch (IOException | RuntimeException ignored) {
                // nothing was written, or what was is unreferenced and Lucene's deleter removes it
            }
            return false;
        }
        held.remove(file.name);
        let(file);
        tier.countEvicted(file.length);
        tier.countDisk(file.purpose, file.length);
        return true;
    }

    private static void copy(List<ByteBuffer> buffers, IndexOutput output) throws IOException {
        final byte[] scratch = new byte[8192];
        for (ByteBuffer buffer : buffers) {
            final ByteBuffer view = buffer.duplicate();
            while (view.hasRemaining()) {
                final int n = Math.min(scratch.length, view.remaining());
                view.get(scratch, 0, n);
                output.writeBytes(scratch, 0, n);
            }
        }
    }

    /** A file written to memory while it fits, moving to disk where it is once it does not. */
    private final class TieredOutput extends IndexOutput {
        private final String name;
        private final IOContext context;
        private final LocalSegmentTier.Purpose purpose;
        private final BufferedChecksum checksum = new BufferedChecksum(new CRC32());
        private ByteBuffersDataOutput memory = new ByteBuffersDataOutput(12, 16, ByteBuffersDataOutput.ALLOCATE_BB_ON_HEAP, b -> {});
        private IndexOutput disk;
        private long reserved;
        private long pointer;
        private boolean closedOutput;
        volatile boolean deletedWhileOpen;

        TieredOutput(String name, IOContext context, LocalSegmentTier.Purpose purpose) {
            super("tiered(" + name + ")", name);
            this.name = name;
            this.context = context;
            this.purpose = purpose;
        }

        boolean onDisk() {
            return disk != null;
        }

        private void ensureRoom(int length) throws IOException {
            final long needed = memory.size() + length;
            if (needed <= reserved) {
                return;
            }
            if (needed > tier.maxFileBytes()) {
                spill();
                return;
            }
            // Growing with the file, from one block to a step at a time, never past the largest file it may become: a
            // segment's stored fields keep four files open for as long as it fills, on every shard of the node, and a
            // whole step reserved for each would take the budget while holding a few bytes.
            final long step = Math.min(LocalSegmentTier.RESERVE_STEP, Math.max(SMALL_FILE, reserved));
            final long more = Math.max(needed - reserved, Math.min(step, tier.maxFileBytes() - reserved));
            if (tier.reserve(more)) {
                reserved += more;
            } else {
                spill();
            }
        }

        private void spill() throws IOException {
            final IndexOutput out = in.createOutput(name, context);
            try {
                copy(memory.toBufferList(), out);
            } catch (IOException | RuntimeException e) {
                org.opensearch.common.util.io.IOUtils.closeWhileHandlingException(out);
                throw e;
            }
            tier.countSpilledWhileWriting(memory.size());
            disk = out;
            memory = null;
            tier.release(reserved);
            reserved = 0;
            synchronized (TieredSegmentDirectory.this) {
                writing.remove(name, this);
            }
        }

        @Override
        public void writeByte(byte b) throws IOException {
            checksum.update(b);
            if (disk == null) {
                ensureRoom(1);
            }
            if (disk != null) {
                disk.writeByte(b);
            } else {
                memory.writeByte(b);
            }
            pointer++;
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            checksum.update(b, offset, length);
            if (disk == null) {
                ensureRoom(length);
            }
            if (disk != null) {
                disk.writeBytes(b, offset, length);
            } else {
                memory.writeBytes(b, offset, length);
            }
            pointer += length;
        }

        @Override
        public long getFilePointer() {
            return pointer;
        }

        @Override
        public long getChecksum() {
            return checksum.getValue();
        }

        @Override
        public void close() throws IOException {
            if (closedOutput) {
                return;
            }
            closedOutput = true;
            if (disk != null) {
                disk.close();
                tier.countDisk(purpose, pointer);
                return;
            }
            // Charged what it occupies: a file smaller than one block is copied to an array of its own length, so the
            // many tiny files a flush writes do not each hold a whole block; a bigger one wastes less than its last block.
            final List<ByteBuffer> buffers;
            final long occupies;
            if (pointer < SMALL_FILE) {
                buffers = List.of(ByteBuffer.wrap(memory.toArrayCopy()).asReadOnlyBuffer());
                occupies = pointer;
            } else {
                buffers = memory.toBufferList();
                occupies = Math.max(pointer, memory.ramBytesUsed());
            }
            memory = null;
            synchronized (TieredSegmentDirectory.this) {
                writing.remove(name, this);
                if (closed || deletedWhileOpen) {
                    tier.release(reserved);
                    return;
                }
                if (occupies > reserved) {
                    tier.charge(occupies - reserved);
                } else {
                    tier.release(reserved - occupies);
                }
                final RamFile file = new RamFile(TieredSegmentDirectory.this, name, purpose, buffers, pointer, occupies);
                held.put(name, file);
                tier.held(file);
            }
            tier.evictIfHigh();
            tier.countMemory(purpose, pointer);
        }
    }

    /** A file written straight to disk, counted when it closes. */
    private final class CountingOutput extends IndexOutput {
        private final IndexOutput out;
        private final LocalSegmentTier.Purpose purpose;
        private boolean counted;

        CountingOutput(IndexOutput out, LocalSegmentTier.Purpose purpose) {
            super(out.toString(), out.getName());
            this.out = out;
            this.purpose = purpose;
        }

        @Override
        public void writeByte(byte b) throws IOException {
            out.writeByte(b);
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            out.writeBytes(b, offset, length);
        }

        @Override
        public long getFilePointer() {
            return out.getFilePointer();
        }

        @Override
        public long getChecksum() throws IOException {
            return out.getChecksum();
        }

        @Override
        public void close() throws IOException {
            try {
                out.close();
            } finally {
                if (counted == false) {
                    counted = true;
                    tier.countDisk(purpose, out.getFilePointer());
                }
            }
        }
    }
}
