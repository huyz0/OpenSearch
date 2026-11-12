/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * One node's write log in the object store: the blobs of each epoch, read and written by name.
 *
 * <p><b>Every name is computed.</b> A blob lives at {@code nodelog/<node>/e=<epoch>/<ordinal>}, both numbers
 * zero-padded to twenty digits, and ordinals within an epoch start at zero and have no gaps. A writer knows the next
 * name, a successor learns the open epochs from the node's lease, and cleanup deletes by name, so nothing in normal
 * running lists the store. The one search there is, for the first free slot when fencing, probes by name.
 *
 * <p><b>Appends never overwrite.</b> Each append is create-if-absent, so a fence in a slot stops the writer there. The
 * writer appends one slot at a time and never skips one, which is what makes "every blob below the fence" exactly the
 * set of blobs the writer landed: see {@link #fence}.
 *
 * <p>This class holds no state beyond where the log is; what the next ordinal is, which epochs are open and how far the
 * log may be trimmed belong to the writer that owns it.
 */
public final class NodeLogStore {

    /** How many leading bytes a header read fetches first; a longer header costs a second read. */
    static final int HEADER_PROBE_BYTES = 64 * 1024;
    /** The most names one delete request carries, which is the most S3 takes. */
    static final int DELETE_BATCH = 1000;
    /** How many slots a fence tries before giving up on a writer that keeps taking the next one. */
    static final int FENCE_ATTEMPTS = 1000;

    private final BlobStore blobStore;
    private final BlobPath nodeBase;
    private final String nodeId;

    /**
     * Creates the log of one node.
     *
     * @param blobStore the object store
     * @param base the cluster's base path
     * @param nodeId the node whose log this is
     */
    public NodeLogStore(BlobStore blobStore, BlobPath base, String nodeId) {
        this.blobStore = Objects.requireNonNull(blobStore, "blobStore");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.nodeBase = base.add("nodelog").add(nodeId);
    }

    /**
     * Returns the node whose log this is.
     *
     * @return the node id
     */
    public String nodeId() {
        return nodeId;
    }

    /**
     * Returns a slot's blob name.
     *
     * @param ordinal the slot
     * @return the zero-padded name
     */
    public static String name(long ordinal) {
        if (ordinal < 0) {
            throw new IllegalArgumentException("ordinal must not be negative, got " + ordinal);
        }
        return String.format(Locale.ROOT, "%020d", ordinal);
    }

    /**
     * Returns where an epoch's blobs live.
     *
     * @param epoch the epoch
     * @return its path
     */
    public BlobPath epochPath(long epoch) {
        if (epoch < 0) {
            throw new IllegalArgumentException("epoch must not be negative, got " + epoch);
        }
        return nodeBase.add("e=" + String.format(Locale.ROOT, "%020d", epoch));
    }

    private BlobContainer container(long epoch) {
        return blobStore.blobContainer(epochPath(epoch));
    }

    /**
     * Writes a blob into a slot, refusing if the slot is taken.
     *
     * @param epoch the epoch
     * @param ordinal the slot, which must be the one after the writer's last landed blob
     * @param blob the encoded blob
     * @throws FileAlreadyExistsException if the slot is taken: by a fence, so the epoch is closed to this writer
     * @throws IOException if the write fails, or its outcome is unknown; the writer must not use the epoch again
     */
    public void append(long epoch, long ordinal, byte[] blob) throws IOException {
        container(epoch).writeBlob(name(ordinal), new ByteArrayInputStream(blob), blob.length, true);
    }

    /**
     * Closes an epoch to its writer: puts a fence where the writer must stop, and returns the lowest fence in the epoch.
     *
     * <p>{@code fromOrdinal} must not be past the first free slot. The landed hint the node's lease records is always
     * safe: every slot below the first free one is taken, so the search is a run of existence checks that doubles its
     * step until it finds a free slot and then halves back to the first one. With a hint at most a few seconds old that
     * is a handful of checks, each priced as a read rather than a listing.
     *
     * <p><b>A writer appending back to back.</b> Aiming at the first free slot races the writer's next PUT, which is
     * headed for that same slot, and the writer is always a request ahead. So after a lost race the fence aims ahead of
     * it, by a gap that doubles. The writer never skips a slot, so it must reach that fence and stop there. The fence
     * then fills the gap from the bottom, one slot at a time: the writer can take at most the slots below the barrier,
     * so this ends, and the first fence it lands in the gap is where the writer stopped. Every slot below that one is
     * the writer's, which is what {@link #lastHeaderBelow} relies on.
     *
     * <p>A fence another successor has already placed is shared rather than added to.
     *
     * @param epoch the epoch to close
     * @param fromOrdinal a slot at or before the first free one
     * @param fencedBy the node putting the fence
     * @param term the term that node has just won, recorded for diagnosis
     * @return the lowest fence's ordinal: every slot below it holds one of the writer's blobs
     * @throws IOException if the store fails, or the writer kept taking the next slot
     */
    public long fence(long epoch, long fromOrdinal, String fencedBy, long term) throws IOException {
        final BlobContainer container = container(epoch);
        final byte[] fence = NodeLogBlob.encodeFence(fencedBy, term);
        long slot = firstFree(container, fromOrdinal);
        long gap = 0;
        for (int attempt = 0; attempt < FENCE_ATTEMPTS; attempt++) {
            // A fence just below the first free slot closed the epoch already: another successor's, which this one shares.
            if (slot > 0 && readEntry(epoch, slot - 1).orElse(null) instanceof NodeLogBlob.Fence) {
                return slot - 1;
            }
            final long target = slot + gap;
            if (tryFence(container, target, fence)) {
                return gap == 0 ? target : fillBelow(epoch, container, slot, target, fence);
            }
            // The writer, or another successor's fence, took it first.
            if (readEntry(epoch, target).orElse(null) instanceof NodeLogBlob.Fence) {
                return gap == 0 ? target : fillBelow(epoch, container, slot, target, fence);
            }
            slot = firstFree(container, target + 1);
            gap = gap == 0 ? 1 : Math.min(gap << 1, MAX_FENCE_GAP);
        }
        throw new IOException("could not fence epoch " + epoch + " of " + nodeId + ": its writer kept taking the next slot");
    }

    /** The widest gap a fence leaves ahead of a writer it keeps losing to. */
    static final long MAX_FENCE_GAP = 1024;

    private static boolean tryFence(BlobContainer container, long slot, byte[] fence) throws IOException {
        try {
            container.writeBlob(name(slot), new ByteArrayInputStream(fence), fence.length, true);
            return true;
        } catch (FileAlreadyExistsException taken) {
            return false;
        }
    }

    /**
     * With a barrier fence at {@code barrier}, fences the lowest free slot from {@code from} up, and returns the lowest
     * fence below or at the barrier: where the writer stopped.
     */
    private long fillBelow(long epoch, BlobContainer container, long from, long barrier, byte[] fence) throws IOException {
        for (long slot = from; slot < barrier; slot++) {
            if (tryFence(container, slot, fence)) {
                return slot;
            }
            if (readEntry(epoch, slot).orElse(null) instanceof NodeLogBlob.Fence) {
                return slot;
            }
        }
        return barrier;
    }

    /**
     * Returns an epoch's first free slot, searching from a slot every slot below which is taken.
     *
     * @param epoch the epoch
     * @param fromOrdinal a slot at or before the first free one
     * @return the first free slot
     * @throws IOException if an existence check fails
     */
    public long firstFree(long epoch, long fromOrdinal) throws IOException {
        return firstFree(container(epoch), fromOrdinal);
    }

    private static long firstFree(BlobContainer container, long fromOrdinal) throws IOException {
        if (container.blobExists(name(fromOrdinal)) == false) {
            return fromOrdinal;
        }
        // fromOrdinal is taken. Double until a free slot, then halve back: taken slots form a prefix.
        long taken = fromOrdinal;
        long step = 1;
        long free;
        while (true) {
            final long probe = taken + step;
            if (container.blobExists(name(probe)) == false) {
                free = probe;
                break;
            }
            taken = probe;
            step <<= 1;
        }
        while (free - taken > 1) {
            final long mid = taken + (free - taken) / 2;
            if (container.blobExists(name(mid))) {
                taken = mid;
            } else {
                free = mid;
            }
        }
        return free;
    }

    /**
     * Reads what a slot holds: a header of records, or a fence.
     *
     * @param epoch the epoch
     * @param ordinal the slot
     * @return the header or fence; empty if the slot is free
     * @throws IOException if the read fails or the blob is malformed
     */
    public Optional<NodeLogBlob.Entry> readEntry(long epoch, long ordinal) throws IOException {
        final BlobContainer container = container(epoch);
        final String name = name(ordinal);
        final byte[] leading;
        try {
            leading = readRange(container, name, 0, HEADER_PROBE_BYTES);
        } catch (NoSuchFileException missing) {
            return Optional.empty();
        }
        final long end = NodeLogBlob.headerEnd(leading);
        if (end < 0) {
            throw new IOException("node-log blob " + epochPath(epoch).buildAsString() + name + " is too short to hold a header");
        }
        if (end <= leading.length) {
            return Optional.of(NodeLogBlob.decode(leading));
        }
        if (end > Integer.MAX_VALUE) {
            throw new IOException("node-log header of " + end + " bytes is too large to read");
        }
        return Optional.of(NodeLogBlob.decode(readRange(container, name, 0, (int) end)));
    }

    /**
     * Returns the header of the last blob of records below a slot, skipping fences.
     *
     * <p>Its running index lists every shard with records the writer had not published, and where, so one header is
     * enough to find every slice in the epoch.
     *
     * @param epoch the epoch
     * @param below the slot to look below, typically a fence
     * @param floor the lowest slot worth reading, typically the trim point
     * @return the header; empty if no slot in {@code [floor, below)} holds records
     * @throws IOException if a read fails
     */
    public Optional<NodeLogBlob.Header> lastHeaderBelow(long epoch, long below, long floor) throws IOException {
        for (long ordinal = below - 1; ordinal >= Math.max(0L, floor); ordinal--) {
            final Optional<NodeLogBlob.Entry> entry = readEntry(epoch, ordinal);
            if (entry.isPresent() && entry.get() instanceof NodeLogBlob.Header header) {
                return Optional.of(header);
            }
        }
        return Optional.empty();
    }

    /**
     * Reads one slice of a blob.
     *
     * @param epoch the epoch
     * @param header the blob's header
     * @param slice one of its slices
     * @return the slice's records, in replay order
     * @throws IOException if the read fails or a record is malformed
     */
    public List<WalRecord> readSlice(long epoch, NodeLogBlob.Header header, NodeLogBlob.Slice slice) throws IOException {
        if (header.slices().contains(slice) == false) {
            throw new IllegalArgumentException("slice " + slice + " is not in blob " + header.ordinal());
        }
        final byte[] bytes = readRange(container(epoch), name(header.ordinal()), header.bodyOffset() + slice.offset(), slice.length());
        if (bytes.length != slice.length()) {
            throw new IOException("node-log slice of " + slice.length() + " bytes read as " + bytes.length);
        }
        return NodeLogBlob.parseSlice(bytes);
    }

    /**
     * Returns the whole body of a blob of records, every slice of it, in one read: for a reader that wants the slices of
     * many shards in the same blob.
     *
     * @param epoch the epoch
     * @param header the blob's header
     * @return the body's bytes, which {@link #parseSlice} reads a slice of
     * @throws IOException if the read fails or comes back short
     */
    public byte[] readBody(long epoch, NodeLogBlob.Header header) throws IOException {
        final long length = bodyLength(header);
        if (length > Integer.MAX_VALUE) {
            throw new IOException("node-log body of " + length + " bytes is too large to read");
        }
        final byte[] bytes = readRange(container(epoch), name(header.ordinal()), header.bodyOffset(), (int) length);
        if (bytes.length != length) {
            throw new IOException("node-log body of " + length + " bytes read as " + bytes.length);
        }
        return bytes;
    }

    /**
     * The records of one slice, out of a body {@link #readBody} read.
     *
     * @param body the body
     * @param slice the slice, one of the header's
     * @return its records
     * @throws IOException if they cannot be parsed
     */
    public static List<WalRecord> parseSlice(byte[] body, NodeLogBlob.Slice slice) throws IOException {
        if (slice.offset() < 0 || slice.offset() + slice.length() > body.length) {
            throw new IOException(
                "node-log slice at " + slice.offset() + " of " + slice.length() + " bytes is past a body of " + body.length
            );
        }
        return NodeLogBlob.parseSlice(java.util.Arrays.copyOfRange(body, (int) slice.offset(), (int) slice.offset() + slice.length()));
    }

    /** How long a blob's body is: up to the end of its last slice. */
    public static long bodyLength(NodeLogBlob.Header header) {
        long end = 0;
        for (NodeLogBlob.Slice slice : header.slices()) {
            end = Math.max(end, slice.offset() + slice.length());
        }
        return end;
    }

    /**
     * Deletes the blobs in a range of slots, including fences; slots already empty are skipped.
     *
     * @param epoch the epoch
     * @param fromInclusive the first slot
     * @param toExclusive the slot after the last
     * @throws IOException if a delete fails
     */
    public void deleteRange(long epoch, long fromInclusive, long toExclusive) throws IOException {
        if (toExclusive <= fromInclusive) {
            return;
        }
        final BlobContainer container = container(epoch);
        final List<String> batch = new ArrayList<>(DELETE_BATCH);
        for (long ordinal = fromInclusive; ordinal < toExclusive; ordinal++) {
            batch.add(name(ordinal));
            if (batch.size() == DELETE_BATCH) {
                container.deleteBlobsIgnoringIfNotExists(List.copyOf(batch));
                batch.clear();
            }
        }
        if (batch.isEmpty() == false) {
            container.deleteBlobsIgnoringIfNotExists(batch);
        }
    }

    private static byte[] readRange(BlobContainer container, String name, long position, int length) throws IOException {
        try (InputStream in = container.readBlob(name, position, length)) {
            return in.readNBytes(length);
        }
    }
}
