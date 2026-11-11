/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The format of one blob of a node's write log: the records of every shard the node wrote in one window, and an
 * index that says where each shard's records are.
 *
 * <p><b>Why one blob holds many shards.</b> A log per shard costs a PUT per write per shard, and a node holding
 * thousands of slowly written shards pays roughly one PUT per write. One log per node pays one PUT per window,
 * however many shards were written in it.
 *
 * <p><b>Layout.</b> A fixed prefix (magic, format version, header length), then the header, then the body. The body is
 * one slice per (shard, term), each in the newline-delimited {@link WalRecord} encoding the per-shard log uses, so a
 * slice parses with the same code. The header lists this blob's slices with their offsets into the body, and carries a
 * <em>running index</em>: for every shard with records the node has not yet published, the ordinals of this epoch that
 * hold them. The last blob before a fence therefore says where every unpublished record of every shard is, and a
 * successor reads one header and then only its own slices.
 *
 * <p>A fence is a blob of its own kind, put by a successor in the next free slot of an epoch so that the writer can
 * append nothing after it. It carries a different magic, so a reader cannot take it for records.
 */
public final class NodeLogBlob {

    /** Starts every blob of records. */
    static final int RECORDS_MAGIC = 0x4E4C4752;
    /** Starts every fence. */
    static final int FENCE_MAGIC = 0x4E4C4746;
    /** The format this class writes, and the only one it reads. */
    static final byte VERSION = 2;
    /** Magic, version and header length. */
    static final int PREFIX_BYTES = Integer.BYTES + 1 + Integer.BYTES;

    private NodeLogBlob() {}

    /**
     * One shard of one index incarnation.
     *
     * <p>Identified by the uuid and shard number alone. The index's name travels with it, so a reader holding only a
     * dead node's log can find the shard's head and commit, which are kept by name; two keys that differ only in a
     * missing name are the same shard.
     *
     * @param indexUuid the index's uuid, so a recreated index under the same name is a different shard
     * @param shard the shard number
     * @param indexName the index's name, or null where it is not known
     */
    public record ShardKey(String indexUuid, int shard, String indexName) implements Comparable<ShardKey> {
        /** Validates. */
        public ShardKey {
            Objects.requireNonNull(indexUuid, "indexUuid");
            if (shard < 0) {
                throw new IllegalArgumentException("shard must not be negative, got " + shard);
            }
        }

        /**
         * Creates a key whose index name is not known.
         *
         * @param indexUuid the index's uuid
         * @param shard the shard number
         */
        public ShardKey(String indexUuid, int shard) {
            this(indexUuid, shard, null);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ShardKey key && indexUuid.equals(key.indexUuid) && shard == key.shard;
        }

        @Override
        public int hashCode() {
            return 31 * indexUuid.hashCode() + shard;
        }

        @Override
        public int compareTo(ShardKey other) {
            final int byIndex = indexUuid.compareTo(other.indexUuid);
            return byIndex != 0 ? byIndex : Integer.compare(shard, other.shard);
        }

        @Override
        public String toString() {
            return (indexName == null ? "" : indexName + "/") + indexUuid + "[" + shard + "]";
        }
    }

    /**
     * An inclusive run of ordinals.
     *
     * @param from the first ordinal
     * @param to the last ordinal, at least {@code from}
     */
    public record Range(long from, long to) {
        /** Validates. */
        public Range {
            if (from < 0 || to < from) {
                throw new IllegalArgumentException("not a range of ordinals: [" + from + ", " + to + "]");
            }
        }
    }

    /**
     * The records one shard wrote at one term in one window.
     *
     * @param shard the shard
     * @param term the writer's term for the shard
     * @param records the records, in the order they must replay
     */
    public record ShardBatch(ShardKey shard, long term, List<WalRecord> records) {
        /** Validates and copies. */
        public ShardBatch {
            Objects.requireNonNull(shard, "shard");
            records = List.copyOf(records);
        }
    }

    /**
     * Where one shard's records are in a blob.
     *
     * @param shard the shard
     * @param term the writer's term for the shard
     * @param offset where the slice starts, counted from the start of the body
     * @param length the slice's length in bytes
     * @param records how many records it holds
     * @param maxSeqNo the highest sequence number among them, or {@link SequenceNumbers#UNASSIGNED_SEQ_NO} if none carries one
     */
    public record Slice(ShardKey shard, long term, long offset, int length, int records, long maxSeqNo) {
    }

    /** What a slot of an epoch holds: records, or a fence. */
    public sealed interface Entry permits Header, Fence {}

    /**
     * The header of a blob of records.
     *
     * @param nodeId the node that wrote it
     * @param epoch the epoch it belongs to
     * @param ordinal its ordinal within the epoch
     * @param trimPoint the lowest ordinal the node still needed when it wrote this blob; everything below was deletable
     * @param slices this blob's slices, in body order
     * @param index for every shard with unpublished records, the ordinals of this epoch holding them, this blob included
     * @param bodyOffset where the body starts, counted from the start of the blob
     */
    public record Header(String nodeId, long epoch, long ordinal, long trimPoint, List<Slice> slices, Map<ShardKey, List<Range>> index,
        long bodyOffset) implements Entry {
        /**
         * Returns this blob's slices for one shard, in body order.
         *
         * @param shard the shard
         * @return its slices; empty if it wrote nothing in this blob
         */
        public List<Slice> slicesOf(ShardKey shard) {
            final List<Slice> out = new ArrayList<>();
            for (Slice slice : slices) {
                if (slice.shard().equals(shard)) {
                    out.add(slice);
                }
            }
            return out;
        }

        /**
         * Returns the ordinals of this epoch, up to this blob, that hold records of one shard.
         *
         * @param shard the shard
         * @return the ranges, ascending; empty if the shard has nothing unpublished
         */
        public List<Range> ordinalsOf(ShardKey shard) {
            return index.getOrDefault(shard, List.of());
        }
    }

    /**
     * A fence: the slot a successor took so the writer can append nothing after it.
     *
     * @param fencedBy the node that put it there
     * @param term the term that node had just won, for diagnosis only
     */
    public record Fence(String fencedBy, long term) implements Entry {
    }

    /** An encoded blob of records and the header that describes it. */
    public record Encoded(byte[] bytes, Header header) {
    }

    /**
     * Encodes one window's records.
     *
     * <p>The running index given is the one the node held before this window; this blob's own ordinal is added to
     * every shard it holds a slice of, and every run below the trim point is dropped, so the header written always
     * describes the log from the trim point to this blob.
     *
     * @param nodeId the writing node
     * @param epoch the epoch
     * @param ordinal this blob's ordinal
     * @param trimPoint the lowest ordinal the node still needs
     * @param batches the window's records, one batch per (shard, term), in the order they must replay
     * @param priorIndex the running index before this window
     * @return the bytes and their header
     * @throws IOException if a record cannot be serialized
     */
    public static Encoded encode(
        String nodeId,
        long epoch,
        long ordinal,
        long trimPoint,
        List<ShardBatch> batches,
        Map<ShardKey, List<Range>> priorIndex
    ) throws IOException {
        Objects.requireNonNull(nodeId, "nodeId");
        if (trimPoint > ordinal) {
            throw new IllegalArgumentException("trim point " + trimPoint + " is past the blob it is written in, " + ordinal);
        }
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        final List<Slice> slices = new ArrayList<>();
        final Map<ShardKey, List<Range>> index = new TreeMap<>();
        for (Map.Entry<ShardKey, List<Range>> entry : priorIndex.entrySet()) {
            final List<Range> kept = trimBelow(entry.getValue(), trimPoint);
            if (kept.isEmpty() == false) {
                index.put(entry.getKey(), kept);
            }
        }
        for (ShardBatch batch : batches) {
            if (batch.records().isEmpty()) {
                continue;
            }
            final long offset = body.size();
            long maxSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            boolean first = true;
            for (WalRecord record : batch.records()) {
                if (first == false) {
                    body.write('\n');
                }
                first = false;
                body.write(BytesReference.toBytes(record.toBytes()));
                maxSeqNo = Math.max(maxSeqNo, record.seqNo());
            }
            final long length = body.size() - offset;
            if (length > Integer.MAX_VALUE) {
                throw new IOException("one shard's records in a window exceed " + Integer.MAX_VALUE + " bytes");
            }
            slices.add(new Slice(batch.shard(), batch.term(), offset, (int) length, batch.records().size(), maxSeqNo));
            index.put(batch.shard(), withOrdinal(index.getOrDefault(batch.shard(), List.of()), ordinal));
        }

        final ByteArrayOutputStream headerBytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(headerBytes)) {
            out.writeUTF(nodeId);
            out.writeLong(epoch);
            out.writeLong(ordinal);
            out.writeLong(trimPoint);
            // Each index named once, however many of its shards the blob holds or indexes.
            final Map<String, String> names = new LinkedHashMap<>();
            for (Slice slice : slices) {
                nameOnce(names, slice.shard());
            }
            for (ShardKey shard : index.keySet()) {
                nameOnce(names, shard);
            }
            final Map<String, Integer> positions = new HashMap<>();
            out.writeInt(names.size());
            for (Map.Entry<String, String> name : names.entrySet()) {
                positions.put(name.getKey(), positions.size());
                out.writeUTF(name.getKey());
                out.writeUTF(name.getValue() == null ? "" : name.getValue());
            }
            out.writeInt(slices.size());
            for (Slice slice : slices) {
                writeShard(out, positions, slice.shard());
                out.writeLong(slice.term());
                out.writeLong(slice.offset());
                out.writeInt(slice.length());
                out.writeInt(slice.records());
                out.writeLong(slice.maxSeqNo());
            }
            out.writeInt(index.size());
            for (Map.Entry<ShardKey, List<Range>> entry : index.entrySet()) {
                writeShard(out, positions, entry.getKey());
                out.writeInt(entry.getValue().size());
                for (Range range : entry.getValue()) {
                    out.writeLong(range.from());
                    out.writeLong(range.to());
                }
            }
        }
        final ByteArrayOutputStream blob = new ByteArrayOutputStream(PREFIX_BYTES + headerBytes.size() + body.size());
        try (DataOutputStream out = new DataOutputStream(blob)) {
            out.writeInt(RECORDS_MAGIC);
            out.writeByte(VERSION);
            out.writeInt(headerBytes.size());
            headerBytes.writeTo(out);
            body.writeTo(out);
        }
        final Header header = new Header(
            nodeId,
            epoch,
            ordinal,
            trimPoint,
            List.copyOf(slices),
            Collections.unmodifiableMap(new LinkedHashMap<>(index)),
            PREFIX_BYTES + headerBytes.size()
        );
        return new Encoded(blob.toByteArray(), header);
    }

    /**
     * Encodes a fence.
     *
     * @param fencedBy the node putting it
     * @param term the term that node has just won
     * @return the bytes
     */
    public static byte[] encodeFence(String fencedBy, long term) {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(FENCE_MAGIC);
            out.writeByte(VERSION);
            out.writeUTF(fencedBy);
            out.writeLong(term);
        } catch (IOException e) {
            throw new AssertionError("writing to memory cannot fail", e);
        }
        return bytes.toByteArray();
    }

    /**
     * Returns how many leading bytes of a blob hold its prefix and header, or {@code -1} if more bytes are needed to
     * tell. A fence is complete in whatever its first bytes are.
     *
     * @param leading the blob's leading bytes, at least {@link #PREFIX_BYTES} of them unless the blob is shorter
     * @return the length of prefix and header, or {@code -1}
     * @throws IOException if the bytes are not a node-log blob
     */
    static long headerEnd(byte[] leading) throws IOException {
        if (leading.length < PREFIX_BYTES) {
            if (leading.length >= Integer.BYTES && magicOf(leading) == FENCE_MAGIC) {
                return leading.length;
            }
            return -1L;
        }
        final int magic = magicOf(leading);
        if (magic == FENCE_MAGIC) {
            return leading.length;
        }
        if (magic != RECORDS_MAGIC) {
            throw new IOException("not a node-log blob: magic " + Integer.toHexString(magic));
        }
        final int headerLength = ((leading[5] & 0xFF) << 24) | ((leading[6] & 0xFF) << 16) | ((leading[7] & 0xFF) << 8) | (leading[8]
            & 0xFF);
        if (headerLength < 0) {
            throw new IOException("corrupt node-log blob: negative header length");
        }
        return (long) PREFIX_BYTES + headerLength;
    }

    /**
     * Decodes what a slot holds from the blob's leading bytes, which must cover the prefix and header.
     *
     * @param leading the leading bytes
     * @return the header, or the fence
     * @throws IOException if the bytes are malformed or of another format version
     */
    public static Entry decode(byte[] leading) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(leading))) {
            final int magic = in.readInt();
            final byte version = in.readByte();
            if (version != VERSION) {
                throw new IOException("node-log blob of format version " + version + "; this node reads " + VERSION);
            }
            if (magic == FENCE_MAGIC) {
                return new Fence(in.readUTF(), in.readLong());
            }
            if (magic != RECORDS_MAGIC) {
                throw new IOException("not a node-log blob: magic " + Integer.toHexString(magic));
            }
            final int headerLength = in.readInt();
            if (leading.length < PREFIX_BYTES + (long) headerLength) {
                throw new IOException("node-log header is " + headerLength + " bytes; only " + (leading.length - PREFIX_BYTES) + " given");
            }
            final String nodeId = in.readUTF();
            final long epoch = in.readLong();
            final long ordinal = in.readLong();
            final long trimPoint = in.readLong();
            final int nameCount = in.readInt();
            final List<String[]> names = new ArrayList<>(nameCount);
            for (int i = 0; i < nameCount; i++) {
                final String uuid = in.readUTF();
                final String name = in.readUTF();
                names.add(new String[] { uuid, name.isEmpty() ? null : name });
            }
            final int sliceCount = in.readInt();
            final List<Slice> slices = new ArrayList<>(sliceCount);
            for (int i = 0; i < sliceCount; i++) {
                slices.add(new Slice(readShard(in, names), in.readLong(), in.readLong(), in.readInt(), in.readInt(), in.readLong()));
            }
            final int indexCount = in.readInt();
            final Map<ShardKey, List<Range>> index = new LinkedHashMap<>();
            for (int i = 0; i < indexCount; i++) {
                final ShardKey shard = readShard(in, names);
                final int ranges = in.readInt();
                final List<Range> runs = new ArrayList<>(ranges);
                for (int r = 0; r < ranges; r++) {
                    runs.add(new Range(in.readLong(), in.readLong()));
                }
                index.put(shard, List.copyOf(runs));
            }
            return new Header(
                nodeId,
                epoch,
                ordinal,
                trimPoint,
                List.copyOf(slices),
                Collections.unmodifiableMap(index),
                PREFIX_BYTES + (long) headerLength
            );
        }
    }

    /**
     * Parses one slice's bytes into its records.
     *
     * @param slice the slice's bytes, exactly
     * @return the records, in replay order
     * @throws IOException if a record is malformed
     */
    public static List<WalRecord> parseSlice(byte[] slice) throws IOException {
        final List<WalRecord> records = new ArrayList<>();
        int start = 0;
        for (int i = 0; i <= slice.length; i++) {
            if (i != slice.length && slice[i] != '\n') {
                continue;
            }
            if (i > start) {
                records.add(WalRecord.fromStream(new ByteArrayInputStream(slice, start, i - start)));
            }
            start = i + 1;
        }
        return records;
    }

    /**
     * Adds an ordinal to a shard's runs, which must all be below it.
     *
     * @param runs ascending runs
     * @param ordinal an ordinal above every run
     * @return the runs with the ordinal added, merged into the last run when adjacent
     */
    public static List<Range> withOrdinal(List<Range> runs, long ordinal) {
        final List<Range> out = new ArrayList<>(runs);
        if (out.isEmpty() == false) {
            final Range last = out.get(out.size() - 1);
            if (ordinal <= last.to()) {
                throw new IllegalArgumentException("ordinal " + ordinal + " is not past the shard's last run " + last);
            }
            if (ordinal == last.to() + 1) {
                out.set(out.size() - 1, new Range(last.from(), ordinal));
                return List.copyOf(out);
            }
        }
        out.add(new Range(ordinal, ordinal));
        return List.copyOf(out);
    }

    /**
     * Drops the part of a shard's runs below an ordinal.
     *
     * @param runs ascending runs
     * @param trimPoint the lowest ordinal to keep
     * @return the runs at or above it
     */
    public static List<Range> trimBelow(List<Range> runs, long trimPoint) {
        final List<Range> out = new ArrayList<>(runs.size());
        for (Range run : runs) {
            if (run.to() < trimPoint) {
                continue;
            }
            out.add(run.from() >= trimPoint ? run : new Range(trimPoint, run.to()));
        }
        return List.copyOf(out);
    }

    private static int magicOf(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
    }

    private static void nameOnce(Map<String, String> names, ShardKey shard) {
        if (shard.indexName() != null || names.containsKey(shard.indexUuid()) == false) {
            names.put(shard.indexUuid(), shard.indexName() != null ? shard.indexName() : names.get(shard.indexUuid()));
        }
    }

    private static void writeShard(DataOutputStream out, Map<String, Integer> positions, ShardKey shard) throws IOException {
        out.writeInt(positions.get(shard.indexUuid()));
        out.writeInt(shard.shard());
    }

    private static ShardKey readShard(DataInputStream in, List<String[]> names) throws IOException {
        final int position = in.readInt();
        if (position < 0 || position >= names.size()) {
            throw new IOException("corrupt node-log header: index " + position + " of " + names.size() + " named");
        }
        final String[] name = names.get(position);
        return new ShardKey(name[0], in.readInt(), name[1]);
    }
}
