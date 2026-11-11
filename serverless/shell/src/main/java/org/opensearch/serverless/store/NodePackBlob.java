/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The format of a node pack: one immutable blob holding the segment blobs that several shards, of any indices, published
 * on one node in the same slot.
 *
 * <p>A pack is a prefix, a header, then the blocks back to back. Each block is one segment blob exactly as a shard would
 * have written it on its own (its own {@code SEG1} header, then its files), so a shard's manifest can point at a file's
 * range of the pack and read it with no knowledge of the pack around it. Different indices, with different mappings,
 * share bytes here and nothing else: every block belongs to one shard of one index.
 *
 * <p>The header lists every block's shard, name, offset and length. Nothing on the read path needs it, since a
 * manifest carries each file's offset; it is what collection reads to learn which shards to ask whether the pack is
 * still referenced, so no listing is ever needed to find a pack's owners.
 *
 * <pre>
 * prefix:  magic "NPK1" (4) | version (1) | header length (4)
 * header:  node (UTF) | epoch (8) | ordinal (8) | block count (4)
 *          per block: index uuid (UTF) | index name (UTF) | shard (4) | name (UTF) | offset (8) | length (8)
 * body:    the blocks, in header order, the first starting where the header ends
 * </pre>
 */
public final class NodePackBlob {

    /** {@code NPK1}. */
    static final int MAGIC = 0x4E504B31;
    /** The format version this node writes. */
    static final byte VERSION = 2;
    /** The first version, whose blocks name their index by uuid alone; still read. */
    static final byte VERSION_UUID_ONLY = 1;
    /** Magic, version and header length. */
    static final int PREFIX_BYTES = 4 + 1 + 4;

    private NodePackBlob() {}

    /**
     * One block of a pack: one shard's segment blob.
     *
     * @param indexUuid the index the shard belongs to
     * @param shard the shard number
     * @param name the segment blob's name, as the shard would have named it on its own ({@code _5.seg})
     * @param offset where the block starts in the pack
     * @param length the block's length
     * @param indexName the index's name, which with the uuid locates the shard's manifest; empty in a pack of the first
     *     version
     */
    public record Block(String indexUuid, int shard, String name, long offset, long length, String indexName) {

        /**
         * A block whose index name is not recorded.
         *
         * @param indexUuid the index
         * @param shard the shard
         * @param name the block's name
         * @param offset where it starts
         * @param length how long it is
         */
        public Block(String indexUuid, int shard, String name, long offset, long length) {
            this(indexUuid, shard, name, offset, length, "");
        }

        /**
         * Validates a block.
         *
         * @param indexUuid the index
         * @param shard the shard
         * @param name the block's name
         * @param offset where it starts
         * @param length how long it is
         * @param indexName the index's name, or empty
         */
        public Block {
            Objects.requireNonNull(indexUuid, "indexUuid");
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(indexName, "indexName");
            if (indexUuid.isEmpty() || name.isEmpty() || shard < 0 || offset < 0L || length < 0L) {
                throw new IllegalArgumentException(
                    "invalid pack block [" + indexUuid + "][" + shard + "][" + name + "] at [" + offset + ":" + length + "]"
                );
            }
        }
    }

    /**
     * What a block will hold before the pack is written: everything but its offset.
     *
     * @param indexUuid the index
     * @param shard the shard
     * @param name the segment blob's name
     * @param length its length
     * @param indexName the index's name
     */
    public record Planned(String indexUuid, int shard, String name, long length, String indexName) {
    }

    /**
     * A shard a pack holds blocks of.
     *
     * @param indexUuid the index
     * @param shard the shard number
     * @param indexName the index's name, or empty in a pack of the first version
     */
    public record ShardKey(String indexUuid, int shard, String indexName) {

        /**
         * A shard named by uuid alone.
         *
         * @param indexUuid the index
         * @param shard the shard number
         */
        public ShardKey(String indexUuid, int shard) {
            this(indexUuid, shard, "");
        }
    }

    /**
     * A decoded header.
     *
     * @param nodeId the node that wrote the pack
     * @param epoch the epoch it was written in
     * @param ordinal its slot in that epoch
     * @param blocks every block, in body order
     * @param bodyStart where the first block starts, which is also the length of prefix and header
     */
    public record Header(String nodeId, long epoch, long ordinal, List<Block> blocks, long bodyStart) {

        /**
         * Every shard with a block in the pack, in first-block order: whom collection asks.
         *
         * @return the shards
         */
        public Set<ShardKey> shards() {
            final Set<ShardKey> shards = new LinkedHashSet<>();
            for (Block block : blocks) {
                shards.add(new ShardKey(block.indexUuid(), block.shard(), block.indexName()));
            }
            return shards;
        }

        /**
         * Finds one shard's block by name.
         *
         * @param indexUuid the index
         * @param shard the shard
         * @param name the block's name
         * @return the block, if the pack holds it
         */
        public Optional<Block> block(String indexUuid, int shard, String name) {
            for (Block block : blocks) {
                if (block.shard() == shard && block.indexUuid().equals(indexUuid) && block.name().equals(name)) {
                    return Optional.of(block);
                }
            }
            return Optional.empty();
        }

        /**
         * The pack's whole length: prefix, header and every block.
         *
         * @return its length in bytes
         */
        public long length() {
            return blocks.isEmpty() ? bodyStart : blocks.get(blocks.size() - 1).offset() + blocks.get(blocks.size() - 1).length();
        }
    }

    /**
     * Lays out a pack: works out each block's offset and encodes the prefix and header that go before them.
     *
     * @param nodeId the writing node
     * @param epoch its pack epoch
     * @param ordinal the pack's slot
     * @param planned the blocks, in the order they will be written
     * @return the header, whose blocks carry their offsets, and its encoded bytes
     * @throws IOException if encoding fails
     */
    public static Encoded encode(String nodeId, long epoch, long ordinal, List<Planned> planned) throws IOException {
        Objects.requireNonNull(nodeId, "nodeId");
        if (planned.isEmpty()) {
            throw new IllegalArgumentException("a pack holds at least one block");
        }
        // Offsets count the header, whose length depends on the offsets only through their count, not their
        // values (they are fixed-width), so one pass with placeholder offsets gives the length.
        final byte[] sizing = headerBytes(nodeId, epoch, ordinal, placeholders(planned));
        final long bodyStart = PREFIX_BYTES + (long) sizing.length;
        final List<Block> blocks = new ArrayList<>(planned.size());
        long offset = bodyStart;
        for (Planned block : planned) {
            blocks.add(new Block(block.indexUuid(), block.shard(), block.name(), offset, block.length(), block.indexName()));
            offset += block.length();
        }
        final byte[] header = headerBytes(nodeId, epoch, ordinal, blocks);
        assert header.length == sizing.length;
        final ByteArrayOutputStream out = new ByteArrayOutputStream(PREFIX_BYTES + header.length);
        try (DataOutputStream data = new DataOutputStream(out)) {
            data.writeInt(MAGIC);
            data.writeByte(VERSION);
            data.writeInt(header.length);
            data.write(header);
        }
        return new Encoded(new Header(nodeId, epoch, ordinal, List.copyOf(blocks), bodyStart), out.toByteArray());
    }

    /**
     * A laid-out pack's header and the bytes that start the pack.
     *
     * @param header the decoded form, with offsets
     * @param bytes the prefix and header as written
     */
    public record Encoded(Header header, byte[] bytes) {
    }

    private static List<Block> placeholders(List<Planned> planned) {
        final List<Block> blocks = new ArrayList<>(planned.size());
        for (Planned block : planned) {
            blocks.add(new Block(block.indexUuid(), block.shard(), block.name(), 0L, block.length(), block.indexName()));
        }
        return blocks;
    }

    private static byte[] headerBytes(String nodeId, long epoch, long ordinal, List<Block> blocks) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataOutputStream data = new DataOutputStream(out)) {
            data.writeUTF(nodeId);
            data.writeLong(epoch);
            data.writeLong(ordinal);
            data.writeInt(blocks.size());
            for (Block block : blocks) {
                data.writeUTF(block.indexUuid());
                data.writeUTF(block.indexName());
                data.writeInt(block.shard());
                data.writeUTF(block.name());
                data.writeLong(block.offset());
                data.writeLong(block.length());
            }
        }
        return out.toByteArray();
    }

    /**
     * Where the header ends, from a pack's leading bytes.
     *
     * @param leading the first bytes of the pack
     * @return the length of prefix and header, or -1 if the bytes are too few to say
     */
    public static long headerEnd(byte[] leading) {
        if (leading.length < PREFIX_BYTES) {
            return -1L;
        }
        final int headerLength = ((leading[5] & 0xFF) << 24) | ((leading[6] & 0xFF) << 16) | ((leading[7] & 0xFF) << 8) | (leading[8]
            & 0xFF);
        return PREFIX_BYTES + (long) headerLength;
    }

    /**
     * Decodes a header from a pack's leading bytes, which must cover the prefix and header.
     *
     * @param leading the leading bytes
     * @return the header
     * @throws IOException if the bytes are not a pack, of another version, or too few
     */
    public static Header decode(byte[] leading) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(leading))) {
            final int magic = in.readInt();
            if (magic != MAGIC) {
                throw new IOException("not a node pack: magic " + Integer.toHexString(magic));
            }
            final byte version = in.readByte();
            if (version != VERSION && version != VERSION_UUID_ONLY) {
                throw new IOException("node pack of format version " + version + "; this node reads up to " + VERSION);
            }
            final int headerLength = in.readInt();
            if (headerLength < 0 || leading.length < PREFIX_BYTES + (long) headerLength) {
                throw new IOException("node pack header is " + headerLength + " bytes; only " + (leading.length - PREFIX_BYTES) + " given");
            }
            final String nodeId = in.readUTF();
            final long epoch = in.readLong();
            final long ordinal = in.readLong();
            final int count = in.readInt();
            if (count < 0) {
                throw new IOException("node pack header names " + count + " blocks");
            }
            final List<Block> blocks = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                try {
                    final String indexUuid = in.readUTF();
                    final String indexName = version == VERSION_UUID_ONLY ? "" : in.readUTF();
                    blocks.add(new Block(indexUuid, in.readInt(), in.readUTF(), in.readLong(), in.readLong(), indexName));
                } catch (IllegalArgumentException e) {
                    throw new IOException("malformed node pack header", e);
                }
            }
            return new Header(nodeId, epoch, ordinal, List.copyOf(blocks), PREFIX_BYTES + (long) headerLength);
        }
    }
}
