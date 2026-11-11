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
 * One node's packs in the object store: written, read and deleted by computed name.
 *
 * <p>A pack lives at {@code packs/<node>/e=<epoch>/<ordinal>}, both numbers zero-padded to twenty digits, beside the
 * node's write log rather than under any shard, because it holds blocks of many shards of many indices. A node starts a
 * new epoch each time it starts, so names never repeat across processes, and ordinals within an epoch count up from
 * zero, so whoever knows an epoch's range can name every pack in it without listing.
 *
 * <p>Packs are opt-in. With them off, every shard keeps its own blob per segment under its own term container, and
 * nothing here is used.
 *
 * <p>A pack is written once, with create-if-absent, and never changed. It becomes reachable only through a shard's
 * manifest swap, made after the pack has landed, so a crash between the two leaves an unreferenced pack for collection
 * and nothing half-written that any reader can see.
 */
public final class NodePackStore {

    /** The prefix every node's packs live under, beside {@code nodelog}. */
    public static final String ROOT = "packs";
    /** How many leading bytes a header read fetches first; a longer header costs a second read. */
    static final int HEADER_PROBE_BYTES = 64 * 1024;
    /** The most names one delete request carries, which is the most S3 takes. */
    static final int DELETE_BATCH = 1000;

    private final BlobStore blobStore;
    private final BlobPath base;
    private final String nodeId;

    /**
     * Creates the packs of one node.
     *
     * @param blobStore the object store
     * @param base the cluster's base path
     * @param nodeId the node whose packs these are
     */
    public NodePackStore(BlobStore blobStore, BlobPath base, String nodeId) {
        this.blobStore = Objects.requireNonNull(blobStore, "blobStore");
        this.base = Objects.requireNonNull(base, "base");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
    }

    /**
     * Returns the node whose packs these are.
     *
     * @return the node id
     */
    public String nodeId() {
        return nodeId;
    }

    /**
     * Returns a pack's blob name within its epoch.
     *
     * @param ordinal the pack's slot
     * @return the zero-padded name
     */
    public static String name(long ordinal) {
        if (ordinal < 0) {
            throw new IllegalArgumentException("ordinal must not be negative, got " + ordinal);
        }
        return String.format(Locale.ROOT, "%020d", ordinal);
    }

    /**
     * Returns an epoch's directory name.
     *
     * @param epoch the epoch
     * @return {@code e=<epoch>}, zero-padded
     */
    public static String epochDir(long epoch) {
        if (epoch < 0) {
            throw new IllegalArgumentException("epoch must not be negative, got " + epoch);
        }
        return "e=" + String.format(Locale.ROOT, "%020d", epoch);
    }

    /**
     * Returns where one node's epoch of packs lives, for any node: what a reader resolves a pack location against.
     *
     * @param base the cluster's base path
     * @param nodeId the node that wrote the packs
     * @param epoch the epoch
     * @return its path
     */
    public static BlobPath epochPath(BlobPath base, String nodeId, long epoch) {
        return base.add(ROOT).add(nodeId).add(epochDir(epoch));
    }

    private BlobContainer container(long epoch) {
        return blobStore.blobContainer(epochPath(base, nodeId, epoch));
    }

    /**
     * One block to write: a shard's segment blob, opened as many times as the upload needs.
     *
     * @param indexUuid the index
     * @param shard the shard
     * @param name the segment blob's name
     * @param length its exact length
     * @param source opens its bytes from the first, each time it is called
     * @param indexName the index's name, which collection needs to find the shard's manifest
     */
    public record Part(String indexUuid, int shard, String name, long length, NodePackInputStream.Source source, String indexName) {

        /**
         * A part whose index name is not recorded, so collection can never judge its pack unreferenced.
         *
         * @param indexUuid the index
         * @param shard the shard
         * @param name the block's name
         * @param length its length
         * @param source its bytes
         */
        public Part(String indexUuid, int shard, String name, long length, NodePackInputStream.Source source) {
            this(indexUuid, shard, name, length, source, "");
        }

        /**
         * Validates a part.
         *
         * @param indexUuid the index
         * @param shard the shard
         * @param name the block's name
         * @param length its length
         * @param source its bytes
         * @param indexName the index's name, or empty
         */
        public Part {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(indexName, "indexName");
            if (length < 0L) {
                throw new IllegalArgumentException("a pack part has a length, got " + length);
            }
        }
    }

    /**
     * Writes a pack into a slot, refusing if the slot is taken.
     *
     * @param epoch the writer's current epoch
     * @param ordinal the slot, which the writer has not used before
     * @param parts the blocks, in the order they go in the pack
     * @return the pack's header, whose blocks say where each part landed
     * @throws FileAlreadyExistsException if the slot is taken, which only a reused epoch can cause
     * @throws IOException if the write fails; the pack may or may not exist, and nothing points at it either way
     */
    public NodePackBlob.Header write(long epoch, long ordinal, List<Part> parts) throws IOException {
        final List<NodePackBlob.Planned> planned = new ArrayList<>(parts.size());
        for (Part part : parts) {
            planned.add(new NodePackBlob.Planned(part.indexUuid(), part.shard(), part.name(), part.length(), part.indexName()));
        }
        final NodePackBlob.Encoded encoded = NodePackBlob.encode(nodeId, epoch, ordinal, planned);
        final List<NodePackInputStream.Source> sources = new ArrayList<>(parts.size());
        final List<Long> lengths = new ArrayList<>(parts.size());
        for (Part part : parts) {
            sources.add(part.source());
            lengths.add(part.length());
        }
        try (NodePackInputStream stream = new NodePackInputStream(encoded.bytes(), sources, lengths)) {
            container(epoch).writeBlob(name(ordinal), stream, stream.length(), true);
        }
        return encoded.header();
    }

    /**
     * Reads a pack's header.
     *
     * @param epoch the epoch
     * @param ordinal the slot
     * @return the header; empty if there is no pack in the slot
     * @throws IOException if the read fails or the blob is not a pack
     */
    public Optional<NodePackBlob.Header> readHeader(long epoch, long ordinal) throws IOException {
        final BlobContainer container = container(epoch);
        final String name = name(ordinal);
        final byte[] leading;
        try {
            leading = readRange(container, name, 0L, HEADER_PROBE_BYTES);
        } catch (NoSuchFileException missing) {
            return Optional.empty();
        }
        final long end = NodePackBlob.headerEnd(leading);
        if (end < 0L) {
            throw new IOException("node pack " + epochPath(base, nodeId, epoch).buildAsString() + name + " is too short to hold a header");
        }
        if (end <= leading.length) {
            return Optional.of(NodePackBlob.decode(leading));
        }
        if (end > Integer.MAX_VALUE) {
            throw new IOException("node pack header of " + end + " bytes is too large to read");
        }
        return Optional.of(NodePackBlob.decode(readRange(container, name, 0L, (int) end)));
    }

    /**
     * Opens a range of a pack: one block, or one file of one block.
     *
     * @param epoch the epoch
     * @param ordinal the slot
     * @param offset where the range starts
     * @param length how long it is
     * @return its bytes
     * @throws IOException if the read fails
     */
    public InputStream readRange(long epoch, long ordinal, long offset, long length) throws IOException {
        return container(epoch).readBlob(name(ordinal), offset, length);
    }

    /**
     * Deletes packs by name. Packs already gone are ignored.
     *
     * @param epoch the epoch
     * @param ordinals the slots to delete
     * @throws IOException if a delete fails
     */
    public void delete(long epoch, List<Long> ordinals) throws IOException {
        final BlobContainer container = container(epoch);
        final List<String> batch = new ArrayList<>(Math.min(DELETE_BATCH, ordinals.size()));
        for (long ordinal : ordinals) {
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
