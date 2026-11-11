/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.blobstore.BlobPath;

import java.io.IOException;

/**
 * Where a published segment file's bytes are: a blob in a term container, and the range of it the file occupies.
 *
 * <p>A file published on its own is a whole blob named after it, and the manifest records only its term container,
 * {@code t=<term>}. A file published in a pack -- one immutable blob holding every file a publish added -- is a range
 * of that pack, recorded as {@code t=<term>/<pack>#<offset>:<length>}. A file published in a node pack -- one blob
 * holding segment blobs of many shards, of any indices, that one node published together (see {@link NodePackStore})
 * -- is a range of that pack, outside the shard: {@code @<node>/e=<epoch>/<ordinal>#<offset>:<length>}. One manifest
 * can name all three: a commit inherits files across failovers and upgrades without moving them.
 *
 * @param termDir the term container
 * @param blob the blob's name in it: the file's own name, or the pack's
 * @param offset where the file starts in the blob; zero for a file of its own
 * @param length the file's length in a pack, or -1 for a file of its own, whose length the manifest records apart
 */
public record FileLocation(String termDir, String blob, long offset, long length) {

    /**
     * Validates a location.
     *
     * @param termDir the term container
     * @param blob the blob's name
     * @param offset where the file starts in the blob
     * @param length the file's length in a pack, or -1
     */
    public FileLocation {
        if (termDir == null || termDir.isEmpty() || blob == null || blob.isEmpty()) {
            throw new IllegalArgumentException("a file location names a term container and a blob");
        }
        if (offset < 0L || length < -1L || (length == -1L && offset != 0L)) {
            throw new IllegalArgumentException("invalid file range [" + offset + ":" + length + "] in [" + blob + "]");
        }
    }

    /**
     * The location of a file published as a blob of its own.
     *
     * @param termDir the term container
     * @param fileName the file, which is also the blob's name
     * @return its location
     */
    public static FileLocation whole(String termDir, String fileName) {
        return new FileLocation(termDir, fileName, 0L, -1L);
    }

    /**
     * The location of a file published in a pack.
     *
     * @param termDir the term container
     * @param pack the pack's blob name
     * @param offset where the file starts in the pack
     * @param length the file's length
     * @return its location
     */
    public static FileLocation packed(String termDir, String pack, long offset, long length) {
        if (termDir.startsWith(NODE_PACK_PREFIX)) {
            throw new IllegalArgumentException("a term container never starts with " + NODE_PACK_PREFIX);
        }
        return range(termDir, pack, offset, length);
    }

    /** What starts a node pack's recorded location, which no term container's name does. */
    static final String NODE_PACK_PREFIX = "@";

    /**
     * The location of a file published in a node pack.
     *
     * @param nodeId the node that wrote the pack
     * @param epoch the pack's epoch
     * @param ordinal the pack's slot
     * @param offset where the file starts in the pack
     * @param length the file's length
     * @return its location
     */
    public static FileLocation nodePacked(String nodeId, long epoch, long ordinal, long offset, long length) {
        if (nodeId == null || nodeId.isEmpty() || nodeId.indexOf('/') >= 0 || nodeId.indexOf('#') >= 0) {
            throw new IllegalArgumentException("invalid node id [" + nodeId + "] for a node pack");
        }
        return range(NODE_PACK_PREFIX + nodeId + "/" + NodePackStore.epochDir(epoch), NodePackStore.name(ordinal), offset, length);
    }

    private static FileLocation range(String termDir, String pack, long offset, long length) {
        if (length < 0L) {
            throw new IllegalArgumentException("a packed file has a length");
        }
        return new FileLocation(termDir, pack, offset, length);
    }

    /**
     * Reads a location as a manifest records it.
     *
     * @param fileName the file the location is of
     * @param recorded the manifest's value for it
     * @return the location
     * @throws IOException if the value is malformed
     */
    public static FileLocation parse(String fileName, String recorded) throws IOException {
        if (recorded.startsWith(NODE_PACK_PREFIX)) {
            return parseNodePacked(fileName, recorded);
        }
        final int slash = recorded.indexOf('/');
        if (slash < 0) {
            return whole(recorded, fileName);
        }
        final int hash = recorded.indexOf('#', slash);
        final int colon = hash < 0 ? -1 : recorded.indexOf(':', hash);
        if (slash == 0 || hash <= slash + 1 || colon <= hash + 1 || colon == recorded.length() - 1) {
            throw new IOException("malformed location [" + recorded + "] of [" + fileName + "]");
        }
        try {
            return packed(
                recorded.substring(0, slash),
                recorded.substring(slash + 1, hash),
                Long.parseLong(recorded.substring(hash + 1, colon)),
                Long.parseLong(recorded.substring(colon + 1))
            );
        } catch (IllegalArgumentException e) {
            throw new IOException("malformed location [" + recorded + "] of [" + fileName + "]", e);
        }
    }

    private static FileLocation parseNodePacked(String fileName, String recorded) throws IOException {
        // @<node>/e=<epoch>/<ordinal>#<offset>:<length>
        final int hash = recorded.indexOf('#');
        final int slash = hash < 0 ? -1 : recorded.lastIndexOf('/', hash);
        final int nodeEnd = recorded.indexOf('/');
        final int colon = hash < 0 ? -1 : recorded.indexOf(':', hash);
        if (nodeEnd <= 1 || slash <= nodeEnd + 1 || hash <= slash + 1 || colon <= hash + 1 || colon == recorded.length() - 1) {
            throw new IOException("malformed location [" + recorded + "] of [" + fileName + "]");
        }
        try {
            final String epochDir = recorded.substring(nodeEnd + 1, slash);
            if (epochDir.startsWith("e=") == false) {
                throw new IOException("malformed location [" + recorded + "] of [" + fileName + "]");
            }
            final FileLocation parsed = nodePacked(
                recorded.substring(1, nodeEnd),
                Long.parseLong(epochDir.substring(2)),
                Long.parseLong(recorded.substring(slash + 1, hash)),
                Long.parseLong(recorded.substring(hash + 1, colon)),
                Long.parseLong(recorded.substring(colon + 1))
            );
            if (parsed.encode().equals(recorded) == false) {
                throw new IOException("malformed location [" + recorded + "] of [" + fileName + "]");
            }
            return parsed;
        } catch (IllegalArgumentException e) {
            throw new IOException("malformed location [" + recorded + "] of [" + fileName + "]", e);
        }
    }

    /**
     * Whether the file is in a node pack, outside the shard, rather than in one of the shard's term containers.
     *
     * @return true for a file in a node pack
     */
    public boolean isNodePacked() {
        return termDir.startsWith(NODE_PACK_PREFIX);
    }

    /**
     * The container the file's blob is in: a term container under the shard, or a node's pack epoch beside the
     * node logs.
     *
     * <p>A shard's base path is {@code <base>/segments/<shard>}, so the cluster's base, where node packs live, is two
     * levels up. Resolving it here keeps every reader of a manifest from having to carry the base along.
     *
     * @param shardBase the shard's base path
     * @return the container's path
     */
    public BlobPath containerPath(BlobPath shardBase) {
        if (isNodePacked() == false) {
            return shardBase.add(termDir);
        }
        final int slash = termDir.indexOf('/');
        final BlobPath base = shardBase.parent().parent();
        return base.add(NodePackStore.ROOT).add(termDir.substring(1, slash)).add(termDir.substring(slash + 1));
    }

    /**
     * Whether the file is a range of a pack rather than a blob of its own.
     *
     * @return true for a packed file
     */
    public boolean isPacked() {
        return length >= 0L;
    }

    /**
     * The value a manifest records for this location.
     *
     * @return {@code t=<term>} for a file of its own, {@code t=<term>/<pack>#<offset>:<length>} for a packed one, and
     *         {@code @<node>/e=<epoch>/<ordinal>#<offset>:<length>} for one in a node pack
     */
    public String encode() {
        return isPacked() ? termDir + "/" + blob + "#" + offset + ":" + length : termDir;
    }

    /**
     * The blob the file's bytes are in, as {@code <termDir>/<blob>}: what keeps a blob from being swept, and what
     * identifies its bytes in a cache.
     *
     * @return the blob's path under the shard
     */
    public String referencedBlob() {
        return termDir + "/" + blob;
    }
}
