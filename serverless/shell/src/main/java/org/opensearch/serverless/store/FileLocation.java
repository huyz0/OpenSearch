/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import java.io.IOException;

/**
 * Where a published segment file's bytes are: a blob in a term container, and the range of it the file occupies.
 *
 * <p>A file published on its own is a whole blob named after it, and the manifest records only its term container,
 * {@code t=<term>}. A file published in a pack -- one immutable blob holding every file a publish added -- is a range
 * of that pack, recorded as {@code t=<term>/<pack>#<offset>:<length>}. One manifest can name both: a commit
 * inherits files across failovers and upgrades without moving them.
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
     * @return {@code t=<term>} for a file of its own, {@code t=<term>/<pack>#<offset>:<length>} for a packed one
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
