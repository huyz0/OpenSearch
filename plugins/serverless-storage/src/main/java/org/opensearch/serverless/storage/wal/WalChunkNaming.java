/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

/**
 * Object naming for WAL chunks, per the {@code wal/<writer-epoch>/log-<seq>} layout in
 * rfc-serverless-opensearch.md &sect;6.1. Chunks are keyed under a writer epoch so a superseded
 * writer's tail chunks can be fenced and discarded on failover (&sect;6.4).
 */
public final class WalChunkNaming {

    /** Prefix every WAL chunk blob name starts with, ahead of its chunk sequence number. */
    public static final String LOG_BLOB_PREFIX = "log-";

    private WalChunkNaming() {}

    /**
     * Builds the blob name (without the writer-epoch directory) for a WAL chunk.
     *
     * @param writerEpoch the writer epoch the chunk belongs under (not incorporated into the name itself)
     * @param chunkSequence the chunk's sequence number
     * @return the blob name, e.g. {@code log-42}
     */
    public static String blobName(String writerEpoch, long chunkSequence) {
        return LOG_BLOB_PREFIX + chunkSequence;
    }

    /**
     * Builds the full {@code wal/<writer-epoch>/log-<seq>} blob path for a WAL chunk.
     *
     * @param writerEpoch the writer epoch the chunk belongs under
     * @param chunkSequence the chunk's sequence number
     * @return the full blob path
     */
    public static String blobPath(String writerEpoch, long chunkSequence) {
        return "wal/" + writerEpoch + "/" + blobName(writerEpoch, chunkSequence);
    }

    /**
     * Inverse of {@link #blobName}: recovers the chunk sequence from a blob name it produced.
     *
     * @param blobName a blob name previously produced by {@link #blobName}
     * @return the chunk sequence number encoded in {@code blobName}
     */
    public static long parseChunkSequence(String blobName) {
        if (blobName.startsWith(LOG_BLOB_PREFIX) == false) {
            throw new IllegalArgumentException("not a WAL chunk blob name: " + blobName);
        }
        return Long.parseLong(blobName.substring(LOG_BLOB_PREFIX.length()));
    }
}
