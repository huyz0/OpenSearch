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

    public static final String LOG_BLOB_PREFIX = "log-";

    private WalChunkNaming() {}

    public static String blobName(String writerEpoch, long chunkSequence) {
        return LOG_BLOB_PREFIX + chunkSequence;
    }

    public static String blobPath(String writerEpoch, long chunkSequence) {
        return "wal/" + writerEpoch + "/" + blobName(writerEpoch, chunkSequence);
    }

    /** Inverse of {@link #blobName}: recovers the chunk sequence from a blob name it produced. */
    public static long parseChunkSequence(String blobName) {
        if (blobName.startsWith(LOG_BLOB_PREFIX) == false) {
            throw new IllegalArgumentException("not a WAL chunk blob name: " + blobName);
        }
        return Long.parseLong(blobName.substring(LOG_BLOB_PREFIX.length()));
    }
}
