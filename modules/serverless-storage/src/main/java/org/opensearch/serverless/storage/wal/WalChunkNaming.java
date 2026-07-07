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

    private WalChunkNaming() {}

    public static String blobName(String writerEpoch, long chunkSequence) {
        return "log-" + chunkSequence;
    }

    public static String blobPath(String writerEpoch, long chunkSequence) {
        return "wal/" + writerEpoch + "/" + blobName(writerEpoch, chunkSequence);
    }
}
