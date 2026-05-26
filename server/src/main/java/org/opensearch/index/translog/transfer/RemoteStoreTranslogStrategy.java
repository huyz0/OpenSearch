/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Strategy interface for translog snapshot transfer and download.
 */
@ExperimentalApi
public interface RemoteStoreTranslogStrategy {
    boolean transferSnapshot(
        ShardId shardId,
        TransferSnapshot transferSnapshot,
        TranslogTransferListener listener,
        CryptoMetadata cryptoMetadata
    ) throws IOException;

    boolean downloadTranslog(ShardId shardId, String primaryTerm, String generation, Path location) throws IOException;

    default boolean download(ShardId shardId, Path location, org.apache.logging.log4j.Logger logger, boolean seedRemote, long timestamp)
        throws IOException {
        return false;
    }
}
