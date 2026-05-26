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
    /**
     * Transfers a translog snapshot to the remote translog store.
     *
     * @param shardId the shard ID
     * @param transferSnapshot the translog snapshot to transfer
     * @param listener listener to handle transfer completion and failure events
     * @param cryptoMetadata encryption metadata for secure uploads
     * @return true if the transfer was successful, false otherwise
     * @throws IOException in case of I/O error during transfer
     */
    boolean transferSnapshot(
        ShardId shardId,
        TransferSnapshot transferSnapshot,
        TranslogTransferListener listener,
        CryptoMetadata cryptoMetadata
    ) throws IOException;

    /**
     * Downloads specific translog files for a commit/generation to a local path.
     *
     * @param shardId the shard ID
     * @param primaryTerm the primary term of the translog files to download
     * @param generation the translog generation
     * @param location the local directory path to download files to
     * @return true if the download completed successfully, false otherwise
     * @throws IOException in case of I/O error during download
     */
    boolean downloadTranslog(ShardId shardId, String primaryTerm, String generation, Path location) throws IOException;

    /**
     * Performs a full download of translog files up to a specific timestamp, optionally seeding the remote store state.
     *
     * @param shardId the shard ID
     * @param location the local directory path to download files to
     * @param logger logger instance for tracing download events
     * @param seedRemote if true, indicates that remote state should be seeded/initialized
     * @param timestamp the timestamp threshold up to which translog files should be downloaded
     * @return true if the download completed successfully, false otherwise
     * @throws IOException in case of I/O error during download
     */
    default boolean download(ShardId shardId, Path location, org.apache.logging.log4j.Logger logger, boolean seedRemote, long timestamp)
        throws IOException {
        return false;
    }
}
