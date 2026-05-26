/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Default implementation of RemoteStoreTranslogStrategy which delegates to TranslogTransferManager.
 */
public final class DefaultRemoteStoreTranslogStrategy implements RemoteStoreTranslogStrategy {

    private final TranslogTransferManager translogTransferManager;

    public DefaultRemoteStoreTranslogStrategy(final TranslogTransferManager translogTransferManager) {
        this.translogTransferManager = translogTransferManager;
    }

    @Override
    public boolean transferSnapshot(
        final ShardId shardId,
        final TransferSnapshot transferSnapshot,
        final TranslogTransferListener listener,
        final CryptoMetadata cryptoMetadata
    ) throws IOException {
        return translogTransferManager.transferSnapshot(transferSnapshot, listener, cryptoMetadata);
    }

    @Override
    public boolean downloadTranslog(final ShardId shardId, final String primaryTerm, final String generation, final Path location)
        throws IOException {
        return translogTransferManager.downloadTranslog(primaryTerm, generation, location);
    }
}
