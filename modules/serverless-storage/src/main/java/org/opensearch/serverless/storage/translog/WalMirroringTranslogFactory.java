/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.translog;

import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogFactory;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.serverless.storage.wal.WalChunkService;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * {@link TranslogFactory} that produces a {@link WalMirroringTranslog}: a local translog, exactly
 * as durable and recoverable on this node as {@link org.opensearch.index.translog.InternalTranslogFactory}
 * produces today, plus a mirror of every operation into a {@link WalChunkService}-backed WAL chunk
 * for durability against loss of the node (rfc-serverless-opensearch.md &sect;6.4).
 */
public final class WalMirroringTranslogFactory implements TranslogFactory {

    private final WalChunkService walChunkService;

    public WalMirroringTranslogFactory(WalChunkService walChunkService) {
        this.walChunkService = walChunkService;
    }

    @Override
    public Translog newTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BooleanSupplier startedPrimarySupplier
    ) throws IOException {
        return newTranslog(
            config,
            translogUUID,
            deletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            startedPrimarySupplier,
            TranslogOperationHelper.DEFAULT
        );
    }

    @Override
    public Translog newTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BooleanSupplier startedPrimarySupplier,
        TranslogOperationHelper translogOperationHelper
    ) throws IOException {
        return new WalMirroringTranslog(
            config,
            translogUUID,
            deletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            translogOperationHelper,
            walChunkService
        );
    }
}
