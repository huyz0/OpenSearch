/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.translog;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.index.translog.LocalTranslog;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.serverless.storage.wal.WalRecord;

import java.io.IOException;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * A {@link LocalTranslog} that additionally mirrors every appended operation into the node-level
 * {@link WalChunkService}, per rfc-serverless-opensearch.md &sect;6.4. Local translog files remain
 * the source of truth for recovery on this node exactly as they are today; the WAL chunk mirror is
 * what makes the operation durable against loss of this node, ahead of the next segment bundle and
 * commit manifest.
 *
 * <p>This deliberately keeps the entire recovery/replay/generation-rolling machinery of {@link
 * Translog} untouched -- only {@link #add} is overridden -- rather than reimplementing translog
 * file management on top of the WAL chunk format.
 */
public class WalMirroringTranslog extends LocalTranslog {

    private final WalChunkService walChunkService;
    private final String indexUuid;
    private final int shardId;

    public WalMirroringTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        TranslogOperationHelper translogOperationHelper,
        WalChunkService walChunkService
    ) throws IOException {
        super(
            config,
            translogUUID,
            deletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            translogOperationHelper,
            null
        );
        this.walChunkService = walChunkService;
        this.indexUuid = config.getShardId().getIndex().getUUID();
        this.shardId = config.getShardId().getId();
    }

    @Override
    public Location add(final Operation operation) throws IOException {
        Location location = super.add(operation);
        BytesStreamOutput out = new BytesStreamOutput();
        Operation.writeOperation(out, operation);
        walChunkService.append(
            new WalRecord(indexUuid, shardId, operation.seqNo(), org.opensearch.core.common.bytes.BytesReference.toBytes(out.bytes()))
        );
        // Flushing per-operation gives the same per-write durability guarantee a local translog
        // fsync gives; a deployment that wants real cross-shard group-commit batching would flush
        // WalChunkService on a timer/size threshold from a shared scheduler instead, independent
        // of any single shard's Translog.
        walChunkService.flush();
        return location;
    }
}
