/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.apache.lucene.index.SegmentInfos;
import org.opensearch.index.engine.DocumentIndexWriter;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;

import java.io.IOException;

/**
 * An {@link InternalEngine} whose durability is object-store-native (rfc-serverless-opensearch.md
 * &sect;5/&sect;6): local Lucene commits happen exactly as they do today (unchanged recovery,
 * merge, and translog-file semantics), but every commit is additionally packaged into a segment
 * bundle and published as the shard's head via {@link ObjectStoreCommitHeadPublisher} immediately
 * after the local Lucene commit completes.
 *
 * <p>If this writer has been fenced out (a different primary term now holds the shard's head --
 * e.g. its lease expired and another node took over) the local Lucene commit has already happened
 * by the time that's discovered, since object-store publication can only happen after the commit
 * whose files it packages exists. The commit stays valid locally (it does not corrupt anything),
 * but the engine is failed so this node stops serving as the shard's writer, matching how any
 * other fatal engine condition (e.g. a translog failure) is handled today.
 */
public class ObjectStoreWriterEngine extends InternalEngine {

    private final ObjectStoreCommitHeadPublisher headPublisher;
    private final String indexUuid;
    private final int shardId;

    public ObjectStoreWriterEngine(EngineConfig engineConfig, ObjectStoreCommitHeadPublisher headPublisher) {
        super(engineConfig);
        this.headPublisher = headPublisher;
        this.indexUuid = engineConfig.getShardId().getIndex().getUUID();
        this.shardId = engineConfig.getShardId().getId();
    }

    @Override
    protected void commitIndexWriter(final DocumentIndexWriter writer, final String translogUUID) throws IOException {
        super.commitIndexWriter(writer, translogUUID);

        try {
            SegmentInfos segmentInfos = store.readLastCommittedSegmentsInfo();
            long primaryTerm = engineConfig.getPrimaryTermSupplier().getAsLong();
            long generation = segmentInfos.getGeneration();
            long maxSeqNo = Long.parseLong(segmentInfos.userData.get(SequenceNumbers.MAX_SEQ_NO));
            long localCheckpoint = Long.parseLong(segmentInfos.userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));

            boolean published = headPublisher.publishCommitAsHead(
                store.directory(),
                segmentInfos,
                indexUuid,
                shardId,
                primaryTerm,
                generation,
                maxSeqNo,
                localCheckpoint,
                new WalPosition(String.valueOf(primaryTerm), 0),
                0,
                PruningStats.empty()
            );
            if (published == false) {
                throw new EngineException(
                    engineConfig.getShardId(),
                    "fenced out publishing commit generation " + generation + " under term " + primaryTerm
                );
            }
        } catch (final EngineException ex) {
            failEngine("object-store commit publication fenced out", ex);
            throw ex;
        } catch (final Exception ex) {
            failEngine("object-store commit publication failed", ex);
            throw new IOException(ex);
        }
    }
}
