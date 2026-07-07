/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.translog;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.serverless.storage.wal.WalChunkNaming;
import org.opensearch.serverless.storage.wal.WalChunkReader;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.serverless.storage.wal.WalRecord;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;

public class WalMirroringTranslogTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "wal-mirror-idx";

    private ShardId shardId;
    private IndexSettings indexSettings;
    private BlobContainer blobContainer;
    private WalChunkService walChunkService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Index index = new Index("wal-mirror-index", INDEX_UUID);
        shardId = new ShardId(index, 0);
        indexSettings = IndexSettingsModule.newIndexSettings(index, org.opensearch.common.settings.Settings.EMPTY);

        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        walChunkService = new WalChunkService(blobContainer, "epoch-0");
    }

    private WalMirroringTranslog newTranslog(Path translogPath) throws Exception {
        TranslogConfig config = new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE, "node-0", false);
        String translogUUID = Translog.createEmptyTranslog(translogPath, SequenceNumbers.UNASSIGNED_SEQ_NO, shardId, 1L);
        return new WalMirroringTranslog(
            config,
            translogUUID,
            new DefaultTranslogDeletionPolicy(-1, -1, Integer.MAX_VALUE),
            () -> SequenceNumbers.UNASSIGNED_SEQ_NO,
            () -> 1L,
            seqNo -> {},
            TranslogOperationHelper.DEFAULT,
            walChunkService
        );
    }

    public void testAppendedOperationsAreMirroredIntoAWalChunk() throws Exception {
        Path path = createTempDir();
        try (WalMirroringTranslog translog = newTranslog(path)) {
            translog.add(new Translog.Index("id-1", 0, 1, "{\"field\":1}".getBytes("UTF-8")));
            translog.add(new Translog.Index("id-2", 1, 1, "{\"field\":2}".getBytes("UTF-8")));
        }

        List<WalRecord> allRecords = new java.util.ArrayList<>();
        for (long chunkSeq = 0; chunkSeq < 2; chunkSeq++) {
            String blobName = WalChunkNaming.blobName("epoch-0", chunkSeq);
            byte[] chunkBytes;
            try (java.io.InputStream in = blobContainer.readBlob(blobName)) {
                chunkBytes = in.readAllBytes();
            }
            allRecords.addAll(WalChunkReader.readRecords(chunkBytes));
        }

        assertEquals(2, allRecords.size());
        assertEquals(INDEX_UUID, allRecords.get(0).indexUuid());
        assertEquals(0, allRecords.get(0).shardId());
        assertEquals(0L, allRecords.get(0).seqNo());
        assertEquals(1L, allRecords.get(1).seqNo());
    }

    public void testLocalRecoveryStillWorksExactlyAsAPlainLocalTranslog() throws Exception {
        Path path = createTempDir();
        try (WalMirroringTranslog translog = newTranslog(path)) {
            translog.add(new Translog.Index("id-1", 0, 1, "{\"field\":1}".getBytes("UTF-8")));
            translog.add(new Translog.Index("id-2", 1, 1, "{\"field\":2}".getBytes("UTF-8")));

            java.util.Set<Long> seqNosSeen = new java.util.HashSet<>();
            try (Translog.Snapshot snapshot = translog.newSnapshot()) {
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    seqNosSeen.add(op.seqNo());
                }
            }
            assertEquals(java.util.Set.of(0L, 1L), seqNosSeen);
        }
    }
}
