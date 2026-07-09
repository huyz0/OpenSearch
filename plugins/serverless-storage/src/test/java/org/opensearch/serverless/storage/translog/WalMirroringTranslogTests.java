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
import org.opensearch.common.blobstore.support.FilterBlobContainer;
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
        assertEquals(1L, allRecords.get(0).primaryTerm());
        assertEquals(0L, allRecords.get(0).seqNo());
        assertEquals(1L, allRecords.get(1).primaryTerm());
        assertEquals(1L, allRecords.get(1).seqNo());
    }

    public void testLastFlushedWalChunkSequenceAdvancesWithEachMirroredOperation() throws Exception {
        Path path = createTempDir();
        try (WalMirroringTranslog translog = newTranslog(path)) {
            assertEquals("nothing mirrored yet", -1L, translog.lastFlushedWalChunkSequence());

            translog.add(new Translog.Index("id-1", 0, 1, "{\"field\":1}".getBytes("UTF-8")));
            long afterFirst = translog.lastFlushedWalChunkSequence();
            assertTrue("the first mirrored operation must advance the watermark past -1", afterFirst >= 0);

            translog.add(new Translog.Index("id-2", 1, 1, "{\"field\":2}".getBytes("UTF-8")));
            long afterSecond = translog.lastFlushedWalChunkSequence();
            assertTrue(
                "each op flushes to its own chunk today (flush-per-op), so the sequence must strictly advance",
                afterSecond > afterFirst
            );
        }
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

    public void testAddSurvivesTransientWalMirrorFailuresWithinTheRetryBudget() throws Exception {
        FailNTimesBlobContainer faulty = new FailNTimesBlobContainer(blobContainer, WalMirroringTranslog.MAX_MIRROR_FLUSH_ATTEMPTS - 1);
        WalChunkService flakyWalChunkService = new WalChunkService(faulty, "epoch-0");

        Path path = createTempDir();
        TranslogConfig config = new TranslogConfig(shardId, path, indexSettings, BigArrays.NON_RECYCLING_INSTANCE, "node-0", false);
        String translogUUID = Translog.createEmptyTranslog(path, SequenceNumbers.UNASSIGNED_SEQ_NO, shardId, 1L);
        try (
            WalMirroringTranslog translog = new WalMirroringTranslog(
                config,
                translogUUID,
                new DefaultTranslogDeletionPolicy(-1, -1, Integer.MAX_VALUE),
                () -> SequenceNumbers.UNASSIGNED_SEQ_NO,
                () -> 1L,
                seqNo -> {},
                TranslogOperationHelper.DEFAULT,
                flakyWalChunkService
            )
        ) {
            // Must not throw: the first (MAX_MIRROR_FLUSH_ATTEMPTS - 1) flush attempts fail, but
            // the last one within the retry budget succeeds.
            translog.add(new Translog.Index("id-1", 0, 1, "{\"field\":1}".getBytes("UTF-8")));
        }

        // The successful attempt lands at whatever sequence number it reached (earlier attempts
        // that failed before writing still consume a sequence number), so scan by prefix rather
        // than assuming it's log-0.
        List<WalRecord> allRecords = new java.util.ArrayList<>();
        for (String blobName : blobContainer.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).keySet()) {
            byte[] chunkBytes;
            try (java.io.InputStream in = blobContainer.readBlob(blobName)) {
                chunkBytes = in.readAllBytes();
            }
            allRecords.addAll(WalChunkReader.readRecords(chunkBytes));
        }
        assertEquals(1, allRecords.size());
    }

    public void testAddFailsAfterExhaustingTheRetryBudget() throws Exception {
        FailNTimesBlobContainer faulty = new FailNTimesBlobContainer(blobContainer, WalMirroringTranslog.MAX_MIRROR_FLUSH_ATTEMPTS);
        WalChunkService alwaysFlakyWalChunkService = new WalChunkService(faulty, "epoch-0");

        Path path = createTempDir();
        TranslogConfig config = new TranslogConfig(shardId, path, indexSettings, BigArrays.NON_RECYCLING_INSTANCE, "node-0", false);
        String translogUUID = Translog.createEmptyTranslog(path, SequenceNumbers.UNASSIGNED_SEQ_NO, shardId, 1L);
        try (
            WalMirroringTranslog translog = new WalMirroringTranslog(
                config,
                translogUUID,
                new DefaultTranslogDeletionPolicy(-1, -1, Integer.MAX_VALUE),
                () -> SequenceNumbers.UNASSIGNED_SEQ_NO,
                () -> 1L,
                seqNo -> {},
                TranslogOperationHelper.DEFAULT,
                alwaysFlakyWalChunkService
            )
        ) {
            expectThrows(
                java.io.IOException.class,
                () -> translog.add(new Translog.Index("id-1", 0, 1, "{\"field\":1}".getBytes("UTF-8")))
            );
        }
    }

    /** Fails the first {@code failureCount} writeBlob calls, then delegates normally. */
    private static final class FailNTimesBlobContainer extends FilterBlobContainer {

        // FilterBlobContainer keeps its own delegate reference private and doesn't override
        // readRegister/compareAndSwapRegister (inheriting BlobContainer's own
        // UnsupportedOperationException-throwing defaults instead of delegating) -- WalChunkService
        // now needs both for its CAS-based chunk sequence allocation, so this class keeps its own
        // reference to delegate to directly.
        private final BlobContainer delegate;
        private int remainingFailures;

        FailNTimesBlobContainer(BlobContainer delegate, int failureCount) {
            super(delegate);
            this.delegate = delegate;
            this.remainingFailures = failureCount;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new FailNTimesBlobContainer(child, remainingFailures);
        }

        @Override
        public synchronized void writeBlob(String blobName, java.io.InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws java.io.IOException {
            if (remainingFailures > 0) {
                remainingFailures--;
                throw new java.io.IOException("injected transient failure writing " + blobName);
            }
            super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public java.util.Optional<org.opensearch.common.blobstore.BlobRegister> readRegister(String blobName) throws java.io.IOException {
            return delegate.readRegister(blobName);
        }

        @Override
        public org.opensearch.common.blobstore.BlobRegisterCasResult compareAndSwapRegister(
            String blobName,
            long expectedGeneration,
            org.opensearch.core.common.bytes.BytesReference newValue
        ) throws java.io.IOException {
            return delegate.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }
    }
}
