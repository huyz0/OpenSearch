/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.security.StaticEncryptionKeyProvider;
import org.opensearch.test.OpenSearchTestCase;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.util.List;

public class WalReplayRecoveryTests extends OpenSearchTestCase {

    private BlobContainer newBlobContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    private static byte[] serialize(Translog.Operation operation) throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        Translog.Operation.writeOperation(out, operation);
        return BytesReference.toBytes(out.bytes());
    }

    public void testReplayReturnsNothingWhenActivationWalPositionIsNegative() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        List<Translog.Operation> operations = WalReplayRecovery.replayOperations(blobContainer, "idx", 0, 1, null, -1);
        assertTrue(operations.isEmpty());
    }

    public void testReplayReturnsNothingWhenThereIsNoNewChunkPastTheManifestPosition() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalPosition lastDurable = new WalPosition("epoch-0", 4);
        // activationWalPosition (exclusive) is 5, so only chunk sequence 5+ would be new -- but
        // fromChunkSequenceInclusive is 5 too (lastDurable.offset()+1), so there's no gap at all.
        List<Translog.Operation> operations = WalReplayRecovery.replayOperations(blobContainer, "idx", 0, 1, lastDurable, 5);
        assertTrue(operations.isEmpty());
    }

    public void testReplayFetchesFiltersAndDecodesOperationsAcrossMultipleChunks() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0");

        Translog.Index op0 = new Translog.Index("doc0", 0, 1, "src0".getBytes("UTF-8"));
        Translog.Index op1 = new Translog.Index("doc1", 1, 1, "src1".getBytes("UTF-8"));
        Translog.Index op2 = new Translog.Index("doc2", 2, 1, "src2".getBytes("UTF-8"));

        // Chunk 0: this shard's op0, plus a different shard's record that must be excluded.
        service.append(new WalRecord("idx", 0, 1, 0, serialize(op0)));
        service.append(new WalRecord("other-idx", 0, 1, 0, serialize(op0)));
        assertEquals(0, service.flush());

        // Chunk 1: op1 tagged term=1 (this is the ReplayFloor term, must be included).
        service.append(new WalRecord("idx", 0, 1, 1, serialize(op1)));
        assertEquals(1, service.flush());

        // Chunk 2: op2, appended after activation would have snapshotted -- must be excluded by
        // the position cutoff even though it's correctly tagged, matching WalReplayFencing.tla's
        // FixedReplay design (this simulates a stale writer appending post-fencing).
        service.append(new WalRecord("idx", 0, 1, 2, serialize(op2)));
        assertEquals(2, service.flush());

        // No prior manifest at all (brand new shard) -- replay from the very start, but bounded by
        // activationWalPosition=2 (exclusive), so only chunks 0 and 1 are visible.
        List<Translog.Operation> operations = WalReplayRecovery.replayOperations(blobContainer, "idx", 0, 1, null, 2);

        assertEquals(2, operations.size());
        assertEquals(0L, operations.get(0).seqNo());
        assertEquals(1L, operations.get(1).seqNo());
    }

    public void testReplayHonorsTheManifestsLastDurableWalPositionAsALowerBound() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0");

        Translog.Index op0 = new Translog.Index("doc0", 0, 1, "src0".getBytes("UTF-8"));
        Translog.Index op1 = new Translog.Index("doc1", 1, 1, "src1".getBytes("UTF-8"));

        service.append(new WalRecord("idx", 0, 1, 0, serialize(op0)));
        assertEquals(0, service.flush()); // already covered by the manifest below

        service.append(new WalRecord("idx", 0, 1, 1, serialize(op1)));
        assertEquals(1, service.flush()); // new since the manifest

        WalPosition lastDurable = new WalPosition("epoch-0", 0); // covers chunk 0
        List<Translog.Operation> operations = WalReplayRecovery.replayOperations(blobContainer, "idx", 0, 1, lastDurable, 2);

        assertEquals(1, operations.size());
        assertEquals(1L, operations.get(0).seqNo());
    }

    public void testReplayExcludesRecordsBelowTheTermFloor() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0");

        Translog.Index staleTermOp = new Translog.Index("doc0", 0, 1, "src0".getBytes("UTF-8"));
        Translog.Index currentTermOp = new Translog.Index("doc1", 1, 2, "src1".getBytes("UTF-8"));

        service.append(new WalRecord("idx", 0, 1, 0, serialize(staleTermOp))); // term 1, below floor
        service.append(new WalRecord("idx", 0, 2, 1, serialize(currentTermOp))); // term 2, at floor
        assertEquals(0, service.flush());

        // Activating under term 3 -- ReplayFloor is term 2, so only the term=2 record qualifies.
        List<Translog.Operation> operations = WalReplayRecovery.replayOperations(blobContainer, "idx", 0, 2, null, 1);

        assertEquals(1, operations.size());
        assertEquals(1L, operations.get(0).seqNo());
    }

    private static SecretKey newAesKey() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(256);
        return keyGenerator.generateKey();
    }

    // Regression test for a real bug: WalReplayRecovery previously never decrypted a record's
    // payload before handing it to Translog.Operation#readOperation, so recovery after a crash
    // (with encryption + WAL mirroring both enabled) failed with an opaque deserialization error
    // instead of returning the real operation. Reproduced directly against
    // EncryptingWalChunkService (the real write-side wrapper ObjectStoreWriterEngine uses) rather
    // than hand-encrypting a record, so this exercises the exact bytes a real crash recovery would
    // read back.
    public void testReplayDecryptsRecordsWhenAnEncryptionKeyProviderIsSupplied() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService delegate = new WalChunkService(blobContainer, "epoch-0");
        StaticEncryptionKeyProvider keyProvider = new StaticEncryptionKeyProvider(newAesKey());
        EncryptingWalChunkService encryptingService = new EncryptingWalChunkService(delegate, keyProvider);

        Translog.Index op0 = new Translog.Index("doc0", 0, 1, "src0".getBytes("UTF-8"));
        encryptingService.append(new WalRecord("idx", 0, 1, 0, serialize(op0)));
        encryptingService.flush();

        List<Translog.Operation> operations = WalReplayRecovery.replayOperations(blobContainer, "idx", 0, 1, null, 1, keyProvider);

        assertEquals(1, operations.size());
        assertEquals(0L, operations.get(0).seqNo());
        assertEquals(Translog.Operation.Type.INDEX, operations.get(0).opType());
    }

    public void testReplayWithoutAnEncryptionKeyProviderFailsToDecodeEncryptedRecords() throws Exception {
        // The negative case, proving the two overloads genuinely differ in behavior rather than
        // one silently ignoring its own argument: replaying encrypted records through the
        // six-argument (no-decryption) overload must fail, not silently succeed with garbage.
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService delegate = new WalChunkService(blobContainer, "epoch-0");
        EncryptingWalChunkService encryptingService = new EncryptingWalChunkService(delegate, new StaticEncryptionKeyProvider(newAesKey()));

        Translog.Index op0 = new Translog.Index("doc0", 0, 1, "src0".getBytes("UTF-8"));
        encryptingService.append(new WalRecord("idx", 0, 1, 0, serialize(op0)));
        encryptingService.flush();

        expectThrows(Exception.class, () -> WalReplayRecovery.replayOperations(blobContainer, "idx", 0, 1, null, 1));
    }

    public void testListChunkSequencesInRangeSortsAndFiltersCorrectly() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0");
        for (int i = 0; i < 5; i++) {
            service.append(new WalRecord("idx", 0, 1, i, ("v" + i).getBytes("UTF-8")));
            assertEquals(i, service.flush());
        }
        List<Long> sequences = WalReplayRecovery.listChunkSequencesInRange(blobContainer, 1, 4);
        assertEquals(List.of(1L, 2L, 3L), sequences);
    }
}
