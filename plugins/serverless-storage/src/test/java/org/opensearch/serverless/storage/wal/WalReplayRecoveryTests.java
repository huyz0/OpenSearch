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
import org.opensearch.serverless.storage.security.RegisterDelegatingBlobContainer;
import org.opensearch.serverless.storage.security.StaticEncryptionKeyProvider;
import org.opensearch.test.OpenSearchTestCase;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.InputStream;
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

    /**
     * S3-efficiency regression test: this range lookup must never list the whole shared container --
     * chunk sequences are a single counter shared across every shard using this container, so a full
     * listing costs O(every live chunk from every shard, ever written), unrelated to how large this
     * one replay's own range actually is. Probing each candidate sequence directly keeps the cost
     * bounded to the range size instead.
     */
    public void testListChunkSequencesInRangeNeverListsTheWholeContainer() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0");
        for (int i = 0; i < 5; i++) {
            service.append(new WalRecord("idx", 0, 1, i, ("v" + i).getBytes("UTF-8")));
            assertEquals(i, service.flush());
        }

        List<Long> sequences = WalReplayRecovery.listChunkSequencesInRange(
            new ListBlobsByPrefixForbiddenBlobContainer(blobContainer),
            1,
            4
        );
        assertEquals(List.of(1L, 2L, 3L), sequences);
    }

    // Kill-mid-WAL-chunk (rfc-serverless-opensearch.md &sect;17): WalChunkService#writeChunk is a
    // two-step write -- claimNextChunkSequence() durably advances the shared CAS register first,
    // then writeBlob() writes the chunk's actual content -- so a kill (or any hard fault) between
    // those two steps permanently orphans the claimed sequence: the register generation has already
    // advanced, but no blob ever lands at that sequence, and claimNextChunkSequence never reuses a
    // number once claimed. Recovery must tolerate that gap cleanly, the same "before-state
    // untouched, retry succeeds" shape ObjectStoreCommitHeadPublisherTests' kill-mid-bundle-upload/
    // kill-mid-manifest-write tests already established for the writer's other two-step writes.
    /**
     * An in-process retry must rewrite <em>its own already-claimed</em> sequence, not claim a fresh
     * one.
     *
     * <p><b>This test previously asserted the exact opposite</b> ("the retry must claim a fresh
     * sequence, never reuse the orphaned one", expecting sequence 1 and asserting that {@code log-0}
     * does not exist). That expectation encoded a real acked-and-lost bug as though it were a
     * requirement, so it is inverted here deliberately rather than worked around.
     *
     * <p>Why claiming a fresh sequence per attempt was wrong. A chunk sequence is claimed by a CAS on
     * the shared register, and {@code ObjectStoreWriterEngine}'s {@code activationWalPosition} fences
     * replay at whatever that register's generation was when a new writer activated. With a fresh
     * claim per attempt: attempt 1 claims sequence 0; a new writer elsewhere snapshots its activation
     * bound as 1; attempt 2 claims sequence 1 and succeeds; {@code flush()} returns and the operation
     * is acknowledged. Replay is bounded <em>exclusively</em> by 1, so the chunk that actually
     * carries that acknowledged operation is excluded, and the operation is silently lost. Reusing
     * the sequence claimed <em>before</em> the snapshot gives the correct semantics: the claim is the
     * moment the sequence is reserved, so a record written under it is visible to any writer that
     * activated after the claim.
     *
     * <p>Two secondary consequences point the same way. Burning a sequence per failed attempt left a
     * permanent hole that every later replay pays a {@code blobExists} probe for; and if a failing
     * attempt ever did leave a partial blob behind, the old behaviour stranded that torn blob
     * mid-sequence while writing the good chunk somewhere else -- which, since replay now only
     * tolerates a corrupt chunk at the very last sequence of its fenced range, fails recovery loudly.
     * Rewriting the same name overwrites the partial with the complete bytes instead
     * ({@code writeBlob} is called with {@code failIfAlreadyExists=false}, which deletes and
     * rewrites), and every attempt writes byte-identical content because the records are serialized
     * once, outside the retry loop.
     *
     * <p>The genuine orphan case -- a sequence claimed by a process that died before writing
     * anything, which nothing ever retries -- is unaffected by this and is covered by
     * {@link #testReplaySkipsASequenceThatWasClaimedButNeverWritten} below.
     */
    public void testAnInProcessRetryRewritesItsOwnClaimedSequenceInsteadOfBurningIt() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        OneShotFailingOnWriteBlobContainer faulty = new OneShotFailingOnWriteBlobContainer(blobContainer);
        WalChunkService service = new WalChunkService(faulty, "epoch-0");

        Translog.Index op0 = new Translog.Index("doc0", 0, 1, "src0".getBytes("UTF-8"));
        service.append(new WalRecord("idx", 0, 1, 0, serialize(op0)));

        // The injected fault throws before delegating, so the first attempt writes nothing at all --
        // the claim on sequence 0 is still ours and there is no partial blob to worry about.
        long chunkSequence = service.flush();
        assertEquals("the retry must rewrite the sequence this call already claimed", 0, chunkSequence);
        assertTrue(
            "and that sequence must hold the complete chunk after the retry succeeds",
            blobContainer.blobExists(WalChunkNaming.blobName("epoch-0", 0))
        );
        assertEquals(
            "a failed attempt must not burn a sequence: the register advanced exactly once, for the one claim",
            1L,
            service.currentChunkSequenceUpperBound()
        );

        List<Translog.Operation> operations = WalReplayRecovery.replayOperations(blobContainer, "idx", 0, 1, null, 2);
        assertEquals("recovery must find the operation at the sequence that was claimed for it", 1, operations.size());
        assertEquals(0L, operations.get(0).seqNo());
    }

    /**
     * The orphan that genuinely still happens: a sequence is claimed, every write attempt for it
     * fails, and the flush gives up -- so nothing is ever written there and no later attempt reuses
     * it, because the records go back into the buffer and the next flush claims a sequence of its
     * own. Replay must step over the resulting hole rather than treating a missing blob as the end of
     * the range or as corruption.
     *
     * <p>This is the property the previous version of the test above was really reaching for; it just
     * produced the hole by a route that is no longer how holes arise (and whose old behaviour was
     * itself the bug). Producing it honestly here also exercises the buffer-restore path: the record
     * that the failed flush could not write is still buffered, and the succeeding flush writes it.
     */
    public void testReplaySkipsASequenceThatWasClaimedButNeverWritten() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        // Exactly enough failures to exhaust one flush's whole retry budget, and no more, so the
        // second flush succeeds.
        FailFirstNWritesBlobContainer faulty = new FailFirstNWritesBlobContainer(blobContainer, WalChunkService.MAX_WRITE_ATTEMPTS);
        WalChunkService service = new WalChunkService(faulty, "epoch-0");

        Translog.Index op0 = new Translog.Index("doc0", 0, 1, "src0".getBytes("UTF-8"));
        service.append(new WalRecord("idx", 0, 1, 0, serialize(op0)));

        expectThrows(IOException.class, service::flush);
        assertFalse(
            "sequence 0 was claimed but never written -- a permanent hole",
            blobContainer.blobExists(WalChunkNaming.blobName("epoch-0", 0))
        );
        assertEquals("the failed flush must have put its record back for a later one to retry", 1, service.bufferedRecordCount());

        long chunkSequence = service.flush();
        assertEquals("the next flush claims its own sequence; it cannot reuse a claim it did not make", 1, chunkSequence);

        List<Translog.Operation> operations = WalReplayRecovery.replayOperations(blobContainer, "idx", 0, 1, null, 2);
        assertEquals(
            "recovery must skip the hole at sequence 0 and recover exactly the operation that actually landed",
            1,
            operations.size()
        );
        assertEquals(0L, operations.get(0).seqNo());
    }

    // A torn/corrupt LAST chunk in the replay range is indistinguishable from a predecessor that
    // crashed mid-append (the chunk it was writing when killed, never fully flushed) -- must be
    // treated as "nothing more was durably written" and stop replay there, not fail the whole
    // recovery over a write this shard's own activation never depended on completing.
    public void testReplayStopsCleanlyAtATruncatedLastChunkInsteadOfFailingTheWholeRecovery() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0");

        Translog.Index op0 = new Translog.Index("doc0", 0, 1, "src0".getBytes("UTF-8"));
        Translog.Index op1 = new Translog.Index("doc1", 1, 1, "src1".getBytes("UTF-8"));

        service.append(new WalRecord("idx", 0, 1, 0, serialize(op0)));
        assertEquals(0, service.flush()); // a good, fully-written chunk.

        service.append(new WalRecord("idx", 0, 1, 1, serialize(op1)));
        assertEquals(1, service.flush()); // this one gets torn below, simulating a killed mid-write.

        String blobName = WalChunkNaming.blobName("epoch-0", 1);
        byte[] fullBytes;
        try (InputStream in = blobContainer.readBlob(blobName)) {
            fullBytes = in.readAllBytes();
        }
        byte[] truncated = java.util.Arrays.copyOf(fullBytes, fullBytes.length / 2);
        blobContainer.writeBlob(blobName, new java.io.ByteArrayInputStream(truncated), truncated.length, false);

        List<Translog.Operation> operations = WalReplayRecovery.replayOperations(blobContainer, "idx", 0, 1, null, 2);

        assertEquals("only the good chunk's operation must be returned, not a thrown exception", 1, operations.size());
        assertEquals(0L, operations.get(0).seqNo());
    }

    // The same torn-chunk condition, but NOT at the end of the replay range -- a later, intact
    // chunk exists past it, so this is real corruption (data written after a corrupted chunk can't
    // exist unless the corrupted chunk itself was once valid and later damaged) and must fail loudly
    // rather than silently dropping a gap in the middle of the replayed sequence.
    public void testReplayFailsLoudlyOnATruncatedChunkThatIsNotTheLastOne() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0");

        Translog.Index op0 = new Translog.Index("doc0", 0, 1, "src0".getBytes("UTF-8"));
        Translog.Index op1 = new Translog.Index("doc1", 1, 1, "src1".getBytes("UTF-8"));
        Translog.Index op2 = new Translog.Index("doc2", 2, 1, "src2".getBytes("UTF-8"));

        service.append(new WalRecord("idx", 0, 1, 0, serialize(op0)));
        assertEquals(0, service.flush());
        service.append(new WalRecord("idx", 0, 1, 1, serialize(op1))); // this one gets torn below.
        assertEquals(1, service.flush());
        service.append(new WalRecord("idx", 0, 1, 2, serialize(op2)));
        assertEquals(2, service.flush()); // a good chunk AFTER the torn one.

        String blobName = WalChunkNaming.blobName("epoch-0", 1);
        byte[] fullBytes;
        try (InputStream in = blobContainer.readBlob(blobName)) {
            fullBytes = in.readAllBytes();
        }
        byte[] truncated = java.util.Arrays.copyOf(fullBytes, fullBytes.length / 2);
        blobContainer.writeBlob(blobName, new java.io.ByteArrayInputStream(truncated), truncated.length, false);

        expectThrows(WalFormatException.class, () -> WalReplayRecovery.replayOperations(blobContainer, "idx", 0, 1, null, 3));
    }

    /**
     * Fails the first {@code writeBlob} call, then delegates normally. It throws <em>before</em>
     * delegating, so the failed attempt writes nothing at all -- which is what makes it the right
     * fixture for a transient store error that an in-process retry recovers from, as opposed to a
     * process death (nothing retries at all then) or a torn write (nothing here produces one).
     */
    private static final class OneShotFailingOnWriteBlobContainer extends RegisterDelegatingBlobContainer {

        private boolean failed;

        OneShotFailingOnWriteBlobContainer(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new OneShotFailingOnWriteBlobContainer(child);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            if (failed == false) {
                failed = true;
                throw new IOException("injected kill-mid-WAL-chunk failure writing " + blobName);
            }
            super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }
    }

    /** Fails the first {@code failures} calls to {@code writeBlob}, then delegates normally. */
    private static final class FailFirstNWritesBlobContainer extends RegisterDelegatingBlobContainer {

        private final int failures;
        private int attempts;

        FailFirstNWritesBlobContainer(BlobContainer delegate, int failures) {
            super(delegate);
            this.failures = failures;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new FailFirstNWritesBlobContainer(child, failures);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            if (++attempts <= failures) {
                throw new IOException("injected failure writing " + blobName);
            }
            super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }
    }

    /** Fails any call to {@code listBlobsByPrefix} -- proves a range lookup never falls back to listing the whole container. */
    private static final class ListBlobsByPrefixForbiddenBlobContainer extends RegisterDelegatingBlobContainer {

        ListBlobsByPrefixForbiddenBlobContainer(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new ListBlobsByPrefixForbiddenBlobContainer(child);
        }

        @Override
        public java.util.Map<String, org.opensearch.common.blobstore.BlobMetadata> listBlobsByPrefix(String blobNamePrefix)
            throws IOException {
            throw new AssertionError("listBlobsByPrefix must never be called by a range-bounded chunk lookup");
        }
    }
}
