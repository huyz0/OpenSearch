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
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

/**
 * A property-based upgrade over this plugin's existing WAL replay coverage, which so far is entirely
 * example-based (hand-picked scenarios in {@code WalReplayRecoveryTests}). Checks {@code
 * WalReplayRecovery.replayOperations} against a real, independently-computed oracle across many
 * randomized WAL histories -- the Java counterpart of {@code formal/WalReplayFencing.tla}'s verified
 * {@code FixedReplay} design (bound by <em>both</em> a term floor <em>and</em> a chunk-sequence
 * cutoff, never by either alone): a real WAL history randomly interleaves records from several
 * simulated writer "incarnations" (increasing primary terms, not necessarily arriving in term order
 * across chunks -- the exact "stale writer keeps appending after being superseded" shape {@code
 * WalReplayFencing.tla}'s counterexample models), and this fuzzer asserts replay returns <em>exactly</em>
 * the records that satisfy both bounds, for many random {@code (minPrimaryTerm, fromChunkSequence,
 * activationWalPosition)} combinations -- not just the one or two scenarios a hand-written test
 * happened to construct.
 *
 * <p>This is a real regression guard against relaxing either bound: e.g. a future refactor that
 * accidentally drops the position cutoff and relies on the term filter alone would silently
 * reintroduce {@code WalReplayFencing.tla}'s already-proven-unsound {@code NaiveReplay} design --
 * this fuzzer catches that the moment any trial happens to construct the counterexample shape
 * (a stale writer's post-fencing append), which random generation across enough trials reliably does.
 */
public class WalReplayFencingPropertyFuzzTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "wal-fuzz-idx";
    private static final int SHARD_ID = 0;
    private static final int TRIAL_COUNT = 20;
    private static final int CHUNK_COUNT = 15;
    private static final int MAX_RECORDS_PER_CHUNK = 4;
    private static final int MAX_TERM = 5;
    private static final int COMBOS_PER_TRIAL = 15;

    private record GroundTruthRecord(long chunkSequence, long primaryTerm, long seqNo) {}

    public void testReplayReturnsExactlyTheRecordsSatisfyingBothTheTermFloorAndThePositionCutoff() throws Exception {
        for (int trial = 0; trial < TRIAL_COUNT; trial++) {
            long seed = randomLong();
            try {
                runOneTrial(new Random(seed));
            } catch (AssertionError | Exception e) {
                throw new AssertionError("fuzz trial failed with seed=" + seed + " (rerun this seed to reproduce)", e);
            }
        }
    }

    private void runOneTrial(Random random) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer container = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        WalChunkService service = new WalChunkService(container, "fuzz-epoch-" + random.nextInt());

        List<GroundTruthRecord> groundTruth = new ArrayList<>();
        long[] chunkSequences = new long[CHUNK_COUNT];
        long nextSeqNo = 0;

        for (int chunkIndex = 0; chunkIndex < CHUNK_COUNT; chunkIndex++) {
            int recordCount = 1 + random.nextInt(MAX_RECORDS_PER_CHUNK);
            // Terms are drawn independently per record, not monotonically increasing with chunk
            // index -- this is what lets a "stale writer still appending after being superseded"
            // interleaving actually occur across the simulated history, the exact shape
            // WalReplayFencing.tla's counterexample needs.
            long[] termsThisChunk = new long[recordCount];
            long[] seqNosThisChunk = new long[recordCount];
            for (int r = 0; r < recordCount; r++) {
                long term = 1 + random.nextInt(MAX_TERM);
                long seqNo = nextSeqNo++;
                termsThisChunk[r] = term;
                seqNosThisChunk[r] = seqNo;
                service.append(toWalRecord(term, seqNo));
            }
            long chunkSequence = service.flush();
            assertTrue("test setup: flush must return a real chunk sequence", chunkSequence >= 0);
            chunkSequences[chunkIndex] = chunkSequence;
            for (int r = 0; r < recordCount; r++) {
                groundTruth.add(new GroundTruthRecord(chunkSequence, termsThisChunk[r], seqNosThisChunk[r]));
            }
        }

        for (int combo = 0; combo < COMBOS_PER_TRIAL; combo++) {
            long minPrimaryTerm = 1 + random.nextInt(MAX_TERM + 1); // may exceed MAX_TERM, excluding everything
            int fromChunkIndex = random.nextInt(CHUNK_COUNT);
            int uptoChunkIndex = fromChunkIndex + random.nextInt(CHUNK_COUNT - fromChunkIndex + 1);
            long fromChunkSequenceInclusive = chunkSequences[fromChunkIndex];
            long activationWalPosition = uptoChunkIndex < CHUNK_COUNT ? chunkSequences[uptoChunkIndex] : chunkSequences[CHUNK_COUNT - 1] + 1;

            WalPosition lastDurable = fromChunkSequenceInclusive == 0 ? null : new WalPosition("fuzz-epoch", fromChunkSequenceInclusive - 1);

            List<Translog.Operation> replayed = WalReplayRecovery.replayOperations(
                container,
                INDEX_UUID,
                SHARD_ID,
                minPrimaryTerm,
                lastDurable,
                activationWalPosition
            );
            Set<Long> actualSeqNos = new HashSet<>();
            for (Translog.Operation op : replayed) {
                actualSeqNos.add(op.seqNo());
            }

            Set<Long> expectedSeqNos = new HashSet<>();
            for (GroundTruthRecord record : groundTruth) {
                boolean inRange = record.chunkSequence() >= fromChunkSequenceInclusive && record.chunkSequence() < activationWalPosition;
                boolean meetsTermFloor = record.primaryTerm() >= minPrimaryTerm;
                if (inRange && meetsTermFloor) {
                    expectedSeqNos.add(record.seqNo());
                }
            }

            assertEquals(
                String.format(
                    Locale.ROOT,
                    "replay must return exactly the records satisfying both the term floor (>=%d) and the "
                        + "position cutoff [%d, %d) -- neither alone",
                    minPrimaryTerm,
                    fromChunkSequenceInclusive,
                    activationWalPosition
                ),
                expectedSeqNos,
                actualSeqNos
            );
        }
    }

    private static WalRecord toWalRecord(long primaryTerm, long seqNo) throws Exception {
        Translog.NoOp op = new Translog.NoOp(seqNo, primaryTerm, "fuzz");
        BytesStreamOutput out = new BytesStreamOutput();
        Translog.Operation.writeOperation(out, op);
        return new WalRecord(INDEX_UUID, SHARD_ID, primaryTerm, seqNo, org.opensearch.core.common.bytes.BytesReference.toBytes(out.bytes()));
    }
}
