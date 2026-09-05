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
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

/**
 * Regression tests for report finding D6: the corrupt-tail heuristic in {@link WalReplayRecovery}
 * used to fire for whatever chunk happened to be the last one <em>present</em> in the replay range,
 * logging a WARN and silently dropping its acknowledged operations. It now fires only for the true
 * final sequence of the fenced range, and reports the truncation to the caller instead of burying it.
 */
public class WalReplayRecoveryCorruptTailTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "replay-idx";
    private static final int SHARD_ID = 0;

    private BlobContainer blobContainer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    private void writeChunk(long chunkSequence, long seqNo) throws IOException {
        Translog.Index op = new Translog.Index(String.valueOf(seqNo), seqNo, 1L, ("doc-" + seqNo).getBytes("UTF-8"));
        BytesStreamOutput out = new BytesStreamOutput();
        Translog.Operation.writeOperation(out, op);
        byte[] chunk = WalChunkWriter.write(List.of(new WalRecord(INDEX_UUID, SHARD_ID, 1L, seqNo, BytesReference.toBytes(out.bytes()))));
        blobContainer.writeBlob(WalChunkNaming.blobName("epoch-0", chunkSequence), new ByteArrayInputStream(chunk), chunk.length, false);
    }

    private void writeCorruptChunk(long chunkSequence) throws IOException {
        // Valid magic and version so the reader gets past those and fails on the checksum, exactly
        // like a real bit flip in a chunk body would.
        final byte[] chunk = WalChunkWriter.write(List.of(new WalRecord(INDEX_UUID, SHARD_ID, 1L, 999L, new byte[] { 1, 2, 3, 4 })));
        chunk[chunk.length - 1] ^= 0x7F;
        blobContainer.writeBlob(WalChunkNaming.blobName("epoch-0", chunkSequence), new ByteArrayInputStream(chunk), chunk.length, false);
        // Sanity: this really is unreadable, so the tests below are exercising the branch they claim.
        expectThrows(WalFormatException.class, () -> WalChunkReader.readRecords(chunk));
    }

    /**
     * A hole in the sequence space -- which a failed write used to leave behind on every retry --
     * made a mid-stream corrupt chunk look like "the last chunk in this replay range" to the old
     * heuristic, so it was tolerated and everything from it onward was dropped. It must fail loudly:
     * chunk 5 is not the final sequence of the fenced range (which ends at 8), and it is a chunk that
     * later chunks were written after.
     */
    public void testACorruptChunkThatIsNotTheFinalSequenceOfTheRangeFailsLoudly() throws Exception {
        writeChunk(0, 0);
        writeCorruptChunk(5);
        // Sequences 1-4 and 6-7 are holes: no blob exists for them, so listChunkSequencesInRange
        // returns exactly [0, 5] and chunk 5 is the LAST EXISTING one -- which is precisely what the
        // old heuristic keyed off.
        expectThrows(WalFormatException.class, () -> WalReplayRecovery.replayOperations(blobContainer, INDEX_UUID, SHARD_ID, 0L, null, 8L));
    }

    /**
     * The genuinely-tolerable case: the corrupt chunk IS the final sequence of the fenced range, i.e.
     * the only sequence a predecessor could still have been mid-write on when this writer snapshotted
     * its bound. Replay stops there, keeps everything before it, and -- the part that used to be
     * missing entirely -- tells the caller the recovery is incomplete.
     */
    public void testACorruptFinalChunkStopsReplayAndReportsAnIncompleteRecovery() throws Exception {
        writeChunk(0, 0);
        writeChunk(1, 1);
        writeCorruptChunk(2);

        WalReplayRecovery.ReplayResult result = WalReplayRecovery.replayOperationsWithReport(
            blobContainer,
            INDEX_UUID,
            SHARD_ID,
            0L,
            null,
            3L,
            null
        );
        assertEquals("everything before the corrupt tail must still replay", 2, result.operations().size());
        assertTrue("the caller must be told this recovery dropped acknowledged operations", result.isIncomplete());
        assertEquals(2L, result.truncatedTailChunkSequence());
    }

    /** A clean range reports a complete recovery -- proof {@code isIncomplete} is a real signal, not always true. */
    public void testACleanRangeReportsACompleteRecovery() throws Exception {
        writeChunk(0, 0);
        writeChunk(1, 1);

        WalReplayRecovery.ReplayResult result = WalReplayRecovery.replayOperationsWithReport(
            blobContainer,
            INDEX_UUID,
            SHARD_ID,
            0L,
            null,
            2L,
            null
        );
        assertEquals(2, result.operations().size());
        assertFalse(result.isIncomplete());
        assertEquals(-1L, result.truncatedTailChunkSequence());
    }
}
