/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

public class WalChunkWriterReaderTests extends OpenSearchTestCase {

    public void testRoundTripSingleRecord() throws Exception {
        WalRecord record = new WalRecord("index-a", 0, 1, 1, randomByteArrayOfLength(128));
        byte[] chunk = WalChunkWriter.write(List.of(record));

        List<WalRecord> parsed = WalChunkReader.readRecords(chunk);
        assertEquals(1, parsed.size());
        assertEquals(record, parsed.get(0));
    }

    // The core scenario the format exists for: one node's WAL service group-commits operations
    // from many different shards (rfc-serverless-opensearch.md section 6.4) into a single chunk.
    public void testGroupCommitAcrossManyShardsPreservesOrderAndPerShardFiltering() throws Exception {
        List<WalRecord> written = new ArrayList<>();
        for (int shard = 0; shard < 5; shard++) {
            for (int seq = 0; seq < 10; seq++) {
                written.add(new WalRecord("index-" + (shard % 2), shard, 1, seq, randomByteArrayOfLength(randomIntBetween(0, 256))));
            }
        }

        byte[] chunk = WalChunkWriter.write(written);
        List<WalRecord> parsed = WalChunkReader.readRecords(chunk);

        assertEquals(written, parsed); // exact order preserved, not just set-equality

        List<WalRecord> shard3Only = WalChunkReader.filterByShard(parsed, "index-1", 3);
        assertEquals(10, shard3Only.size());
        for (WalRecord record : shard3Only) {
            assertTrue(record.belongsTo("index-1", 3));
        }
    }

    public void testEmptyPayloadRecordRoundTrips() throws Exception {
        WalRecord record = new WalRecord("idx", 0, 1, 0, new byte[0]);
        byte[] chunk = WalChunkWriter.write(List.of(record));
        List<WalRecord> parsed = WalChunkReader.readRecords(chunk);
        assertEquals(List.of(record), parsed);
    }

    public void testEmptyChunkOfZeroRecordsRoundTrips() throws Exception {
        byte[] chunk = WalChunkWriter.write(List.of());
        assertEquals(List.of(), WalChunkReader.readRecords(chunk));
    }

    // Regression test for a real bug: WalFormatException extends IOException, so an explicit
    // `throw new WalFormatException(...)` inside the same try block as a trailing
    // `catch (IOException e)` was re-caught by that generic clause and re-wrapped into an
    // unhelpful "failed to parse WAL chunk" message, destroying the specific diagnosis.
    public void testCorruptedChunkFailsChecksumVerificationWithSpecificMessage() {
        WalRecord record = new WalRecord("idx", 0, 1, 0, randomByteArrayOfLength(64));
        byte[] chunk = WalChunkWriter.write(List.of(record));
        byte[] corrupted = chunk.clone();
        corrupted[20] ^= 0xFF; // last byte of the record's 4-byte shardId field, inside the header

        WalFormatException e = expectThrows(WalFormatException.class, () -> WalChunkReader.readRecords(corrupted));
        assertTrue(e.getMessage(), e.getMessage().contains("checksum mismatch"));
    }

    public void testTruncatedChunkIsRejectedWithSpecificMessage() {
        WalRecord record = new WalRecord("idx", 0, 1, 0, randomByteArrayOfLength(64));
        byte[] chunk = WalChunkWriter.write(List.of(record));
        byte[] truncated = new byte[chunk.length - 10];
        System.arraycopy(chunk, 0, truncated, 0, truncated.length);

        WalFormatException e = expectThrows(WalFormatException.class, () -> WalChunkReader.readRecords(truncated));
        assertTrue(e.getMessage(), e.getMessage().contains("truncated"));
    }

    // Regression test for a real bug: recordCount was used to presize an ArrayList before the
    // trailing checksum was ever verified, with only a `< 0` check -- a corrupted recordCount
    // field (magic/version intact) could pre-size an absurdly large allocation instead of failing
    // closed with a clean WalFormatException.
    public void testImpossiblyLargeRecordCountIsRejectedBeforeAllocatingAnything() {
        WalRecord record = new WalRecord("idx", 0, 1, 0, randomByteArrayOfLength(64));
        byte[] chunk = WalChunkWriter.write(List.of(record));
        byte[] corrupted = chunk.clone();
        // recordCount is the 4-byte int immediately after the 4-byte magic + 4-byte version header,
        // i.e. bytes [8, 12). Overwrite it with a huge, clearly-impossible value.
        corrupted[8] = 0x7F;
        corrupted[9] = (byte) 0xFF;
        corrupted[10] = (byte) 0xFF;
        corrupted[11] = (byte) 0xFF;

        WalFormatException e = expectThrows(WalFormatException.class, () -> WalChunkReader.readRecords(corrupted));
        assertTrue(e.getMessage(), e.getMessage().contains("impossibly large"));
    }

    public void testNotAWalChunkIsRejectedWithSpecificMessage() {
        WalFormatException e = expectThrows(WalFormatException.class, () -> WalChunkReader.readRecords(randomByteArrayOfLength(100)));
        assertTrue(e.getMessage(), e.getMessage().contains("bad magic header"));
    }

    public void testBlobNamingIncludesWriterEpochAndSequence() {
        assertEquals("wal/epoch-7/log-42", WalChunkNaming.blobPath("epoch-7", 42));
        assertEquals("log-42", WalChunkNaming.blobName("epoch-7", 42));
    }
}
