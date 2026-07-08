/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/** Parses a {@link WalChunkWriter}-produced chunk back into its ordered list of {@link WalRecord}s. */
public final class WalChunkReader {

    private WalChunkReader() {}

    public static List<WalRecord> readRecords(byte[] chunkBytes) throws WalFormatException {
        try {
            ByteArrayInputStream rawIn = new ByteArrayInputStream(chunkBytes);
            DataInputStream in = new DataInputStream(rawIn);

            byte[] magic = new byte[WalChunkWriter.MAGIC.length];
            in.readFully(magic);
            for (int i = 0; i < magic.length; i++) {
                if (magic[i] != WalChunkWriter.MAGIC[i]) {
                    throw new WalFormatException("not a WAL chunk: bad magic header");
                }
            }

            int version = in.readInt();
            if (version != WalChunkWriter.FORMAT_VERSION) {
                throw new WalFormatException("unsupported WAL chunk format version " + version);
            }

            int recordCount = in.readInt();
            if (recordCount < 0) {
                throw new WalFormatException("negative record count " + recordCount);
            }

            List<WalRecord> records = new ArrayList<>(recordCount);
            for (int i = 0; i < recordCount; i++) {
                int indexUuidLength = in.readUnsignedShort();
                byte[] indexUuidBytes = new byte[indexUuidLength];
                in.readFully(indexUuidBytes);
                String indexUuid = new String(indexUuidBytes, StandardCharsets.UTF_8);

                int shardId = in.readInt();
                long primaryTerm = in.readLong();
                long seqNo = in.readLong();

                int payloadLength = in.readInt();
                if (payloadLength < 0) {
                    throw new WalFormatException("negative payload length for record " + i);
                }
                byte[] payload = new byte[payloadLength];
                in.readFully(payload);

                records.add(new WalRecord(indexUuid, shardId, primaryTerm, seqNo, payload));
            }

            int bodyLength = chunkBytes.length - rawIn.available();
            long expectedChecksum = crc32c(chunkBytes, 0, bodyLength);
            long actualChecksum = in.readLong();
            if (actualChecksum != expectedChecksum) {
                throw new WalFormatException("WAL chunk checksum mismatch: corrupt or truncated chunk");
            }

            return records;
        } catch (EOFException e) {
            throw new WalFormatException("truncated WAL chunk", e);
        } catch (WalFormatException e) {
            // Already the specific exception (bad magic, checksum mismatch, etc.) -- rethrow
            // as-is. WalFormatException IS-A IOException, so without this clause the generic
            // catch below would re-wrap it into a useless generic message.
            throw e;
        } catch (IOException e) {
            throw new WalFormatException("failed to parse WAL chunk", e);
        }
    }

    /** Convenience filter for a reader/writer engine replaying only the operations for its own shard. */
    public static List<WalRecord> filterByShard(List<WalRecord> records, String indexUuid, int shardId) {
        List<WalRecord> filtered = new ArrayList<>();
        for (WalRecord record : records) {
            if (record.belongsTo(indexUuid, shardId)) {
                filtered.add(record);
            }
        }
        return filtered;
    }

    /**
     * A necessary, but on its own <b>not sufficient</b>, piece of replay fencing: excludes records
     * from a term that never validly held the lease at all. What it does <em>not</em> catch: a
     * writer N1 holding term T1, unaware its lease has already been reassigned to N2/T2 (paused,
     * slow GC, network delay before it next tries to publish and discovers the fencing), can keep
     * appending WAL records correctly tagged {@code term=T1} for a real window after T1 stopped
     * being current -- {@code WalChunkService#append} performs no fencing check of its own, it is
     * unconditional. Those late records are indistinguishable, by term alone, from N1's legitimate
     * pre-fencing writes: term-tagging says "written by whoever believed T1 was current," not
     * "written while T1 actually was current." A correct replay-time safety argument needs either
     * a real append-time fencing token (not implemented -- would need a check against the live
     * {@code ShardHead} on every append, in tension with this service's whole reason for existing:
     * cheap, unsynchronized buffering) or a different cutoff entirely, tied to the actual moment of
     * lease transfer rather than to term identity. Do not treat this method as a complete fencing
     * mechanism until that's resolved -- rfc-serverless-opensearch.md &sect;16 Phase 2 tracks this
     * as still open.
     */
    public static List<WalRecord> filterByShardAndMinimumTerm(List<WalRecord> records, String indexUuid, int shardId, long minPrimaryTerm) {
        List<WalRecord> filtered = new ArrayList<>();
        for (WalRecord record : records) {
            if (record.belongsTo(indexUuid, shardId) && record.primaryTerm() >= minPrimaryTerm) {
                filtered.add(record);
            }
        }
        return filtered;
    }

    private static long crc32c(byte[] data, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(data, offset, length);
        return crc.getValue();
    }
}
