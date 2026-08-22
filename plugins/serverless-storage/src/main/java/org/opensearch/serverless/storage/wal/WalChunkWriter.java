/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.CRC32C;
import java.util.zip.CheckedOutputStream;

/**
 * Serializes a group-committed batch of {@link WalRecord}s from potentially many shards on one
 * node into a single chunk, per rfc-serverless-opensearch.md &sect;6.4. Unlike segment bundles
 * (read via ranged fetches), a WAL chunk is always read and replayed as a whole, so the format
 * uses one trailing checksum over the entire chunk rather than a separately-checksummed header.
 *
 * <p>Wire format (big-endian):
 * <pre>
 *   magic          4 bytes   = 'W','C','H','1'
 *   formatVersion  4 bytes   int, currently 2
 *   recordCount    4 bytes   int
 *   record[0..n)             repeated recordCount times, in append order:
 *     indexUuidLen 2 bytes   unsigned short
 *     indexUuid    variable  UTF-8 bytes
 *     shardId      4 bytes   int
 *     primaryTerm  8 bytes   long (added in format version 2, see WalRecord's own javadoc for why
 *                            fencing needs this under a shared, node-level writer epoch)
 *     seqNo        8 bytes   long
 *     payloadLen   4 bytes   int
 *     payload      variable  raw bytes (opaque -- possibly per-record ciphertext)
 *   chunkChecksum  8 bytes   long, CRC32C of every byte written above
 * </pre>
 */
public final class WalChunkWriter {

    static final byte[] MAGIC = { 'W', 'C', 'H', '1' };
    static final int FORMAT_VERSION = 2;

    private WalChunkWriter() {}

    /**
     * Serializes {@code records} into a single chunk blob's bytes, per this class's wire format.
     *
     * <p>Computes the trailing checksum incrementally as the body is written (via {@link
     * CheckedOutputStream}) rather than in a separate pass over a fully-materialized copy of the
     * body -- for a large group-commit batch, re-copying the whole body just to checksum it would
     * double the allocation/copy cost of every flush for no reason; {@link ByteArrayOutputStream
     * #toByteArray()} is called exactly once, at the very end, to hand back the final result.
     *
     * @param records the records to serialize, in append order
     * @return the serialized chunk bytes, including header, records, and trailing checksum
     */
    public static byte[] write(List<WalRecord> records) {
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            CRC32C crc = new CRC32C();
            DataOutputStream out = new DataOutputStream(new CheckedOutputStream(buf, crc));

            out.write(MAGIC);
            out.writeInt(FORMAT_VERSION);
            out.writeInt(records.size());

            for (WalRecord record : records) {
                byte[] indexUuidBytes = record.indexUuid().getBytes(StandardCharsets.UTF_8);
                if (indexUuidBytes.length > 0xFFFF) {
                    throw new IllegalArgumentException("indexUuid too long: " + record.indexUuid());
                }
                out.writeShort(indexUuidBytes.length);
                out.write(indexUuidBytes);
                out.writeInt(record.shardId());
                out.writeLong(record.primaryTerm());
                out.writeLong(record.seqNo());
                out.writeInt(record.payload().length);
                out.write(record.payload());
            }
            out.flush();

            // The checksum itself must not be covered by its own value -- written directly to
            // `buf`, bypassing the checked stream above.
            DataOutputStream trailer = new DataOutputStream(buf);
            trailer.writeLong(crc.getValue());
            trailer.flush();

            return buf.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
