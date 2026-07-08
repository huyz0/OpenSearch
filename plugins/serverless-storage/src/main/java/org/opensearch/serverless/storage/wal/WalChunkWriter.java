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

    public static byte[] write(List<WalRecord> records) {
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buf);

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

            CRC32C crc = new CRC32C();
            byte[] bodyBytes = buf.toByteArray();
            crc.update(bodyBytes);
            out.writeLong(crc.getValue());
            out.flush();

            return buf.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
