/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32C;

/**
 * Packs the new Lucene files of one or more commits into a single immutable
 * {@link SegmentBundle} blob, per rfc-serverless-opensearch.md &sect;6.2.
 *
 * <p>Wire format (all multi-byte integers big-endian, via {@link DataOutputStream}):
 * <pre>
 *   magic            4 bytes   = 'S','B','L','1'
 *   formatVersion    4 bytes   int, currently 1
 *   entryCount       4 bytes   int
 *   entry[0..n)                repeated entryCount times, in bundle order:
 *     nameLength     2 bytes   unsigned short, UTF-8 byte length of the file name
 *     name           variable  UTF-8 bytes
 *     length         8 bytes   long, byte length of the file's content
 *     checksum       8 bytes   long, CRC32C of the file's content (unsigned 32-bit value)
 *   headerChecksum   8 bytes   long, CRC32C of every header byte written above
 *   body                       concatenated file contents, in the same order as the entries
 * </pre>
 * File offsets are not stored explicitly &mdash; they are the cumulative sum of preceding file
 * lengths, starting immediately after the header. This makes the header self-consistent by
 * construction: there is no way to encode an offset that disagrees with the lengths.
 */
public final class BundleWriter {

    static final byte[] MAGIC = { 'S', 'B', 'L', '1' };
    static final int FORMAT_VERSION = 1;

    private BundleWriter() {}

    /**
     * Packs the given files into a single immutable bundle blob.
     *
     * @param files the files to pack, in the order they should appear in the bundle.
     * @return the packed bundle, with its parsed file-entry map.
     */
    public static SegmentBundle write(List<BundleFileContent> files) {
        try {
            ByteArrayOutputStream headerBuf = new ByteArrayOutputStream();
            DataOutputStream header = new DataOutputStream(headerBuf);
            header.write(MAGIC);
            header.writeInt(FORMAT_VERSION);
            header.writeInt(files.size());

            for (BundleFileContent file : files) {
                byte[] nameBytes = file.name().getBytes(StandardCharsets.UTF_8);
                if (nameBytes.length > 0xFFFF) {
                    throw new IllegalArgumentException("file name too long: " + file.name());
                }
                header.writeShort(nameBytes.length);
                header.write(nameBytes);
                header.writeLong(file.content().length);
                header.writeLong(checksum(file.content()));
            }
            header.flush();

            long headerChecksum = checksum(headerBuf.toByteArray());
            header.writeLong(headerChecksum);
            header.flush();

            byte[] headerBytes = headerBuf.toByteArray();
            long bodyLength = files.stream().mapToLong(f -> f.content().length).sum();
            if (headerBytes.length + bodyLength > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("bundle too large: " + (headerBytes.length + bodyLength) + " bytes");
            }

            byte[] bundleBytes = new byte[(int) (headerBytes.length + bodyLength)];
            System.arraycopy(headerBytes, 0, bundleBytes, 0, headerBytes.length);

            Map<String, BundleFileEntry> entries = new LinkedHashMap<>();
            long offset = headerBytes.length;
            for (BundleFileContent file : files) {
                System.arraycopy(file.content(), 0, bundleBytes, (int) offset, file.content().length);
                entries.put(file.name(), new BundleFileEntry(file.name(), offset, file.content().length, checksum(file.content())));
                offset += file.content().length;
            }

            return new SegmentBundle(bundleBytes, entries);
        } catch (IOException e) {
            // ByteArrayOutputStream/DataOutputStream never actually throw for in-memory buffers;
            // surfacing as unchecked keeps this a pure, exception-free-in-practice API.
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Computes the CRC32C checksum of an entire byte array.
     *
     * @param data the bytes to checksum.
     * @return the CRC32C checksum, as an unsigned 32-bit value.
     */
    static long checksum(byte[] data) {
        CRC32C crc = new CRC32C();
        crc.update(data);
        return crc.getValue();
    }

    /**
     * Computes the CRC32C checksum of a byte range.
     *
     * @param data the array containing the bytes to checksum.
     * @param offset the start offset of the range within {@code data}.
     * @param length the length in bytes of the range.
     * @return the CRC32C checksum, as an unsigned 32-bit value.
     */
    static long checksum(byte[] data, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(data, offset, length);
        return crc.getValue();
    }
}
