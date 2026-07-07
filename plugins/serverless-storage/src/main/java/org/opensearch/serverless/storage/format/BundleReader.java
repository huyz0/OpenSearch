/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses a {@link SegmentBundle} header and extracts individual file contents, verifying
 * checksums so a corrupted or truncated bundle fails closed rather than handing bad bytes to
 * Lucene (rfc-serverless-opensearch.md &sect;17, "must fail closed").
 */
public final class BundleReader {

    private BundleReader() {}

    /**
     * Parses the header at the start of {@code bundleBytes}. The array may be the full bundle or
     * just a prefix, as long as it contains at least the whole header.
     */
    public static BundleHeader parseHeader(byte[] bundleBytes) throws BundleFormatException {
        try {
            ByteArrayInputStream rawIn = new ByteArrayInputStream(bundleBytes);
            DataInputStream in = new DataInputStream(rawIn);

            byte[] magic = new byte[BundleWriter.MAGIC.length];
            in.readFully(magic);
            for (int i = 0; i < magic.length; i++) {
                if (magic[i] != BundleWriter.MAGIC[i]) {
                    throw new BundleFormatException("not a segment bundle: bad magic header");
                }
            }

            int version = in.readInt();
            if (version != BundleWriter.FORMAT_VERSION) {
                throw new BundleFormatException("unsupported bundle format version " + version);
            }

            int entryCount = in.readInt();
            if (entryCount < 0) {
                throw new BundleFormatException("negative entry count " + entryCount);
            }

            Map<String, BundleFileEntry> entries = new LinkedHashMap<>(entryCount);
            long offset = 0; // relative to end of header; converted to absolute once headerLength is known
            long[] lengths = new long[entryCount];
            long[] checksums = new long[entryCount];
            String[] names = new String[entryCount];

            for (int i = 0; i < entryCount; i++) {
                int nameLength = in.readUnsignedShort();
                byte[] nameBytes = new byte[nameLength];
                in.readFully(nameBytes);
                names[i] = new String(nameBytes, StandardCharsets.UTF_8);
                lengths[i] = in.readLong();
                if (lengths[i] < 0) {
                    throw new BundleFormatException("negative length for file '" + names[i] + "'");
                }
                checksums[i] = in.readLong();
            }

            int headerLengthWithoutTrailer = bundleBytes.length - rawIn.available();
            long expectedHeaderChecksum = BundleWriter.checksum(bundleBytes, 0, headerLengthWithoutTrailer);
            long actualHeaderChecksum = in.readLong();
            if (actualHeaderChecksum != expectedHeaderChecksum) {
                throw new BundleFormatException("bundle header checksum mismatch: corrupt or truncated header");
            }

            int headerLength = bundleBytes.length - rawIn.available();
            offset = headerLength;
            for (int i = 0; i < entryCount; i++) {
                BundleFileEntry entry = new BundleFileEntry(names[i], offset, lengths[i], checksums[i]);
                if (entries.put(names[i], entry) != null) {
                    throw new BundleFormatException("duplicate file name in bundle header: " + names[i]);
                }
                offset += lengths[i];
            }

            return new BundleHeader(headerLength, entries);
        } catch (EOFException e) {
            throw new BundleFormatException("truncated bundle header", e);
        } catch (BundleFormatException e) {
            // Already the specific exception (bad magic, checksum mismatch, duplicate name,
            // etc.) -- rethrow as-is. BundleFormatException IS-A IOException, so without this
            // clause the generic catch below would re-wrap it into a useless generic message.
            throw e;
        } catch (IOException e) {
            throw new BundleFormatException("failed to parse bundle header", e);
        }
    }

    /**
     * Extracts and checksum-verifies one file's content from the full bundle bytes.
     *
     * @throws BundleFormatException if the entry's byte range falls outside the array, or the
     *                                extracted bytes do not match the entry's stored checksum.
     */
    public static byte[] extractFile(byte[] bundleBytes, BundleFileEntry entry) throws BundleFormatException {
        long endExclusive = entry.offset() + entry.length();
        if (entry.offset() < 0 || entry.length() < 0 || endExclusive > bundleBytes.length || endExclusive < entry.offset()) {
            throw new BundleFormatException(
                "file '"
                    + entry.name()
                    + "' range ["
                    + entry.offset()
                    + ","
                    + endExclusive
                    + ") is outside bundle of length "
                    + bundleBytes.length
            );
        }
        if (entry.length() > Integer.MAX_VALUE) {
            throw new BundleFormatException("file '" + entry.name() + "' is too large to extract in one call: " + entry.length());
        }
        byte[] content = new byte[(int) entry.length()];
        System.arraycopy(bundleBytes, (int) entry.offset(), content, 0, content.length);

        long actualChecksum = BundleWriter.checksum(content);
        if (actualChecksum != entry.checksum()) {
            throw new BundleFormatException(
                "checksum mismatch for file '" + entry.name() + "': expected " + entry.checksum() + " but computed " + actualChecksum
            );
        }
        return content;
    }
}
