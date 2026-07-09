/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import java.util.Objects;

/**
 * Describes one logical file packed into a {@link SegmentBundle}: its name, the byte range it
 * occupies in the bundle body (relative to the end of the bundle header), and a CRC32C checksum
 * of its raw bytes so corruption is detected on read rather than handed silently to Lucene.
 */
public final class BundleFileEntry {

    private final String name;
    private final long offset;
    private final long length;
    private final long checksum;

    /**
     * Describes one packed file's location and integrity checksum within a bundle.
     *
     * @param name the file's logical name within the bundle.
     * @param offset the byte offset of the file's content, relative to the end of the bundle header.
     * @param length the length in bytes of the file's content.
     * @param checksum the CRC32C checksum of the file's raw bytes.
     */
    public BundleFileEntry(String name, long offset, long length, long checksum) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0, got " + offset);
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0, got " + length);
        }
        this.name = Objects.requireNonNull(name, "name");
        this.offset = offset;
        this.length = length;
        this.checksum = checksum;
    }

    /** The file's logical name within the bundle. */
    public String name() {
        return name;
    }

    /** The byte offset of the file's content, relative to the end of the bundle header. */
    public long offset() {
        return offset;
    }

    /** The length in bytes of the file's content. */
    public long length() {
        return length;
    }

    /** The CRC32C checksum of the file's raw bytes. */
    public long checksum() {
        return checksum;
    }

    /** @param o the object to compare against. */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BundleFileEntry)) return false;
        BundleFileEntry that = (BundleFileEntry) o;
        return offset == that.offset && length == that.length && checksum == that.checksum && name.equals(that.name);
    }

    /** Consistent with {@link #equals}. */
    @Override
    public int hashCode() {
        return Objects.hash(name, offset, length, checksum);
    }

    /** Diagnostic form only, not a wire format. */
    @Override
    public String toString() {
        return "BundleFileEntry{name='" + name + "', offset=" + offset + ", length=" + length + ", checksum=" + checksum + '}';
    }
}
