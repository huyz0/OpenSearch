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

    public String name() {
        return name;
    }

    public long offset() {
        return offset;
    }

    public long length() {
        return length;
    }

    public long checksum() {
        return checksum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BundleFileEntry)) return false;
        BundleFileEntry that = (BundleFileEntry) o;
        return offset == that.offset && length == that.length && checksum == that.checksum && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, offset, length, checksum);
    }

    @Override
    public String toString() {
        return "BundleFileEntry{name='" + name + "', offset=" + offset + ", length=" + length + ", checksum=" + checksum + '}';
    }
}
