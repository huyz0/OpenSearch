/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.manifest;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Where one logical Lucene file lives: a byte range inside a named segment bundle. This is the
 * unit a {@link CommitManifest}'s file map is built from (rfc-serverless-opensearch.md &sect;6.3).
 */
public final class FileReference implements Writeable {

    private final String bundleName;
    private final long offset;
    private final long length;
    private final long checksum;

    /**
     * Creates a reference to a byte range within a segment bundle.
     *
     * @param bundleName the name of the segment bundle this file lives in
     * @param offset the byte offset of this file within the bundle
     * @param length the length of this file, in bytes
     * @param checksum the checksum of this file's contents
     */
    public FileReference(String bundleName, long offset, long length, long checksum) {
        this.bundleName = Objects.requireNonNull(bundleName, "bundleName");
        this.offset = offset;
        this.length = length;
        this.checksum = checksum;
    }

    /**
     * Deserializes a file reference previously written by {@link #writeTo}.
     *
     * @param in the stream to read from
     */
    public FileReference(StreamInput in) throws IOException {
        this(in.readString(), in.readVLong(), in.readVLong(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(bundleName);
        out.writeVLong(offset);
        out.writeVLong(length);
        out.writeLong(checksum);
    }

    /** The name of the segment bundle this file lives in. */
    public String bundleName() {
        return bundleName;
    }

    /** The byte offset of this file within the bundle. */
    public long offset() {
        return offset;
    }

    /** The length of this file, in bytes. */
    public long length() {
        return length;
    }

    /** The checksum of this file's contents. */
    public long checksum() {
        return checksum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FileReference)) return false;
        FileReference that = (FileReference) o;
        return offset == that.offset && length == that.length && checksum == that.checksum && bundleName.equals(that.bundleName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bundleName, offset, length, checksum);
    }

    @Override
    public String toString() {
        return "FileReference{bundle='" + bundleName + "', offset=" + offset + ", length=" + length + '}';
    }
}
