/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import org.opensearch.common.blobstore.BlobContainer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Persists and retrieves {@link SegmentBundle}s against a real {@link BlobContainer}
 * (rfc-serverless-opensearch.md &sect;6.1/&sect;6.2). This is the seam between the
 * storage-backend-agnostic bundle format and an actual repository (FS, S3, GCS, Azure, ...);
 * the format itself never depends on {@link BlobContainer} so it stays trivially unit-testable
 * in memory, while this class carries the I/O.
 */
public final class BlobContainerBundleStore implements BundleFileReader {

    private final BlobContainer blobContainer;

    public BlobContainerBundleStore(BlobContainer blobContainer) {
        this.blobContainer = blobContainer;
    }

    /**
     * Packs {@code files} into one bundle and uploads it under {@code bundleName} in a single
     * write. Uses {@link BlobContainer#writeBlobAtomic} rather than plain {@code writeBlob}:
     * bundles must be atomically visible-or-absent, never observable half-written, since readers
     * fetch byte ranges out of them concurrently with no coordination (rfc-serverless-opensearch.md
     * &sect;6.2).
     */
    public SegmentBundle writeBundle(String bundleName, List<BundleFileContent> files) throws IOException {
        SegmentBundle bundle = BundleWriter.write(files);
        try (InputStream in = new ByteArrayInputStream(bundle.bytes())) {
            blobContainer.writeBlobAtomic(bundleName, in, bundle.length(), true);
        }
        return bundle;
    }

    /**
     * Fetches and parses just the header of a previously written bundle. Since the header length
     * isn't known up front, this reads a generous prefix (default 64&nbsp;KiB, which comfortably
     * covers realistic segment counts per bundle) and re-fetches with the exact size if the
     * header turns out to be larger than the prefix &mdash; a bundle with a header that large
     * would be a pathological outlier (tens of thousands of files in one bundle).
     */
    public BundleHeader readHeader(String bundleName, long bundleLength) throws IOException {
        long prefixLength = Math.min(bundleLength, 64L * 1024);
        byte[] prefix = readRange(bundleName, 0, prefixLength);
        try {
            return BundleReader.parseHeader(prefix);
        } catch (BundleFormatException tooShort) {
            if (prefixLength >= bundleLength) {
                throw tooShort;
            }
            // Header didn't fit in the prefix (or the bundle itself is small enough that the
            // prefix already covered it and parsing genuinely failed) -- retry the header parse
            // against the whole object rather than guessing a bigger prefix size.
            byte[] whole = readRange(bundleName, 0, bundleLength);
            return BundleReader.parseHeader(whole);
        }
    }

    /** Fetches and checksum-verifies exactly the bytes of one file described by {@code entry}. */
    @Override
    public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
        byte[] raw = readRange(bundleName, entry.offset(), entry.length());
        // extractFile expects the entry's offset to be relative to the start of the array it's
        // given; since we ranged-fetched exactly [offset, offset+length), rebase the entry to 0.
        BundleFileEntry rebased = new BundleFileEntry(entry.name(), 0, entry.length(), entry.checksum());
        return BundleReader.extractFile(raw, rebased);
    }

    private byte[] readRange(String bundleName, long position, long length) throws IOException {
        if (length > Integer.MAX_VALUE) {
            throw new IOException("range too large to read in one call: " + length + " bytes");
        }
        try (InputStream in = blobContainer.readBlob(bundleName, position, length)) {
            return in.readAllBytes();
        }
    }
}
