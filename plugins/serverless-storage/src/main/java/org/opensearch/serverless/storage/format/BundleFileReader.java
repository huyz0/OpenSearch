/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import java.io.IOException;

/**
 * The narrow read surface {@link org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer}
 * actually needs from a bundle store: fetch one file's checksum-verified bytes out of a named
 * bundle. Extracted as an interface (rather than depending on {@link BlobContainerBundleStore}
 * directly) so a caching layer -- {@link LocalDiskCachingBundleStore} -- can sit in front of the
 * real object-store-backed implementation without the materializer needing to know the
 * difference (rfc-serverless-opensearch.md &sect;9's "directory tier").
 */
public interface BundleFileReader {

    /**
     * Fetches one file's checksum-verified bytes out of a named bundle.
     *
     * @param bundleName the name of the bundle blob containing the file.
     * @param entry the file's location and expected checksum within that bundle.
     * @return the file's raw bytes, verified against {@code entry}'s checksum.
     * @throws IOException if the bundle cannot be read or the checksum does not match.
     */
    byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException;

    /**
     * Streams one file's bytes out of a named bundle, without ever holding the whole file in heap.
     *
     * <p><b>This exists because {@link #readFile}'s {@code byte[]} return type is a hard product
     * ceiling, not merely an inefficiency.</b> A {@code byte[]} cannot exceed {@code
     * Integer.MAX_VALUE}, so a single Lucene file larger than 2&nbsp;GiB -- which
     * {@code CompactionPolicy}'s target bundle size explicitly permits compaction to produce -- can
     * never be read back by anything, and the shard becomes permanently unopenable and
     * uncompactable with no recovery path short of hand-editing the manifest. On top of that, the
     * eager materialization path used to allocate the same file three or four times over
     * concurrently (ranged read, extract copy, decrypt copy, cache copy), none of it accounted
     * against a circuit breaker.
     *
     * <p><b>The returned stream is not checksum-verified by the callee.</b> {@link #readFile}
     * verifies the whole file's CRC32C before returning, which is only possible because it has all
     * the bytes at once. A streaming caller must compute {@code entry.checksum()} incrementally as
     * it consumes and fail if it does not match at the end -- {@code
     * ObjectStoreCommitMaterializer} does exactly that. Handing back an unverified stream and
     * documenting the obligation is the honest trade for removing the ceiling; silently returning
     * unverified bytes from a method named like the verified one would not be.
     *
     * <p>The default implementation simply wraps {@link #readFile}, so every existing caching and
     * fallback layer keeps working unchanged (and keeps its verification): only implementations
     * that can genuinely stream -- {@link BlobContainerBundleStore}, which already has a ranged
     * {@code openRange} -- need to override it.
     *
     * @param bundleName the name of the bundle blob containing the file.
     * @param entry the file's location and expected length within that bundle.
     * @return a stream over exactly this file's bytes; the caller closes it.
     * @throws IOException if the bundle cannot be read.
     */
    default java.io.InputStream openFile(String bundleName, BundleFileEntry entry) throws IOException {
        return new java.io.ByteArrayInputStream(readFile(bundleName, entry));
    }
}
