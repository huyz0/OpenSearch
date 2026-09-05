/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A bundle that has been <em>planned</em> but not materialized: its header bytes and its per-file
 * entry map are fully computed, and its body is produced on demand by streaming each file through
 * in order.
 *
 * <p><b>Why this exists.</b> {@code BundleWriter.write} used to allocate one
 * {@code byte[header + every file body concatenated]} on top of the caller's own per-file arrays,
 * which meant a second full copy of the whole commit in heap on the flush thread, outside any
 * circuit breaker -- and, worse, a hard ceiling: {@code byte[]} cannot exceed {@code
 * Integer.MAX_VALUE}, so a commit whose total exceeded 2&nbsp;GiB threw {@code
 * IllegalArgumentException("bundle too large")} out of the publish path, which {@code
 * commitIndexWriter}'s catch-all turns into {@code failEngine}. That made a large-enough shard fail
 * permanently on every flush with no recovery path.
 *
 * <p>The header is computable from {@code (name, length, checksum)} alone -- file offsets are not
 * stored, they are the cumulative sum of preceding lengths -- so nothing needs the body in hand to
 * plan the layout. Once planned, the upload is a stream: header, then each body, in the same order
 * the entries record. There is no concatenated array and no 2&nbsp;GiB cap on the bundle total.
 *
 * <p>{@link #openStream()} may be called more than once and returns an independent stream each time,
 * because a blob store's write can legitimately be retried.
 *
 * <p>The per-file arrays inside {@link BundleFileContent} are still fully in heap -- this class
 * removes the <em>second</em> copy and the total-size cap, not the first copy. Removing that too
 * means changing {@code BundleFileContent} to carry a stream or a directory-plus-name rather than
 * bytes, which is a change to the writer engine's publish path rather than to this format.
 */
public final class BundleLayout {

    private final byte[] headerBytes;
    private final Map<String, BundleFileEntry> entriesByName;
    private final List<BundleFileContent> files;
    private final long totalLength;

    BundleLayout(byte[] headerBytes, Map<String, BundleFileEntry> entriesByName, List<BundleFileContent> files, long totalLength) {
        this.headerBytes = headerBytes;
        this.entriesByName = entriesByName;
        this.files = files;
        this.totalLength = totalLength;
    }

    /** Location metadata for every planned file, keyed by logical file name, in bundle order. */
    public Map<String, BundleFileEntry> entries() {
        return entriesByName;
    }

    /** The total length of the bundle blob this layout describes: header plus every file body. */
    public long totalLength() {
        return totalLength;
    }

    /** The length of just the header, i.e. the offset of the first file body. */
    public int headerLength() {
        return headerBytes.length;
    }

    /**
     * Opens a fresh stream over the whole bundle: header first, then every file body in entry order.
     *
     * <p>A {@link SequenceInputStream} rather than a piped stream or a background writer thread: the
     * bodies are already in memory as arrays, so there is nothing to produce concurrently, and a
     * sequence stream keeps the upload entirely on the caller's thread with no extra buffering.
     *
     * @return an independent stream over exactly {@link #totalLength()} bytes.
     */
    public InputStream openStream() {
        List<InputStream> parts = new ArrayList<>(files.size() + 1);
        parts.add(new ByteArrayInputStream(headerBytes));
        for (BundleFileContent file : files) {
            parts.add(new ByteArrayInputStream(file.content()));
        }
        return new SequenceInputStream(Collections.enumeration(parts));
    }

    /**
     * Materializes the whole bundle into one array.
     *
     * <p>Only for callers that genuinely need the bytes in hand -- reading a file back out of an
     * in-memory bundle, and the format's own tests. The upload path deliberately does not use this;
     * see this class's own javadoc.
     *
     * @throws IOException if the bundle is larger than a {@code byte[]} can hold, which is exactly
     *                      the ceiling {@link #openStream()} exists to avoid.
     */
    byte[] materialize() throws IOException {
        if (totalLength > Integer.MAX_VALUE) {
            throw new IOException(
                "bundle is " + totalLength + " bytes, too large to materialize into a single array -- use openStream() instead"
            );
        }
        byte[] bundleBytes = new byte[(int) totalLength];
        System.arraycopy(headerBytes, 0, bundleBytes, 0, headerBytes.length);
        int offset = headerBytes.length;
        for (BundleFileContent file : files) {
            System.arraycopy(file.content(), 0, bundleBytes, offset, file.content().length);
            offset += file.content().length;
        }
        return bundleBytes;
    }
}
