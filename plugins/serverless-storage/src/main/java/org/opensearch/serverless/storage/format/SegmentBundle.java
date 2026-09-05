/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

/**
 * The result of packing a set of files into a bundle: the raw bytes to upload as a single blob,
 * and the per-file location metadata (absolute byte offsets within those bytes) that gets
 * embedded verbatim into a commit manifest's file map
 * (rfc-serverless-opensearch.md &sect;6.2/&sect;6.3).
 */
public final class SegmentBundle {

    private final BundleLayout layout;
    /** Materialized on first {@link #bytes()} call and cached; {@code null} until then. */
    private volatile byte[] materialized;

    /**
     * Wraps a planned bundle.
     *
     * @param layout the planned header, entry map and streamable body.
     */
    SegmentBundle(BundleLayout layout) {
        this.layout = layout;
    }

    /**
     * The full bundle contents (header + concatenated file bodies) as one array.
     *
     * <p><b>Materialized lazily, and no longer allocated just because a caller wanted the entry
     * map.</b> Packing used to build this array eagerly, so every publish paid a second full copy of
     * the whole commit in heap even though the upload path is the only thing that ever needed the
     * bytes -- and it needed them only because the upload was a single {@code writeBlobAtomic} over
     * an array. That upload now streams (see {@link #openStream()}), so on the publish path this
     * method is never called at all.
     *
     * <p>It also carried a hard ceiling: a {@code byte[]} cannot exceed {@code Integer.MAX_VALUE}, so
     * a commit above 2&nbsp;GiB threw out of the publish path and, via {@code commitIndexWriter}'s
     * catch-all, failed the engine permanently on every subsequent flush. Callers that genuinely
     * need the bytes (reading a file back out of an in-memory bundle, and this format's own tests)
     * still hit that ceiling, because an array cannot escape it -- but they are no longer on the
     * path that decides whether a shard can publish.
     *
     * @return the full bundle contents.
     * @throws UncheckedIOException if the bundle is too large to hold in a single array; use
     *                               {@link #openStream()} instead.
     */
    public byte[] bytes() {
        byte[] local = materialized;
        if (local == null) {
            synchronized (this) {
                local = materialized;
                if (local == null) {
                    try {
                        local = layout.materialize();
                    } catch (IOException tooLarge) {
                        throw new UncheckedIOException(tooLarge);
                    }
                    materialized = local;
                }
            }
        }
        return local;
    }

    /**
     * Opens a fresh stream over the whole bundle, without ever holding it in one array -- the shape
     * the upload path uses. See {@link BundleLayout#openStream()}.
     *
     * @return an independent stream over exactly {@link #length()} bytes.
     */
    public InputStream openStream() {
        return layout.openStream();
    }

    /** The full bundle length in bytes -- known from the layout, without materializing anything. */
    public long length() {
        return layout.totalLength();
    }

    /** Location metadata for every packed file, keyed by logical file name. */
    public Map<String, BundleFileEntry> entries() {
        return layout.entries();
    }

    /** Location metadata for every packed file, in bundle order. */
    public List<BundleFileEntry> entryList() {
        return List.copyOf(layout.entries().values());
    }
}
