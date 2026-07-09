/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import java.util.Map;

/** A parsed {@link SegmentBundle} header: how many bytes it occupied, and the file map it described. */
public final class BundleHeader {

    private final int headerLength;
    private final Map<String, BundleFileEntry> entries;

    /**
     * Wraps an already-parsed bundle header.
     *
     * @param headerLength number of bytes the header occupies at the start of the bundle blob.
     * @param entries the file map described by the header, keyed by file name.
     */
    BundleHeader(int headerLength, Map<String, BundleFileEntry> entries) {
        this.headerLength = headerLength;
        this.entries = entries;
    }

    /** Number of bytes the header occupies at the start of the bundle blob. */
    public int headerLength() {
        return headerLength;
    }

    /** The file map described by the header, keyed by file name. */
    public Map<String, BundleFileEntry> entries() {
        return entries;
    }
}
