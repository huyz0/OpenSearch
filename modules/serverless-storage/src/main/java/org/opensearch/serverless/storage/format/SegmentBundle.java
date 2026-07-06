/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import java.util.List;
import java.util.Map;

/**
 * The result of packing a set of files into a bundle: the raw bytes to upload as a single blob,
 * and the per-file location metadata (absolute byte offsets within those bytes) that gets
 * embedded verbatim into a commit manifest's file map
 * (rfc-serverless-opensearch.md &sect;6.2/&sect;6.3).
 */
public final class SegmentBundle {

    private final byte[] bytes;
    private final Map<String, BundleFileEntry> entriesByName;

    SegmentBundle(byte[] bytes, Map<String, BundleFileEntry> entriesByName) {
        this.bytes = bytes;
        this.entriesByName = entriesByName;
    }

    /** The full bundle contents (header + concatenated file bodies), ready to upload as one blob. */
    public byte[] bytes() {
        return bytes;
    }

    public long length() {
        return bytes.length;
    }

    /** Location metadata for every packed file, keyed by logical file name. */
    public Map<String, BundleFileEntry> entries() {
        return entriesByName;
    }

    public List<BundleFileEntry> entryList() {
        return List.copyOf(entriesByName.values());
    }
}
