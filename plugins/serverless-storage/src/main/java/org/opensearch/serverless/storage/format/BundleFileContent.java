/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import java.util.Objects;

/** One logical file to be packed into a {@link SegmentBundle}, supplied as raw bytes. */
public final class BundleFileContent {

    private final String name;
    private final byte[] content;

    /**
     * Wraps one file's raw bytes.
     *
     * @param name the file's logical name within the bundle.
     * @param content the file's raw bytes.
     */
    public BundleFileContent(String name, byte[] content) {
        this.name = Objects.requireNonNull(name, "name");
        this.content = Objects.requireNonNull(content, "content");
    }

    /** The file's logical name within the bundle. */
    public String name() {
        return name;
    }

    /** The file's raw bytes. */
    public byte[] content() {
        return content;
    }
}
