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

    public BundleFileContent(String name, byte[] content) {
        this.name = Objects.requireNonNull(name, "name");
        this.content = Objects.requireNonNull(content, "content");
    }

    public String name() {
        return name;
    }

    public byte[] content() {
        return content;
    }
}
