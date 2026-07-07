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
 * Thrown when a segment bundle is malformed, truncated, or fails checksum verification. Bundle
 * corruption must fail closed (rfc-serverless-opensearch.md &sect;17) rather than silently
 * returning partial or wrong data.
 */
public class BundleFormatException extends IOException {

    public BundleFormatException(String message) {
        super(message);
    }

    public BundleFormatException(String message, Throwable cause) {
        super(message, cause);
    }
}
