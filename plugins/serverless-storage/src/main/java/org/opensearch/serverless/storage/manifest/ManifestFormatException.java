/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.manifest;

import java.io.IOException;

/**
 * Thrown when a manifest blob is structurally unreadable: wrong format version, an impossible
 * declared body length, or a failed trailing checksum. The exact mirror of {@code
 * BundleFormatException} and {@code WalFormatException} for the third persisted format, and an
 * {@link IOException} for the same reason those are -- every caller already handles a manifest read
 * failing with an {@code IOException}, and a corrupt manifest is not a different <em>kind</em> of
 * problem to those callers, only a more specific one to a human reading the log.
 */
public class ManifestFormatException extends IOException {

    /**
     * Creates an exception describing why a manifest blob could not be read.
     *
     * @param message what was wrong with the blob.
     */
    public ManifestFormatException(String message) {
        super(message);
    }

    /**
     * Creates an exception describing why a manifest blob could not be read, wrapping a cause.
     *
     * @param message what was wrong with the blob.
     * @param cause the underlying failure.
     */
    public ManifestFormatException(String message, Throwable cause) {
        super(message, cause);
    }
}
