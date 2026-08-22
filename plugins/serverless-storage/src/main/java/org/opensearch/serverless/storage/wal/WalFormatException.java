/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import java.io.IOException;

/**
 * Thrown when a WAL chunk is malformed, truncated, or fails checksum verification. Recovery must
 * fail closed on a corrupt chunk rather than silently replaying a partial or wrong operation
 * stream (rfc-serverless-opensearch.md &sect;17).
 */
public class WalFormatException extends IOException {

    /**
     * Signals a malformed, truncated, or checksum-failing WAL chunk with no underlying cause.
     *
     * @param message description.
     */
    public WalFormatException(String message) {
        super(message);
    }

    /**
     * Signals a malformed, truncated, or checksum-failing WAL chunk caused by an underlying failure.
     *
     * @param message description.
     * @param cause description.
     */
    public WalFormatException(String message, Throwable cause) {
        super(message, cause);
    }
}
