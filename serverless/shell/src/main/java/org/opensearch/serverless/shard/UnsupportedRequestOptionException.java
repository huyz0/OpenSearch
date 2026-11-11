/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

/** A known capability refusal: retains the native exception contract and receives HTTP 501. */
public final class UnsupportedRequestOptionException extends UnsupportedOperationException {
    UnsupportedRequestOptionException(String message) {
        super(message);
    }
}
