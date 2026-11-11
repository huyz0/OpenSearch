/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

/** Minimal timeout abstraction so the shell does not depend on a specific TimeValue import path. */
@FunctionalInterface
public interface TimeValueLike {
    /**
     * Returns the timeout.
     *
     * @return the timeout in milliseconds
     */
    long millis();
}
