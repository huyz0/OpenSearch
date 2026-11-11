/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import java.io.IOException;

/** The index named belongs to a plugin and is not reachable through the request path. */
public final class SystemIndexException extends IOException {

    private final String index;

    /**
     * Creates the refusal.
     *
     * @param index the system index that was named
     */
    public SystemIndexException(String index) {
        super("[" + index + "] belongs to a plugin and is not reachable through the request path");
        this.index = index;
    }

    /**
     * Returns the index that was refused.
     *
     * @return the index name
     */
    public String index() {
        return index;
    }
}
