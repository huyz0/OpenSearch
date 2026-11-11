/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import java.io.IOException;

/** The index named does not exist. */
public final class NoSuchIndexException extends IOException {

    private final String index;

    /**
     * Creates the exception.
     *
     * @param index the index that does not exist
     */
    public NoSuchIndexException(String index) {
        super("no such index: " + index);
        this.index = index;
    }

    /**
     * Returns the index that does not exist.
     *
     * <p>Carried rather than left in the message so that a caller translating this into another
     * vocabulary -- {@code ServerlessClient} turns it into core's {@code IndexNotFoundException} --
     * does not have to parse prose to name the index.
     *
     * @return the index name
     */
    public String index() {
        return index;
    }
}
