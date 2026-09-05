/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import java.io.IOException;

/** Thrown when creating an index whose descriptor register already exists. */
public class IndexAlreadyExistsException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param indexName the index that already exists
     */
    public IndexAlreadyExistsException(String indexName) {
        super("index [" + indexName + "] already exists");
    }
}
