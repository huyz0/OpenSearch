/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import java.io.IOException;

/** A document does not exist, and there was no {@code upsert} or {@code doc_as_upsert} to fall back to. */
public final class DocumentMissingException extends IOException {

    private final String index;
    private final String id;

    /**
     * Creates the exception.
     *
     * @param index the index
     * @param id the document that is not there
     */
    public DocumentMissingException(String index, String id) {
        super("document missing: [" + index + "]/[" + id + "]");
        this.index = index;
        this.id = id;
    }

    /**
     * Returns the index.
     *
     * @return the index name
     */
    public String index() {
        return index;
    }

    /**
     * Returns the document id.
     *
     * @return the id
     */
    public String id() {
        return id;
    }
}
