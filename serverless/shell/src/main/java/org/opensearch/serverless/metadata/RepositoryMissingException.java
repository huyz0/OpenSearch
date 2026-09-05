/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import java.io.IOException;

/** Thrown when naming a repository that has not been registered. */
public class RepositoryMissingException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param name the repository that does not exist
     */
    public RepositoryMissingException(String name) {
        super("no such repository: " + name);
    }
}
