/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import java.io.IOException;

/** Thrown when registering a repository whose descriptor register already exists. */
public class RepositoryAlreadyExistsException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param name the repository that already exists
     */
    public RepositoryAlreadyExistsException(String name) {
        super("repository [" + name + "] already exists");
    }
}
