/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import java.io.IOException;

/** Thrown when taking a snapshot whose name already exists within its repository. */
public class SnapshotAlreadyExistsException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param repo the repository
     * @param name the snapshot that already exists
     */
    public SnapshotAlreadyExistsException(String repo, String name) {
        super("snapshot [" + name + "] already exists in repository [" + repo + "]");
    }
}
