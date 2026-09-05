/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import java.io.IOException;

/** Thrown when deleting a repository that still holds snapshots. */
public class RepositoryInUseException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param name the repository still in use
     * @param snapshotCount how many snapshots it still holds
     */
    public RepositoryInUseException(String name, int snapshotCount) {
        super("repository [" + name + "] still holds " + snapshotCount + " snapshot(s); delete them first");
    }
}
