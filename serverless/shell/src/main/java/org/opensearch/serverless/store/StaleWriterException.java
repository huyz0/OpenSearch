/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import java.io.IOException;

/**
 * Thrown when a writer tries to publish a commit after a newer term already has.
 *
 * <p>This is a zombie being told so. Its segment bytes are already harmless — they went under its own
 * term prefix, which nothing reads — but it must not be allowed to name them as the current commit.
 */
public class StaleWriterException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param attemptedTerm the term that tried to publish
     * @param currentTerm the term already published, or -1 if a concurrent swap was detected
     */
    /**
     * Creates the exception from a message, for a subclass that has a more specific diagnosis.
     *
     * @param message what happened
     */
    protected StaleWriterException(String message) {
        super(message);
    }

    /**
     * Creates the exception for a publish the manifest refused.
     *
     * @param attemptedTerm the term this node tried to publish at
     * @param currentTerm the term the manifest already holds, or negative when it changed concurrently
     */
    public StaleWriterException(long attemptedTerm, long currentTerm) {
        super(
            "refusing to publish at term "
                + attemptedTerm
                + (currentTerm >= 0 ? "; the manifest already holds term " + currentTerm : "; the manifest changed concurrently")
        );
    }
}
