/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.membership;

import java.io.IOException;

/**
 * Thrown by a renewal that found this node's lease changed by someone else since it last wrote it.
 *
 * <p>The change is evidence, not noise: a node taking one of this node's shards revokes this lease first,
 * and a collector removes it only after it lapsed. Either way this node may no longer own what it holds,
 * and must treat the renewal as a lapse -- suspend acknowledgement and re-read every head -- before it
 * writes again. The next renewal starts the lease afresh.
 */
public class LeaseRevokedException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param nodeId this node's id
     * @param expectedGeneration the generation this node last wrote
     * @param foundGeneration the generation the register holds now
     */
    public LeaseRevokedException(String nodeId, long expectedGeneration, long foundGeneration) {
        super(
            "the lease of ["
                + nodeId
                + "] was changed by someone else (expected generation "
                + expectedGeneration
                + ", found "
                + foundGeneration
                + "): it was revoked by a node taking one of its shards, or collected after lapsing"
        );
    }
}
