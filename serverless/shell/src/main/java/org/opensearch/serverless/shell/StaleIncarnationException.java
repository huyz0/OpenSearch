/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Thrown instead of acknowledging a write, or answering a read, from a shard whose index has since been
 * deleted or recreated.
 *
 * <p>Retryable: the request was routed by a resolution that has gone stale, and resolving the name again
 * routes it to the incarnation that exists now, or finds that none does.
 */
public class StaleIncarnationException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param shardId the shard, carrying the uuid of the incarnation that is gone
     */
    public StaleIncarnationException(ShardId shardId) {
        super(
            "refusing to answer from "
                + shardId
                + " ["
                + shardId.getIndex().getUUID()
                + "]: that index has been deleted or recreated since this request was routed; retry"
        );
    }
}
