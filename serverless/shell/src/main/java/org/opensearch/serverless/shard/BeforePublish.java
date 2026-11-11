/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/** What must happen before a commit's manifest is written, given the commit's digest. */
@FunctionalInterface
public interface BeforePublish {
    /**
     * Runs before the manifest is swapped; a failure stops the publish.
     *
     * @param shardId the shard
     * @param shards the index's shard count
     * @param term the publishing writer's term
     * @param digest the commit's digest
     * @throws IOException if it could not be done
     */
    void accept(ShardId shardId, int shards, long term, org.opensearch.serverless.store.PruningDigest digest) throws IOException;
}
