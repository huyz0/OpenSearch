/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shard;

import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.SegmentPublisher;

import java.io.IOException;
import java.util.Map;

/**
 * Many shards' staged commits checkpointed with one write, for bucket members, whose commits share one register.
 */
public final class CheckpointBatch {

    private CheckpointBatch() {}

    /** Writes the prepared commits of many shards, and says how each swap went. */
    @FunctionalInterface
    public interface Swap {
        /**
         * Writes the commits.
         *
         * @param prepared each shard's staged commit and the generation it swaps over
         * @return each shard's outcome; a shard missing from it counts as failed
         * @throws IOException if the write fails, which leaves every commit staged
         */
        Map<ShardId, BlobRegisterCasResult> swap(Map<ShardId, SegmentPublisher.PendingCheckpoint> prepared) throws IOException;
    }

    /**
     * What a batch did.
     *
     * @param written the shards whose commit was written, with that commit
     * @param failed the shards that could not be checkpointed, with why; their commits stay staged
     */
    public record Outcome(Map<ShardId, CommitManifest> written, Map<ShardId, Exception> failed) {
    }
}
