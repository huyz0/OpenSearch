/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.deepsnapshot.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * The result of copying one shard: the shard generation {@link
 * org.opensearch.repositories.Repository#snapshotShard} reported, which {@link
 * IndexDeepSnapshotAction}'s finalize step must hand back to the repository unchanged -- the same
 * value {@code shardGeneration} names in {@code DeepSnapshotOrchestrationIT}, this is what makes
 * that spike's manual wiring into a real response type.
 */
public class ShardDeepSnapshotResponse extends ActionResponse implements ToXContentObject {

    private final String shardGeneration;

    /** @param shardGeneration the shard generation the repository reported for the copy just written. */
    public ShardDeepSnapshotResponse(String shardGeneration) {
        this.shardGeneration = shardGeneration;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardDeepSnapshotResponse}.
     */
    public ShardDeepSnapshotResponse(StreamInput in) throws IOException {
        super(in);
        this.shardGeneration = in.readString();
    }

    /** @param out stream to write this response's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(shardGeneration);
    }

    /** The shard generation the repository reported for the copy just written. */
    public String shardGeneration() {
        return shardGeneration;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("shard_generation", shardGeneration).endObject();
    }
}
