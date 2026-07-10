/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/** The result of an {@link IndexSnapshotRestoreAction} request. */
public class IndexSnapshotRestoreResponse extends ActionResponse implements ToXContentObject {

    private final int shardCount;

    /**
     * Creates a response.
     *
     * @param shardCount how many shards were restored -- always the index's full shard count, since
     *                    every shard is validated to have a pin under {@code snapshotId} before any
     *                    shard is actually restored (see {@link IndexSnapshotRestoreAction}'s own
     *                    javadoc for the limits of that guarantee).
     */
    public IndexSnapshotRestoreResponse(int shardCount) {
        this.shardCount = shardCount;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link IndexSnapshotRestoreResponse}.
     */
    public IndexSnapshotRestoreResponse(StreamInput in) throws IOException {
        super(in);
        this.shardCount = in.readVInt();
    }

    /** @param out stream to write {@link #shardCount()} to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shardCount);
    }

    /** How many shards were restored. */
    public int shardCount() {
        return shardCount;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("acknowledged", true).field("shard_count", shardCount).endObject();
    }
}
