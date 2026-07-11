/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/** The result of a {@link ShardPartitionRewriteAction} request. */
public class ShardPartitionRewriteResponse extends ActionResponse implements ToXContentObject {

    private final boolean rewritten;

    /**
     * Creates a response.
     *
     * @param rewritten whether the shard had a partition descriptor and a rewrite was actually
     *                  performed and published -- {@code false} means the shard was never a split
     *                  target, was already physically rewritten by a prior call, or has no
     *                  published head; either way, this call is always a safe no-op, never an
     *                  error, when nothing needed doing.
     */
    public ShardPartitionRewriteResponse(boolean rewritten) {
        this.rewritten = rewritten;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardPartitionRewriteResponse}.
     */
    public ShardPartitionRewriteResponse(StreamInput in) throws IOException {
        super(in);
        this.rewritten = in.readBoolean();
    }

    /** @param out stream to write {@link #rewritten()} to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(rewritten);
    }

    /** Whether the shard had a partition descriptor and a rewrite was actually performed and published. */
    public boolean rewritten() {
        return rewritten;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("rewritten", rewritten).endObject();
    }
}
