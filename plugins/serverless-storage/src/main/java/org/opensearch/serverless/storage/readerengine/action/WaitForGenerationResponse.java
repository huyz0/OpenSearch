/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/** The result of a {@link WaitForGenerationAction} request. */
public class WaitForGenerationResponse extends ActionResponse implements ToXContentObject {

    private final boolean reached;
    private final boolean shardTracked;

    /**
     * Creates a response.
     *
     * @param reached {@code true} if the requested generation was reached before the timeout.
     * @param shardTracked {@code false} if no reader engine for the requested shard is currently
     *                     tracked on the receiving node at all (never registered, or already
     *                     collected after closing) -- distinct from {@code reached=false}, which
     *                     means the shard exists but hadn't caught up in time.
     */
    public WaitForGenerationResponse(boolean reached, boolean shardTracked) {
        this.reached = reached;
        this.shardTracked = shardTracked;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link WaitForGenerationResponse}.
     */
    public WaitForGenerationResponse(StreamInput in) throws IOException {
        super(in);
        this.reached = in.readBoolean();
        this.shardTracked = in.readBoolean();
    }

    /** @param out stream to write {@link #reached()}/{@link #shardTracked()} to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(reached);
        out.writeBoolean(shardTracked);
    }

    /** {@code true} if the requested generation was reached before the timeout. */
    public boolean reached() {
        return reached;
    }

    /** {@code false} if no reader engine for the requested shard is currently tracked on the receiving node at all. */
    public boolean shardTracked() {
        return shardTracked;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("reached", reached).field("shard_tracked", shardTracked).endObject();
    }
}
