/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.serverless.storage.writerengine.ShardActivityRegistry;

import java.io.IOException;

/** The result of a {@link ShardIdleTimeAction} request. */
public class ShardIdleTimeResponse extends ActionResponse implements ToXContentObject {

    private final boolean tracked;
    private final long millisSinceLastActivity;

    /**
     * Creates a response reporting a tracked shard's idle time.
     *
     * @param millisSinceLastActivity how long it's been since the shard's writer engine last saw a real client write.
     */
    public ShardIdleTimeResponse(long millisSinceLastActivity) {
        this.tracked = true;
        this.millisSinceLastActivity = millisSinceLastActivity;
    }

    /**
     * Creates a response reporting no writer engine for the requested shard is currently tracked
     * on this node -- see {@link ShardActivityRegistry}'s own javadoc for why this is a normal,
     * expected outcome (the shard isn't hosted here, or its engine already closed), not an error.
     */
    public static ShardIdleTimeResponse notTracked() {
        return new ShardIdleTimeResponse(false, -1);
    }

    private ShardIdleTimeResponse(boolean tracked, long millisSinceLastActivity) {
        this.tracked = tracked;
        this.millisSinceLastActivity = millisSinceLastActivity;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardIdleTimeResponse}.
     */
    public ShardIdleTimeResponse(StreamInput in) throws IOException {
        super(in);
        this.tracked = in.readBoolean();
        this.millisSinceLastActivity = in.readZLong();
    }

    /** @param out stream to write this response's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(tracked);
        out.writeZLong(millisSinceLastActivity);
    }

    /**
     * Whether a writer engine for the requested shard is currently tracked on this node. {@code
     * false} means the shard isn't hosted here or its engine already closed -- {@link
     * #millisSinceLastActivity()} is meaningless in that case, not zero or some other sentinel
     * that could be mistaken for a real answer.
     */
    public boolean tracked() {
        return tracked;
    }

    /**
     * How long it's been since the shard's writer engine last saw a real client write. Only
     * meaningful when {@link #tracked()} is {@code true}.
     */
    public long millisSinceLastActivity() {
        return millisSinceLastActivity;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("tracked", tracked);
        if (tracked) {
            builder.field("millis_since_last_activity", millisSinceLastActivity);
        }
        return builder.endObject();
    }
}
