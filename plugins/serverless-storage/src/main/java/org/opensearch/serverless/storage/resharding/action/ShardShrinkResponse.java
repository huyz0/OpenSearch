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

/** The result of a {@link ShardShrinkAction} request. */
public class ShardShrinkResponse extends ActionResponse implements ToXContentObject {

    private final boolean acknowledged;

    /**
     * Creates a response.
     *
     * @param acknowledged whether the merge completed successfully.
     */
    public ShardShrinkResponse(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardShrinkResponse}.
     */
    public ShardShrinkResponse(StreamInput in) throws IOException {
        super(in);
        this.acknowledged = in.readBoolean();
    }

    /** @param out stream to write {@link #acknowledged()} to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
    }

    /** Whether the merge completed successfully. */
    public boolean acknowledged() {
        return acknowledged;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("acknowledged", acknowledged).endObject();
    }
}
