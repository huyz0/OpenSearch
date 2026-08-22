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

/** The result of a {@link PollNowAction} request. */
public class PollNowResponse extends ActionResponse implements ToXContentObject {

    private final boolean polled;

    /**
     * Creates a response.
     *
     * @param polled {@code true} if a reader engine for the requested shard was found on the
     *               receiving node and polled; {@code false} if none is currently tracked there.
     */
    public PollNowResponse(boolean polled) {
        this.polled = polled;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link PollNowResponse}.
     */
    public PollNowResponse(StreamInput in) throws IOException {
        super(in);
        this.polled = in.readBoolean();
    }

    /** @param out stream to write {@link #polled()} to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(polled);
    }

    /** {@code true} if a reader engine for the requested shard was found on the receiving node and polled. */
    public boolean polled() {
        return polled;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("polled", polled).endObject();
    }
}
