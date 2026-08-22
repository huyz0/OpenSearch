/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.serverless.storage.nodecapacity.NodeCapacitySignal;

import java.io.IOException;

/** Wraps a {@link NodeCapacitySignal} as an {@link ActionResponse}. */
public class NodeCapacityResponse extends ActionResponse implements ToXContentObject {

    private final NodeCapacitySignal signal;

    /**
     * Creates a response.
     *
     * @param signal the signal to wrap.
     */
    public NodeCapacityResponse(NodeCapacitySignal signal) {
        this.signal = signal;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeCapacityResponse}.
     */
    public NodeCapacityResponse(StreamInput in) throws IOException {
        this.signal = new NodeCapacitySignal(in);
    }

    /** @param out stream to write this response's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        signal.writeTo(out);
    }

    /** The wrapped signal. */
    public NodeCapacitySignal signal() {
        return signal;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return signal.toXContent(builder, params);
    }
}
