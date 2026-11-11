/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Asks a search peer which of the shards it reads the sender will serve once ready, and which of their blocks it holds.
 *
 * <p>Sent by a node joining to warm, to each search peer. The answer is a {@link HotSetResponse}.
 */
public final class HotSetRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/cache/hot_set";

    private final String joinerId;

    /**
     * Creates a request.
     *
     * @param joinerId the joining node, which the peer ranks as a search member
     */
    public HotSetRequest(String joinerId) {
        this.joinerId = joinerId;
    }

    /**
     * Reads a request off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public HotSetRequest(StreamInput in) throws IOException {
        super(in);
        this.joinerId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(joinerId);
    }

    /**
     * Returns the joining node.
     *
     * @return its id
     */
    public String joinerId() {
        return joinerId;
    }
}
