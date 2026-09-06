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
 * A request for one node's own statistics, sent to that node.
 *
 * <p>Carries nothing. A node's statistics are a description of itself, so there is no selector to send:
 * the receiver answers about the receiver, exactly as it would to {@code GET /_serverless/stats} arriving
 * on its own HTTP port. Which nodes are asked is the sender's decision, made before any of these are sent.
 *
 * <p>It still travels as a request rather than being replaced by an HTTP call from node to node, so that
 * it goes over the same authenticated transport as every other hop here — a peer answers a caller it can
 * verify, and the answer is not reachable by anyone who can merely open a socket.
 */
public final class ForwardedStatsRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/node/stats";

    /** Creates a request. */
    public ForwardedStatsRequest() {}

    /**
     * Reads a request off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public ForwardedStatsRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
