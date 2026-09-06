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
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;

/**
 * One node's statistics, as that node rendered them.
 *
 * <p><b>Carried as the rendered JSON rather than as fields.</b> The alternative is a wire format that
 * enumerates every number {@code /_serverless/stats} reports, which would be a second account of that
 * document — and the moment a section is added to one and not the other, a node answering for itself and
 * the same node answering through a peer disagree. The sender does not read the body; it copies it into
 * the answer under the node's id, so there is exactly one place that decides what a node's statistics are.
 *
 * <p>The cost of that choice is that the body is opaque here and cannot be filtered or aggregated on the
 * way through. Nothing wants to: the endpoint reports counters rather than a diagnosis, and summing
 * counters across nodes is precisely the derived number the single-node endpoint refuses to invent.
 */
public final class ForwardedStatsResponse extends TransportResponse {

    private final String nodeId;
    private final String statistics;

    /**
     * Creates a response.
     *
     * @param nodeId the node these describe
     * @param statistics the node's statistics, as a JSON object
     */
    public ForwardedStatsResponse(String nodeId, String statistics) {
        this.nodeId = nodeId;
        this.statistics = statistics;
    }

    /**
     * Reads a response off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public ForwardedStatsResponse(StreamInput in) throws IOException {
        this.nodeId = in.readString();
        this.statistics = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeString(statistics);
    }

    /**
     * Returns the node these describe.
     *
     * @return the node id
     */
    public String nodeId() {
        return nodeId;
    }

    /**
     * Returns the node's statistics.
     *
     * @return a JSON object
     */
    public String statistics() {
        return statistics;
    }
}
