/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaleup.action;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * One node's raw contribution to a {@link ScaleUpCandidatesAction} evaluation: exactly what this
 * node's {@code ReaderShardActivityRegistry#snapshotQueriesPerMinute()} reports, unfiltered and
 * unevaluated -- mirrors {@code org.opensearch.serverless.storage.scaletozero.action.NodeScaleToZeroCandidatesResponse}'s shape.
 */
public class NodeScaleUpCandidatesResponse extends BaseNodeResponse {

    private final List<ShardQueryRateEntry> queryRates;

    /**
     * Creates a node-level response.
     *
     * @param node the node this response came from.
     * @param queryRates every reader shard's queries-per-minute estimate this node has tracked.
     */
    public NodeScaleUpCandidatesResponse(DiscoveryNode node, List<ShardQueryRateEntry> queryRates) {
        super(node);
        this.queryRates = queryRates;
    }

    /**
     * Deserializes a node-level response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeScaleUpCandidatesResponse}.
     */
    public NodeScaleUpCandidatesResponse(StreamInput in) throws IOException {
        super(in);
        this.queryRates = in.readList(ShardQueryRateEntry::new);
    }

    /** @param out stream to write this response's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(queryRates);
    }

    /** Every reader shard's queries-per-minute estimate this node has tracked. */
    public List<ShardQueryRateEntry> queryRates() {
        return queryRates;
    }
}
