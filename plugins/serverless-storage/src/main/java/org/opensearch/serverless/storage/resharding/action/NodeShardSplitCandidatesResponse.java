/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * One node's raw contribution to a {@link ShardSplitCandidatesAction} evaluation: exactly what this
 * node's {@code ShardActivityRegistry#snapshotWritesPerMinute()} reports, unfiltered and
 * unevaluated -- mirrors {@code org.opensearch.serverless.storage.scaleup.action.NodeScaleUpCandidatesResponse}'s shape.
 */
public class NodeShardSplitCandidatesResponse extends BaseNodeResponse {

    private final List<ShardWriteRateEntry> writeRates;

    /**
     * Creates a node-level response.
     *
     * @param node the node this response came from.
     * @param writeRates every writer shard's writes-per-minute estimate this node has tracked.
     */
    public NodeShardSplitCandidatesResponse(DiscoveryNode node, List<ShardWriteRateEntry> writeRates) {
        super(node);
        this.writeRates = writeRates;
    }

    /**
     * Deserializes a node-level response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeShardSplitCandidatesResponse}.
     */
    public NodeShardSplitCandidatesResponse(StreamInput in) throws IOException {
        super(in);
        this.writeRates = in.readList(ShardWriteRateEntry::new);
    }

    /** @param out stream to write this response's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(writeRates);
    }

    /** Every writer shard's writes-per-minute estimate this node has tracked. */
    public List<ShardWriteRateEntry> writeRates() {
        return writeRates;
    }
}
