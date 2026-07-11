/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.serverless.storage.readerengine.action.ShardLagEntry;
import org.opensearch.serverless.storage.writerengine.action.IdleShardEntry;

import java.io.IOException;
import java.util.List;

/**
 * One node's raw contribution to a {@link ScaleToZeroCandidatesAction} evaluation: exactly what
 * {@link org.opensearch.serverless.storage.writerengine.action.NodeIdleShardsAction} and {@link
 * org.opensearch.serverless.storage.readerengine.action.NodeManifestLagAction} would each report
 * for this node, bundled into one node-level response so {@link TransportScaleToZeroCandidatesAction}
 * only needs a single fan-out round-trip per node rather than two.
 */
public class NodeScaleToZeroCandidatesResponse extends BaseNodeResponse {

    private final List<IdleShardEntry> idleShards;
    private final List<ShardLagEntry> laggingShards;
    private final List<IdleShardEntry> readerIdleShards;

    /**
     * Creates a node-level response.
     *
     * @param node the node this response came from.
     * @param idleShards every writer shard's idle time this node has tracked, same as {@link
     *                   org.opensearch.serverless.storage.writerengine.action.NodeIdleShardsResponse#entries()}.
     * @param laggingShards every reader shard's manifest-generation lag this node has tracked,
     *                      same as {@link org.opensearch.serverless.storage.readerengine.action.NodeManifestLagResponse#entries()}.
     * @param readerIdleShards every reader shard's query idle time this node has tracked (rfc-serverless-opensearch.md
     *                         &sect;7.3's reader-shard scale-to-zero signal) -- reuses {@link
     *                         IdleShardEntry}'s generic {@code (indexUuid, shardId, millis)} shape,
     *                         here meaning query idle time rather than write idle time.
     */
    public NodeScaleToZeroCandidatesResponse(
        DiscoveryNode node,
        List<IdleShardEntry> idleShards,
        List<ShardLagEntry> laggingShards,
        List<IdleShardEntry> readerIdleShards
    ) {
        super(node);
        this.idleShards = idleShards;
        this.laggingShards = laggingShards;
        this.readerIdleShards = readerIdleShards;
    }

    /**
     * Deserializes a node-level response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeScaleToZeroCandidatesResponse}.
     */
    public NodeScaleToZeroCandidatesResponse(StreamInput in) throws IOException {
        super(in);
        this.idleShards = in.readList(IdleShardEntry::new);
        this.laggingShards = in.readList(ShardLagEntry::new);
        this.readerIdleShards = in.readList(IdleShardEntry::new);
    }

    /** @param out stream to write this response's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(idleShards);
        out.writeList(laggingShards);
        out.writeList(readerIdleShards);
    }

    /** Every writer shard's idle time this node has tracked. */
    public List<IdleShardEntry> idleShards() {
        return idleShards;
    }

    /** Every reader shard's manifest-generation lag this node has tracked. */
    public List<ShardLagEntry> laggingShards() {
        return laggingShards;
    }

    /** Every reader shard's query idle time this node has tracked. */
    public List<IdleShardEntry> readerIdleShards() {
        return readerIdleShards;
    }
}
