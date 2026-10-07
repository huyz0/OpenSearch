/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.cluster;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Builds the {@link ClusterState} this node holds, from the descriptors and assignments it knows about.
 *
 * <p>This is the mechanism at the centre of {@code rfc-serverless-shell.md} §5. The state it produces is
 * a <em>cache</em>, not a truth: it is never published, never diffed, never agreed. It contains only
 * what this node hosts, which is what makes its cost proportional to the working set rather than to the
 * number of indices in the world.
 *
 * <p>Two nodes will disagree, permanently and by design. Nothing reconciles them, because no safety
 * property reads a projected view — ownership is arbitrated per-shard by compare-and-swap.
 *
 * <p><b>Versions are per-node and are not comparable across nodes.</b> The counter here exists only to
 * satisfy {@link ClusterState}'s own monotonicity. It must never be fed to
 * {@code IndexShard#updateShardState} as the applying version — S1 reproduced what happens if it is
 * (a silently ignored update), and the shard-head term is used there instead. See
 * {@code s1-findings.md} and §5.3.
 */
public final class LocalViewProjector {

    private final ClusterName clusterName;
    private final DiscoveryNode localNode;
    private final AtomicLong version = new AtomicLong(0);

    /** One index as last projected: reused while its descriptor and its shards' terms are the same. */
    private record Projected(IndexDescriptor descriptor, Map<Integer, Long> terms, IndexMetadata metadata, IndexRoutingTable routing) {
    }

    /**
     * The indices of the last projection, by name.
     *
     * <p>Every activation projects the whole view -- every index the node serves or hosts -- and building an index's
     * metadata parses its mapping. That made taking a shard cost a node in proportion to what it already held: 4 ms a
     * projection at a hundred shards, 44 ms at six hundred, and a dead node's shards queued behind each other's
     * projections. An index whose descriptor is the same object and whose terms are the same is the same projection.
     * Matched on the descriptor's identity, not its equality: equality leaves out fields the metadata is built from, and a
     * refreshed descriptor is a new object.
     */
    private volatile Map<String, Projected> lastProjected = Map.of();

    /**
     * Creates a projector for one node.
     *
     * @param clusterName the configured cluster name
     * @param localNode this node
     */
    public LocalViewProjector(ClusterName clusterName, DiscoveryNode localNode) {
        this.clusterName = clusterName;
        this.localNode = localNode;
    }

    /**
     * Projects a view containing exactly the given indices, with the given shards assigned here.
     *
     * @param descriptors the indices this node should know about
     * @param assignments the shards this node owns, from shard-heads
     * @return the node-local cluster state
     * @throws IOException if a descriptor's mapping cannot be parsed
     */
    public ClusterState project(Collection<IndexDescriptor> descriptors, Collection<ShardAssignment> assignments) throws IOException {
        final Map<String, Map<Integer, Long>> termsByIndex = new HashMap<>();
        for (ShardAssignment assignment : assignments) {
            termsByIndex.computeIfAbsent(assignment.indexName(), k -> new HashMap<>()).put(assignment.shardId(), assignment.term());
        }

        final Metadata.Builder metadata = Metadata.builder();
        final RoutingTable.Builder routing = RoutingTable.builder();

        final Map<String, Projected> previous = lastProjected;
        final Map<String, Projected> projected = new HashMap<>();
        for (IndexDescriptor descriptor : descriptors) {
            final Map<Integer, Long> terms = termsByIndex.getOrDefault(descriptor.name(), Map.of());
            Projected index = previous.get(descriptor.name());
            if (index == null || index.descriptor() != descriptor || index.terms().equals(terms) == false) {
                final IndexMetadata indexMetadata = descriptor.toIndexMetadata(terms);
                index = new Projected(descriptor, terms, indexMetadata, routingFor(indexMetadata, terms));
            }
            projected.put(descriptor.name(), index);
            metadata.put(index.metadata(), false);
            routing.add(index.routing());
        }
        lastProjected = projected;

        return ClusterState.builder(clusterName)
            .version(version.incrementAndGet())
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build())
            .metadata(metadata.build())
            .routingTable(routing.build())
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
    }

    /**
     * A routing table naming this node as the holder of every assigned shard, and leaving the rest
     * unassigned. There is no allocator to consult: assignment came from a compare-and-swap that already
     * happened, so this records a fact rather than making a decision.
     */
    private IndexRoutingTable routingFor(IndexMetadata indexMetadata, Map<Integer, Long> assignedShards) {
        final Index index = indexMetadata.getIndex();
        final IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index);
        for (int shard = 0; shard < indexMetadata.getNumberOfShards(); shard++) {
            final ShardId shardId = new ShardId(index, shard);
            final IndexShardRoutingTable.Builder shardTable = new IndexShardRoutingTable.Builder(shardId);
            if (assignedShards.containsKey(shard)) {
                shardTable.addShard(
                    ShardRouting.newUnassigned(
                        shardId,
                        true,
                        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "serverless assignment")
                    ).initialize(localNode.getId(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE).moveToStarted()
                );
            } else {
                shardTable.addShard(
                    ShardRouting.newUnassigned(
                        shardId,
                        true,
                        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "not assigned to this node")
                    )
                );
            }
            builder.addIndexShard(shardTable.build());
        }
        return builder.build();
    }

    /**
     * Returns the version most recently projected. Meaningful only on this node.
     *
     * @return the local view version
     */
    public long currentVersion() {
        return version.get();
    }

    /**
     * Reports whether a routing entry is in the started state, used by tests and callers that need to
     * distinguish an assigned shard from a merely-known one.
     *
     * @param routing the routing entry
     * @return true when started
     */
    public static boolean isStarted(ShardRouting routing) {
        return routing != null && routing.state() == ShardRoutingState.STARTED;
    }
}
