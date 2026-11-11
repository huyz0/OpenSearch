/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.HotShardSpread;
import org.opensearch.serverless.cluster.ReaderPlacement;
import org.opensearch.serverless.membership.NodeLease;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What a search node can hand a joining one: the shards it holds as a reader that the joiner will be sent searches
 * for once it is ready, and the blocks of each that it holds warm.
 *
 * <p>A joiner cannot work this out for itself. Which shards it will serve depends on the shards that exist, and a
 * cluster holds far too many for a node to rank them all; a peer holds a bounded set of readers and can rank just
 * those. It ranks them as coordinators will once the joiner advertises the search role: the joiner is added as a
 * search member and a shard counts if the joiner lands among the readers its searches go to, which is the first one
 * for a cold shard and as many as the spread has widened to for a hot one.
 *
 * <p>Frozen views are left out. They serve a point-in-time search that will finish where it started.
 */
public final class CacheHotSet {

    /**
     * One shard the joiner will serve, and the blocks to copy for it.
     *
     * @param indexName the index
     * @param indexUuid the index's uuid
     * @param shard the shard number
     * @param keys block cache keys, hottest first
     */
    public record HotShard(String indexName, String indexUuid, int shard, List<String> keys) {
    }

    private CacheHotSet() {}

    /**
     * Returns the shards held here as readers that a joiner will serve, with their warm blocks.
     *
     * @param node this node
     * @param members the live leases, as this node sees them
     * @param joinerId the joining node
     * @param spread this node's view of how wide each hot shard is spread
     * @return the joiner's share of this node's readers
     */
    public static List<HotShard> forJoiner(ServerlessNode node, Collection<NodeLease> members, String joinerId, HotShardSpread spread) {
        final List<NodeLease> withJoiner = withSearchRole(members, joinerId);
        final Map<String, ShardId> byScope = new HashMap<>();
        for (ShardId reader : node.reconciler().readerShards()) {
            final var service = node.indicesService().indexService(reader.getIndex());
            if (service == null || ServerlessNode.frozenViewIdOf(service.getIndexSettings()) != null) {
                continue;
            }
            final int width = Math.max(1, spread.widthOf(reader.getIndexName(), reader.id()));
            final List<String> ranked = ReaderPlacement.candidatesFor(
                reader.getIndexName(),
                reader.id(),
                withJoiner,
                ServerlessNode.ROLE_SEARCH,
                width
            );
            if (ranked.contains(joinerId)) {
                byScope.put(ServerlessNode.storageUuidOf(service.getIndexSettings()) + "#" + reader.id(), reader);
            }
        }
        final Map<String, List<String>> resident = node.blockCache().residentByScope(byScope.keySet());
        final List<HotShard> hot = new ArrayList<>(byScope.size());
        for (Map.Entry<String, ShardId> entry : byScope.entrySet()) {
            final ShardId shard = entry.getValue();
            hot.add(
                new HotShard(shard.getIndexName(), shard.getIndex().getUUID(), shard.id(), resident.getOrDefault(entry.getKey(), List.of()))
            );
        }
        return hot;
    }

    /** The members as coordinators will see them once the joiner is ready: the joiner advertising the search role. */
    static List<NodeLease> withSearchRole(Collection<NodeLease> members, String joinerId) {
        final List<NodeLease> leases = new ArrayList<>(members.size() + 1);
        for (NodeLease lease : members) {
            if (lease.nodeId().equals(joinerId) == false) {
                leases.add(lease);
            }
        }
        // Placement ranks by id and role alone, so the joiner's other fields do not matter here.
        leases.add(new NodeLease(joinerId, joinerId, "joining", Set.of(ServerlessNode.ROLE_SEARCH), Long.MAX_VALUE));
        return leases;
    }
}
