/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The other half of the target: a hundred million indices <em>at up to a hundred shards each</em>.
 *
 * <h2>Why this half had no evidence</h2>
 *
 * Everything measured so far counts per <em>index</em>: cluster state versions per creation, bytes per
 * creation, object store reads per resolution. All of it was measured on one-shard indices. A hundred
 * million times a hundred is ten billion shard identities, and nothing had asked whether the per-index costs
 * stay per-index when an index has a hundred shards, or quietly become per-shard.
 *
 * <p>That is the question worth asking of this dimension, because a cost that is flat per index and linear
 * per shard is indistinguishable from a flat one until someone uses more than one shard.
 *
 * <h2>Why placement can be tested without a cluster</h2>
 *
 * A serverless index publishes no routing table. {@link ComputedRoutingTable#build} is a pure function of
 * the index metadata and the eligible node list, which is the whole point of computing placement rather than
 * storing it: there is nothing to publish, nothing to acknowledge, and nothing that has to agree except the
 * function itself. So a hundred shards can be placed here, deterministically, with no nodes started.
 */
public class ShardCountDimensionTests extends OpenSearchTestCase {

    private static final int MAX_SHARDS = 100;
    private static final int CANDIDATES = 3;

    private static IndexMetadata index(String name, int shards) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                    .build()
            )
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .build();
    }

    private static List<String> nodes(int count) {
        List<String> ids = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ids.add(String.format(Locale.ROOT, "node-%03d", i));
        }
        return ids;
    }

    /** Every shard of a hundred-shard index is placed, and placed somewhere eligible. */
    public void testEveryShardOfAHundredIsPlaced() {
        List<String> eligible = nodes(12);

        IndexRoutingTable table = ComputedRoutingTable.build(index("serverless_tenant-wide", MAX_SHARDS), eligible, CANDIDATES);

        assertEquals("one routing entry per shard", MAX_SHARDS, table.shards().size());
        Set<Integer> seen = new HashSet<>();
        for (IndexShardRoutingTable shard : table) {
            seen.add(shard.shardId().id());
            for (ShardRouting routing : shard) {
                assertTrue("a shard placed on an ineligible node is a shard nobody will open", eligible.contains(routing.currentNodeId()));
            }
        }
        assertEquals("shard ids 0..99, none missing and none invented", MAX_SHARDS, seen.size());
    }

    /**
     * Placement is a function, so the same inputs give the same answer every time.
     *
     * <p>This is what replaces publication. Nothing distributes the table, so every node has to derive it
     * independently and arrive at the same result; a placement that varied per call would route two nodes to
     * different copies of the same shard without anything detecting the disagreement.
     */
    public void testPlacingAHundredShardsIsDeterministic() {
        List<String> eligible = nodes(12);
        IndexMetadata metadata = index("serverless_tenant-wide", MAX_SHARDS);

        IndexRoutingTable first = ComputedRoutingTable.build(metadata, eligible, CANDIDATES);
        IndexRoutingTable second = ComputedRoutingTable.build(metadata, eligible, CANDIDATES);

        for (IndexShardRoutingTable shard : first) {
            assertEquals(
                "shard " + shard.shardId().id() + " must land in the same place on every node that computes it",
                shard.primaryShard().currentNodeId(),
                second.shard(shard.shardId().id()).primaryShard().currentNodeId()
            );
        }
    }

    /**
     * A hundred shards spread over the cluster rather than piling onto one node.
     *
     * <p>Asserted as a bound rather than as an exact distribution, because rendezvous hashing is not a
     * round robin and expecting perfect balance would be asserting the wrong thing. What matters is that no
     * node takes so large a share that the index is effectively unsharded, which is what a broken hash or a
     * constant would produce.
     */
    public void testAHundredShardsDoNotAllLandOnOneNode() {
        List<String> eligible = nodes(10);

        IndexRoutingTable table = ComputedRoutingTable.build(index("serverless_tenant-wide", MAX_SHARDS), eligible, CANDIDATES);

        Map<String, Integer> perNode = new HashMap<>();
        for (IndexShardRoutingTable shard : table) {
            perNode.merge(shard.primaryShard().currentNodeId(), 1, Integer::sum);
        }

        assertTrue("every node should take some share of a hundred shards across ten nodes", perNode.size() > 1);
        int busiest = perNode.values().stream().mapToInt(Integer::intValue).max().orElseThrow();
        assertTrue(
            "one node holding " + busiest + " of " + MAX_SHARDS + " primaries is not placement, it is a constant",
            busiest < MAX_SHARDS / 2
        );
    }

    /**
     * Shard count changes the routing table and nothing else about what has to be stored.
     *
     * <p>The descriptor is one object whatever the shard count, so a hundred-shard index costs the same one
     * conditional write to create and the same one read to resolve as a one-shard index. That is what keeps
     * ten billion shards affordable: the shards are derived, and derived things are free to store.
     *
     * <p>Asserted through the serialised descriptor rather than by inspecting fields, since the size of the
     * stored object is the thing that would grow if shard identities were being recorded rather than
     * computed.
     */
    public void testTheStoredRecordDoesNotGrowWithShardCount() throws java.io.IOException {
        int oneShard = serialisedSize(index("serverless_tenant-narrow", 1));
        int hundredShards = serialisedSize(index("serverless_tenant-narrow", MAX_SHARDS));

        assertEquals(
            "a hundred shards must cost the same stored bytes as one, or per-index cost is really per-shard " + "cost wearing a disguise",
            oneShard,
            hundredShards
        );
    }

    /** The descriptor's own wire size, which is what the stored object is made of. */
    private static int serialisedSize(IndexMetadata metadata) throws java.io.IOException {
        try (org.opensearch.common.io.stream.BytesStreamOutput out = new org.opensearch.common.io.stream.BytesStreamOutput()) {
            org.opensearch.cluster.metadata.IndexDescriptor.from(metadata).writeTo(out);
            return out.bytes().length();
        }
    }
}
