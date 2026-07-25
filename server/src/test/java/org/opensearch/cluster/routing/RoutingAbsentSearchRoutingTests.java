/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;

/**
 * A2. Searching an index that is present in metadata and absent from the routing table used to throw
 * {@link IndexNotFoundException} -- a 404, saying the index does not exist, about an index that does.
 *
 * <p>It now behaves the way an index whose shards merely happen to be unassigned already behaves: the
 * search succeeds at the coordinator and each shard reports no copy available. That is the same
 * failure callers already handle, and it is the one the serverless reactivation filter is racing to
 * prevent rather than a new mode.
 *
 * <p>The alternative -- skipping such an index outright -- was rejected. It would have returned HTTP
 * 200 with zero hits for an index that exists and holds data, which is a silent wrong answer where
 * this is a loud one.
 */
public class RoutingAbsentSearchRoutingTests extends OpenSearchTestCase {

    private static final String INDEX = "idx";

    private ThreadPool threadPool;
    private OperationRouting operationRouting;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        operationRouting = new OperationRouting(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    /**
     * The change itself. Three shards in metadata, no routing entry: three shard iterators, none with
     * a copy to route to. Before the change this threw IndexNotFoundException instead.
     */
    public void testSearchingARoutingAbsentIndexYieldsShardsWithNoCopies() {
        ClusterState state = stateWithoutRouting(3);

        GroupShardsIterator<ShardIterator> groups = operationRouting.searchShards(state, new String[] { INDEX }, null, null);

        assertEquals("one iterator per shard in metadata, not zero", 3, groups.size());
        for (ShardIterator shards : groups) {
            assertEquals("...and no copy to route any of them to", 0, shards.size());
        }
    }

    /**
     * The distinction the fix must not lose: an index in neither metadata nor routing is genuinely
     * missing, and 404 is the correct answer for it. Metadata is checked first precisely so this
     * keeps working.
     */
    public void testAGenuinelyMissingIndexStillThrows() {
        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().build())
            .routingTable(RoutingTable.builder().build())
            .nodes(DiscoveryNodes.builder().build())
            .build();

        expectThrows(IndexNotFoundException.class, () -> operationRouting.searchShards(empty, new String[] { "nonexistent" }, null, null));
    }

    /**
     * Control: an index whose routing entry exists but whose shards are all unassigned already
     * produced exactly this shape. That equivalence is the justification for the fix -- absence is
     * being made to look like the state core already handles, not given a behaviour of its own.
     */
    public void testAnUnassignedIndexAlreadyBehavesTheSameWay() {
        IndexMetadata indexMetadata = indexMetadata(3);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata).build())
            .nodes(DiscoveryNodes.builder().build())
            .build();

        GroupShardsIterator<ShardIterator> groups = operationRouting.searchShards(state, new String[] { INDEX }, null, null);

        assertEquals(3, groups.size());
        for (ShardIterator shards : groups) {
            assertEquals(0, shards.size());
        }
    }

    private static ClusterState stateWithoutRouting(int shards) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata(shards), false).build())
            .routingTable(RoutingTable.builder().build())
            .nodes(DiscoveryNodes.builder().build())
            .build();
    }

    private static IndexMetadata indexMetadata(int shards) {
        return IndexMetadata.builder(INDEX)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .build();
    }
}
