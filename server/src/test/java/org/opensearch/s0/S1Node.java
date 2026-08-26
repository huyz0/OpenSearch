/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.s0;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.SearchService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * One self-contained serverless node: its own {@link NodeEnvironment}, {@link ClusterService} and data
 * plane, with no control plane. Several may exist in one JVM, which is the point — S1 tests what S0
 * could not, namely that two nodes holding *different* node-local views both work.
 *
 * <p>Disposable, like the rest of {@code org.opensearch.s0}.
 */
final class S1Node implements Closeable {

    final String name;
    final DiscoveryNode discoveryNode;
    final Settings settings;
    final ThreadPool threadPool;
    final NodeEnvironment nodeEnv;
    final ClusterService clusterService;
    final IndicesService indicesService;
    final SearchService searchService;

    S1Node(String name, Path home) throws Exception {
        this.name = name;
        this.settings = Settings.builder().put("node.name", name).put("cluster.name", "s1-cluster").put("path.home", home).build();
        final Environment environment = TestEnvironment.newEnvironment(settings);
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        this.threadPool = new TestThreadPool(name);
        this.nodeEnv = new NodeEnvironment(settings, environment);
        this.discoveryNode = new DiscoveryNode(
            name,
            OpenSearchTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        this.clusterService = S0Wiring.clusterService(settings, clusterSettings, threadPool, discoveryNode);
        this.indicesService = S0Wiring.indicesService(settings, environment, nodeEnv, threadPool, clusterService, clusterSettings);
        this.indicesService.start();
        this.searchService = S0Wiring.searchService(
            settings,
            clusterSettings,
            threadPool,
            clusterService,
            indicesService,
            new BigArrays(new PageCacheRecycler(settings), null, name)
        );
        this.searchService.start();
    }

    /** Build the node-local view this node should hold, containing only the indices named. */
    ClusterState project(long version, IndexMetadata... indices) {
        final Metadata.Builder metadata = Metadata.builder();
        final RoutingTable.Builder routing = RoutingTable.builder();
        for (IndexMetadata im : indices) {
            metadata.put(im, false);
            routing.addAsRecovery(im);
        }
        return ClusterState.builder(new ClusterName("s1-cluster"))
            .version(version)
            .nodes(DiscoveryNodes.builder().add(discoveryNode).localNodeId(discoveryNode.getId()).build())
            .metadata(metadata.build())
            .routingTable(routing.build())
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
    }

    void apply(ClusterState state) throws Exception {
        S0Wiring.applyLocally(clusterService, state);
    }

    /** The four-call reconciler S0 measured, with the shard-head term supplied from outside. */
    IndexShard openAndStart(IndexMetadata indexMetadata, long shardHeadTerm) throws Exception {
        final IndexService indexService = indicesService.createIndex(indexMetadata, Collections.emptyList(), false);
        indexService.updateMapping(null, indexMetadata);

        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final ShardRouting initializing = TestShardRouting.newShardRouting(
            shardId,
            discoveryNode.getId(),
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );
        final IndexShard shard = S0Wiring.openShard(indexService, initializing, discoveryNode, clusterService);
        shard.markAsRecovering("s1-store", new org.opensearch.indices.recovery.RecoveryState(initializing, discoveryNode, null));
        final org.opensearch.action.support.PlainActionFuture<Boolean> recovered = org.opensearch.action.support.PlainActionFuture
            .newFuture();
        shard.recoverFromStore(recovered);
        if (recovered.actionGet() == false) {
            throw new AssertionError("recoverFromStore reported failure on " + name);
        }
        final ShardRouting started = initializing.moveToStarted();
        shard.updateShardState(
            started,
            shardHeadTerm,
            null,
            shardHeadTerm,
            Set.of(started.allocationId().getId()),
            new IndexShardRoutingTable.Builder(shardId).addShard(started).build(),
            clusterService.state().nodes()
        );
        return shard;
    }

    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(searchService, indicesService, clusterService, nodeEnv);
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }
}
