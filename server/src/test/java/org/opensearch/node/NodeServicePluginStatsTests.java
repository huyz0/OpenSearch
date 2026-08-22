/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.search.SearchTransportService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.discovery.Discovery;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.index.SegmentReplicationStatsTracker;
import org.opensearch.indices.IndicesService;
import org.opensearch.ingest.IngestService;
import org.opensearch.monitor.MonitorService;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginNodeStats;
import org.opensearch.plugins.PluginsService;
import org.opensearch.ratelimitting.admissioncontrol.AdmissionControlService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.backpressure.SearchBackpressureService;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.tasks.TaskCancellationMonitoringService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Phase B of core-pluggability-refactor-plan.md: NodeService#collectPluginStats gathers every installed
 * plugin's {@link Plugin#nodeStats()} contribution into {@link NodeStats#getPluginStats()}, keyed by each
 * entry's {@code getWriteableName()}. This is the regression test for that collection logic -- the
 * generic seam a plugin other than the native-allocator one can now use again.
 */
public class NodeServicePluginStatsTests extends OpenSearchTestCase {

    /** A minimal, hand-rolled PluginNodeStats for the test -- never actually round-tripped over the wire here. */
    private static final class FakePluginNodeStats implements PluginNodeStats {
        private final String name;
        private final long value;

        FakePluginNodeStats(String name, long value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String getWriteableName() {
            return name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(value);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field("value", value);
        }
    }

    private static final class OneStatPlugin extends Plugin {
        private final PluginNodeStats stats;

        OneStatPlugin(PluginNodeStats stats) {
            this.stats = stats;
        }

        @Override
        public List<PluginNodeStats> nodeStats() {
            return List.of(stats);
        }
    }

    private NodeService createNodeService(PluginsService pluginsService) {
        TransportService transportService = mock(TransportService.class);
        DiscoveryNode localNode = new DiscoveryNode("test_node", buildNewFakeTransportAddress(), Version.CURRENT);
        when(transportService.getLocalNode()).thenReturn(localNode);

        return new NodeService(
            Settings.EMPTY,
            mock(ThreadPool.class),
            mock(MonitorService.class),
            mock(Discovery.class),
            transportService,
            mock(IndicesService.class),
            pluginsService,
            mock(CircuitBreakerService.class),
            mock(ScriptService.class),
            null, // httpServerTransport
            mock(IngestService.class),
            mock(ClusterService.class),
            new SettingsFilter(Collections.emptyList()),
            null, // responseCollectorService - not needed when adaptiveSelection=false
            mock(SearchTransportService.class),
            mock(IndexingPressureService.class),
            null, // aggregationUsageService
            mock(SearchBackpressureService.class),
            mock(SearchPipelineService.class),
            null, // nodeCacheService
            mock(TaskCancellationMonitoringService.class),
            null, // resourceUsageCollectorService
            mock(SegmentReplicationStatsTracker.class),
            mock(RepositoriesService.class),
            mock(AdmissionControlService.class),
            null // cacheService
        );
    }

    private NodeStats statsWithPluginStats(NodeService nodeService, boolean pluginStats) {
        return nodeService.stats(
            CommonStatsFlags.NONE,
            false, // os
            false, // process
            false, // jvm
            false, // threadPool
            false, // fs
            false, // transport
            false, // http
            false, // circuitBreaker
            false, // script
            false, // discoveryStats
            false, // ingest
            false, // adaptiveSelection
            false, // scriptCache
            false, // indexingPressure
            false, // shardIndexingPressure
            false, // searchBackpressure
            false, // clusterManagerThrottling
            false, // weightedRoutingStats
            false, // fileCacheStats
            false, // fileCacheDetailed
            false, // taskCancellation
            false, // searchPipelineStats
            false, // resourceUsageStats
            false, // segmentReplicationTrackerStats
            false, // repositoriesStats
            false, // admissionControl
            false, // cacheService
            false, // remoteStoreNodeStats
            false, // nativeMemory
            pluginStats
        );
    }

    public void testPluginStatsCollectedWhenRequested() {
        PluginsService pluginsService = mock(PluginsService.class);
        FakePluginNodeStats stats = new FakePluginNodeStats("test_plugin_stats", 42L);
        when(pluginsService.filterPlugins(Plugin.class)).thenReturn(List.of(new OneStatPlugin(stats)));

        NodeStats nodeStats = statsWithPluginStats(createNodeService(pluginsService), true);

        assertEquals(1, nodeStats.getPluginStats().size());
        assertSame(stats, nodeStats.getPluginStats().get("test_plugin_stats"));
    }

    public void testPluginStatsFromMultiplePluginsAreAllCollected() {
        PluginsService pluginsService = mock(PluginsService.class);
        FakePluginNodeStats first = new FakePluginNodeStats("plugin_one", 1L);
        FakePluginNodeStats second = new FakePluginNodeStats("plugin_two", 2L);
        when(pluginsService.filterPlugins(Plugin.class)).thenReturn(List.of(new OneStatPlugin(first), new OneStatPlugin(second)));

        NodeStats nodeStats = statsWithPluginStats(createNodeService(pluginsService), true);

        assertEquals(2, nodeStats.getPluginStats().size());
        assertSame(first, nodeStats.getPluginStats().get("plugin_one"));
        assertSame(second, nodeStats.getPluginStats().get("plugin_two"));
    }

    public void testPluginStatsNotCollectedWhenNotRequested() {
        PluginsService pluginsService = mock(PluginsService.class);
        when(pluginsService.filterPlugins(Plugin.class)).thenReturn(
            List.of(new OneStatPlugin(new FakePluginNodeStats("should_not_appear", 0L)))
        );

        NodeStats nodeStats = statsWithPluginStats(createNodeService(pluginsService), false);

        assertTrue("pluginStats=false must not collect anything, even if a plugin has stats", nodeStats.getPluginStats().isEmpty());
    }

    public void testNoPluginsMeansEmptyMapNotNull() {
        PluginsService pluginsService = mock(PluginsService.class);
        when(pluginsService.filterPlugins(Plugin.class)).thenReturn(Collections.emptyList());

        NodeStats nodeStats = statsWithPluginStats(createNodeService(pluginsService), true);

        assertNotNull(nodeStats.getPluginStats());
        assertTrue(nodeStats.getPluginStats().isEmpty());
    }
}
