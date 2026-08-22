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
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.discovery.Discovery;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.index.SegmentReplicationStatsTracker;
import org.opensearch.indices.IndicesService;
import org.opensearch.ingest.IngestService;
import org.opensearch.monitor.MonitorService;
import org.opensearch.plugin.stats.NativeAllocatorPoolStats;
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

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for how the native-allocator pool stats reach {@code _nodes/stats} after their migration
 * onto the generic {@link PluginNodeStats}/pluginStats path: the owning plugin (arrow-base in
 * production, a stand-in here) contributes a {@link NativeAllocatorPoolStats} from
 * {@link Plugin#nodeStats()}, {@code NodeService.stats(..., pluginStats=true)} collects it under its
 * {@code getWriteableName()} ({@code "native_allocator"}), and the {@code pluginStats=false} path
 * collects nothing. The legacy {@code nativeMemory} flag no longer gates any dedicated collection.
 */
public class NodeServiceNativeMemoryTests extends OpenSearchTestCase {

    /** Stand-in for ArrowBasePlugin's nodeStats() contribution. */
    private static final class AllocatorStatsPlugin extends Plugin {
        private final NativeAllocatorPoolStats stats;

        AllocatorStatsPlugin(NativeAllocatorPoolStats stats) {
            this.stats = stats;
        }

        @Override
        public List<PluginNodeStats> nodeStats() {
            return stats == null ? List.of() : List.of(stats);
        }
    }

    private NodeService createNodeService(List<Plugin> plugins) {
        TransportService transportService = mock(TransportService.class);
        DiscoveryNode localNode = new DiscoveryNode("test_node", buildNewFakeTransportAddress(), Version.CURRENT);
        when(transportService.getLocalNode()).thenReturn(localNode);
        PluginsService pluginsService = mock(PluginsService.class);
        when(pluginsService.filterPlugins(Plugin.class)).thenReturn(plugins);

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

    private static NodeStats callStats(NodeService nodeService, boolean pluginStats) {
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
            false, // nativeMemory (vestigial: gates nothing dedicated anymore)
            pluginStats
        );
    }

    /**
     * With pluginStats=true and a plugin contributing allocator stats, the snapshot lands in the
     * pluginStats map under the contribution's writeable name.
     */
    public void testStatsWithPluginStatsTrueAndContributionPresent() {
        NativeAllocatorPoolStats expected = new NativeAllocatorPoolStats(
            1024L,
            2048L,
            List.of(new NativeAllocatorPoolStats.PoolStats("flight", 100L, 200L, 2048L))
        );
        NodeService nodeService = createNodeService(List.of(new AllocatorStatsPlugin(expected)));

        NodeStats nodeStats = callStats(nodeService, true);

        assertSame(
            "allocator stats must be collected under their writeable name",
            expected,
            nodeStats.getPluginStats().get(NativeAllocatorPoolStats.WRITEABLE_NAME)
        );
    }

    /**
     * With pluginStats=true but no plugin contributing (allocator plugin absent, or its allocator not
     * built), the map is simply empty.
     */
    public void testStatsWithPluginStatsTrueAndNoContribution() {
        NodeService nodeService = createNodeService(List.of(new AllocatorStatsPlugin(null)));

        NodeStats nodeStats = callStats(nodeService, true);

        assertTrue("pluginStats must be empty when no plugin contributes", nodeStats.getPluginStats().isEmpty());
    }

    /**
     * With pluginStats=false nothing is collected, regardless of what plugins would contribute.
     */
    public void testStatsWithPluginStatsFalse() {
        NativeAllocatorPoolStats expected = new NativeAllocatorPoolStats(
            4096L,
            8192L,
            List.of(new NativeAllocatorPoolStats.PoolStats("flight", 100L, 200L, 2048L))
        );
        NodeService nodeService = createNodeService(List.of(new AllocatorStatsPlugin(expected)));

        NodeStats nodeStats = callStats(nodeService, false);

        assertTrue("pluginStats must be empty when the pluginStats flag is off", nodeStats.getPluginStats().isEmpty());
    }
}
