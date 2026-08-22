/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.node;

import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.search.SearchTransportService;
import org.opensearch.cluster.routing.WeightedRoutingStats;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.cache.service.CacheService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.discovery.Discovery;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.index.SegmentReplicationStatsTracker;
import org.opensearch.index.store.remote.filecache.NodeCacheService;
import org.opensearch.indices.IndicesService;
import org.opensearch.ingest.IngestService;
import org.opensearch.monitor.MonitorService;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.node.remotestore.RemoteStoreNodeStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginNodeStats;
import org.opensearch.plugins.PluginsService;
import org.opensearch.ratelimitting.admissioncontrol.AdmissionControlService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.aggregations.support.AggregationUsageService;
import org.opensearch.search.backpressure.SearchBackpressureService;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.tasks.TaskCancellationMonitoringService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Services exposed to nodes
 *
 * @opensearch.internal
 */
public class NodeService implements Closeable {
    private final Settings settings;
    private final ThreadPool threadPool;
    private final MonitorService monitorService;
    private final TransportService transportService;
    private final IndicesService indicesService;
    private final PluginsService pluginService;
    private final CircuitBreakerService circuitBreakerService;
    private final IngestService ingestService;
    private final SettingsFilter settingsFilter;
    private final ScriptService scriptService;
    private final HttpServerTransport httpServerTransport;
    private final ResponseCollectorService responseCollectorService;
    private final ResourceUsageCollectorService resourceUsageCollectorService;
    private final SearchTransportService searchTransportService;
    private final IndexingPressureService indexingPressureService;
    private final AggregationUsageService aggregationUsageService;
    private final SearchBackpressureService searchBackpressureService;
    private final SearchPipelineService searchPipelineService;
    private final ClusterService clusterService;
    private final Discovery discovery;
    @Nullable
    private final NodeCacheService nodeCacheService;
    private final TaskCancellationMonitoringService taskCancellationMonitoringService;
    private final RepositoriesService repositoriesService;
    private final AdmissionControlService admissionControlService;
    private final SegmentReplicationStatsTracker segmentReplicationStatsTracker;
    private final CacheService cacheService;

    NodeService(
        Settings settings,
        ThreadPool threadPool,
        MonitorService monitorService,
        Discovery discovery,
        TransportService transportService,
        IndicesService indicesService,
        PluginsService pluginService,
        CircuitBreakerService circuitBreakerService,
        ScriptService scriptService,
        @Nullable HttpServerTransport httpServerTransport,
        IngestService ingestService,
        ClusterService clusterService,
        SettingsFilter settingsFilter,
        ResponseCollectorService responseCollectorService,
        SearchTransportService searchTransportService,
        IndexingPressureService indexingPressureService,
        AggregationUsageService aggregationUsageService,
        SearchBackpressureService searchBackpressureService,
        SearchPipelineService searchPipelineService,
        @Nullable NodeCacheService nodeCacheService,
        TaskCancellationMonitoringService taskCancellationMonitoringService,
        ResourceUsageCollectorService resourceUsageCollectorService,
        SegmentReplicationStatsTracker segmentReplicationStatsTracker,
        RepositoriesService repositoriesService,
        AdmissionControlService admissionControlService,
        CacheService cacheService
    ) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.monitorService = monitorService;
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.discovery = discovery;
        this.pluginService = pluginService;
        this.circuitBreakerService = circuitBreakerService;
        this.httpServerTransport = httpServerTransport;
        this.ingestService = ingestService;
        this.settingsFilter = settingsFilter;
        this.scriptService = scriptService;
        this.responseCollectorService = responseCollectorService;
        this.searchTransportService = searchTransportService;
        this.indexingPressureService = indexingPressureService;
        this.aggregationUsageService = aggregationUsageService;
        this.searchBackpressureService = searchBackpressureService;
        this.searchPipelineService = searchPipelineService;
        this.clusterService = clusterService;
        this.nodeCacheService = nodeCacheService;
        this.taskCancellationMonitoringService = taskCancellationMonitoringService;
        this.resourceUsageCollectorService = resourceUsageCollectorService;
        this.repositoriesService = repositoriesService;
        this.admissionControlService = admissionControlService;
        clusterService.addStateApplier(ingestService);
        clusterService.addStateApplier(searchPipelineService);
        this.segmentReplicationStatsTracker = segmentReplicationStatsTracker;
        this.cacheService = cacheService;
    }

    public NodeInfo info(
        boolean settings,
        boolean os,
        boolean process,
        boolean jvm,
        boolean threadPool,
        boolean transport,
        boolean http,
        boolean plugin,
        boolean ingest,
        boolean aggs,
        boolean indices,
        boolean searchPipeline
    ) {
        NodeInfo.Builder builder = NodeInfo.builder(Version.CURRENT, Build.CURRENT, transportService.getLocalNode());
        if (settings) {
            builder.setSettings(settingsFilter.filter(this.settings));
        }
        if (os) {
            builder.setOs(monitorService.osService().info());
        }
        if (process) {
            builder.setProcess(monitorService.processService().info());
        }
        if (jvm) {
            builder.setJvm(monitorService.jvmService().info());
        }
        if (threadPool) {
            builder.setThreadPool(this.threadPool.info());
        }
        if (transport) {
            builder.setTransport(transportService.info());
        }
        if (http && httpServerTransport != null) {
            builder.setHttp(httpServerTransport.info());
        }
        if (plugin && pluginService != null) {
            builder.setPlugins(pluginService.info());
        }
        if (ingest && ingestService != null) {
            builder.setIngest(ingestService.info());
        }
        if (aggs && aggregationUsageService != null) {
            builder.setAggsInfo(aggregationUsageService.info());
        }
        if (indices) {
            builder.setTotalIndexingBuffer(indicesService.getTotalIndexingBufferBytes());
        }
        if (searchPipeline && searchPipelineService != null) {
            builder.setSearchPipelineInfo(searchPipelineService.info());
        }
        return builder.build();
    }

    public NodeStats stats(
        CommonStatsFlags indices,
        boolean os,
        boolean process,
        boolean jvm,
        boolean threadPool,
        boolean fs,
        boolean transport,
        boolean http,
        boolean circuitBreaker,
        boolean script,
        boolean discoveryStats,
        boolean ingest,
        boolean adaptiveSelection,
        boolean scriptCache,
        boolean indexingPressure,
        boolean shardIndexingPressure,
        boolean searchBackpressure,
        boolean clusterManagerThrottling,
        boolean weightedRoutingStats,
        boolean fileCacheStats,
        boolean fileCacheDetailed,
        boolean taskCancellation,
        boolean searchPipelineStats,
        boolean resourceUsageStats,
        boolean segmentReplicationTrackerStats,
        boolean repositoriesStats,
        boolean admissionControl,
        boolean cacheService,
        boolean remoteStoreNodeStats,
        // nativeMemory: retained for API stability. It used to gate the dedicated native-allocator
        // pool-stats collection, which migrated to the generic pluginStats path (the arrow-base
        // plugin's Plugin#nodeStats() contribution, gated by the pluginStats flag below); the
        // process-level native-memory estimate it also covered is now always captured.
        boolean nativeMemory,
        boolean pluginStats
    ) {
        // for indices stats we want to include previous allocated shards stats as well (it will
        // only be applied to the sensible ones to use, like refresh/merge/flush/indexing stats)
        return new NodeStats(
            transportService.getLocalNode(),
            System.currentTimeMillis(),
            indices.anySet() ? indicesService.stats(indices) : null,
            os ? monitorService.osService().stats() : null,
            process ? monitorService.processService().stats() : null,
            jvm ? monitorService.jvmService().stats() : null,
            threadPool ? this.threadPool.stats() : null,
            fs ? monitorService.fsService().stats() : null,
            transport ? transportService.stats() : null,
            http ? (httpServerTransport == null ? null : httpServerTransport.stats()) : null,
            circuitBreaker ? circuitBreakerService.stats() : null,
            script ? scriptService.stats() : null,
            discoveryStats ? discovery.stats() : null,
            ingest ? ingestService.stats() : null,
            adaptiveSelection ? responseCollectorService.getAdaptiveStats(searchTransportService.getPendingSearchRequests()) : null,
            resourceUsageStats ? resourceUsageCollectorService.stats() : null,
            scriptCache ? scriptService.cacheStats() : null,
            indexingPressure ? this.indexingPressureService.nodeStats() : null,
            shardIndexingPressure ? this.indexingPressureService.shardStats(indices) : null,
            searchBackpressure ? this.searchBackpressureService.nodeStats() : null,
            clusterManagerThrottling ? this.clusterService.getClusterManagerService().getThrottlingStats() : null,
            weightedRoutingStats ? WeightedRoutingStats.getInstance() : null,
            fileCacheStats && nodeCacheService != null ? nodeCacheService.aggregateStats() : null,
            fileCacheDetailed && nodeCacheService != null ? nodeCacheService.fileCacheStatsOnly() : null,
            fileCacheDetailed && nodeCacheService != null ? nodeCacheService.combinedBlockCacheStats() : null,
            taskCancellation ? this.taskCancellationMonitoringService.stats() : null,
            searchPipelineStats ? this.searchPipelineService.stats() : null,
            segmentReplicationTrackerStats ? this.segmentReplicationStatsTracker.getTotalRejectionStats() : null,
            repositoriesStats ? this.repositoriesService.getRepositoriesStats() : null,
            admissionControl ? this.admissionControlService.stats() : null,
            cacheService ? this.cacheService.stats(indices) : null,
            remoteStoreNodeStats ? new RemoteStoreNodeStats() : null,
            // Always capture the process-level native memory estimate on this data node.
            // Serialized over the wire so the coordinator renders the source node's value,
            // not its own. Returns -1 on non-Linux platforms or when /proc/self/status is
            // unreadable.
            cachedProcessNativeMemoryBytes(),
            pluginStats ? collectPluginStats() : null
        );
    }

    /**
     * How long one {@code /proc/self/status} reading is reused.
     *
     * <p>{@link OsProbe#getProcessNativeMemoryBytes()} reads and line-scans {@code /proc/self/status} and
     * then queries two {@code MemoryMXBean} pools -- roughly 50-200 microseconds of syscall and parsing --
     * and it ran on every {@code stats(...)} call regardless of what the caller actually asked for. The
     * {@code nativeMemory} flag that used to gate it is now ignored on purpose (see the parameter's own
     * comment: the value is deliberately always captured), so gating it again would change what
     * {@code _nodes/stats} reports rather than only what it costs.
     *
     * <p>A short reuse window removes the repetition without changing the value anybody sees: a resident
     * set does not move meaningfully inside a second, and the several stats consumers on a node (monitoring
     * polls, the coordinator fanning out, internal callers) frequently sample within one.
     */
    private static final long NATIVE_MEMORY_CACHE_NANOS = TimeUnit.SECONDS.toNanos(1);

    private volatile long cachedNativeMemoryBytes = -1L;
    // Initialised exactly one window in the past rather than to a sentinel, so the first call always reads
    // the probe and the comparison below never has to special-case "nothing cached yet" (nor risk the
    // overflow a Long.MIN_VALUE sentinel would produce in the subtraction).
    private volatile long cachedNativeMemoryAtNanos = System.nanoTime() - NATIVE_MEMORY_CACHE_NANOS;

    /**
     * The process-level native memory estimate, re-read at most once per {@link #NATIVE_MEMORY_CACHE_NANOS}.
     *
     * <p>Unsynchronised on purpose: two callers racing past the window both read the probe and both publish,
     * which costs one redundant read and can never publish anything other than a genuine reading.
     */
    private long cachedProcessNativeMemoryBytes() {
        long now = System.nanoTime();
        if (now - cachedNativeMemoryAtNanos < NATIVE_MEMORY_CACHE_NANOS) {
            return cachedNativeMemoryBytes;
        }
        long bytes = OsProbe.getInstance().getProcessNativeMemoryBytes();
        cachedNativeMemoryBytes = bytes;
        cachedNativeMemoryAtNanos = now;
        return bytes;
    }

    /**
     * Collects every installed plugin's own
     * {@link Plugin#nodeStats()} contribution into one map, keyed by each entry's
     * {@code getWriteableName()} -- the same key {@link NodeStats#toXContent} renders it under and the
     * transport wire format frames it by. A later plugin overwriting an earlier one under the same key is
     * a plugin-authoring bug (two plugins both naming their {@link PluginNodeStats} the same
     * {@code getWriteableName()}), not something this method tries to detect or resolve.
     */
    private Map<String, PluginNodeStats> collectPluginStats() {
        Map<String, PluginNodeStats> collected = new HashMap<>();
        for (Plugin plugin : pluginService.filterPlugins(Plugin.class)) {
            for (PluginNodeStats stats : plugin.nodeStats()) {
                collected.put(stats.getWriteableName(), stats);
            }
        }
        return collected;
    }

    public IngestService getIngestService() {
        return ingestService;
    }

    public MonitorService getMonitorService() {
        return monitorService;
    }

    public SearchBackpressureService getSearchBackpressureService() {
        return searchBackpressureService;
    }

    public TaskCancellationMonitoringService getTaskCancellationMonitoringService() {
        return taskCancellationMonitoringService;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(indicesService);
    }

    /**
     * Wait for the node to be effectively closed.
     * @see IndicesService#awaitClose(long, TimeUnit)
     */
    public boolean awaitClose(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return indicesService.awaitClose(timeout, timeUnit);
    }

}
