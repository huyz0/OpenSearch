/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.os.OsService;
import org.opensearch.monitor.process.ProcessService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Node info, node stats, indices stats and refresh, answered for plugins from what this node holds.
 *
 * <p><b>Why plugins ask.</b> A metrics exporter reads node and index statistics through the client on every
 * scrape; Index Management, alerting and anomaly detection refresh their own system indices after writing them.
 * The client refused all four, so the exporter answered nothing and those plugins failed after their first write.
 *
 * <p><b>Only this node.</b> On a classic node these requests fan out to every node the request names, through
 * the cluster manager's node list. Here no node answers for another, as the shell's REST stats endpoint does
 * not: a request that names other nodes gets this node's answer and a failure for each of them that says to ask
 * that node. A metrics exporter scrapes every node anyway, so nothing is lost and no request crosses nodes.
 *
 * <p><b>Only what is open.</b> Index statistics and refresh cover the shards open on this node. A shard nobody
 * has opened has no statistics to report and no unrefreshed writes, so leaving it out is the answer core would
 * give if it could ask. The cost is bounded by the shards this node serves, never by the indices that exist.
 */
final class PluginNodeAnswers {

    private PluginNodeAnswers() {}

    /** The monitoring services core's node runs, built once per node the first time a plugin asks. */
    static final class Monitors {
        final OsService os;
        final ProcessService process;
        final JvmService jvm;
        final FsService fs;

        Monitors(ServerlessNode node) throws IOException {
            this.os = new OsService(node.settings);
            this.process = new ProcessService(node.settings);
            this.jvm = new JvmService(node.settings);
            this.fs = new FsService(node.settings, node.nodeEnvironment(), null);
        }
    }

    static NodesInfoResponse nodesInfo(ServerlessNode node, NodesInfoRequest request) throws IOException {
        final DiscoveryNode local = node.localNode();
        final List<NodeInfo> answered = new ArrayList<>();
        final List<FailedNodeException> failures = new ArrayList<>();
        if (selectsHere(node, request.nodesIds(), failures)) {
            final Set<String> metrics = request.requestedMetrics();
            final Monitors monitors = node.monitors();
            answered.add(
                new NodeInfo(
                    Version.CURRENT,
                    Build.CURRENT,
                    local,
                    metrics.contains(NodesInfoRequest.Metric.SETTINGS.metricName()) ? node.settings : null,
                    metrics.contains(NodesInfoRequest.Metric.OS.metricName()) ? monitors.os.info() : null,
                    metrics.contains(NodesInfoRequest.Metric.PROCESS.metricName()) ? monitors.process.info() : null,
                    metrics.contains(NodesInfoRequest.Metric.JVM.metricName()) ? monitors.jvm.info() : null,
                    metrics.contains(NodesInfoRequest.Metric.THREAD_POOL.metricName()) ? node.threadPool().info() : null,
                    metrics.contains(NodesInfoRequest.Metric.TRANSPORT.metricName()) ? node.transportService.info() : null,
                    metrics.contains(NodesInfoRequest.Metric.HTTP.metricName()) && node.httpServerTransport != null
                        ? node.httpServerTransport.info()
                        : null,
                    // The plugins this node runs: the anomaly-detection plugin places models only on nodes that report it.
                    metrics.contains(NodesInfoRequest.Metric.PLUGINS.metricName()) ? node.pluginsService.info() : null,
                    null,
                    null,
                    null,
                    null
                )
            );
        }
        return new NodesInfoResponse(node.clusterService().state().getClusterName(), answered, failures);
    }

    static NodesStatsResponse nodesStats(ServerlessNode node, NodesStatsRequest request) throws IOException {
        final List<NodeStats> answered = new ArrayList<>();
        final List<FailedNodeException> failures = new ArrayList<>();
        if (selectsHere(node, request.nodesIds(), failures)) {
            final Set<String> metrics = request.requestedMetrics();
            final Monitors monitors = node.monitors();
            answered.add(
                new NodeStats(
                    node.localNode(),
                    System.currentTimeMillis(),
                    request.indices().anySet() ? node.indicesService().stats(request.indices()) : null,
                    metrics.contains(NodesStatsRequest.Metric.OS.metricName()) ? monitors.os.stats() : null,
                    metrics.contains(NodesStatsRequest.Metric.PROCESS.metricName()) ? monitors.process.stats() : null,
                    metrics.contains(NodesStatsRequest.Metric.JVM.metricName()) ? monitors.jvm.stats() : null,
                    metrics.contains(NodesStatsRequest.Metric.THREAD_POOL.metricName()) ? node.threadPool().stats() : null,
                    metrics.contains(NodesStatsRequest.Metric.FS.metricName()) ? monitors.fs.stats() : null,
                    metrics.contains(NodesStatsRequest.Metric.TRANSPORT.metricName()) ? node.transportService.stats() : null,
                    metrics.contains(NodesStatsRequest.Metric.HTTP.metricName()) && node.httpServerTransport != null
                        ? node.httpServerTransport.stats()
                        : null,
                    metrics.contains(NodesStatsRequest.Metric.BREAKER.metricName())
                        ? node.services().circuitBreakerService().stats()
                        : null,
                    metrics.contains(NodesStatsRequest.Metric.SCRIPT.metricName()) ? node.services().scriptService().stats() : null,
                    null,
                    null,
                    null,
                    null,
                    metrics.contains(NodesStatsRequest.Metric.SCRIPT_CACHE.metricName())
                        ? node.services().scriptService().cacheStats()
                        : null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    0L
                )
            );
        }
        return new NodesStatsResponse(node.clusterService().state().getClusterName(), answered, failures);
    }

    static IndicesStatsResponse indicesStats(ServerlessNode node, IndicesStatsRequest request) {
        final List<ShardStats> stats = new ArrayList<>();
        for (IndexShard shard : openShards(node, request.indices())) {
            if (shard.routingEntry() == null) {
                continue;
            }
            final CommonStats common = new CommonStats(node.indicesService().getIndicesQueryCache(), shard, request.flags());
            ShardStats.Builder builder = new ShardStats.Builder().shardRouting(shard.routingEntry())
                .shardPath(shard.shardPath())
                .commonStats(common);
            try {
                builder = builder.commitStats(shard.commitStats())
                    .seqNoStats(shard.seqNoStats())
                    .retentionLeaseStats(shard.getRetentionLeaseStats())
                    .pollingIngestStats(shard.pollingIngestStats());
            } catch (org.apache.lucene.store.AlreadyClosedException e) {
                // Closed while being read: its common statistics stand, as core reports them.
            }
            stats.add(builder.build());
        }
        return new IndicesStatsResponse(stats.toArray(new ShardStats[0]), stats.size(), stats.size(), 0, List.of());
    }

    static RefreshResponse refresh(ServerlessNode node, RefreshRequest request) throws IOException {
        int total = 0;
        int refreshed = 0;
        for (IndexShard shard : openShards(node, request.indices())) {
            total++;
            if (node.writes.refresh(shard.shardId(), "plugin refresh")) {
                refreshed++;
            }
        }
        // The response's constructor is not public; its parser is, and builds the same object.
        final String body = "{\"_shards\":{\"total\":" + total + ",\"successful\":" + refreshed + ",\"failed\":0}}";
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                body
            )
        ) {
            return RefreshResponse.fromXContent(parser);
        }
    }

    /** The open shards whose index matches one of the patterns; every open shard when none is given. */
    private static List<IndexShard> openShards(ServerlessNode node, String[] patterns) {
        final boolean all = patterns == null
            || patterns.length == 0
            || (patterns.length == 1 && ("_all".equals(patterns[0]) || "*".equals(patterns[0])));
        final List<IndexShard> shards = new ArrayList<>();
        if (node.reconciler == null) {
            return shards;
        }
        for (ShardId shardId : node.reconciler.openShards()) {
            if (all || Regex.simpleMatch(patterns, shardId.getIndexName())) {
                final IndexShard shard = node.reconciler.shard(shardId);
                if (shard != null) {
                    shards.add(shard);
                }
            }
        }
        return shards;
    }

    /**
     * Whether the request selects this node, recording a failure for every other node it selects.
     *
     * <p>Node names, ids, {@code _local}, {@code _all} and attribute filters are resolved by core against the
     * node's view, so they mean what they mean on a classic node.
     */
    private static boolean selectsHere(ServerlessNode node, String[] nodeIds, List<FailedNodeException> failures) {
        final ClusterState view = node.clusterService().state();
        final String localId = view.nodes().getLocalNodeId();
        boolean here = false;
        for (String id : view.nodes().resolveNodes(nodeIds)) {
            if (id.equals(localId)) {
                here = true;
            } else {
                failures.add(new FailedNodeException(id, "this node answers only for itself; send the request to node [" + id + "]", null));
            }
        }
        return here;
    }
}
