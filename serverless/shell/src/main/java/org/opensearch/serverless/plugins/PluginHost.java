/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.plugins;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.script.ScriptService;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

/**
 * What a plugin's {@code createComponents} is handed, gathered by the node that runs it.
 *
 * @param client the node's client
 * @param clusterService the node's cluster service
 * @param threadPool the node's thread pool
 * @param resourceWatcherService the node's file watcher
 * @param scriptService the node's script service
 * @param xContentRegistry the registry searches parse with
 * @param environment the node's environment
 * @param nodeEnvironment the node's data paths
 * @param namedWriteableRegistry the node's wire registry
 * @param indexNameExpressionResolver the node's index name resolver
 * @param tracer the node's tracer
 * @param metricsRegistry the node's metrics registry
 */
public record PluginHost(Client client, ClusterService clusterService, ThreadPool threadPool, ResourceWatcherService resourceWatcherService,
    ScriptService scriptService, NamedXContentRegistry xContentRegistry, Environment environment, NodeEnvironment nodeEnvironment,
    NamedWriteableRegistry namedWriteableRegistry, IndexNameExpressionResolver indexNameExpressionResolver, Tracer tracer,
    MetricsRegistry metricsRegistry) {
}
