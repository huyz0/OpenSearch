/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.node;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.serverless.plugins.ServerlessPlugins;

import java.net.InetAddress;
import java.util.Set;

/**
 * Builds what every part of a node shares: its plugins and settings, its thread pool, its environment and identity,
 * and core's cluster, indices and search services. Nothing is started.
 */
final class NodeAssembly {

    private NodeAssembly() {}

    /**
     * Fills in a node under construction.
     *
     * @param node the node, whose own services exist but nothing they depend on
     * @param settings must carry {@code node.name}, {@code cluster.name} and {@code path.home}
     * @param extraPlugins plugins to run besides the installed ones
     * @throws Exception if the node environment cannot be created or the data plane cannot be built
     */
    static void assemble(NodeContext node, Settings settings, java.util.List<org.opensearch.plugins.Plugin> extraPlugins) throws Exception {
        // A node says it is one, before any plugin is constructed, exactly as core's Node does. Plugins read
        // client.type in their constructors to tell a node from a transport client, and with it unset the
        // security plugin decides it is a client and turns off its REST filter and its HTTP transport.
        settings = Settings.builder()
            .put(settings)
            .put(org.opensearch.transport.client.Client.CLIENT_TYPE_SETTING_S.getKey(), "node")
            .build();
        // Plugins first, before the settings are settled, because a plugin gets to contribute to them.
        //
        // The order is load-bearing. PluginsService is built from what the caller supplied, so a plugin is
        // constructed with the operator's configuration; updatedSettings then layers each plugin's
        // additionalSettings underneath it, so an explicit setting always beats a plugin's; and the shell's
        // own defaults go underneath both, so they apply only when nobody else had an opinion. Putting the
        // shell's transport choice on top -- which is where it used to be -- would have made
        // additionalSettings decorative for the one thing node.plugins most use it for.
        node.pluginsService = NodeServices.buildPluginsService(settings, new Environment(settings, null));
        final java.util.List<org.opensearch.plugins.Plugin> installed = new java.util.ArrayList<>(
            node.pluginsService.filterPlugins(org.opensearch.plugins.Plugin.class)
        );
        // Ones handed to this constructor come after the installed ones, so an operator's installation is
        // not silently outranked by something a test or an embedder passed in.
        installed.addAll(extraPlugins);
        node.plugins = new ServerlessPlugins(installed);
        node.settings = NodeServices.withShellDefaults(node.plugins.settingsWith(settings));
        node.searchSettings = new SearchSettings(node.settings);
        settings = node.settings;

        node.nodeName = settings.get("node.name", "serverless-node");
        node.views = new LocalViewApplier(
            node.nodeName,
            () -> node.localNode.getId(),
            node::clusterService,
            () -> node.membershipSource,
            () -> node.metadataPlane,
            node.startup::projectView
        );
        // Only with node.plugins installed: the shell's own code never asks for a cluster manager, so a node without
        // node.plugins keeps a view that names none.
        node.views.decorateWith(node::withPluginOwners);
        node.views.namesClusterManager(node.plugins.plugins().isEmpty() == false);
        // Role is a runtime attribute a node advertises, not a topology decision baked into a
        // cluster. One binary; an operator gets asymmetric scaling from two Deployments differing by one
        // node.environment variable, and role switching under load stays a policy question.
        node.roles = rolesFrom(settings);
        node.blockCache = NodeComponents.blockCache(settings);
        node.writes.writeBackpressureMax = WriteSettings.BACKPRESSURE_MAX_IN_FLIGHT.get(settings);
        final org.opensearch.serverless.usage.Guardrails guardrails = new org.opensearch.serverless.usage.Guardrails(
            settings,
            org.opensearch.serverless.bucket.BucketSettings.GROUP::get,
            () -> usageOf(node.metadataPlane),
            () -> node.threadPool == null
                ? Integer.MAX_VALUE
                : node.threadPool.info(org.opensearch.threadpool.ThreadPool.Names.SEARCH).getMax(),
            System::currentTimeMillis
        );
        node.attach(org.opensearch.serverless.usage.Guardrails.class, guardrails);
        node.writes.writePools = WritePools.of(settings, guardrails);
        node.writes.writeBackpressure = NodeComponents.writeBackpressure(settings, node.writes.writeBackpressureMax);
        // Refused here, at construction, rather than on the first shard it takes.
        node.nodeLogCommitCheckpointMillis = LogCommitCheckpoint.nodeValue(settings);
        node.logCommitCheckpointMillis = node.nodeLogCommitCheckpointMillis;
        // Rebuilt from the settled settings, so anything a plugin contributed is visible to everything
        // below.
        node.environment = new Environment(settings, null);
        node.clusterSettings = new ClusterSettings(settings, node.plugins.clusterSettings());

        // A pool of its own for fanned-out shard work. The coordinator of a search waits on GENERIC for
        // its shard tasks; when those tasks also ran on GENERIC, enough concurrent searches parked every
        // thread waiting for tasks no thread was free to run. A forwarded search handler ran on SEARCH and
        // blocked on the query phase forked to SEARCH, with the same shape. Both now run here, and this
        // pool waits on nothing that runs on it.
        node.threadPool = NodeThreadPools.build(settings, node.plugins.executorBuilders(settings));
        node.services = new NodeServices(
            settings,
            node.plugins,
            node.threadPool,
            () -> node.metadataPlane,
            () -> node,
            node::indicesService
        );
        node.readerValidation = new ReaderValidation(
            () -> node.localNode.getId(),
            () -> node.reconciler,
            () -> node.signals,
            node.threadPool,
            node.readerServing::forgetReader
        );
        node.readerValidation.setLiveOwnerCheckMillis(SearchSettings.READER_CHECK_LIVE_OWNER_MILLIS.get(settings));
        // An open fetches the blocks it will read concurrently rather than one round trip at a time, on GENERIC as
        // the log replay's reads do; the opening thread reads too, so a busy pool costs width only.
        node.blockCache.prefetchingWith(node.threadPool.generic());
        // A read's prefetch hints fetch ahead on the same pool, so a query's segments wait for their round trips together.
        node.blockCache.prefetchingReads(org.opensearch.serverless.store.CacheSettings.READ_PREFETCH.get(settings));
        boolean success = false;
        try {
            node.nodeEnvironment = new NodeEnvironment(settings, node.environment);
            node.localNode = NodeComponents.discoveryNode(
                node.nodeName,
                node.nodeEnvironment.nodeId(),
                new TransportAddress(InetAddress.getLoopbackAddress(), 0),
                node.roles
            );
            node.clusterService = node.services.buildClusterService(node.clusterSettings, node.localNode);
            node.indicesService = node.services.buildIndicesService(
                node.environment,
                node.clusterSettings,
                node.pluginsService,
                node.nodeEnvironment,
                node.clusterService,
                node.storage.engineFactoryProviders(),
                node.storage.directoryFactories()
            );
            org.opensearch.serverless.schema.SharedAnalysis.install(node.indicesService);
            node.searchService = node.services.buildSearchService(node.clusterSettings, node.clusterService, node.indicesService);
            node.nodeClient = NodeComponents.nodeClient(settings, node.threadPool, node.services);
            success = true;
        } finally {
            if (success == false) {
                node.closeAfterFailedConstruction();
            }
        }
    }

    /**
     * Reads a node's roles from its settings.
     *
     * <p>Unset means both roles, as one node of a small deployment runs. Set but empty means neither: a node that only
     * coordinates, taking requests and sending each write to a shard's writer and each search to search nodes, while
     * holding no shard itself. {@code Settings.getAsList} answers its default for an empty value, which made an empty
     * setting mean both roles, so the value is split here instead. An unknown role is refused, since a misspelled
     * {@code search} would otherwise start a node that silently serves nothing.
     *
     * @param settings the node settings
     * @return the roles, possibly empty
     * @throws IllegalArgumentException if a role is not one this node knows
     */
    static Set<String> rolesFrom(Settings settings) {
        final String raw = settings.get(NodeSettings.ROLES.getKey());
        if (raw == null) {
            return Set.of(NodeContext.ROLE_INGEST, NodeContext.ROLE_SEARCH);
        }
        final Set<String> roles = new java.util.TreeSet<>();
        for (String role : org.opensearch.core.common.Strings.splitStringByCommaToArray(raw)) {
            final String trimmed = role.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (trimmed.equals(NodeContext.ROLE_INGEST) == false && trimmed.equals(NodeContext.ROLE_SEARCH) == false) {
                throw new IllegalArgumentException(
                    "unknown role ["
                        + trimmed
                        + "] in ["
                        + NodeSettings.ROLES.getKey()
                        + "]; use ingest, search, both, or an empty value to only coordinate"
                );
            }
            roles.add(trimmed);
        }
        return Set.copyOf(roles);
    }

    private static org.opensearch.serverless.store.StoreUsage usageOf(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return plane != null && plane.blobStore() instanceof org.opensearch.serverless.store.MeteredBlobStore metered
            ? metered.usage()
            : null;
    }
}
