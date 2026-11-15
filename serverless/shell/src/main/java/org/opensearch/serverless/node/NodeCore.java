/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.node;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.PluginsService;
import org.opensearch.rest.RestController;
import org.opensearch.search.SearchService;
import org.opensearch.serverless.cluster.LocalViewProjector;
import org.opensearch.serverless.membership.MembershipSource;
import org.opensearch.serverless.plugins.PluginClusterSettings;
import org.opensearch.serverless.plugins.ServerlessPlugins;
import org.opensearch.serverless.shard.ShardReconciler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Set;

/**
 * A node's identity, its settings and plugins, and the core OpenSearch services it runs on: what every part of the node
 * reads, and what {@link NodeContext}'s own services are built over. Filled in while the node is assembled.
 */
public abstract class NodeCore {

    protected static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(NodeContext.class);

    /** Accepts writer activation: takes ownership of shards and indexes into them. */
    public static final String ROLE_INGEST = "ingest";

    /** Accepts reader activation: serves search from object-store-backed segments. */
    public static final String ROLE_SEARCH = "search";

    protected String nodeName;
    protected Settings settings;
    protected ServerlessPlugins plugins;
    protected ThreadPool threadPool;
    /** Builds the core services and the shared collaborators the node and its plugins are handed. */
    protected NodeServices services;
    /** The check that keeps a reader from serving a commit behind the log, and its memo. */
    protected ReaderValidation readerValidation;
    protected NodeEnvironment nodeEnvironment;
    protected ClusterService clusterService;
    protected IndicesService indicesService;
    protected SearchService searchService;
    public PluginsService pluginsService;
    /** Whether an index belongs to a plugin; none does until the wiring says which. */
    protected volatile java.util.function.Predicate<String> systemIndex = index -> false;
    protected RestController restController;
    public TransportService transportService;
    public HttpServerTransport httpServerTransport;
    protected NodeClient nodeClient;
    public volatile DiscoveryNode localNode;
    volatile MembershipSource membershipSource;
    public volatile org.opensearch.serverless.metadata.MetadataPlane metadataPlane;
    protected volatile ReaderPush push;
    private final java.util.Map<Class<?>, Object> parts = new java.util.concurrent.ConcurrentHashMap<>();
    protected Set<String> roles;
    protected org.opensearch.serverless.store.BlockCache blockCache;

    volatile org.opensearch.serverless.node.ReconcileSignals signals = org.opensearch.serverless.node.ReconcileSignals.NONE;
    volatile LocalViewProjector projector;
    volatile ShardReconciler reconciler;
    public volatile boolean started;

    /** The node's environment, rebuilt from its settled settings. */
    protected Environment environment;

    /** The node's cluster settings: core's, the shell's own and those its plugins declare. */
    protected ClusterSettings clusterSettings;

    private PluginClusterSettings pluginClusterSettings;
    /** The search settings this node was started with. */
    protected SearchSettings searchSettings;

    /** {@return this node's name} */
    public String nodeName() {
        return nodeName;
    }

    /** {@return the client the REST layer hands to handlers} */
    public NodeClient nodeClient() {
        return nodeClient;
    }

    /** {@return the membership view the health endpoint reports, if one was given} */
    public MembershipSource membershipSource() {
        return membershipSource;
    }

    /** {@return the metadata plane this node reads and writes, once it has one} */
    public org.opensearch.serverless.metadata.MetadataPlane metadataPlane() {
        return metadataPlane;
    }

    /** The services this node builds on demand for its handlers: registries, scripts, pipelines, the client, and the rest. */
    public NodeServices services() {
        return services;
    }

    /**
     * The store type that makes a reader lazy.
     *
     * <p>Registered on every node; whether a shard uses it is decided per shard when it is opened, by
     * {@code ShardReconciler}. A writer keeps a local copy it can merge into; a reader reads blocks.
     */
    /**
     * Returns this node's block cache, whose counters say how much was actually fetched.
     *
     * @return the block cache
     */
    public org.opensearch.serverless.store.BlockCache blockCache() {
        return blockCache;
    }

    /**
     * Registers who hears about writes and about ownership going wrong.
     *
     * <p>Optional. A node with no signals set still converges through the backstop pass; it just does so
     * on the clock's schedule rather than on its own evidence.
     *
     * @param signals the listener, or null to go back to hearing nothing
     */
    public void setSignals(org.opensearch.serverless.node.ReconcileSignals signals) {
        this.signals = signals == null ? org.opensearch.serverless.node.ReconcileSignals.NONE : signals;
    }

    /** The index settings this node validates against: core's, the shell's own and those its plugins declare. */
    public org.opensearch.common.settings.IndexScopedSettings indexScopedSettings() {
        return services.indexScopedSettings();
    }

    /** {@return the signal sink, so callers on the request path can report what they saw} */
    public org.opensearch.serverless.node.ReconcileSignals signals() {
        return signals;
    }

    /**
     * Returns this deployment's cluster name.
     *
     * <p>A name, and only a name: there is no cluster object behind it. It is reported because every
     * OpenSearch response that carries one carries it, and a client reading it should find what it
     * configured.
     *
     * @return the configured cluster name
     */
    public String clusterName() {
        return org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.get(settings).value();
    }

    /** What this node pushes to its shards' search nodes, and asks of their writers. */
    public ReaderPush push() {
        return push;
    }

    /**
     * Attaches a part a higher layer built for this node, so code in that layer finds it from the node alone.
     *
     * @param type the part's type, which is its key
     * @param part the part
     * @param <T> the part's type
     */
    public <T> void attach(Class<T> type, T part) {
        parts.put(type, part);
    }

    /**
     * The part attached under a type.
     *
     * @param type the part's type
     * @param <T> the part's type
     * @return the part, or null if none is attached yet
     */
    public <T> T part(Class<T> type) {
        return type.cast(parts.get(type));
    }

    /**
     * Supplies the membership view the health endpoint reports. Optional: a node with none reports that
     * it can see one member, itself, which is true.
     *
     * @param source the membership source, or null
     */
    public void setMembershipSource(MembershipSource source) {
        this.membershipSource = source;
    }

    /** {@return the search settings this node was started with: fan-out width, pattern caps and rollup use} */
    public SearchSettings searchSettings() {
        return searchSettings;
    }

    /** {@return the node's settings} */
    public Settings settings() {
        return settings;
    }

    /**
     * Reports whether an index belongs to a plugin, and is therefore not reachable through REST.
     *
     * @param index the index name
     * @return true if it is a system index
     */
    public boolean isSystemIndex(String index) {
        return systemIndex.test(index);
    }

    /** {@return the plugins this node is running} */
    public ServerlessPlugins plugins() {
        return plugins;
    }

    /** {@return the roles this node advertises} */
    public Set<String> roles() {
        return roles;
    }

    /** The plugins' dynamic settings applied from the cluster settings register, built the first time they are asked for. */
    public synchronized PluginClusterSettings pluginClusterSettings() {
        if (pluginClusterSettings == null) {
            pluginClusterSettings = new PluginClusterSettings(clusterService, settings, threadPool, plugins.pluginNodeSettings());
        }
        return pluginClusterSettings;
    }

    /** {@return this node's shard reconciler} */
    public ShardReconciler reconciler() {
        return reconciler;
    }

    /** {@return this node's view projector} */
    public LocalViewProjector projector() {
        return projector;
    }

    void ensureStarted() {
        if (started == false) {
            throw new IllegalStateException("node " + nodeName + " has not been started");
        }
    }

    /** {@return the address the HTTP layer actually bound to} */
    public org.opensearch.core.common.transport.BoundTransportAddress boundHttpAddress() {
        return httpServerTransport.boundAddress();
    }

    /** {@return the address the transport layer actually bound to} */
    public org.opensearch.core.common.transport.BoundTransportAddress boundTransportAddress() {
        return transportService.boundAddress();
    }

    static String shardKey(String indexName, int shardNumber) {
        return indexName + "#" + shardNumber;
    }

    /** {@return this node's identity as the data plane sees it} */
    public DiscoveryNode localNode() {
        return localNode;
    }

    /** {@return the cluster service. Its state is a node-local view, not a consensus object} */
    public ClusterService clusterService() {
        return clusterService;
    }

    /** {@return the indices service} */
    public IndicesService indicesService() {
        return indicesService;
    }

    /** {@return the search service} */
    public SearchService searchService() {
        return searchService;
    }

    /** {@return the thread pool} */
    public ThreadPool threadPool() {
        return threadPool;
    }

    /** {@return the node environment, which owns the data paths} */
    public NodeEnvironment nodeEnvironment() {
        return nodeEnvironment;
    }

    /** Reports whether the node has been started. */
    public boolean isStarted() {
        return started;
    }
}
