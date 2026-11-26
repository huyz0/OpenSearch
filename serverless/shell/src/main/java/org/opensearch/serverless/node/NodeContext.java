/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.node;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.cluster.ShardAssignment;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;

/**
 * A serverless node as the layers below its wiring see it: the core it runs on ({@link NodeCore}) and the services that
 * write, read, fence, activate and serve its shards, with the state they keep between them.
 *
 * <p>Wiring the node -- its plugins' actions, the REST layer, the transport's handlers -- is the job of the class that
 * extends this, above every package that uses it. What those upper layers give back down comes through narrow hooks:
 * {@link #transportBound}, {@link #push}, and parts attached with {@link #attach}.
 */
public abstract class NodeContext extends NodeCore {

    /**
     * Builds what every part of a node shares: its plugins and settings, its thread pool, its environment and identity,
     * and core's cluster, indices and search services. Nothing is started.
     *
     * @param settings must carry {@code node.name}, {@code cluster.name} and {@code path.home}
     * @param extraPlugins plugins to run besides the installed ones
     * @throws Exception if the node environment cannot be created or the data plane cannot be built
     */
    protected NodeContext(Settings settings, java.util.List<org.opensearch.plugins.Plugin> extraPlugins) throws Exception {
        NodeAssembly.assemble(this, settings, extraPlugins);
    }

    /**
     * Lets go of what a constructor that failed part way had taken. The node environment above all: it holds
     * {@code node.lock}, and a constructor that threw after taking it leaked the file handle for the life of the process,
     * which on a real node keeps the data directory locked so a restart cannot use it.
     */
    protected final void closeAfterFailedConstruction() {
        IOUtils.closeWhileHandlingException(nodeEnvironment);
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    /**
     * Marks in a view which node the plugins' indices answer to; the plugin-free node leaves it as it is.
     *
     * @param view the projected view
     * @return the view to apply
     */
    protected ClusterState withPluginOwners(ClusterState view) {
        return view;
    }

    /** The plugins' own indices, kept in every view; none on a plugin-free node. */
    protected java.util.Collection<IndexDescriptor> pluginDescriptors() {
        return java.util.List.of();
    }

    /**
     * Adds to a projected view the aliases the plugins' indices answer to; the plugin-free node adds none.
     *
     * @param projected the projected view
     * @param descriptors every index in it
     * @return the view to apply
     */
    protected ClusterState withPluginAliases(ClusterState projected, java.util.Collection<IndexDescriptor> descriptors) {
        return projected;
    }

    /** Runs once the node has started, before its transport accepts requests: the plugins name their actions here. */
    protected void beforeAcceptingRequests() {}

    /**
     * Runs once the transport has bound and the node knows its own address: the layers above build what talks to
     * peers here, register its handlers, and set {@link #push}.
     */
    protected abstract void transportBound();

    /** Runs once the node has started and its reconciler exists: the plugins' actions and services start here. */
    protected void afterStarted() {}

    /** Starts this node. */
    final NodeStartup startup = new NodeStartup(this);

    /** Starts this node. */
    public NodeStartup startup() {
        return startup;
    }

    /** Starts this node's write log and renews its lease. */
    final NodeLogStart nodeLogs = new NodeLogStart(this);

    /** Writes this node's documents. */
    public final NodeWritePath writes = new NodeWritePath(this);

    /** Fences this node's writes. */
    public final WriteFences fences = new WriteFences(this);
    final WriterHibernation hibernation = new WriterHibernation();

    /** Grows mappings for this node's writes. */
    final MappingGrowthPath mappings = new MappingGrowthPath(this);

    /** Keeps this node's lease and heads current. */
    final NodeLeases leases = new NodeLeases(this);

    /** Activates this node's writers. */
    final WriterActivation activation = new WriterActivation(this);

    /** Serves this node's readers. */
    final ReaderServing readerServing = new ReaderServing(this);

    /** Builds this node's engines and directories. */
    final NodeStorage storage = new NodeStorage(this);

    /** Keeps this node's index descriptors current. */
    final DescriptorRefresh descriptorRefresh = new DescriptorRefresh(this);

    /** Reads this node's documents. */
    final NodeDocumentReads reads = new NodeDocumentReads(this);

    /** Writes documents on this node: single index and delete operations and batches. */
    public NodeWritePath writes() {
        return writes;
    }

    /** Reads documents held on this node: get and explain. */
    public NodeDocumentReads reads() {
        return reads;
    }

    /** The writers this node has closed for idleness while keeping their heads. */
    public WriterHibernation hibernation() {
        return hibernation;
    }

    /** Decides whether a write may go ahead on this node: the shard fence, its incarnation, and the lease and head. */
    public WriteFences fences() {
        return fences;
    }

    /** Keeps this node's lease and heads current. */
    public NodeLeases leases() {
        return leases;
    }

    /** Activates writers on this node and opens frozen views. */
    public WriterActivation activation() {
        return activation;
    }

    /** Serves shards as readers on this node. */
    public ReaderServing readers() {
        return readerServing;
    }

    /** Tracks this node's standing in the fleet. */
    final NodeCapacity nodeCapacity = new NodeCapacity(this);

    /**
     * Tracks this node's standing in the fleet: capacity and refusals at the cap, the fleet's size, and where a takeover
     * or a hand-off should go.
     *
     * @return the tracker
     */
    public NodeCapacity nodeCapacity() {
        return nodeCapacity;
    }

    /** The memory tier writers put freshly flushed segment files in, and the count of what they write to local disk. */
    public org.opensearch.serverless.store.LocalSegmentTier localSegments() {
        return storage.localSegments();
    }

    /**
     * The mapping and settings versions of the descriptor this node writes an index under, or null if it writes none.
     *
     * @param indexName the index
     * @return the mapping version and the settings version, or null
     */
    public long[] hostedDescriptorVersions(String indexName) {
        final IndexDescriptor descriptor = descriptorRefresh.hosted.get(indexName);
        return descriptor == null ? null : new long[] { descriptor.mappingVersion(), descriptor.settingsVersion() };
    }

    /**
    * Gives this node's REST surface a metadata plane to read and write.
    *
    * <p>Set after construction because the plane is a deployment-level object and the REST routes are
    * registered while the node is still being built. Until it is set, the admin endpoints answer 503
    * saying exactly that, rather than answering as though nothing existed.
    *
    * @param plane the metadata plane, or null
    */
    public void setMetadataPlane(org.opensearch.serverless.metadata.MetadataPlane plane) {
        this.metadataPlane = plane;
        if (plane != null) {
            setMembershipSource(plane.membership());
            // Members are owned through their bucket's head on every node, whatever its settings: nodes that disagreed
            // on which head owns an index could both take it.
            plane.bucketHeads().register(localNode.getId(), () -> reconciler().openShards());
            plane.buckets().configure(settings);
            try {
                // Publish the lease immediately. It is this node's entry in the address book that peers
                // use to forward writes here, so a node that has not renewed yet is not merely invisible
                // -- it is unroutable, and a client's write would be refused with nowhere to send it.
                nodeLogs.nodeLog(plane).renew();
            } catch (java.io.IOException e) {
                logger.warn(
                    "Initial lease publication failed and peers cannot route to this node (error_type={})",
                    e.getClass().getSimpleName()
                );
            }
        }
        afterPlaneSet();
    }

    /** Runs once a plane is set, after the node has adopted it. */
    protected void afterPlaneSet() {}

    /** The shard heads this node has read lately. */
    final ShardHeadSightings sightings = new ShardHeadSightings(() -> metadataPlane);

    /** {@return the shard heads this node has read lately, for routing hints and for readers in the same pass} */
    public ShardHeadSightings sightings() {
        return sightings;
    }

    /**
     * Returns whether a new index with these settings is grouped into a bucket while small: the index's setting, else
     * the cluster's, else this node's default.
     *
     * @param indexSettings the new index's settings
     * @return true to group it
     */
    public boolean bucketsNewIndex(Settings indexSettings) {
        return org.opensearch.serverless.bucket.BucketPolicy.wanted(
            indexSettings,
            descriptorRefresh.clusterBucketPolicy,
            org.opensearch.serverless.bucket.BucketSettings.enabled(settings)
        );
    }

    /** The node's own checkpoint age for log commits, and the one in force; see {@link LogCommitCheckpoint}. */
    volatile long nodeLogCommitCheckpointMillis = org.opensearch.serverless.store.CommitStaging.DEFAULT_MAX_STAGED_MILLIS;
    volatile long logCommitCheckpointMillis = org.opensearch.serverless.store.CommitStaging.DEFAULT_MAX_STAGED_MILLIS;

    /** The planes whose pack ledger this node has looked at for packs to collect; see {@link NodePackWiring}. */
    final java.util.Set<org.opensearch.serverless.metadata.MetadataPlane> packLedgersChecked = java.util.Collections.newSetFromMap(
        new java.util.IdentityHashMap<>()
    );

    /**
     * Sets how long the descriptors of open indices go between re-reads when nothing else has read them;
     * zero re-reads them on every pass, which is the library default. See
     * {@code ServerlessBootstrap#DESCRIPTOR_REFRESH_INTERVAL}.
     *
     * @param millis the interval
     * @return this, for chaining
     */
    public NodeContext setDescriptorRefreshIntervalMillis(long millis) {
        descriptorRefresh.descriptorRefreshIntervalMillis = Math.max(0L, millis);
        return this;
    }

    /** {@return this node's write log, once it has started it} */
    public NodeLogWriter nodeLogWriter() {
        return nodeLogs.nodeLogWriter;
    }

    /** Applies the node-local view and counts its cost; see {@link LocalViewApplier}. */
    protected LocalViewApplier views;

    public ClusterState projectAndApply(java.util.Collection<IndexDescriptor> extra, java.util.Collection<ShardAssignment> opening)
        throws Exception {
        return views.projectAndApply(extra, opening);
    }

    /**
     * The local views activations have projected and applied: milliseconds projecting, milliseconds applying, and how many
     * -- fewer than the activations when they were batched.
     *
     * @return projecting, applying, then the count
     */
    public long[] localViewMillis() {
        return views.localViewMillis();
    }

    /**
     * Sets how long between periodic head verifications; zero verifies on every pass, as this used to.
     *
     * @param millis the interval
     * @return this, for chaining
     */
    public NodeContext setHeadVerifyIntervalMillis(long millis) {
        this.leases().headVerifyIntervalMillis = Math.max(0L, millis);
        return this;
    }

    /**
     * Sets how long a reader's check stands before its head is read again; zero reads it on every check. For tests.
     *
     * @param millis the window
     */
    public void setReaderCheckMillis(long millis) {
        readerValidation.setReaderCheckMillis(millis);
    }

    /** {@return what the write back-pressure is doing: its limit, writes in flight, append time and refusals} */
    public org.opensearch.serverless.store.WriteBackpressure.Stats writeBackpressure() {
        return writes.writeBackpressure.stats();
    }

    /**
     * Remembers the plane this node is serving.
     *
     * <p>The block-cache directory factory is handed a location and settings, not a metadata plane, so
     * it has to reach one through the node. A node that had only ever been given a plane as a method
     * argument would build local-only directories and silently serve nothing — which is how this was
     * found.
     */
    void adopt(org.opensearch.serverless.metadata.MetadataPlane plane) {
        if (metadataPlane == null) {
            metadataPlane = plane;
        }
    }

    /** This node's per-shard bookkeeping around its reconciler. */
    final NodeShards shards = new NodeShards(this);

    /** {@return this node's per-shard bookkeeping: marking used, publishing and releasing shards} */
    public NodeShards shards() {
        return shards;
    }

    /** The transport secrets as last read, re-read at most once per lease TTL. */
    final TransportSecretCache transportSecrets = new TransportSecretCache(() -> metadataPlane);

    /** {@return this node's cache of the transport secrets its nodes present to one another} */
    public TransportSecretCache secrets() {
        return transportSecrets;
    }

    /**
     * Hands this node a new node-local view. The shell is the second caller of
     * {@link ClusterApplier#onNewClusterState}; {@code Coordinator} is the first, and is absent here.
     *
     * @param state the projected view for this node
     * @param timeout how long to wait for the state to be applied
     * @throws Exception if application fails or times out
     */
    public void applyLocalView(ClusterState state, TimeValueLike timeout) throws Exception {
        views.applyLocalView(state, timeout);
    }

    // ---------------------------------------------------------------- shard fences and the local view

}
