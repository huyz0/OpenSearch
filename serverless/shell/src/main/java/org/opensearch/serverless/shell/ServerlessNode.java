/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.PluginsService;
import org.opensearch.rest.RestController;
import org.opensearch.search.SearchService;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.cluster.LocalViewProjector;
import org.opensearch.serverless.cluster.ShardAssignment;
import org.opensearch.serverless.membership.MembershipSource;
import org.opensearch.serverless.rest.ServerlessHealthHandler;
import org.opensearch.serverless.rest.ServerlessRootHandler;
import org.opensearch.serverless.shard.ShardReconciler;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Netty4ModulePlugin;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.usage.UsageService;

import java.io.Closeable;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A serverless node: the OpenSearch data plane with no control plane.
 *
 * <p>This is the serverless shell. It constructs {@link IndicesService}
 * and {@link SearchService} directly and drives them from a node-local {@link ClusterState} handed to
 * {@link ClusterApplier}, rather than from a consensus-published one. It contains no
 * {@code Coordinator}, no {@code AllocationService}, no {@code GatewayMetaState} and no {@code Node} —
 * a property asserted by test, not by inspection.
 *
 * <p>Two properties of this construction are load-bearing and are commented at their call sites: the
 * applier requires a {@link NodeConnectionsService}, and the data plane refuses to activate a primary
 * at term 0.
 */
public final class ServerlessNode implements Closeable {

    /**
     * The pool forwarded searches and explains are answered on, and the heartbeat's head scan reads on.
     *
     * <p>Not the coordinator's own shard tasks any more: those run on GENERIC (see {@code SearchFanout}),
     * so a search that waits on GENERIC for tasks that also ran on GENERIC cannot park every thread
     * waiting for work no thread is free to do. See the thread pool's construction for why it is
     * separate at all.
     */
    public static final String FANOUT_POOL = "serverless_fanout";

    /**
     * The pool a forwarded write is applied on, at its owner.
     *
     * <p>Not WRITE. A write that arrives at a node that does not own its shard is forwarded, and the WRITE thread
     * that took it waits for the owner's answer; when the owner applied forwarded writes on WRITE too, every node's
     * WRITE threads could end up waiting on every other node's, and none was free to apply what the others were
     * waiting for. A fleet of six under a few hundred writes a second stopped acknowledging almost entirely until
     * the forwards timed out. A forwarded write is applied locally and forwards nothing further, so this pool waits
     * on nothing that runs on a pool a forward waits on.
     */
    public static final String FORWARDED_WRITE_POOL = "serverless_forwarded_write";

    /**
     * The thread a node renews its lease on, and nothing else does.
     *
     * <p>A lease that is not renewed in time is a node its peers may take everything from, so renewal cannot wait behind
     * other work. It ran on GENERIC, and under a burst of activations -- each a few object-store round trips -- renewals
     * ran late, leases ran out, peers revoked them, and a busy node became a dead one, which made the burst bigger.
     */
    public static final String LEASE_POOL = "serverless_lease";

    /**
     * The threads shard activations run on, and nothing else does.
     *
     * <p>They ran on GENERIC, which is also where a write waits for the activation of the shard it is for, and where
     * an activation pass waits for all of its own. A fleet run filled all 128 GENERIC threads with such waiters: the
     * activations they waited for sat in GENERIC's queue behind them, one starting only when a waiting write timed out
     * and gave its thread back. A shard whose owner had closed it locally waited ten minutes and more to be re-taken,
     * answering every forwarded write "not open here" meanwhile.
     */
    public static final String ACTIVATION_POOL = "serverless_activation";

    /**
     * Where shards are published, several at once: a node holding hundreds published them one after another on GENERIC,
     * and the passes that piled up behind each other starved everything else waiting there.
     */
    public static final String PUBLISH_POOL = "serverless_publish";

    /** How many shards a node activates at once; see {@code ServerlessBootstrap#ACTIVATION_CONCURRENCY}. */
    public static final String ACTIVATION_CONCURRENCY_SETTING = "serverless.activation.concurrency";

    /** The pool a forwarded get is answered on, at its owner: not GET, for the reason {@link #FORWARDED_WRITE_POOL} is not WRITE. */
    public static final String FORWARDED_READ_POOL = "serverless_forwarded_read";

    static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(ServerlessNode.class);

    /** Accepts writer activation: takes ownership of shards and indexes into them. */
    public static final String ROLE_INGEST = "ingest";

    /** Accepts reader activation: serves search from object-store-backed segments. */
    public static final String ROLE_SEARCH = "search";

    /** The store type that reads published segments lazily, a block at a time. */
    public static final String BLOCK_CACHE_STORE_TYPE = "serverless_block_cache";

    final String nodeName;
    final Settings settings;
    final ServerlessPlugins plugins;
    final ThreadPool threadPool;
    /** Builds the core services and the shared collaborators the node and its plugins are handed. */
    final NodeServices services;
    /** The check that keeps a reader from serving a commit behind the log, and its memo. */
    final ReaderValidation readerValidation;
    final NodeEnvironment nodeEnvironment;
    final ClusterService clusterService;
    final IndicesService indicesService;
    final SearchService searchService;
    final PluginsService pluginsService;
    org.opensearch.serverless.rest.SystemIndices systemIndices;
    final RestController restController;
    final TransportService transportService;
    final HttpServerTransport httpServerTransport;
    final NodeClient nodeClient;
    volatile DiscoveryNode localNode;
    volatile MembershipSource membershipSource;
    volatile org.opensearch.serverless.metadata.MetadataPlane metadataPlane;
    volatile org.opensearch.serverless.transport.ShardRouter router;
    final Set<String> roles;
    final org.opensearch.serverless.store.BlockCache blockCache;
    final Map<String, IndexDescriptor> served = new java.util.concurrent.ConcurrentHashMap<>();
    final Set<Map.Entry<String, Integer>> readerShards = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /** The descriptors of the indices this node holds writer shards of, as last read; the reader half is {@link #served}. */
    final Map<String, IndexDescriptor> hosted = new java.util.concurrent.ConcurrentHashMap<>();
    /** The term each writer shard was acquired at, keyed {@code index#shard}; pruned to what is open when a view is projected. */
    final Map<String, ShardAssignment> writerAssignments = new java.util.concurrent.ConcurrentHashMap<>();
    /**
     * One fence per shard. The write path holds the read side from applying an operation to the engine
     * until it is acknowledged; anything that gives the shard away -- an idle release, a lost head, a
     * lapsed lease -- holds the write side from before the publish until after the close. See
     * {@link #underShardFence}.
     */
    final Map<org.opensearch.core.index.shard.ShardId, java.util.concurrent.locks.ReentrantReadWriteLock> shardFences =
        new java.util.concurrent.ConcurrentHashMap<>();
    /**
     * Shares one object-store PUT between the concurrent writes to a shard. See
     * {@link org.opensearch.serverless.store.WalGroupCommitter}: it batches only what arrives while a PUT
     * is already in flight, so a quiet shard pays exactly what it did before and a busy one stops paying
     * per document.
     */
    final org.opensearch.serverless.store.WalGroupCommitter walCommitter;

    /**
     * Whether writes go to one log for the whole node, written once per window, instead of one log per shard. See
     * {@link NodeLogWriter}.
     */
    final boolean nodeLogEnabled;

    /**
     * Whether a publish writes the files it adds as one blob per segment rather than one blob per file:
     * {@code serverless.segments.pack.enabled}.
     * See {@link org.opensearch.serverless.store.SegmentPublisher#packing}.
     */
    final boolean packSegments;

    /** This node's write log when {@link #nodeLogEnabled}, started with the first metadata plane it writes through. */
    volatile NodeLogWriter nodeLogWriter;

    /** Whether the reconciler has been given {@link #nodeLogWriter}, which may start before the reconciler exists. */
    volatile boolean nodeLogHooked;

    /** How many writes may wait on the object store at once; see WriteBackpressure. */
    final org.opensearch.serverless.store.WriteBackpressure writeBackpressure;
    final int writeBackpressureMax;

    /**
     * How much of its room the write path's limiter is giving writes now: its limit over its most, in (0, 1]. Below 1 the
     * store has been slow to take appends, and other work on the store should give way.
     *
     * @return the share
     */
    public double writeHeadroom() {
        return Math.min(1.0, Math.max(0.0, writeBackpressure.stats().limit() / (double) Math.max(1, writeBackpressureMax)));
    }

    /**
     * Writer shards whose log could not be appended to. Still open and still readable; refusing writes;
     * reopened from the log by the next heartbeat that reaches the store. See {@link NodeWritePath#appendOrRelease}.
     */
    final Set<org.opensearch.core.index.shard.ShardId> writeFenced = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /**
     * Heads this node lost to its own lapsed lease and has not yet given back in the register, with the
     * term it lost them at. Drained on every renewal, so a store that was unreachable when the loss was
     * discovered does not leave a head naming a node that is not serving the shard.
     */
    final Map<org.opensearch.core.index.shard.ShardId, Long> headsToGiveBack = new java.util.concurrent.ConcurrentHashMap<>();
    final Object leaseRenewalLock = new Object();
    /** Serialises head verification: two passes interleaving their releases was the hazard, and waiting is cheaper than skipping. */
    final Object headVerificationLock = new Object();
    /** Whether a head verification is running, so an activation can tell it would only wait behind one. */
    final java.util.concurrent.atomic.AtomicBoolean verifyingHeads = new java.util.concurrent.atomic.AtomicBoolean();
    /**
     * True from the moment this node's lease is found lapsed until a head verification that began after
     * the renewal has read every head. While set, no write is acknowledged and no forwarded write is
     * accepted. See {@link #renewLease}.
     */
    volatile boolean ownershipUnverified;
    /** Bumped on every lapse, so a verification that began before the renewal cannot lift the suspension. */
    final java.util.concurrent.atomic.AtomicLong lapseEpoch = new java.util.concurrent.atomic.AtomicLong();
    /** How many shard-head reads a verification pass has in flight at once. */
    public static final int HEAD_READ_LANES = 8;

    volatile ShardAdmission admission = new ShardAdmission() {
        @Override
        public int maxShardsHeld() {
            return org.opensearch.serverless.reconcile.BackgroundReconciler.DEFAULT_MAX_SHARDS_HELD;
        }

        @Override
        public boolean makeRoom() {
            return false;
        }
    };

    /**
     * Installs the cap and eviction policy readers and views are admitted under.
     *
     * @param admission the policy, normally the node's reconciler
     */
    public void setShardAdmission(ShardAdmission admission) {
        this.admission = admission;
    }

    /** Reader opens this node refused at its shard cap. */
    final java.util.concurrent.atomic.AtomicLong readerOpensRefusedAtCap = new java.util.concurrent.atomic.AtomicLong();
    /** Writer activations this node refused at its shard cap. */
    final java.util.concurrent.atomic.AtomicLong activationsRefusedAtCap = new java.util.concurrent.atomic.AtomicLong();
    /** Shards given up to make room for another. */
    final java.util.concurrent.atomic.AtomicLong evictedForRoom = new java.util.concurrent.atomic.AtomicLong();
    /** Shards given up because a member with room could take them. */
    final java.util.concurrent.atomic.AtomicLong handedOff = new java.util.concurrent.atomic.AtomicLong();
    /** Writes for a shard nobody held that this node sent to a member with room rather than take. */
    final java.util.concurrent.atomic.AtomicLong writesSteered = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Returns how full this node is and what fullness has cost.
     *
     * @return the figures
     */
    public Capacity capacity() {
        return new Capacity(
            admission.maxShardsHeld(),
            reconciler == null ? 0 : reconciler.heldShards().size(),
            readerOpensRefusedAtCap.get(),
            activationsRefusedAtCap.get(),
            evictedForRoom.get(),
            handedOff.get(),
            writesSteered.get()
        );
    }

    /**
     * Counts a writer activation refused at the cap.
     *
     * @param indexName the index
     * @param shard the shard
     */
    public void noteActivationRefusedAtCap(String indexName, int shard) {
        activationsRefusedAtCap.incrementAndGet();
        noteRefusedRecently(indexName, shard);
    }

    /**
     * When each shard turned away at the cap was last turned away: distinct shards, not refusals. A burst of searches asked
     * for the same shards over and over, and counted as events one minute's refusals said the fleet wanted forty-three nodes.
     */
    final Map<String, Long> refusedShards = new java.util.concurrent.ConcurrentHashMap<>();

    void noteRefusedRecently(String indexName, int shard) {
        refusedShards.put(shardKey(indexName, shard), System.nanoTime());
    }

    /**
     * How many distinct shards this node turned away at its cap in the last minute: writer activations and reader opens.
     *
     * @return the count
     */
    public int refusedLastMinute() {
        final long cutoff = System.nanoTime() - java.util.concurrent.TimeUnit.MINUTES.toNanos(1);
        refusedShards.values().removeIf(at -> at < cutoff);
        return refusedShards.size();
    }

    /** How long a shard counts as in use after it was last used. */
    public static final long IN_USE_WINDOW_MILLIS = 60_000L;

    /**
     * How many shards this node used in the last minute: what it holds for demand, rather than as a cache that fills
     * whatever the load.
     *
     * @return the count
     */
    public int inUseLastMinute() {
        if (reconciler == null) {
            return 0;
        }
        final org.opensearch.serverless.metadata.MetadataPlane plane = metadataPlane;
        final long now = plane == null ? System.currentTimeMillis() : plane.clock().getAsLong();
        int used = 0;
        for (org.opensearch.core.index.shard.ShardId shardId : reconciler.openShards()) {
            final java.util.OptionalLong last = reconciler.lastUsed(shardId);
            if (last.isPresent() && now - last.getAsLong() < IN_USE_WINDOW_MILLIS) {
                used++;
            }
        }
        return used;
    }

    /** The share of a node's cap a fleet sized by {@link #fleetSize} aims to use. */
    public static final double TARGET_UTILISATION = FleetPlacement.TARGET_UTILISATION;

    /**
     * Sizes the writer fleet from the load every member renews into its lease. See {@link FleetPlacement#fleetSize}.
     *
     * @param plane the metadata plane
     * @return the members counted and the nodes wanted
     */
    public FleetSize fleetSize(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return placement.fleetSize(plane);
    }

    /**
     * Counts a shard given up, to make room or for a member with room.
     *
     * @param forHandOff true if given up for a member with room
     */
    public void noteEvicted(boolean forHandOff) {
        (forHandOff ? handedOff : evictedForRoom).incrementAndGet();
    }

    final java.util.concurrent.atomic.AtomicLong takeoversOvercommitted = new java.util.concurrent.atomic.AtomicLong();

    /** Counts a dead member's shard taken past the cap, without evicting for it. */
    public void noteTakeoverOvercommitted() {
        takeoversOvercommitted.incrementAndGet();
    }

    /**
     * Dead members' shards taken past the cap, since the node started.
     *
     * @return the count
     */
    public long takeoversOvercommitted() {
        return takeoversOvercommitted.get();
    }

    /** Above this share of its cap a node looks for a member to hand work to. */
    public static final double HAND_OFF_ABOVE = FleetPlacement.HAND_OFF_ABOVE;
    /** At or below this share of its cap a member can take work handed to it. */
    public static final double ROOM_AT_OR_BELOW = FleetPlacement.ROOM_AT_OR_BELOW;
    /** Above this multiple of the fleet's mean held shards a node hands work off, however far it is from its cap. */
    public static final double HAND_OFF_ABOVE_MEAN = FleetPlacement.HAND_OFF_ABOVE_MEAN;
    /** A node handing off for being above the mean hands off down to this multiple of it. */
    public static final double HAND_OFF_DOWN_TO_MEAN = FleetPlacement.HAND_OFF_DOWN_TO_MEAN;
    /** Below this share of its cap a node is never too busy for the mean to matter: a small fleet's few shards stay. */
    public static final double MEAN_MATTERS_FROM = FleetPlacement.MEAN_MATTERS_FROM;

    /** Decides where this node's work belongs across the fleet; the methods below forward to it. */
    final FleetPlacement placement = new FleetPlacement(
        () -> localNode.getId(),
        () -> admission.maxShardsHeld(),
        () -> this.reconciler == null ? 0 : this.reconciler.heldShards().size()
    );

    /**
     * Whether this node holds more than {@link #HAND_OFF_ABOVE} of its cap.
     *
     * @return true when nearly full
     */
    public boolean nearlyFull() {
        return placement.nearlyFull();
    }

    /**
     * The live writer member with the most room, if any. See {@link FleetPlacement#memberWithRoom}.
     *
     * @param plane the metadata plane
     * @return the member's id
     */
    public java.util.Optional<String> memberWithRoom(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return placement.memberWithRoom(plane);
    }

    /**
     * Sets whether requests for a dead member's shard go to the one survivor named for it.
     *
     * @param route whether to
     */
    public void setRouteTakeovers(boolean route) {
        placement.setRouteTakeovers(route);
    }

    /**
     * Whether requests for a dead member's shard go to the one survivor named for it.
     *
     * @return true if they do
     */
    public boolean routesTakeovers() {
        return placement.routesTakeovers();
    }

    /**
     * Sets how far past its cap a member may take a dead member's shards. See {@link FleetPlacement#setTakeoverOvercommit}.
     *
     * @param fraction of the cap
     */
    public void setTakeoverOvercommit(double fraction) {
        placement.setTakeoverOvercommit(fraction);
    }

    /** Counts a write for a dead member's shard sent to the member that takes it over. */
    public void noteTakeoverWriteRouted() {
        placement.noteTakeoverWriteRouted();
    }

    /** Counts a doubt about a dead member's shard this node left to the member that takes it over. */
    public void noteTakeoverDoubtLeft() {
        placement.noteTakeoverDoubtLeft();
    }

    /**
     * Writes routed to, and doubts left to, the member that takes a dead member's shard over.
     *
     * @return routed, then left
     */
    public long[] takeoverRouting() {
        return placement.takeoverRouting();
    }

    /**
     * The member that takes over a shard whose owner's lease has run out. See {@link FleetPlacement#takeoverWinner}.
     *
     * @param plane the metadata plane
     * @param indexName the index
     * @param shard the shard
     * @param deadOwner the owner whose lease ran out, left out of the choice
     * @return the member's id, or empty if no live writer member is known
     */
    public java.util.Optional<String> takeoverWinner(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        String indexName,
        int shard,
        String deadOwner
    ) {
        return placement.takeoverWinner(plane, indexName, shard, deadOwner);
    }

    /**
     * Whether a shard is a dead member's that another live member is to take over. See
     * {@link FleetPlacement#takeoverBelongsElsewhere}.
     *
     * @param plane the metadata plane
     * @param indexName the index
     * @param shard the shard
     * @return true to leave it to the member named
     */
    public boolean takeoverBelongsElsewhere(org.opensearch.serverless.metadata.MetadataPlane plane, String indexName, int shard) {
        return placement.takeoverBelongsElsewhere(plane, indexName, shard);
    }

    /**
     * The mean of held shards over the live writer members. See {@link FleetPlacement#fleetMeanHeld}.
     *
     * @param plane the metadata plane
     * @return the mean, or -1 if no member says
     */
    public double fleetMeanHeld(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return placement.fleetMeanHeld(plane);
    }

    /**
     * Whether this node holds far more than its share of the fleet. See {@link FleetPlacement#aboveFleetShare}.
     *
     * @param plane the metadata plane
     * @return true when above its share
     */
    public boolean aboveFleetShare(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return placement.aboveFleetShare(plane);
    }

    /**
     * The live writer member holding the fewest shards, if below the fleet's mean. See {@link FleetPlacement#memberBelowMean}.
     *
     * @param plane the metadata plane
     * @return the member's id
     */
    public java.util.Optional<String> memberBelowMean(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return placement.memberBelowMean(plane);
    }

    /**
     * Where this node should send work it would otherwise take, if anywhere. See {@link FleetPlacement#handOffTarget}.
     *
     * @param plane the metadata plane
     * @return the member's id
     */
    public java.util.Optional<String> handOffTarget(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return placement.handOffTarget(plane);
    }

    /**
     * Where writer activations spend their time, summed: reading the descriptor, acquiring the head, marking the rollup
     * entry owned, and opening the shard -- restoring its commit and replaying its log -- of which projecting and applying
     * the local view that names it is a part of its own. With {@link #activationsOpened}
     * an operator reads the average of each, and which one a burst of cold shards is waiting on.
     */
    final java.util.concurrent.atomic.AtomicLong[] activationPhases = {
        new java.util.concurrent.atomic.AtomicLong(),
        new java.util.concurrent.atomic.AtomicLong(),
        new java.util.concurrent.atomic.AtomicLong(),
        new java.util.concurrent.atomic.AtomicLong(),
        new java.util.concurrent.atomic.AtomicLong() };
    final java.util.concurrent.atomic.AtomicLong activationsOpened = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Writer activations' time by phase, in milliseconds summed since the node started, and how many opened.
     *
     * @return describe, acquire, mark owned and open, the count of activations that opened, then the part of open spent on
     *         the local view
     */
    public long[] activationPhaseMillis() {
        return new long[] {
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(activationPhases[0].get()),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(activationPhases[1].get()),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(activationPhases[2].get()),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(activationPhases[3].get()),
            activationsOpened.get(),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(activationPhases[4].get()) };
    }

    /** Counts a write for an unheld shard sent to a member with room. */
    public void noteWriteSteered() {
        writesSteered.incrementAndGet();
    }

    volatile org.opensearch.serverless.reconcile.ReconcileSignals signals = org.opensearch.serverless.reconcile.ReconcileSignals.NONE;
    volatile LocalViewProjector projector;
    volatile ShardReconciler reconciler;
    volatile boolean started;

    /** Writes this node's documents. */
    final NodeWritePath writes = new NodeWritePath(this);

    /** Fences this node's writes. */
    final WriteFences fences = new WriteFences(this);

    /** Grows mappings for this node's writes. */
    final MappingGrowthPath mappings = new MappingGrowthPath(this);

    /** Keeps this node's lease and heads current. */
    final NodeLeases leases = new NodeLeases(this);

    /** Activates this node's writers. */
    final WriterActivation activation = new WriterActivation(this);

    /** Serves this node's readers. */
    final ReaderServing readerServing = new ReaderServing(this);

    /**
     * Constructs a node. Nothing is started until {@link #start()} is called.
     *
     * @param settings must carry {@code node.name}, {@code cluster.name} and {@code path.home}
     * @throws Exception if the node environment cannot be created or the data plane cannot be built
     */
    public ServerlessNode(Settings settings) throws Exception {
        this(settings, java.util.List.of());
    }

    /**
     * Creates a node running the given plugins.
     *
     * <p>Plugin instances rather than a directory scan, because that is the shape a test and a daemon can
     * both use: {@code ServerlessBootstrap} can load from disk and hand the instances here, and a test can
     * pass one it wrote. Loading is not this constructor's business.
     *
     * @param settings the node settings
     * @param plugins the plugins to run
     * @throws Exception if the node cannot be built
     */
    public ServerlessNode(Settings settings, java.util.List<org.opensearch.plugins.Plugin> plugins) throws Exception {
        // Plugins first, before the settings are settled, because a plugin gets to contribute to them.
        //
        // The order is load-bearing. PluginsService is built from what the caller supplied, so a plugin is
        // constructed with the operator's configuration; updatedSettings then layers each plugin's
        // additionalSettings underneath it, so an explicit setting always beats a plugin's; and the shell's
        // own defaults go underneath both, so they apply only when nobody else had an opinion. Putting the
        // shell's transport choice on top -- which is where it used to be -- would have made
        // additionalSettings decorative for the one thing plugins most use it for.
        this.pluginsService = NodeServices.buildPluginsService(settings, new Environment(settings, null));
        final java.util.List<org.opensearch.plugins.Plugin> installed = new java.util.ArrayList<>(
            pluginsService.filterPlugins(org.opensearch.plugins.Plugin.class)
        );
        // Ones handed to this constructor come after the installed ones, so an operator's installation is
        // not silently outranked by something a test or an embedder passed in.
        installed.addAll(plugins);
        this.plugins = new ServerlessPlugins(installed);
        this.settings = NodeServices.withShellDefaults(this.plugins.settingsWith(settings));
        settings = this.settings;

        this.nodeName = settings.get("node.name", "serverless-node");
        this.views = new LocalViewApplier(
            nodeName,
            () -> localNode.getId(),
            this::clusterService,
            () -> membershipSource,
            () -> metadataPlane,
            this::projectView
        );
        // Role is a runtime attribute a node advertises, not a topology decision baked into a
        // cluster. One binary; an operator gets asymmetric scaling from two Deployments differing by one
        // environment variable, and role switching under load stays a policy question.
        this.roles = Set.copyOf(settings.getAsList("serverless.roles", java.util.List.of(ROLE_INGEST, ROLE_SEARCH)));
        // Block size is a real tuning knob, not a constant: it trades request count against wasted
        // bytes, and the right value depends on the object store's per-request cost and the index's
        // access pattern. Exposed so that can be measured rather than guessed.
        this.blockCache = new org.opensearch.serverless.store.BlockCache(
            settings.getAsInt("serverless.block_cache.block_size", org.opensearch.serverless.store.BlockCache.DEFAULT_BLOCK_SIZE),
            // Sized from the heap when not configured: the cache lives outside the breakers (accounting it
            // there would trip every request check by its own size), so its bound has to come from what
            // the process actually has rather than from a constant that was right on one machine.
            settings.getAsInt(
                "serverless.block_cache.max_blocks",
                (int) Math.max(
                    256L,
                    Runtime.getRuntime().maxMemory() / 10 / settings.getAsInt(
                        "serverless.block_cache.block_size",
                        org.opensearch.serverless.store.BlockCache.DEFAULT_BLOCK_SIZE
                    )
                )
            )
        );
        // Group commit for the log. Batching only ever happens while a PUT for the same shard is already
        // in flight, so these bound the size of a group rather than schedule one -- there is no window to
        // configure. Setting max_records to 1 restores one PUT per write, which is what this replaced.
        this.writeBackpressureMax = settings.getAsInt("serverless.write.backpressure.max_in_flight", 1_024);
        this.writeBackpressure = new org.opensearch.serverless.store.WriteBackpressure(
            settings.getAsLong("serverless.write.backpressure.target_millis", 1_000L),
            settings.getAsLong("serverless.write.backpressure.budget_millis", 5_000L),
            settings.getAsInt("serverless.write.backpressure.min_in_flight", 4),
            writeBackpressureMax
        );
        this.walCommitter = new org.opensearch.serverless.store.WalGroupCommitter(
            settings.getAsInt(
                "serverless.wal.group_commit.max_records",
                org.opensearch.serverless.store.WalGroupCommitter.DEFAULT_MAX_RECORDS
            ),
            settings.getAsLong("serverless.wal.group_commit.max_bytes", org.opensearch.serverless.store.WalGroupCommitter.DEFAULT_MAX_BYTES)
        );
        this.nodeLogEnabled = settings.getAsBoolean("serverless.wal.node_log.enabled", true);
        this.packSegments = settings.getAsBoolean("serverless.segments.pack.enabled", false);
        // Rebuilt from the settled settings, so anything a plugin contributed is visible to everything
        // below.
        final Environment environment = new Environment(settings, null);
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        // A pool of its own for fanned-out shard work. The coordinator of a search waits on GENERIC for
        // its shard tasks; when those tasks also ran on GENERIC, enough concurrent searches parked every
        // thread waiting for tasks no thread was free to run. A forwarded search handler ran on SEARCH and
        // blocked on the query phase forked to SEARCH, with the same shape. Both now run here, and this
        // pool waits on nothing that runs on it.
        final int processors = org.opensearch.common.util.concurrent.OpenSearchExecutors.allocatedProcessors(settings);
        this.threadPool = new ThreadPool(
            settings,
            new org.opensearch.threadpool.ScalingExecutorBuilder(
                FANOUT_POOL,
                1,
                Math.max(8, 4 * processors),
                org.opensearch.common.unit.TimeValue.timeValueSeconds(30)
            ),
            // Sized and queued like WRITE and GET, whose work they take over for requests that were forwarded.
            new org.opensearch.threadpool.FixedExecutorBuilder(
                settings,
                FORWARDED_WRITE_POOL,
                processors,
                10_000,
                "thread_pool." + FORWARDED_WRITE_POOL
            ),
            new org.opensearch.threadpool.FixedExecutorBuilder(settings, LEASE_POOL, 1, 16, "thread_pool." + LEASE_POOL),
            // Sized past the default activation concurrency, which is what bounds how many are submitted at once;
            // the queue only absorbs a concurrency raised beyond the pool.
            // Twice the activations allowed at once, and never fewer than sixteen: an activation can wait on another's
            // single-flight while holding a thread.
            new org.opensearch.threadpool.FixedExecutorBuilder(settings, PUBLISH_POOL, 8, 100_000, "thread_pool." + PUBLISH_POOL),
            new org.opensearch.threadpool.FixedExecutorBuilder(
                settings,
                ACTIVATION_POOL,
                Math.max(
                    16,
                    2 * settings.getAsInt(
                        ACTIVATION_CONCURRENCY_SETTING,
                        org.opensearch.serverless.reconcile.BackgroundReconciler.DEFAULT_ACTIVATION_CONCURRENCY
                    )
                ),
                1000,
                "thread_pool." + ACTIVATION_POOL
            ),
            new org.opensearch.threadpool.FixedExecutorBuilder(
                settings,
                FORWARDED_READ_POOL,
                Math.max(4, processors),
                1_000,
                "thread_pool." + FORWARDED_READ_POOL
            )
        );
        this.services = new NodeServices(settings, this.plugins, threadPool, () -> metadataPlane, () -> this, this::indicesService);
        this.readerValidation = new ReaderValidation(
            () -> localNode.getId(),
            () -> reconciler,
            () -> signals,
            threadPool,
            this::forgetReader
        );
        // An open fetches the blocks it will read concurrently rather than one round trip at a time, on GENERIC as
        // the log replay's reads do; the opening thread reads too, so a busy pool costs width only.
        if (settings.getAsBoolean("serverless.block_cache.prefetch_on_open", true)) {
            blockCache.prefetchingWith(threadPool.generic());
        }
        boolean success = false;
        NodeEnvironment openedEnvironment = null;
        try {
            // Held in a local as well, because the failure path below cannot read a blank final -- and it
            // has to close this one thing above all others: NodeEnvironment holds node.lock.
            openedEnvironment = new NodeEnvironment(settings, environment);
            this.nodeEnvironment = openedEnvironment;
            // Roles from the lease's roles, never BUILT_IN_ROLES. The role is a lease attribute,
            // and every DiscoveryNode this shell built used to say cluster_manager+data+ingest+warm
            // regardless -- a cluster-manager-role node that merely was not elected, which is exactly the
            // latent arming of cluster-manager-only paths to avoid. Data always: this node opens
            // IndexShards whatever it serves. Ingest only when it accepts writer activation. Never
            // cluster_manager, never core's search role -- that one means "hosts search replicas" and may
            // not be combined with data, which is not what a serverless search node is.
            this.localNode = new DiscoveryNode(
                nodeName,
                nodeEnvironment.nodeId(),
                new TransportAddress(InetAddress.getLoopbackAddress(), 0),
                discoveryAttributesFor(roles),
                discoveryRolesFor(roles),
                Version.CURRENT
            );
            this.clusterService = services.buildClusterService(clusterSettings, localNode);
            this.indicesService = services.buildIndicesService(
                environment,
                clusterSettings,
                pluginsService,
                nodeEnvironment,
                clusterService,
                engineFactoryProviders(),
                directoryFactories()
            );
            this.searchService = services.buildSearchService(clusterSettings, clusterService, indicesService);

            // The client every REST handler is handed, wired to the shell's own operations.
            //
            // It used to be a bare NodeClient with no action registry, which the shell's own handlers
            // could ignore because they reach the node directly. A plugin's handler cannot: getRestHandlers
            // gives it this and nothing else, and every REST handler in the OpenSearch ecosystem is written
            // against it. Left unwired it would have thrown on the first call a real plugin made -- found
            // by writing the shell's authentication as a plugin, whose user-management endpoint needs to
            // write to an index from inside a handler.
            //
            // Overriding doExecute rather than calling initialize(...) is the point: initialize wants a map
            // of ActionType to TransportAction, which is the action layer S6.3 decided not to build. This
            // delegates to the same allowlist ServerlessClient enforces, so a plugin's handler and a
            // plugin's component get identical answers, including identical refusals.
            this.nodeClient = new NodeClient(settings, threadPool) {
                @Override
                public <
                    Request extends org.opensearch.action.ActionRequest,
                    Response extends org.opensearch.core.action.ActionResponse> void doExecute(
                        org.opensearch.action.ActionType<Response> action,
                        Request request,
                        org.opensearch.core.action.ActionListener<Response> listener
                    ) {
                    client().execute(action, request, listener);
                }
            };
            // Before the controller, because every handler it registers is wrapped in this.
            this.systemIndices = new org.opensearch.serverless.rest.SystemIndices(this.plugins.systemIndexPatterns(settings));
            this.restController = buildRestController(clusterSettings);
            final NetworkModule networkModule = services.buildNetworkModule(clusterSettings, restController);
            final Transport transport = networkModule.getTransportSupplier().get();
            this.transportService = new TransportService(
                settings,
                transport,
                null,
                threadPool,
                authenticatedPluginHops(networkModule.getTransportInterceptor()),
                boundAddress -> new DiscoveryNode(
                    nodeName,
                    nodeEnvironment.nodeId(),
                    boundAddress.publishAddress(),
                    discoveryAttributesFor(roles),
                    discoveryRolesFor(roles),
                    Version.CURRENT
                ),
                clusterSettings,
                Set.of(),
                NoopTracer.INSTANCE
            );
            this.httpServerTransport = networkModule.getHttpServerTransportSupplier().get();
            success = true;
        } finally {
            if (success == false) {
                // The node environment too, not just the thread pool. It was missing, and a constructor
                // that threw after taking node.lock leaked the file handle for the life of the process --
                // which on a real node means the data directory stays locked and a restart cannot use it.
                // Found by a test that makes construction fail on purpose; nothing before it did.
                IOUtils.closeWhileHandlingException(openedEnvironment);
                ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * The node's role decides its engine, which is what makes the two shard roles asymmetric rather
     * than merely differently labelled.
     *
     * <p>A node that does not accept writer activation runs {@link org.opensearch.index.engine.ReadOnlyEngine},
     * so a reader shard is <em>structurally</em> unable to write — not merely expected not to. That
     * matters because a reader opens a shard without acquiring its head: there is no compare-and-swap
     * behind it, and safety comes from the engine being incapable rather than from a convention.
     *
     * <p>Readers needing no coordination is the point, not a shortcut.
     * Readers are interchangeable caches, so any search
     * node can serve any shard by reading its manifest, and the directory tier just learns about it.
     */
    private
        java.util.Collection<
            java.util.function.Function<org.opensearch.index.IndexSettings, java.util.Optional<org.opensearch.index.engine.EngineFactory>>>
        engineFactoryProviders() {
        if (roles.contains(ROLE_INGEST)) {
            // Core's own writer engine, with one thing added: it knows this shard's history may continue
            // in an object-store log rather than a local translog, and replays it during recovery so the
            // sequence numbers survive. See ServerlessWriterEngine for why replay cannot happen later.
            final org.opensearch.index.engine.EngineFactory writer = config -> new org.opensearch.serverless.engine.ServerlessWriterEngine(
                config,
                new org.opensearch.serverless.engine.ServerlessWriterEngine.ReplayLog() {
                    @Override
                    public java.util.List<org.opensearch.serverless.store.WalRecord> recordsFor(
                        org.opensearch.core.index.shard.ShardId shardId
                    ) throws java.io.IOException {
                        return reconciler == null ? java.util.List.of() : reconciler.replayRecordsFor(shardId);
                    }

                    @Override
                    public void forEachRecord(
                        org.opensearch.core.index.shard.ShardId shardId,
                        org.opensearch.common.CheckedConsumer<org.opensearch.serverless.store.WalRecord, java.io.IOException> consumer
                    ) throws java.io.IOException {
                        if (reconciler != null) {
                            reconciler.forEachReplayRecord(shardId, consumer);
                        }
                    }

                    @Override
                    public boolean replayed(
                        org.opensearch.core.index.shard.ShardId shardId,
                        org.opensearch.serverless.engine.ReplaySpool spool
                    ) throws java.io.IOException {
                        return reconciler != null && reconciler.retainReplay(shardId, spool);
                    }
                }
            );
            return java.util.List.of(indexSettings -> java.util.Optional.of(writer));
        }
        final org.opensearch.index.engine.EngineFactory readOnly = config -> new org.opensearch.index.engine.ReadOnlyEngine(
            config,
            null,
            null,
            true,
            java.util.function.Function.identity(),
            false
        );
        return java.util.List.of(indexSettings -> java.util.Optional.of(readOnly));
    }

    /**
     * The store type that makes a reader lazy.
     *
     * <p>Registered on every node; whether a shard uses it is decided per shard when it is opened, by
     * {@code ShardReconciler}. A writer keeps a local copy it can merge into; a reader reads blocks.
     */
    /**
     * Where a shard's bytes live, which is not always where its index identity says.
     *
     * <p>A frozen view is opened as an index of its own, because a node cannot hold two shards with the
     * same identity and it may well already be serving the live one. That synthetic identity has a uuid
     * nothing ever wrote under, so the path has to come from the index it is a view <em>of</em> — carried
     * as a setting on the shard the reader opens.
     *
     * <p>Absent for every ordinary shard, which is the overwhelming majority, and then this is the index's
     * own uuid exactly as before.
     */
    static String storageUuidOf(org.opensearch.index.IndexSettings indexSettings) {
        final String source = indexSettings.getSettings().get(STORAGE_UUID_SETTING);
        return source == null ? indexSettings.getIndex().getUUID() : source;
    }

    /** Names the index whose storage a shard should read, when that is not the index it belongs to. */
    public static final String STORAGE_UUID_SETTING = "index.serverless.storage_uuid";

    /**
     * Refuse to start on a plaintext transport.
     *
     * <p>Forwarded requests carry the deployment's transport secret; on a plaintext transport it crosses
     * the wire in the clear, and anyone who can read the wire can forward writes as a member. Off by
     * default because the filesystem-backed, loopback-only configuration every test runs is exactly the
     * case where a plaintext transport is fine; a deployment whose transport port is on a reachable
     * network sets it and gets a refusal instead of a warning.
     */
    public static final org.opensearch.common.settings.Setting<Boolean> REQUIRE_SECURE_TRANSPORT_SETTING =
        org.opensearch.common.settings.Setting.boolSetting(
            "serverless.transport.require_secure",
            false,
            org.opensearch.common.settings.Setting.Property.NodeScope
        );

    /**
     * The frozen view a shard is serving, if it is serving one.
     *
     * <p>A view is opened as an index of its own whose uuid is the view's identifier, and it is the only
     * kind of shard that carries {@link #STORAGE_UUID_SETTING} — so the presence of that setting is what
     * distinguishes one, and its own uuid is the identifier.
     */
    static String frozenViewIdOf(org.opensearch.index.IndexSettings indexSettings) {
        return indexSettings.getSettings().get(STORAGE_UUID_SETTING) == null ? null : indexSettings.getIndex().getUUID();
    }

    private Map<String, org.opensearch.plugins.IndexStorePlugin.DirectoryFactory> directoryFactories() {
        final org.opensearch.plugins.IndexStorePlugin.DirectoryFactory factory =
            new org.opensearch.plugins.IndexStorePlugin.DirectoryFactory() {
                @Override
                public org.apache.lucene.store.Directory newDirectory(
                    org.opensearch.index.IndexSettings indexSettings,
                    org.opensearch.index.shard.ShardPath shardPath
                ) throws java.io.IOException {
                    return open(
                        org.apache.lucene.store.FSDirectory.open(shardPath.resolveIndex()),
                        indexSettings,
                        shardPath.getShardId().id()
                    );
                }

                @Override
                public org.apache.lucene.store.Directory newFSDirectory(
                    java.nio.file.Path location,
                    org.apache.lucene.store.LockFactory lockFactory,
                    org.opensearch.index.IndexSettings indexSettings
                ) throws java.io.IOException {
                    // This hands over a location rather than a ShardPath, so the shard number has to be
                    // recovered from the directory the location sits in
                    // ({data}/indices/{uuid}/{shard}/index).
                    final org.apache.lucene.store.Directory local = org.apache.lucene.store.FSDirectory.open(location, lockFactory);
                    final int shardNumber;
                    try {
                        shardNumber = Integer.parseInt(location.getParent().getFileName().toString());
                    } catch (RuntimeException e) {
                        // An unexpected layout means we cannot say which shard this is, and guessing would
                        // attach one shard's directory to another's data. Local-only is the safe answer.
                        logger.warn("could not derive a shard number from " + location + "; serving it from local disk only", e);
                        return local;
                    }
                    return open(local, indexSettings, shardNumber);
                }
            };
        return Map.of(BLOCK_CACHE_STORE_TYPE, factory);
    }

    /**
     * Builds one shard's directory, whichever way the store asked for it.
     *
     * <p><b>Both entry points come here, and that is the point.</b> They were written separately, and the
     * one core happened to call for a live shard grew the logic while the other kept deriving the storage
     * path from the shard's own uuid. That is right for every ordinary shard and silently wrong for a
     * frozen view, whose uuid is the view's and under which nothing was ever written — the view opened
     * onto an empty directory and reported an index with no segments, which reads like a corrupted shard
     * rather than a shell bug. Whatever decides which method core calls, it cannot decide correctness.
     *
     * @param local the shard's local directory
     * @param indexSettings the shard's settings, which say whose bytes it reads
     * @param shardNumber the shard
     * @return the directory to serve it from
     * @throws java.io.IOException if the manifest or its term containers cannot be read
     */
    private org.apache.lucene.store.Directory open(
        org.apache.lucene.store.Directory local,
        org.opensearch.index.IndexSettings indexSettings,
        int shardNumber
    ) throws java.io.IOException {
        final var plane = metadataPlane;
        if (plane == null) {
            // No metadata plane means no manifest to read; a plain local directory is the truthful
            // fallback rather than a half-built remote one.
            return local;
        }
        final String indexName = indexSettings.getIndex().getName();
        final var shardBase = org.opensearch.serverless.metadata.RegisterMap.shardData(
            plane.basePath(),
            indexName,
            storageUuidOf(indexSettings),
            shardNumber
        );
        final String viewId = frozenViewIdOf(indexSettings);
        // The commit the reconciler is opening this shard at, if it is opening one right now. It read the
        // manifest -- or the view's record -- to decide how to open the shard, and arms it for exactly the
        // window in which this factory runs; taking it here is what makes a reader open one register read
        // rather than one per layer that needs the same answer.
        final ShardReconciler opening = reconciler;
        final java.util.Optional<org.opensearch.serverless.store.CommitManifest> armed = opening == null
            ? java.util.Optional.empty()
            : opening.pendingManifest(indexSettings.getIndex(), shardNumber);
        if (armed.isPresent()) {
            final String scope = (viewId != null ? viewId : storageUuidOf(indexSettings)) + "#" + shardNumber;
            return org.opensearch.serverless.store.BlockCacheDirectory.create(
                local,
                plane.blobStore(),
                shardBase,
                blockCache,
                scope,
                armed.get()
            );
        }
        // A frozen view opens the commit it froze, which is not the one the manifest register holds now --
        // that is the entire point of it. The view's identifier is this index's uuid, so the record is one
        // lookup away.
        if (viewId != null) {
            final var pit = plane.pointInTime(viewId);
            if (pit.isEmpty()) {
                // Released or expired between the search resolving it and the shard opening. An empty
                // directory here would look like an empty index; refusing says what happened.
                throw new java.io.IOException("the point in time [" + viewId + "] is no longer held");
            }
            final var frozenCommit = pit.get().shards().get(shardNumber);
            if (frozenCommit == null) {
                // The same refusal as above, for the other way this commit can be absent. A null manifest
                // is a legitimate "nothing published yet" for a writer opening a fresh shard, and it is
                // exactly the wrong reading here: a view that does not cover this shard would open an
                // empty directory, the shard would answer nothing, and the search would count it as having
                // answered -- a hole in the coverage reported as a complete result. No view reaches this
                // holding no commit for the shard being opened, and if one ever does it says so.
                throw new java.io.IOException(
                    "the point in time [" + viewId + "] holds no commit for shard " + shardNumber + " of " + shardBase.buildAsString()
                );
            }
            return org.opensearch.serverless.store.BlockCacheDirectory.create(
                local,
                plane.blobStore(),
                shardBase,
                blockCache,
                viewId + "#" + shardNumber,
                frozenCommit
            );
        }
        return org.opensearch.serverless.store.BlockCacheDirectory.create(
            local,
            plane.blobStore(),
            shardBase,
            blockCache,
            // The uuid, not the name: a recreated index reuses its name and none of its bytes.
            storageUuidOf(indexSettings) + "#" + shardNumber
        );
    }

    /**
     * Returns this node's block cache, whose counters say how much was actually fetched.
     *
     * @return the block cache
     */
    public org.opensearch.serverless.store.BlockCache blockCache() {
        return blockCache;
    }

    /**
     * The REST surface is an explicit allowlist. Anything not registered here does not
     * exist, and returns 404/501 rather than an empty success.
     */
    private RestController buildRestController(ClusterSettings clusterSettings) {
        final RestController controller = new RestController(
            Set.of(),
            // The seam a plugin authenticates on: it sees every request before the handler does, can
            // refuse it, and can leave an identity in the ThreadContext. Passed here because the
            // controller takes it at construction, which is why plugins are held before this runs.
            plugins.restHandlerWrapper(threadPool.getThreadContext(), Set.of()),
            nodeClient,
            circuitBreakerService(),
            new UsageService()
        );
        // Every shell handler is registered behind the system-index guard, rather than each one checking:
        // a guard a handler has to remember to call is a guard that one handler will not call.
        final java.util.function.UnaryOperator<org.opensearch.rest.RestHandler> guarded =
            handler -> org.opensearch.serverless.rest.SystemIndices.guard(handler, systemIndices);
        controller.registerHandler(
            guarded.apply(
                new ServerlessRootHandler(nodeName, ClusterName.CLUSTER_NAME_SETTING.get(settings).value(), nodeEnvironment::nodeId)
            )
        );
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.IndexAdminHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.DocumentHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.BulkHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.GetHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.MultiGetHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.UpdateHandler(() -> this, () -> metadataPlane)));
        // Plugin routes last, so a plugin cannot take over an endpoint the shell has already claimed:
        // RestController refuses a duplicate path rather than replacing it, and refusing loudly at boot
        // is the right answer -- a plugin that silently replaced the write path would be a system whose
        // behaviour depends on registration order.
        for (org.opensearch.rest.RestHandler handler : plugins.restHandlers(settings, controller, clusterSettings)) {
            controller.registerHandler(handler);
        }
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.SearchHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.CatalogHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.StatsHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.AliasHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.PointInTimeHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.DeleteByQueryHandler(() -> this, () -> metadataPlane)));
        // Snapshot/restore, backed by this deployment's own object store rather than a distinct
        // repository backend.
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.RepositoryHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.SnapshotHandler(() -> metadataPlane, () -> this)));
        // The one piece of /_cluster/* not refused below: cluster/config is one register, exactly
        // like an index descriptor, and this node can answer for it the same honest way it answers for an
        // index -- unlike health, state or stats, which are genuinely cluster-wide and would mislead
        // coming from one node.
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.ClusterSettingsHandler(() -> metadataPlane, () -> this))
        );
        // Cluster-shaped endpoints this deployment can answer honestly. See ClusterHealthHandler for what
        // green is redefined to mean and why, and NodesHandler for why "no node can answer this" stopped
        // being true the moment leases became an address book.
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.ClusterHealthHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.NodesHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.MappingUpdateHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.FieldCapabilitiesHandler(() -> metadataPlane, () -> this))
        );
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.ListIndicesHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.SettingsUpdateHandler(() -> metadataPlane, () -> this))
        );
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.AnalyzeHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.TemplateHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.PipelineHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.StoredScriptHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.SearchPipelineHandler(() -> metadataPlane, () -> this))
        );
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.SearchTemplateHandler(() -> this, () -> metadataPlane))
        );
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.RankEvalHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.RolloverHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.DataStreamHandler(() -> this, () -> metadataPlane)));
        controller.registerHandler(
            guarded.apply(new org.opensearch.serverless.rest.ValidateAndResolveHandler(() -> metadataPlane, () -> this))
        );
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.MultiSearchHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(guarded.apply(new org.opensearch.serverless.rest.ExplainHandler(() -> metadataPlane, () -> this)));
        controller.registerHandler(
            new ServerlessHealthHandler(
                () -> started,
                () -> membershipSource == null ? 1 : membershipSource.current().size(),
                nodeEnvironment::nodeId
            )
        );

        // Then every endpoint a classic node serves and this one refuses, each with its reason.
        RefusedEndpoints.register(controller);
        return controller;
    }

    /**
     * Wraps the plugins' own interceptors in the one that authenticates a plugin's action across a hop.
     *
     * <p>The shell's is outermost on both paths, and that is the point of the ordering rather than an
     * accident of composition: inbound, an unauthenticated plugin action is refused before any plugin's
     * interceptor is given it; outbound, the signature is on the context before a plugin's interceptor
     * runs, so a plugin that inspects or forwards the request sees it as it will go on the wire.
     *
     * <p>Both dependencies are looked up lazily. This runs while the transport service is being built,
     * which is before the router exists and long before the plugins' actions have been constructed.
     *
     * @param fromPlugins what the network module composed from the installed plugins
     * @return the composed interceptor
     */
    private org.opensearch.transport.TransportInterceptor authenticatedPluginHops(
        org.opensearch.transport.TransportInterceptor fromPlugins
    ) {
        final org.opensearch.transport.TransportInterceptor shell = new org.opensearch.serverless.transport.PluginHopAuthentication(
            () -> router,
            name -> pluginActionNames.contains(name)
        );
        return new org.opensearch.transport.TransportInterceptor() {
            @Override
            public <
                T extends org.opensearch.transport.TransportRequest> org.opensearch.transport.TransportRequestHandler<T> interceptHandler(
                    String action,
                    String executor,
                    boolean forceExecution,
                    org.opensearch.transport.TransportRequestHandler<T> actualHandler
                ) {
                return shell.interceptHandler(
                    action,
                    executor,
                    forceExecution,
                    fromPlugins.interceptHandler(action, executor, forceExecution, actualHandler)
                );
            }

            @Override
            public AsyncSender interceptSender(AsyncSender sender) {
                return shell.interceptSender(fromPlugins.interceptSender(sender));
            }
        };
    }

    /**
     * Registers who hears about writes and about ownership going wrong.
     *
     * <p>Optional. A node with no signals set still converges through the backstop pass; it just does so
     * on the clock's schedule rather than on its own evidence.
     *
     * @param signals the listener, or null to go back to hearing nothing
     */
    public void setSignals(org.opensearch.serverless.reconcile.ReconcileSignals signals) {
        this.signals = signals == null ? org.opensearch.serverless.reconcile.ReconcileSignals.NONE : signals;
    }

    /**
     * Returns the signal sink, so callers on the request path can report what they saw.
     *
     * @return the signals, never null
     */
    public org.opensearch.serverless.reconcile.ReconcileSignals signals() {
        return signals;
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
            try {
                // Publish the lease immediately. It is this node's entry in the address book that peers
                // use to forward writes here, so a node that has not renewed yet is not merely invisible
                // -- it is unroutable, and a client's write would be refused with nowhere to send it.
                renewOwnLease(plane);
            } catch (java.io.IOException e) {
                logger.warn("could not publish this node's lease; peers will not be able to route to it", e);
            }
        }
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

    final java.util.Map<String, long[]> appliedDescriptorVersions = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Past this many shards, head sightings older than a lease are dropped. See {@link ShardHeadSightings}.
     */
    public static final int DEFAULT_RECENT_HEADS_PRUNE_ABOVE = ShardHeadSightings.DEFAULT_RECENT_HEADS_PRUNE_ABOVE;

    /** The shard heads this node has read lately; the methods below forward to it. */
    final ShardHeadSightings sightings = new ShardHeadSightings(() -> metadataPlane);

    /**
     * Records a head this node has just read, for the other readers in the same pass and for routing.
     *
     * @param index the index
     * @param shard the shard
     * @param head what was read, or null if there was no head
     */
    public void noteHead(String index, int shard, org.opensearch.serverless.metadata.ShardHead head) {
        sightings.noteHead(index, shard, head);
    }

    /**
     * Sets how many head sightings this node may hold before those older than a lease are dropped.
     *
     * @param entries the threshold
     */
    public void setRecentHeadsPruneAbove(int entries) {
        sightings.setRecentHeadsPruneAbove(entries);
    }

    /**
     * Returns how many shards this node holds a head sighting for, for tests that pin its bound.
     *
     * @return the entry count
     */
    public int recentHeadCount() {
        return sightings.recentHeadCount();
    }

    /**
     * Returns a head read within the given age, if this node has one.
     *
     * @param index the index
     * @param shard the shard
     * @param maxAgeMillis how old the read may be
     * @return the head, or empty if none that young was read
     */
    public java.util.Optional<org.opensearch.serverless.metadata.ShardHead> recentHead(String index, int shard, long maxAgeMillis) {
        return sightings.recentHead(index, shard, maxAgeMillis);
    }

    /**
     * Returns the node last seen owning a shard, if the sighting is no older than one lease TTL.
     *
     * @param index the index
     * @param shard the shard
     * @return the owner's node id, or empty if no head young enough has been read here
     */
    public java.util.Optional<String> ownerHint(String index, int shard) {
        return sightings.ownerHint(index, shard);
    }

    /**
     * Forgets a hint an owner has just refused.
     *
     * @param index the index
     * @param shard the shard
     */
    public void forgetOwner(String index, int shard) {
        sightings.forgetOwner(index, shard);
    }

    /**
     * Applies mapping and settings changes made elsewhere to the shards this node holds open.
     *
     * <p>A mapping or settings update reached only the node that served it; every other node's open
     * shard kept the old one until it was reopened. One descriptor read per open index per heartbeat --
     * the heartbeat already reads a head per shard -- and a refresh only when a version moved.
     */
    java.util.Map<String, java.util.Optional<IndexDescriptor>> refreshDescriptorsOfOpenIndices(
        org.opensearch.serverless.metadata.MetadataPlane plane
    ) {
        final java.util.Set<String> indices = new java.util.LinkedHashSet<>();
        for (org.opensearch.core.index.shard.ShardId shardId : reconciler.openShards()) {
            indices.add(shardId.getIndexName());
        }
        // What was read, for the caller: present, or definitely absent. An index whose read failed is
        // left out, so a transient error is never mistaken for a deletion.
        final java.util.Map<String, java.util.Optional<IndexDescriptor>> read = new java.util.LinkedHashMap<>();
        final long intervalNanos = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(descriptorRefreshIntervalMillis);
        final long nowNanos = System.nanoTime();
        for (String indexName : indices) {
            final Long lastRead = descriptorReadAtNanos.get(indexName);
            if (intervalNanos > 0L && lastRead != null && nowNanos - lastRead < intervalNanos) {
                // Read within the interval -- by an earlier pass, or by the incarnation fence answering a
                // request -- and applied then. Left out of the result, which the caller reads as "not
                // known" rather than "deleted": a deletion is refused at the fence meanwhile.
                continue;
            }
            try {
                final long startedAt = System.nanoTime();
                final var descriptor = plane.describe(indexName);
                read.put(indexName, descriptor);
                descriptorReadAtNanos.put(indexName, startedAt);
                if (descriptor.isEmpty()) {
                    continue;
                }
                applyDescriptor(indexName, descriptor.get());
            } catch (Exception e) {
                logger.warn("could not refresh the descriptor of " + indexName + " on this node's open shards", e);
            }
        }
        descriptorReadAtNanos.keySet().retainAll(indices);
        return read;
    }

    /**
     * What a publish does to the plane's digest rollups before its manifest is written: widen the shard's entry, unless
     * the publishing writer's own mark already keeps the shard from being ruled out.
     */
    static ShardReconciler.BeforePublish widensRollupsOf(org.opensearch.serverless.metadata.MetadataPlane plane) {
        return (id, shards, term, digest) -> plane.rollups()
            .widenUnlessMarked(id.getIndexName(), id.getIndex().getUUID(), shards, id.id(), term, digest);
    }

    /**
     * Brings this node's open shards of an index up to a descriptor just read.
     *
     * <p>From the per-pass refresh and from the incarnation fence, which reads the same register for an
     * index being used and so keeps its mapping and settings current at no further cost.
     */
    void applyDescriptor(String indexName, IndexDescriptor descriptor) throws java.io.IOException {
        // The copies the next projection is built from follow what was read, or a reader opened after a
        // mapping update would be projected with the mapping from before it.
        hosted.computeIfPresent(indexName, (k, v) -> descriptor);
        served.computeIfPresent(indexName, (k, v) -> descriptor);
        final long[] applied = appliedDescriptorVersions.computeIfAbsent(indexName, k -> new long[] { -1L, -1L });
        synchronized (applied) {
            if (applied[0] != descriptor.mappingVersion()) {
                reconciler.refreshMapping(indexName, descriptor);
                applied[0] = descriptor.mappingVersion();
            }
            if (applied[1] != descriptor.settingsVersion()) {
                reconciler.refreshSettings(indexName, descriptor);
                applied[1] = descriptor.settingsVersion();
            }
        }
    }

    /**
     * Applies a descriptor some other read found -- one routing a request, say -- if it is newer than what this node's
     * shards carry. Newer, not merely different: such a read can be older than the one the fence last applied.
     */
    void applyIfNewer(String indexName, IndexDescriptor descriptor, long readStartedNanos) {
        final long[] applied = appliedDescriptorVersions.get(indexName);
        if (applied != null) {
            synchronized (applied) {
                if (descriptor.mappingVersion() <= applied[0] && descriptor.settingsVersion() <= applied[1]) {
                    return;
                }
            }
        }
        try {
            applyDescriptor(indexName, descriptor);
            descriptorReadAtNanos.merge(indexName, readStartedNanos, Math::max);
        } catch (Exception e) {
            logger.warn("could not apply the descriptor of " + indexName + " read while routing", e);
        }
    }

    /** When each open index's descriptor was last read, by the refresh or by the fence, in {@link System#nanoTime} terms. */
    final java.util.Map<String, Long> descriptorReadAtNanos = new java.util.concurrent.ConcurrentHashMap<>();

    volatile long descriptorRefreshIntervalMillis = 0L;

    /**
     * Sets how long the descriptors of open indices go between re-reads when nothing else has read them;
     * zero re-reads them on every pass, which is the library default. See
     * {@code ServerlessBootstrap#DESCRIPTOR_REFRESH_INTERVAL}.
     *
     * @param millis the interval
     * @return this, for chaining
     */
    public ServerlessNode setDescriptorRefreshIntervalMillis(long millis) {
        this.descriptorRefreshIntervalMillis = Math.max(0L, millis);
        return this;
    }

    void renewOwnLease(org.opensearch.serverless.metadata.MetadataPlane plane) throws java.io.IOException {
        if (nodeLogEnabled) {
            // Through the log, which names its epochs and writes under the lock a new epoch's recording takes.
            nodeLog(plane).renew();
            return;
        }
        writeOwnLease(plane, java.util.List.of());
    }

    /**
     * Returns this node's write log, starting it on first use: it reads the epochs an earlier process of this node left in
     * the lease, before anything renews over them.
     */
    NodeLogWriter nodeLog(org.opensearch.serverless.metadata.MetadataPlane plane) throws java.io.IOException {
        final NodeLogWriter started = nodeLogWriter;
        if (started != null && nodeLogHooked) {
            return started;
        }
        synchronized (this) {
            if (nodeLogWriter == null) {
                final NodeLogWriter writer = NodeLogWriter.start(plane, localNode.getId(), (epochs, opensEpoch) -> {
                    final long validUntil = ownLeaseValidUntilMillis;
                    // Only for a new epoch: a renewal is how a lapsed lease comes back, and refusing it left the node
                    // unable ever to renew again.
                    if (opensEpoch && validUntil != 0L && plane.clock().getAsLong() >= validUntil) {
                        // A lapsed lease is renewed only after this node lets go of what it held; recording an epoch is
                        // no way around that.
                        throw new java.io.IOException("this node's lease has lapsed; no new epoch of its log is recorded");
                    }
                    writeOwnLease(plane, epochs);
                },
                    (shardId, term) -> leases.ownsAtTerm(plane, shardId, term),
                    settings.getAsLong(
                        "serverless.wal.node_log.window_millis",
                        org.opensearch.serverless.store.NodeLogCommitter.DEFAULT_WINDOW_MILLIS
                    ),
                    settings.getAsLong(
                        "serverless.wal.node_log.max_bytes",
                        org.opensearch.serverless.store.NodeLogCommitter.DEFAULT_MAX_BYTES
                    ),
                    settings.getAsInt(
                        "serverless.wal.node_log.max_records",
                        org.opensearch.serverless.store.NodeLogCommitter.DEFAULT_MAX_RECORDS
                    )
                );
                nodeLogWriter = writer;
            }
            if (reconciler != null && nodeLogHooked == false) {
                reconciler.setNodeLog(nodeLogWriter);
                nodeLogHooked = true;
            }
            return nodeLogWriter;
        }
    }

    /**
     * Returns this node's write log, if it writes one and has started it.
     *
     * @return the log, or null
     */
    public NodeLogWriter nodeLogWriter() {
        return nodeLogWriter;
    }

    private void writeOwnLease(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        java.util.List<org.opensearch.serverless.store.NodeLogEpoch> nodeLogEpochs
    ) throws java.io.IOException {
        // Stamped from when the renewal began: the lease is valid at least that long after it.
        final long startedAt = plane.clock().getAsLong();
        ownLeaseValidUntilMillis = 0L;
        plane.membership()
            .renew(
                new org.opensearch.serverless.membership.NodeLease(
                    localNode.getId(),
                    localNode.getEphemeralId(),
                    localNode.getAddress().toString(),
                    roles,
                    0L,
                    localNode.getName(),
                    org.opensearch.Version.CURRENT.toString()
                ).withLoad(
                    reconciler == null ? 0 : reconciler.heldShards().size(),
                    admission.maxShardsHeld(),
                    inUseLastMinute(),
                    refusedLastMinute()
                ).withNodeLog(nodeLogEpochs)
            );
        ownLeaseValidUntilMillis = startedAt + plane.leaseTtlMillis();
    }

    /**
     * Until when, by the plane's clock, this node's own lease is known valid: the start of its last successful
     * renewal plus the TTL. Its own, not the membership's, which several nodes in one test share.
     */
    volatile long ownLeaseValidUntilMillis = 0L;

    /** Shards being opened right now, by index and number: no concurrent view may drop their bookkeeping. */
    final java.util.Set<String> openingShards = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /**
     * Reader opens in flight, by index and number, so concurrent callers for one shard share one open.
     *
     * <p>Every item of a multi-get over a shard nobody owns needs the same reader, and they arrive together.
     * Each used to open it for itself: one won the shard lock and the rest failed "failed to obtain in-memory
     * shard lock", which a client reads as documents missing. The fleet run found it as acknowledged writes
     * that were absent on first read-back and present on the second.
     */
    final Map<String, java.util.concurrent.CompletableFuture<org.opensearch.core.index.shard.ShardId>> readerOpens =
        new java.util.concurrent.ConcurrentHashMap<>();

    /** Applies the node-local view and counts its cost; see {@link LocalViewApplier}. */
    final LocalViewApplier views;

    ClusterState projectAndApply(java.util.Collection<IndexDescriptor> extra, java.util.Collection<ShardAssignment> opening)
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

    /** Records that an incarnation was read and found live at {@code startedAtNanos}, for the incarnation fence. */
    void confirmIncarnation(String uuid, long startedAtNanos) {
        incarnationConfirmedAtNanos.merge(uuid, startedAtNanos, Math::max);
    }

    /**
     * Returns this node's shard router, which forwards work to the node that owns a shard.
     *
     * @return the router, or null before {@link #start()}
     */
    public org.opensearch.serverless.transport.ShardRouter router() {
        return router;
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

    /**
     * Starts the node. {@link IndicesService} and {@link SearchService} reject work until started.
     */
    public void start() {
        // Before anything can be sent to this node. A plugin's action is reachable over the transport
        // from the moment its handler registers, which is well before the registry that names it exists
        // -- see pluginActionNames for the ordering. Naming them here is what lets the interceptor
        // demand authentication on the very first one.
        pluginActionNames = declaredPluginActionNames();

        clusterService.start();
        indicesService.start();
        searchService.start();

        transportService.start();
        // Say so, or refuse, when the transport is plaintext. Secure means either the secure netty
        // transport or a plugin that supplies the secure settings a TLS transport is built from.
        final String transportType = NetworkModule.TRANSPORT_TYPE_SETTING.get(settings);
        final boolean secureTransport = Netty4ModulePlugin.NETTY_SECURE_TRANSPORT_NAME.equals(transportType)
            || services.secureSettingsFactories().isEmpty() == false;
        if (secureTransport == false) {
            if (REQUIRE_SECURE_TRANSPORT_SETTING.get(settings)) {
                throw new IllegalStateException(
                    "transport.type is ["
                        + transportType
                        + "] and "
                        + REQUIRE_SECURE_TRANSPORT_SETTING.getKey()
                        + " is true: forwarded requests would carry the deployment secret in the clear"
                );
            }
            logger.warn(
                "transport.type is [{}]: forwarded requests carry the deployment secret in the clear; install a SecureSettingsFactory"
                    + " plugin or keep the transport port off every reachable network",
                transportType
            );
        }
        transportService.acceptIncomingRequests();
        httpServerTransport.start();

        // The identity in the initial view carried a placeholder address, because the real one is not
        // known until the transport binds. Replace it now that it is.
        localNode = transportService.getLocalNode();
        router = new org.opensearch.serverless.transport.ShardRouter(this, transportService, () -> metadataPlane);
        router.registerHandlers();

        // Built here rather than in the constructor: both need the node identity, and the identity is
        // not final until the transport has bound and told us the address it got.
        projector = new LocalViewProjector(ClusterName.CLUSTER_NAME_SETTING.get(settings), localNode);
        reconciler = new ShardReconciler(indicesService, localNode);
        reconciler.setOnShardFailed((failed, cause) -> {
            // Off the failing thread, which may hold engine locks the release needs.
            try {
                threadPool.generic().execute(() -> leases.onShardFailed(failed, cause));
            } catch (Exception rejected) {
                logger.warn("could not schedule the release of failed shard " + failed, rejected);
            }
        });
        started = true;

        // Last, and after `started`, because a plugin's createComponents may use the client -- which
        // refuses to work on a node that has not started. Everything a plugin can reach now exists.
        try {
            plugins.createComponents(this, new org.opensearch.env.Environment(settings, null));
        } catch (Exception e) {
            started = false;
            throw new IllegalStateException("a plugin failed to start; the node will not serve", e);
        }

        // After createComponents, because a plugin's action almost always takes one of its own components,
        // and before onNodeStarted, because a plugin told the node is up may immediately call its own
        // action.
        pluginActions = buildPluginActions();

        // And then tell them the node is up. A plugin that has to read its own index cannot do it while
        // components are being built; this is the event it waits for.
        plugins.onNodeStarted(localNode);
    }

    PluginActions pluginActions = PluginActions.none();

    /**
     * The names of every action the installed plugins declare, known before any of them is built.
     *
     * <p><b>Why not ask {@link #pluginActions}.</b> A {@code HandledTransportAction} registers its own
     * transport handler from its constructor, and those constructors run inside {@link
     * #buildPluginActions()} -- which cannot run until {@code createComponents} has, which cannot run
     * until the node has started, which is after {@code acceptIncomingRequests()}. So each plugin action
     * becomes reachable over the transport strictly before the registry naming it exists, and a check
     * that consulted the registry would wave those requests through for the whole of that window: the
     * duration of every plugin's {@code createComponents} plus the construction loop.
     *
     * <p>The names do not need any of that. They come off {@code ActionPlugin#getActions()}, which is a
     * list of descriptors, so this is filled in before the transport accepts anything and the window
     * closes. Volatile because it is written on the starting thread and read on transport threads.
     */
    volatile Set<String> pluginActionNames = Set.of();

    /**
     * Returns the actions the plugins provide.
     *
     * @return the registry, empty on a node whose plugins declared none
     */
    public PluginActions pluginActions() {
        return pluginActions;
    }

    /**
     * Builds the plugins' own transport actions.
     *
     * <p>What a constructor may be given is listed here rather than discovered: the node's own services,
     * and everything the plugins built. The order is most-specific first, so a plugin component that
     * happens to implement one of core's interfaces wins the parameter over the node's own — which is what
     * a plugin means when it declares that parameter.
     *
     * @return the registry
     */
    /**
     * Every action name the installed plugins declare, read from their handler descriptors.
     *
     * @return the names, empty when no plugin contributes an action
     */
    private Set<String> declaredPluginActionNames() {
        final Set<String> names = new java.util.HashSet<>();
        for (org.opensearch.plugins.ActionPlugin plugin : plugins.filter(org.opensearch.plugins.ActionPlugin.class)) {
            for (org.opensearch.plugins.ActionPlugin.ActionHandler<?, ?> handler : plugin.getActions()) {
                names.add(handler.getAction().name());
            }
        }
        return Set.copyOf(names);
    }

    private PluginActions buildPluginActions() {
        final java.util.List<org.opensearch.plugins.ActionPlugin> actionPlugins = plugins.filter(org.opensearch.plugins.ActionPlugin.class);
        if (actionPlugins.isEmpty()) {
            return PluginActions.none();
        }
        final java.util.List<Object> services = java.util.List.of(
            new org.opensearch.action.support.ActionFilters(new java.util.HashSet<>(plugins.actionFilters())),
            transportService,
            transportService.getTaskManager(),
            threadPool,
            client(),
            clusterService,
            settings,
            namedWriteableRegistry(),
            indexNameExpressionResolver(),
            circuitBreakerService(),
            indicesService
        );
        // Each action's constructor is resolved against the node's services and the components *its own*
        // plugin built -- never another plugin's. The aggregate list used to be offered to every action,
        // so a plugin could take a constructor argument of a type only some other plugin's component
        // satisfied, and get that plugin's private object handed to it by reflection.
        return PluginActions.build(actionPlugins, plugin -> {
            final java.util.List<Object> available = new java.util.ArrayList<>(
                plugins.componentsOf((org.opensearch.plugins.Plugin) plugin)
            );
            available.addAll(services);
            return available;
        });
    }

    /**
     * The most indices a prefix pattern may match before the request is refused.
     *
     * <p>Configurable because it is a policy about how much fan-out a caller may ask for in one request,
     * not a fact about the store — and because a default of ten thousand is unreachable in a test, which
     * would leave the refusal path unexercised.
     *
     * @return the cap
     */
    public int patternCap() {
        return settings.getAsInt(
            "serverless.search.pattern.max_indices",
            org.opensearch.serverless.metadata.MetadataPlane.DEFAULT_PATTERN_CAP
        );
    }

    /** How many shards a search works on at once by default. */
    public static final int DEFAULT_SEARCH_FANOUT_CONCURRENCY = 16;

    /**
     * How many shards a search fans out to at once: the width of its sliding window.
     *
     * <p>A search waits on object-store round trips, not on CPU, so its width is set by how many requests in
     * flight a coordinator should hold rather than by cores. Configurable because that depends on the store.
     *
     * @return the width, at least one
     */
    public int searchFanoutConcurrency() {
        return Math.max(1, settings.getAsInt("serverless.search.fanout.concurrency", DEFAULT_SEARCH_FANOUT_CONCURRENCY));
    }

    /** How many indices a prefix pattern must match before a search reads its group's digest rollups. */
    public static final int DEFAULT_ROLLUP_MIN_INDICES = 32;

    /**
     * Whether a prefix pattern long enough to name one rollup group is resolved from the group's entries rather
     * than by listing every name under the prefix. On by default: a listing costs a page per thousand names, and a
     * store holding a million under one prefix times out before the first page.
     *
     * @return true to resolve from the rollups
     */
    public boolean resolveFromRollups() {
        return settings.getAsBoolean("serverless.search.resolve_from_rollups", true);
    }

    /**
     * How old this node's copy of a rollup group may be and still resolve a pattern. Zero by default -- the group
     * is read for every search -- because a copy that predates an index's activation leaves the index off the
     * search, and a search on its owner would otherwise have seen its refreshed documents. A deployment that
     * accepts that window for fewer reads can set it.
     *
     * @return the age in milliseconds; zero to read the group every time
     */
    public long rollupCacheMillis() {
        return Math.max(0L, settings.getAsLong("serverless.search.rollup.cache_millis", 0L));
    }

    /**
     * How many indices a pattern must match before the digest rollups are consulted: below it, the per-index
     * checks cost less than the group's registers.
     *
     * @return the threshold
     */
    public int rollupMinIndices() {
        return Math.max(1, settings.getAsInt("serverless.search.rollup.min_indices", DEFAULT_ROLLUP_MIN_INDICES));
    }

    /** How many cold shards one search may activate by default. */
    public static final int DEFAULT_SEARCH_ACTIVATION_BUDGET = 1024;

    /**
     * How many shards not open on this node one search may ask for: each may cost a cold open somewhere.
     *
     * <p>A search past it is refused, or with {@code allow_partial_activation} answered from the first shards
     * within it. What the pruning digest skips does not count, so the budget bounds what a query actually
     * wakes, not how many indices its pattern names.
     *
     * @return the budget, at least one
     */
    public int searchActivationBudget() {
        return Math.max(1, settings.getAsInt("serverless.search.max_activations_per_query", DEFAULT_SEARCH_ACTIVATION_BUDGET));
    }

    /**
     * Whether a search skips the shards whose pruning digest rules the query out. On unless
     * {@code serverless.search.prune} says otherwise, which exists to measure what pruning saves.
     *
     * @return true when pruning is on
     */
    public boolean searchPruning() {
        return settings.getAsBoolean("serverless.search.prune", true);
    }

    /** Opens one shard of a frozen view, so a search can read the commit it froze. */
    public org.opensearch.core.index.shard.ShardId openFrozenView(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        org.opensearch.serverless.metadata.PointInTime pit,
        int shardNumber
    ) throws Exception {
        return activation.openFrozenView(plane, pit, shardNumber);
    }

    /** Projects the given truth into this node's local view, applies it, and opens every shard this node owns. */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> applyTruth(
        java.util.Collection<IndexDescriptor> descriptors,
        java.util.Collection<ShardAssignment> owned
    ) throws Exception {
        return activation.applyTruth(descriptors, owned);
    }

    /** Reads this node's truth from the metadata plane and applies it. */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> syncFrom(org.opensearch.serverless.metadata.MetadataPlane plane)
        throws Exception {
        return activation.syncFrom(plane);
    }

    /** Takes ownership of a shard and opens it for writing. */
    public java.util.Optional<org.opensearch.core.index.shard.ShardId> activateWriter(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        String indexName,
        int shardNumber
    ) throws Exception {
        return activation.activateWriter(plane, indexName, shardNumber);
    }

    /** Renews this node's lease and lets go of anything it has lost: {@link #renewLease} then {@link #verifyHeads}, as one pass. */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> heartbeat(org.opensearch.serverless.metadata.MetadataPlane plane)
        throws Exception {
        return leases.heartbeat(plane);
    }

    /** Renews this node's lease -- and, if the lease had already lapsed, re-verifies every held head before a single write can be acknowledged again. */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> renewLease(org.opensearch.serverless.metadata.MetadataPlane plane)
        throws Exception {
        return leases.renewLease(plane);
    }

    java.util.Set<org.opensearch.core.index.shard.ShardId> renewLease(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        boolean verifyAfterLapse
    ) throws Exception {
        return leases.renewLease(plane, verifyAfterLapse);
    }

    /** Reads the head of every writer shard this node holds and lets go of the ones it has lost. */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> verifyHeads(org.opensearch.serverless.metadata.MetadataPlane plane)
        throws Exception {
        return leases.verifyHeads(plane);
    }

    /**
     * How long a node may go without re-reading the head of every writer shard it holds.
     *
     * <p>One lease TTL, where this used to run on every renewal <em>and</em> every backstop -- four scans
     * per TTL at the defaults, each of them one register read per held shard. At the thousand-shard cap
     * that was 480 reads per shard per hour paid to be told nothing had changed, and it is the largest
     * standing cost a node has when it is doing nothing at all.
     *
     * <p><b>Why cutting it is safe, and why it is not cut further.</b> The scan is not what fences a
     * writer. A node refuses to acknowledge past its own lease deadline less the skew margin
     * ({@code ensureOwnLeaseIsStillValid}), which needs no I/O and so happens on every single write; and a
     * successor seals the log at takeover, so anything appended behind that seal is never replayed. A head
     * cannot be taken from a node whose lease is live, because {@code ShardHeadStore#acquire} refuses an
     * owner the liveness oracle says is alive. So in steady state this scan is looking for something that
     * cannot have happened.
     *
     * <p>What it does still catch is worth a bounded wait rather than nothing: a shard of an index deleted
     * and recreated under a new uuid, a head given back and won elsewhere, and the clock-skew edge where
     * two nodes disagree about whether a lease had expired. A lapse takes the unconditional path and is
     * unaffected -- {@code renewLease} scans every head before acknowledgement resumes, which is where the
     * correctness argument actually lives.
     *
     * <p><b>Zero by default, which means the scan still runs on every pass.</b> The saving is real and
     * measured -- between scans a tick reads one register whatever the node holds, against one per shard --
     * but switching it on trades something a deployment may not want to trade, and three existing tests
     * encode the current bargain as a guarantee: a tick reacquires a shard whose lease was lost, a
     * forwarded write finds the owner has moved, a node stops holding what it no longer owns. All of them
     * describe how quickly a node <em>notices</em>, and all of them get slower by up to this interval.
     *
     * <p>What does not get worse is worth saying precisely, because it is what makes the option safe to
     * offer at all. A node cannot acknowledge a write past its own lease deadline less the skew margin
     * ({@code ensureOwnLeaseIsStillValid}), which needs no I/O and so happens on every write; a successor
     * seals the log at takeover, so anything appended behind that seal never replays; and a head cannot be
     * taken from a node whose lease is live, because {@code ShardHeadStore#acquire} refuses an owner the
     * liveness oracle says is alive. A lapse takes the unconditional path regardless. So the exposure a
     * longer interval buys is a node serving reads from a shard it has lost, for up to the interval --
     * bounded, and never an acknowledged write that nobody replays.
     *
     * <p><b>What made a long interval safe to deploy is a signal carried on the node's own lease.</b> A
     * revocation marker written somewhere else fails because it is not atomic with the swap that moves
     * ownership, so its absence proves nothing -- {@code RegisterMap#assignments} says the same of the
     * listing it maintains. A revocation written <em>onto the owner's lease register</em>, before the head
     * is swapped, does not: the taker's swap of the lease and the owner's renewal of it contend on one
     * register, so either the owner renewed first and the taker backs off, or the revocation landed first
     * and the owner's next renewal fails, which it treats as a lapse -- suspend, re-read every head (see
     * {@code BlobLeaseMembership#revoke} and {@link #renewLease}). A node whose renewals keep succeeding has
     * therefore lost no head to a takeover, at one write per renewal however many shards it holds, and
     * {@code ServerlessBootstrap#HEAD_VERIFY_INTERVAL} runs this scan every five minutes as a backstop for a
     * head released by someone other than its owner. The library default below stays per pass, because the
     * tests that encode the old bargain construct nodes directly.
     */
    public static final long DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS = 0L;

    volatile long headVerifyIntervalMillis = DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS;
    volatile long headsVerifiedAtNanos = Long.MIN_VALUE;

    /**
     * Sets how long between periodic head verifications; zero verifies on every pass, as this used to.
     *
     * @param millis the interval
     * @return this, for chaining
     */
    public ServerlessNode setHeadVerifyIntervalMillis(long millis) {
        this.headVerifyIntervalMillis = Math.max(0L, millis);
        return this;
    }

    /** Verifies every held head, but only if {@link #DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS} has passed since the last time. */
    public java.util.Set<org.opensearch.core.index.shard.ShardId> verifyHeadsIfDue(org.opensearch.serverless.metadata.MetadataPlane plane)
        throws Exception {
        return leases.verifyHeadsIfDue(plane);
    }

    boolean namesThisNode(
        java.util.Optional<org.opensearch.serverless.metadata.ShardHead> head,
        org.opensearch.core.index.shard.ShardId shardId
    ) {
        return leases.namesThisNode(head, shardId);
    }

    void releaseLost(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        org.opensearch.core.index.shard.ShardId shardId,
        String reason,
        boolean forgetClaim
    ) throws Exception {
        leases.releaseLost(plane, shardId, reason, forgetClaim);
    }

    /** Serves a shard as a reader: opens the published commit without owning the shard. */
    public org.opensearch.core.index.shard.ShardId serveAsReader(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        String indexName,
        int shardNumber
    ) throws Exception {
        return readerServing.serveAsReader(plane, indexName, shardNumber);
    }

    /** Opens a reader only for the incarnation this request resolved. */
    public org.opensearch.core.index.shard.ShardId serveAsReader(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        String indexName,
        int shardNumber,
        String expectedUuid
    ) throws Exception {
        return readerServing.serveAsReader(plane, indexName, shardNumber, expectedUuid);
    }

    /** How long a reader's check stands before the shard's head is read again, by default. See {@link ReaderValidation}. */
    public static final long READER_CHECK_MILLIS = ReaderValidation.READER_CHECK_MILLIS;

    /**
     * Sets how long a reader's check stands before its head is read again; zero reads it on every check. For tests.
     *
     * @param millis the window
     */
    public void setReaderCheckMillis(long millis) {
        readerValidation.setReaderCheckMillis(millis);
    }

    /** Checks a shard open here before it answers, if it is open as a reader; see {@link ReaderServing#validateReader}. */
    public void ensureReaderCurrent(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        String indexName,
        int shardNumber,
        org.opensearch.core.index.shard.ShardId shardId
    ) throws java.io.IOException {
        readerServing.ensureReaderCurrent(plane, indexName, shardNumber, shardId);
    }

    /**
     * Returns what the write back-pressure is doing: its limit, writes in flight, append time and refusals.
     *
     * @return the stats
     */
    public org.opensearch.serverless.store.WriteBackpressure.Stats writeBackpressure() {
        return writeBackpressure.stats();
    }

    /**
     * What a reader open refused at this node's shard cap says, so a coordinator can tell a full node, which another
     * node may stand in for, from a shard nobody can open.
     */
    public static final String AT_THE_CAP = " shards, the cap; ";

    /** Forgets that this node served a shard as a reader, once it no longer does. */
    public void forgetReader(org.opensearch.core.index.shard.ShardId shardId) {
        readerServing.forgetReader(shardId);
    }

    /** Indexes a document durably: the write reaches the object store's log before it is acknowledged. */
    public WriteOutcome index(org.opensearch.core.index.shard.ShardId shardId, String id, String source) throws java.io.IOException {
        return writes.index(shardId, id, source);
    }

    /** Indexes a document, optionally only if it is still at the sequence number the caller expects. */
    public WriteOutcome index(org.opensearch.core.index.shard.ShardId shardId, String id, String source, long ifSeqNo, long ifPrimaryTerm)
        throws java.io.IOException {
        return writes.index(shardId, id, source, ifSeqNo, ifPrimaryTerm);
    }

    /** Indexes a document, optionally only if no document with that id exists. */
    public WriteOutcome index(
        org.opensearch.core.index.shard.ShardId shardId,
        String id,
        String source,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent
    ) throws java.io.IOException {
        return writes.index(shardId, id, source, ifSeqNo, ifPrimaryTerm, requireAbsent);
    }

    /** Indexes a document, optionally growing the mapping to fit it. */
    public WriteOutcome index(
        org.opensearch.core.index.shard.ShardId shardId,
        String id,
        String source,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent,
        boolean mayGrowMapping
    ) throws java.io.IOException {
        return writes.index(shardId, id, source, ifSeqNo, ifPrimaryTerm, requireAbsent, mayGrowMapping);
    }

    /**
     * How long a read confirming that an index incarnation still exists licenses answering from its shards.
     *
     * <p><b>This is what makes routing from a cached descriptor safe.</b> A coordinator routes by a
     * resolution up to {@code MetadataPlane#DEFAULT_ROUTING_CACHE_MILLIS} old, and a stale one carries the
     * uuid of an incarnation another node has deleted. The uuid check where the request lands cannot catch
     * that when the landing node still holds that incarnation's shard open -- the uuids match, because both
     * are the old one -- and a write was acknowledged into a deleted index. So the landing node answers only
     * if it has read the descriptor and found that very uuid within this window, by its own monotonic clock,
     * and a delete answers only after waiting longer than the window ({@link #DELETE_SETTLE_MILLIS}). A write
     * acknowledged here was therefore acknowledged before any delete of its index returned: concurrent with
     * the delete, never after it.
     *
     * <p>The read is per index and per window, not per request: a shard written a thousand times a second
     * pays one descriptor read a second, and an idle one pays nothing.
     */
    public static final long INCARNATION_FENCE_MILLIS = 1_000L;

    /**
     * How long a delete waits before answering: the fence, plus a margin for two clocks' rates.
     *
     * <p>Durations, not timestamps, are compared on each side, so skew between the nodes does not enter;
     * only drift in how fast their clocks run does, and a quarter of the window is far beyond that.
     */
    public static final long DELETE_SETTLE_MILLIS = INCARNATION_FENCE_MILLIS + 250L;

    volatile long incarnationFenceMillis = INCARNATION_FENCE_MILLIS;

    /** When each incarnation was last confirmed to exist, by uuid, in {@link System#nanoTime} terms. */
    final java.util.concurrent.ConcurrentHashMap<String, Long> incarnationConfirmedAtNanos = new java.util.concurrent.ConcurrentHashMap<>();

    /** Striped, so a burst of writes to one index pays one confirming read rather than one each. */
    final Object[] incarnationLocks = newLocks(64);

    private static Object[] newLocks(int n) {
        final Object[] locks = new Object[n];
        for (int i = 0; i < n; i++) {
            locks[i] = new Object();
        }
        return locks;
    }

    /**
     * Sets the incarnation fence window; zero confirms on every answer. For tests that need the check to
     * run rather than be answered from a recent confirmation.
     *
     * @param millis the window
     */
    public void setIncarnationFenceMillis(long millis) {
        this.incarnationFenceMillis = Math.max(0L, millis);
    }

    /** Refuses to answer from a shard unless its index incarnation was confirmed to exist within the window. */
    public void ensureIncarnationLive(org.opensearch.core.index.shard.ShardId shardId) throws java.io.IOException {
        fences.ensureIncarnationLive(shardId);
    }

    void refuseIfWriteFenced(org.opensearch.core.index.shard.ShardId shardId) throws java.io.IOException {
        fences.refuseIfWriteFenced(shardId);
    }

    /** Reports whether a writer shard is refusing writes after a failed log append. */
    public boolean isWriteFenced(org.opensearch.core.index.shard.ShardId shardId) {
        return fences.isWriteFenced(shardId);
    }

    final Map<org.opensearch.core.index.Index, MappingGrowthPath.MappingGrowth> mappingGrowth =
        new java.util.concurrent.ConcurrentHashMap<>();

    /** How many compare-and-swaps one mapping growth attempts before failing the write. */
    static final int MAPPING_GROWTH_ATTEMPTS = 5;

    void growMapping(org.opensearch.core.index.shard.ShardId shardId, org.opensearch.index.mapper.Mapping addition)
        throws java.io.IOException {
        mappings.growMapping(shardId, addition);
    }

    void ensureOwnLeaseIsStillValid(org.opensearch.core.index.shard.ShardId shardId) throws java.io.IOException {
        fences.ensureOwnLeaseIsStillValid(shardId);
    }

    boolean releaseIfLeaseLapsed(org.opensearch.core.index.shard.ShardId shardId) {
        return fences.releaseIfLeaseLapsed(shardId);
    }

    /** Deletes a document durably: the deletion reaches the object store's log before it is acknowledged. */
    public WriteOutcome delete(org.opensearch.core.index.shard.ShardId shardId, String id) throws java.io.IOException {
        return writes.delete(shardId, id);
    }

    /** Deletes a document, optionally only if it is still at the sequence number the caller expects. */
    public WriteOutcome delete(org.opensearch.core.index.shard.ShardId shardId, String id, long ifSeqNo, long ifPrimaryTerm)
        throws java.io.IOException {
        return writes.delete(shardId, id, ifSeqNo, ifPrimaryTerm);
    }

    /** Applies a batch to one shard: one log append for all of it, then each operation in order. */
    public java.util.List<BulkOutcome> bulk(
        org.opensearch.core.index.shard.ShardId shardId,
        java.util.List<org.opensearch.serverless.store.WalRecord> operations
    ) throws java.io.IOException {
        return writes.bulk(shardId, operations);
    }

    /** Applies a batch whose items may carry conditions. */
    public java.util.List<BulkOutcome> bulkOperations(org.opensearch.core.index.shard.ShardId shardId, java.util.List<BulkOperation> batch)
        throws java.io.IOException {
        return writes.bulkOperations(shardId, batch);
    }

    /**
     * The registry that can read a search body. See {@link NodeServices#searchXContentRegistry}.
     *
     * @return a registry containing the search parsers
     */
    public org.opensearch.core.xcontent.NamedXContentRegistry searchXContentRegistry() {
        return services.searchXContentRegistry();
    }

    /**
     * Returns the search registry with rank-eval's metric parsers added.
     *
     * @return a registry that parses a ranking-evaluation body
     */
    public org.opensearch.core.xcontent.NamedXContentRegistry rankEvalXContentRegistry() {
        return services.rankEvalXContentRegistry();
    }

    /**
     * The file watcher a plugin is given. See {@link NodeServices#resourceWatcherService}.
     *
     * @return the watcher service
     */
    public org.opensearch.watcher.ResourceWatcherService resourceWatcherService() {
        return services.resourceWatcherService();
    }

    /**
     * The script service, with Painless and Mustache registered. See {@link NodeServices#scriptService}.
     *
     * @return the script service
     */
    public org.opensearch.script.ScriptService scriptService() {
        return services.scriptService();
    }

    /**
     * Returns this node's copy of the stored scripts.
     *
     * @return the cache
     */
    public org.opensearch.serverless.script.StoredScripts storedScripts() {
        return services.storedScripts();
    }

    /**
     * Returns the search-pipeline compiler.
     *
     * @return the compiler
     */
    public org.opensearch.search.pipeline.ServerlessSearchPipelines searchPipelines() {
        return services.searchPipelines();
    }

    /**
     * Returns the ingest pipeline compiler. See {@link NodeServices#ingestPipelines}.
     *
     * @return the pipeline compiler
     */
    public org.opensearch.serverless.ingest.IngestPipelines ingestPipelines() {
        return services.ingestPipelines();
    }

    /**
     * The named-writeable registry a plugin is given: the node's own, the one its transport uses.
     *
     * @return the registry
     */
    public NamedWriteableRegistry namedWriteableRegistry() {
        return services.namedWriteableRegistry();
    }

    /**
     * The index-name resolver a plugin is given, which refuses rather than answering wrongly.
     *
     * @return the resolver
     * @see RefusingIndexNameExpressionResolver
     */
    public org.opensearch.cluster.metadata.IndexNameExpressionResolver indexNameExpressionResolver() {
        return services.indexNameExpressionResolver();
    }

    /**
     * What bounds the memory a node's in-flight writes are holding. See {@link NodeServices#indexingPressure}.
     *
     * @return the accounting
     */
    public org.opensearch.index.IndexingPressure indexingPressure() {
        return services.indexingPressure();
    }

    /**
     * The node's circuit breakers: core's own hierarchy, with core's own defaults.
     *
     * @return the breaker service
     */
    public org.opensearch.core.indices.breaker.CircuitBreakerService circuitBreakerService() {
        return services.circuitBreakerService();
    }

    /**
     * The node's array allocator, accounting against the node's breakers.
     *
     * @return the allocator
     */
    public BigArrays bigArrays() {
        return services.bigArrays();
    }

    /**
     * The gate every operation passes through, where a plugin's {@code ActionFilter}s decide.
     *
     * @return the gate, which is empty and free when no plugin installed a filter
     */
    public ActionGate actionGate() {
        return services.actionGate();
    }

    /**
     * Returns the identity service, which is core's no-op unless a plugin supplied one.
     *
     * @return the identity service
     */
    public org.opensearch.identity.IdentityService identityService() {
        return services.identityService();
    }

    /**
     * The {@link org.opensearch.transport.client.Client} this node offers to plugins. See {@link NodeServices#client}.
     *
     * @return the client
     */
    public org.opensearch.transport.client.Client client() {
        return services.client();
    }

    /**
     * Returns the node's settings.
     *
     * @return the settings this node was started with
     */
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
        return systemIndices != null && systemIndices.contains(index);
    }

    /**
     * Returns the plugins this node is running.
     *
     * @return the plugin host
     */
    public ServerlessPlugins plugins() {
        return plugins;
    }

    /**
     * Records that a request touched this shard, on the clock the reconciler will compare against.
     *
     * <p><b>The plane's clock, not the wall clock, and that is not a detail.</b> Idle release compares a
     * stamp made here against the time a reconcile pass was given, and a test drives that pass from a
     * clock it controls. Stamping {@code System.currentTimeMillis()} instead makes every shard look as
     * though it were used far in the future, so nothing is ever idle — which is exactly how the first run
     * of these tests failed, with a controller that was working and a stamp that was not comparable.
     *
     * @param shardId the shard a request used
     */
    public void markUsed(org.opensearch.core.index.shard.ShardId shardId) {
        final org.opensearch.serverless.metadata.MetadataPlane plane = metadataPlane;
        reconciler.markUsed(shardId, plane == null ? System.currentTimeMillis() : plane.clock().getAsLong());
    }

    /**
     * Explains a document using this incarnation's current mapping and records the shard as in use.
     *
     * @param shardId the shard open on this node
     * @param id the document id
     * @param query the query to explain
     * @return whether the document exists and Lucene's explanation
     * @throws java.io.IOException if the shard or its descriptor cannot be read
     */
    public org.opensearch.serverless.shard.ShardExplain.Outcome explain(
        org.opensearch.core.index.shard.ShardId shardId,
        String id,
        org.opensearch.index.query.QueryBuilder query
    ) throws java.io.IOException {
        ensureStarted();
        if (reconciler.shard(shardId) == null) {
            throw new java.io.IOException("cannot explain from " + shardId + ": not open on " + nodeName);
        }
        markUsed(shardId);
        ensureIncarnationLive(shardId);
        final var outcome = org.opensearch.serverless.shard.ShardExplain.execute(searchService(), shardId, id, query);
        ensureIncarnationLive(shardId);
        return outcome;
    }

    /**
     * Reads one document by id, from this node's own copy of the shard.
     *
     * <p><b>Realtime, and that is the whole reason this exists separately from search.</b> A write is
     * applied to the engine and durable in the log before it is acknowledged, but it is not searchable
     * until a refresh and not in the object store until a publish. A get that went through the search
     * path would therefore fail to find a document the caller had just been told was written — which
     * reads as data loss and is not. This reads the live version map, so it sees the write.
     *
     * <p>Whether the answer is realtime depends on which copy this is, and the caller is the one that
     * knows: a writer's copy includes everything acknowledged, a reader's copy includes only what was
     * published. This method does not choose between them — {@code GetHandler} does, and says which it
     * used in the response.
     *
     * @param shardId the shard, open on this node
     * @param id the document id
     * @return what was found, which may be nothing
     * @throws java.io.IOException if the shard is not open here
     */
    public Document get(org.opensearch.core.index.shard.ShardId shardId, String id) throws java.io.IOException {
        ensureStarted();
        final var shard = reconciler.shard(shardId);
        if (shard == null) {
            throw new java.io.IOException("cannot read from " + shardId + ": not open on " + nodeName);
        }
        markUsed(shardId);
        ensureIncarnationLive(shardId);
        final var result = shard.getService()
            .get(
                id,
                null,
                true,
                org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
                org.opensearch.index.VersionType.INTERNAL,
                org.opensearch.search.fetch.subphase.FetchSourceContext.FETCH_SOURCE
            );
        // No searcher to release here: ShardGetService acquires and closes its own, and GetResult is a
        // value rather than a handle.
        // Before answering, found or not: a document from a deleted incarnation and a 404 from one are both
        // answers about an index that is gone. See INCARNATION_FENCE_MILLIS.
        ensureIncarnationLive(shardId);
        if (result.isExists() == false) {
            return Document.absent(id);
        }
        // The sequence identity comes back with the document, and always did -- ShardGetService reads it
        // out of the same doc-values lookup that finds the source. It was simply dropped here, which is
        // why a caller could never obtain the token it needed to send back as if_seq_no.
        return new Document(id, true, result.sourceAsString(), result.getSeqNo(), result.getPrimaryTerm(), result.getVersion());
    }

    /**
     * Publishes a shard's current commit to the object store, fenced by the owning term.
     *
     * @param shardId the shard to publish
     * @param term the term this node owns the shard at
     * @return the manifest published
     * @throws java.io.IOException if the shard is not held here, or a newer term already published
     */
    public org.opensearch.serverless.store.CommitManifest publishShard(org.opensearch.core.index.shard.ShardId shardId, long term)
        throws java.io.IOException {
        ensureStarted();
        return reconciler.publish(shardId, term);
    }

    /**
     * Returns the roles this node advertises.
     *
     * @return the roles
     */
    public Set<String> roles() {
        return roles;
    }

    /**
     * Closes a shard this node has stopped owning.
     *
     * <p>Deliberately separate from {@link #applyTruth}: a shard is never closed because a view omitted
     * it, only because truth says ownership is gone. See {@code ShardReconciler}.
     *
     * @param shardId the shard to release
     * @param reason why, for the log
     */
    public void releaseShard(org.opensearch.core.index.shard.ShardId shardId, String reason) {
        ensureStarted();
        reconciler.releaseShard(shardId, reason);
        forgetShardRecords(shardId);
    }

    /**
     * Drops what this node remembered about a shard once it is no longer open here: the reader records
     * that would otherwise be re-projected on every open, the head it last read, and -- once no shard of
     * the index remains -- the descriptor versions it had applied.
     */
    void forgetShardRecords(org.opensearch.core.index.shard.ShardId shardId) {
        if (reconciler.shard(shardId) != null) {
            return;
        }
        forgetReader(shardId);
        sightings.forgetOwner(shardId.getIndexName(), shardId.id());
        writerAssignments.remove(shardKey(shardId.getIndexName(), shardId.id()));
        if (reconciler.openShards().stream().noneMatch(open -> open.getIndexName().equals(shardId.getIndexName()))) {
            appliedDescriptorVersions.remove(shardId.getIndexName());
            hosted.remove(shardId.getIndexName());
        }
    }

    /**
     * Returns this node's shard reconciler.
     *
     * @return the reconciler, or null before {@link #start()}
     */
    public ShardReconciler reconciler() {
        return reconciler;
    }

    /**
     * Returns this node's view projector.
     *
     * @return the projector, or null before {@link #start()}
     */
    public LocalViewProjector projector() {
        return projector;
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

    final java.util.Map<String, Long> readerTerms = new java.util.concurrent.ConcurrentHashMap<>();

    org.opensearch.core.index.shard.ShardId localShardOf(String indexName, int shardNumber) {
        return reconciler.openShards()
            .stream()
            .filter(s -> s.getIndexName().equals(indexName) && s.id() == shardNumber)
            .findFirst()
            .orElse(null);
    }

    private record CachedTransportSecrets(org.opensearch.serverless.metadata.MetadataPlane plane,
        org.opensearch.serverless.metadata.MetadataPlane.TransportSecrets secrets, long readAt) {
    }

    volatile CachedTransportSecrets transportSecrets;
    final Object transportSecretsLock = new Object();

    /**
     * Returns the transport secrets as last read, re-reading the register at most once per lease TTL and
     * on demand.
     *
     * <p>The bearer used to be read once and kept for the life of the process, so rotating it meant
     * restarting every node. The MAC design ({@code TransportAuthenticator}) verifies under the current
     * generation and, for one window, the previous one, and re-reads when it sees a generation it does
     * not know -- that is the {@code refresh} argument. The TTL bound on the cache is what makes a
     * rotation reach every node inside one lease even when nobody forwards it anything under the new
     * generation.
     *
     * @param forceReread true to re-read now -- a verifier that met a generation newer than its copy
     * @return the secrets, or null before a metadata plane is adopted
     * @throws java.io.UncheckedIOException if the register cannot be read
     */
    public org.opensearch.serverless.metadata.MetadataPlane.TransportSecrets transportSecrets(boolean forceReread) {
        final var plane = metadataPlane;
        if (plane == null) {
            return null;
        }
        final CachedTransportSecrets observed = transportSecrets;
        final long now = plane.clock().getAsLong();
        if (forceReread || expiredSecrets(observed, plane, now)) {
            synchronized (transportSecretsLock) {
                CachedTransportSecrets cached = transportSecrets;
                // Another reader may have refreshed while this one waited. Publish the value and its
                // timestamp together, and coalesce concurrent explicit refreshes of the same snapshot.
                if ((forceReread && cached == observed) || expiredSecrets(cached, plane, plane.clock().getAsLong())) {
                    try {
                        final var fresh = plane.transportSecrets();
                        cached = new CachedTransportSecrets(plane, fresh, plane.clock().getAsLong());
                        transportSecrets = cached;
                    } catch (java.io.IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }
                return cached.secrets();
            }
        }
        return observed.secrets();
    }

    private static boolean expiredSecrets(CachedTransportSecrets cached, org.opensearch.serverless.metadata.MetadataPlane plane, long now) {
        return cached == null || cached.plane() != plane || now < cached.readAt() || now - cached.readAt() > plane.leaseTtlMillis();
    }

    /**
     * Returns the secret this deployment's nodes present to one another on forwarded requests: the
     * current generation of {@link #transportSecrets(boolean)}.
     *
     * @return the secret, or null before a metadata plane is adopted
     */
    public String transportToken() {
        final var secrets = transportSecrets(false);
        return secrets == null ? null : secrets.current();
    }

    void ensureStarted() {
        if (started == false) {
            throw new IllegalStateException("node " + nodeName + " has not been started");
        }
    }

    /**
     * Returns the address the HTTP layer actually bound to.
     *
     * @return the bound HTTP address
     */
    public org.opensearch.core.common.transport.BoundTransportAddress boundHttpAddress() {
        return httpServerTransport.boundAddress();
    }

    /**
     * Returns the address the transport layer actually bound to.
     *
     * @return the bound transport address
     */
    public org.opensearch.core.common.transport.BoundTransportAddress boundTransportAddress() {
        return transportService.boundAddress();
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

    /**
     * The data plane's roles for a node advertising the given lease roles: data always, ingest when it
     * accepts writer activation, never cluster_manager. See the constructor for why.
     *
     * @param leaseRoles the roles from the lease or the {@code serverless.roles} setting
     * @return the discovery roles
     */
    public static Set<DiscoveryNodeRole> discoveryRolesFor(Set<String> leaseRoles) {
        final java.util.SortedSet<DiscoveryNodeRole> out = new java.util.TreeSet<>();
        out.add(DiscoveryNodeRole.DATA_ROLE);
        if (leaseRoles.contains(ROLE_INGEST)) {
            out.add(DiscoveryNodeRole.INGEST_ROLE);
        }
        return Collections.unmodifiableSortedSet(out);
    }

    /**
     * The lease roles as a node attribute, so the serverless role is readable off a {@code DiscoveryNode} too.
     *
     * @param leaseRoles the roles from the lease or the {@code serverless.roles} setting
     * @return the attributes to describe a node with
     */
    public static Map<String, String> discoveryAttributesFor(Set<String> leaseRoles) {
        return Map.of("serverless.roles", String.join(",", new java.util.TreeSet<>(leaseRoles)));
    }

    // ---------------------------------------------------------------- shard fences and the local view

    static String shardKey(String indexName, int shardNumber) {
        return indexName + "#" + shardNumber;
    }

    /** Runs an action that gives a shard away while holding the shard's write fence. */
    public <T> T underShardFence(org.opensearch.core.index.shard.ShardId shardId, java.util.concurrent.Callable<T> action)
        throws Exception {
        return fences.underShardFence(shardId, action);
    }

    java.util.concurrent.locks.Lock enterWritePath(org.opensearch.core.index.shard.ShardId shardId) {
        return fences.enterWritePath(shardId);
    }

    /** Reports whether this node holds a shard as its writer, by evidence no older than the given age. */
    public boolean holdsAsWriter(org.opensearch.core.index.shard.ShardId shardId, long maxHeadAgeMillis) {
        return fences.holdsAsWriter(shardId, maxHeadAgeMillis);
    }

    /** Of two copies of one descriptor, the one with the newer mapping and settings. */
    private static IndexDescriptor newerOf(IndexDescriptor a, IndexDescriptor b) {
        if (b.mappingVersion() != a.mappingVersion()) {
            return b.mappingVersion() > a.mappingVersion() ? b : a;
        }
        return b.settingsVersion() >= a.settingsVersion() ? b : a;
    }

    /**
     * Projects the view this node should hold: every index it serves or hosts, with every open reader
     * and writer shard assigned here, plus whatever is being opened right now.
     *
     * <p>One projection for both roles. The reader path and the writer path each used to project their
     * own half and apply it, so on a dual-role node each open replaced the other's indices in the applied
     * state. Assignments for shards that are no longer open are pruned as they are met, so the record of
     * writer terms follows the reconciler rather than growing with every shard ever held.
     */
    private ClusterState projectView(java.util.Collection<IndexDescriptor> extra, java.util.Collection<ShardAssignment> opening)
        throws java.io.IOException {
        final Map<String, IndexDescriptor> descriptors = new java.util.LinkedHashMap<>();
        for (IndexDescriptor descriptor : served.values()) {
            descriptors.merge(descriptor.name(), descriptor, ServerlessNode::newerOf);
        }
        for (IndexDescriptor descriptor : hosted.values()) {
            descriptors.merge(descriptor.name(), descriptor, ServerlessNode::newerOf);
        }
        for (IndexDescriptor descriptor : extra) {
            descriptors.put(descriptor.name(), descriptor);
        }
        final Map<String, org.opensearch.core.index.shard.ShardId> openByKey = new java.util.HashMap<>();
        for (org.opensearch.core.index.shard.ShardId open : reconciler.openShards()) {
            openByKey.put(shardKey(open.getIndexName(), open.id()), open);
        }
        final java.util.Set<org.opensearch.core.index.shard.ShardId> readers = reconciler.readerShards();
        final Map<String, ShardAssignment> assignments = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : readerShards) {
            final String key = shardKey(entry.getKey(), entry.getValue());
            final org.opensearch.core.index.shard.ShardId open = openByKey.get(key);
            if (open != null && readers.contains(open)) {
                assignments.put(key, new ShardAssignment(entry.getKey(), entry.getValue(), readerTerms.getOrDefault(key, 1L)));
            }
        }
        for (Map.Entry<String, ShardAssignment> entry : new java.util.ArrayList<>(writerAssignments.entrySet())) {
            final org.opensearch.core.index.shard.ShardId open = openByKey.get(entry.getKey());
            if (open != null && readers.contains(open) == false) {
                assignments.put(entry.getKey(), entry.getValue());
            } else if (openingShards.contains(entry.getKey()) == false
                && opening.stream().noneMatch(a -> shardKey(a.indexName(), a.shardId()).equals(entry.getKey()))) {
                    writerAssignments.remove(entry.getKey(), entry.getValue());
                }
        }
        for (ShardAssignment assignment : opening) {
            assignments.put(shardKey(assignment.indexName(), assignment.shardId()), assignment);
        }
        return projector.project(descriptors.values(), assignments.values());
    }

    /**
     * Returns this node's identity as the data plane sees it.
     *
     * @return the local discovery node
     */
    public DiscoveryNode localNode() {
        return localNode;
    }

    /**
     * Returns the cluster service. Its state is a node-local view, not a consensus object.
     *
     * @return the cluster service
     */
    public ClusterService clusterService() {
        return clusterService;
    }

    /**
     * Returns the indices service.
     *
     * @return the indices service
     */
    public IndicesService indicesService() {
        return indicesService;
    }

    /**
     * Returns the search service.
     *
     * @return the search service
     */
    public SearchService searchService() {
        return searchService;
    }

    /**
     * Returns the thread pool.
     *
     * @return the thread pool
     */
    public ThreadPool threadPool() {
        return threadPool;
    }

    /**
     * Returns the node environment, which owns the data paths.
     *
     * @return the node environment
     */
    public NodeEnvironment nodeEnvironment() {
        return nodeEnvironment;
    }

    /**
     * Reports whether the node has been started.
     *
     * @return true once {@link #start()} has been called and before {@link #close()}
     */
    public boolean isStarted() {
        return started;
    }

    @Override
    public void close() {
        started = false;
        // Stop accepting requests first, then close the plugins that were serving them, then the services
        // those plugins were given.
        //
        // ServerlessPlugins#closeAll must be invoked here. Skipping it is invisible while every plugin is a
        // test plugin holding nothing; a plugin that owns a thread pool leaks it out of every node.
        IOUtils.closeWhileHandlingException(httpServerTransport);
        ServerlessPlugins.closeAll(plugins.plugins());
        // After the plugins, because they were given these and may use them on the way out.
        IOUtils.closeWhileHandlingException(services);
        IOUtils.closeWhileHandlingException(transportService, searchService, indicesService, clusterService, nodeEnvironment);
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }
}
