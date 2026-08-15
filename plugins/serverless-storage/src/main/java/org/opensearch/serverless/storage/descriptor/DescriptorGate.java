/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.stats.GatedMappingStatsAggregator;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.DescriptorOnlyCreation;
import org.opensearch.cluster.metadata.DescriptorPrefetch;
import org.opensearch.cluster.metadata.DurableTombstones;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexDescriptorPublisher;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.index.mapper.UnknownFieldRefresh;
import org.opensearch.serverless.storage.placement.ComputedPlacementGate;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Installs the descriptor read path, which was built and never connected.
 *
 * <p>H2e built the resolution seam and H8a taught {@code IndexNameExpressionResolver} to consult it. H17
 * built the pagination seam. Neither has ever had a supplier registered outside a test, so in production the
 * fallback returned null every time and a gated index could not be named by any request. The mechanism was
 * correct and unreachable, which is the failure this area has now shipped three times, twice caught inside a
 * single method (H7a, H8a) and once across the whole area.
 *
 * <p>Modelled on {@code ComputedPlacementGate}, which is the one installer in this plugin that already
 * works. Installing is the whole of the wiring: the registries are static, each node fills the gap locally,
 * and there is nothing to subscribe to or keep in sync.
 *
 * <p><b>Gating is registered last and removed first.</b> An index is gated exactly when its placement is
 * computed, because a gated index has no cluster state entry and therefore can have no published routing
 * table. Deciding those two independently would permit an index with published routing and no metadata, or
 * metadata with no way to place it, and neither is serviceable.
 *
 * <p><b>One-way door worth stating.</b> Turning the serverless setting off stops new indices being gated;
 * it does not bring already-gated indices back into cluster state. Their metadata lives in the descriptor
 * index and resolution for them stops working the moment this gate is uninstalled. Migrating them back is
 * not implemented and is not implied by the setting.
 *
 * <p><b>Uninstalling matters and is easy to forget.</b> The registries outlive any one node, so a node that
 * closes without clearing them leaves a dead {@code Client} answering resolution for whatever runs next. In
 * a test JVM that is every subsequent suite.
 */
public final class DescriptorGate {

    private static final Logger logger = LogManager.getLogger(DescriptorGate.class);

    private static final java.util.concurrent.atomic.AtomicReference<DescriptorBackend> STORE =
        new java.util.concurrent.atomic.AtomicReference<>();

    /**
     * How many gated indices one wildcard may expand to before the request is refused.
     *
     * <p>The number is a judgment call and the two measurements bounding it point at different things.
     * Resolution is the cheap side: S24 measured about 33 ms per thousand names, so a hundred is roughly
     * three milliseconds and nothing cares. The expensive side is fan-out. T20 and T21 measured 118 KB and
     * 3.06 file descriptors per awake shard and 41.5 ms to wake one, and with index per tenant most tenant
     * indices are asleep, so an expansion decides how many sleeping shards a single request wakes. A hundred
     * is about 12 MB and 300 descriptors; a thousand is 118 MB and 3,000.
     *
     * <p>A hundred rather than a thousand because the failure is asymmetric. Too low refuses a request that
     * would have worked, which the caller sees at once and an operator raises in one setting change. Too
     * high wakes a large part of the fleet, which surfaces as memory and descriptor pressure on whichever
     * node coordinated it and does not point back at the wildcard that caused it.
     *
     * <p>The fan-out figure is the weaker of the two inputs: 118 KB is per shard at rest, and a hundred
     * simultaneous wakes is not a hundred times one wake. Worth re-measuring against a real wake storm
     * before this default is defended rather than merely chosen.
     */
    public static final int DEFAULT_WILDCARD_EXPANSION_LIMIT = 100;

    /**
     * The live limit, held here rather than captured at install so the setting can be changed at runtime.
     *
     * <p>Capturing it would make a dynamic setting silently static, which is the shape of defect this area
     * has shipped repeatedly: a mechanism that exists, is configured, and is not consulted.
     */
    private static final java.util.concurrent.atomic.AtomicInteger WILDCARD_EXPANSION_LIMIT = new java.util.concurrent.atomic.AtomicInteger(
        DEFAULT_WILDCARD_EXPANSION_LIMIT
    );

    private DescriptorGate() {}

    /** Applies a new expansion limit, which the plugin wires to its cluster setting. */
    public static void setWildcardExpansionLimit(int limit) {
        WILDCARD_EXPANSION_LIMIT.set(limit);
        logger.info("wildcard expansion over gated indices limited to [{}] indices", limit);
    }

    /** The limit in force, which tests assert rather than assume. */
    public static int wildcardExpansionLimit() {
        return WILDCARD_EXPANSION_LIMIT.get();
    }

    /**
     * Resolves a name through the store.
     *
     * <p>This used to carry a guard against resolving the descriptor system index's own name, because the
     * store read descriptors by issuing a get against that index, the get resolved an index name, and
     * resolution came back here -- a recursion that only terminated while the index was absent from cluster
     * state, which is precisely a fresh cluster. The guard went with the index: an object store read
     * resolves no index name, so there is no loop to close.
     */
    private static IndexDescriptor supply(String name) {
        DescriptorBackend store = STORE.get();
        return remember(store == null ? null : store.get(name));
    }

    private static IndexDescriptor supplyIfFresh(String name) {
        DescriptorBackend store = STORE.get();
        return remember(store == null ? null : store.getIfFresh(name));
    }

    /**
     * Records the descriptor's uuid-to-name entry, which is the only way back from a uuid to a descriptor.
     *
     * <p>Descriptors are keyed by name and a mapping update carries a uuid, so {@link
     * DescriptorBackedMappingStore} keeps a uuid-to-name map and can answer nothing without it. It was
     * populated on the creation and update paths only, so a node that had merely *resolved* an index -- the
     * ordinary case for the node handling a write, which resolves the name to route the request -- would
     * find the map empty, read a null mapping, and then fail its swap sixteen times before reporting
     * "sustained contention" for what is actually a missing lookup.
     *
     * <p>Resolution is the one place that holds a descriptor and its uuid together on every node that will
     * need them, so it is where the entry belongs.
     */
    private static IndexDescriptor remember(IndexDescriptor descriptor) {
        DescriptorBackedMappingStore.registerDescriptor(descriptor);
        return descriptor;
    }

    /**
     * Feeds a descriptor's own mapping to the stats projection, for the path that never touches the store.
     *
     * <p>A gated creation writes its declared mapping inside the descriptor rather than through
     * {@code MappingGenerationStore}, so {@code StatsProjectingMappingStore} would only ever observe dynamic
     * field updates and cluster stats would omit every index that declared its fields up front. Called from
     * both descriptor write hooks for that reason.
     *
     * <p>Type-checked rather than added to the {@code Store} interface deliberately: projecting for the
     * aggregate is not something a mapping store must be able to do, and widening the interface would oblige
     * every future implementation to have an opinion about cluster stats.
     */
    private static void projectMappingForStats(MappingGenerationStore.Store mappingStore, IndexDescriptor descriptor) {
        if (mappingStore instanceof StatsProjectingMappingStore projecting && descriptor != null) {
            projecting.projectDescriptorMapping(descriptor.uuid(), descriptor.mappingGeneration(), descriptor.initialMapping());
        }
    }

    public static void install(
        DescriptorBackend store,
        DescriptorPrefixBackend prefixBackend,
        MappingGenerationStore.Store mappingStore,
        GatedMappingStatsAggregator.Aggregator statsAggregator,
        UnknownFieldRefresh.Refresher fieldRefresher,
        boolean enabled
    ) {
        if (enabled == false) {
            return;
        }
        // The store is published before the supplier that reads it, so no resolution can observe a
        // registered supplier backed by a null store.
        STORE.set(store);
        AbsentIndexDescriptorSuppliers.register(DescriptorGate::supply);
        AbsentIndexDescriptorSuppliers.registerCached(DescriptorGate::supplyIfFresh);
        // T28. Wildcards, which until now matched no gated index at all: every branch of the resolver's
        // matching reads cluster state, and a gated index is absent from it by construction, so T25
        // measured tenant-* over five gated tenants returning nothing and returning it without an error.
        //
        // The limit is read per call rather than captured, so changing the setting takes effect on the next
        // wildcard instead of on the next node restart.
        AbsentIndexDescriptorSuppliers.registerExpander(prefix -> prefixBackend.expandPrefix(prefix, WILDCARD_EXPANSION_LIMIT.get()));
        // C1's prefetcher, which had no registrar and so did nothing at all. The seam and its hook in
        // TransportBulkAction landed together; without this line the hook found nothing installed and
        // returned immediately on every request, which is the "correct and unreachable" shape this area
        // has produced repeatedly and which is invisible because a no-op prefetch looks exactly like a
        // successful one.
        //
        // Warming puts descriptors in the cache the eleven synchronous sites read from. Doing it here rather
        // than at those sites is the whole point: this runs once per request with the full name set, so a
        // bulk touching M tenants resolves M descriptors together instead of one at a time from inside the
        // document loop.
        //
        // Delegated to the backend rather than looped here, because the loop was the bug. This registered a
        // plain get per name, and get blocks. The hook runs in TransportBulkAction.doExecute, on a transport
        // worker and sometimes on the cluster applier thread, both of which assert against blocking. The
        // resulting AssertionError was captured into the gated creation future and re-thrown at the
        // acknowledgement, so four suites failed a create or a delete with a thread assertion pointing at
        // code nowhere near them. Each backend now warms in whatever way is non-blocking for it: one
        // multi-get for the system index, an executor hop for the object store.
        DescriptorPrefetch.register((indexNames, listener) -> {
            try {
                store.warmAsync(indexNames, listener);
            } catch (RuntimeException e) {
                logger.debug("could not prefetch descriptors", e);
                listener.onResponse(null);
            }
        });
        // The write path. H2b dual-writes the descriptor at creation and H4 records deletions as
        // tombstones, and neither has ever had a publisher registered, so no index creation outside a test
        // has written a descriptor. Both go through put rather than create: publish records an index that
        // already exists, and a tombstone deliberately overwrites the live descriptor rather than racing
        // it. The put-if-absent path is create(), which is the uniqueness gate for gated creation (H3) and
        // a different question from recording.
        //
        // Asynchronous because this hook runs on the cluster state thread: Metadata calls it while building
        // a cluster state, so a blocking write deadlocks against the index operation it issues. Registering
        // the blocking put hung the node instead of failing, which is how the constraint was found.
        // Tombstones take the retrying path: they are the one descriptor write that is not safe to lose,
        // since for a gated index there is no cluster state entry and no graveyard entry behind them.
        IndexDescriptorPublisher.register(descriptor -> {
            // Which name is being written and whether it is a tombstone. Descriptor writes are asynchronous,
            // so they are hard to attribute after the fact: a write that lands after a test has finished
            // shows up only as a descriptor reappearing, with nothing saying what wrote it.
            logger.debug("publishing descriptor for [{}], exists [{}]", descriptor.name(), descriptor.exists());
            DescriptorBackedMappingStore.registerDescriptor(descriptor);
            if (descriptor.exists()) {
                store.putAsync(descriptor);
            } else {
                store.putTombstoneAsync(descriptor);
            }
            // The prefix half is a separate store until the name index serves prefix resolution, and a
            // point write to the object store leaves it not knowing the name exists. Dual-writing keeps
            // wildcards answering while point reads move; without it, switching the backend would silently
            // stop every wildcard from matching anything created afterwards, which is the failure T25
            // already measured once from the other direction.
            //
            // Skipped when the two are the same object, which is the unswitched configuration, so an
            // ordinary deployment does exactly one write as before.
            if (prefixBackend instanceof DescriptorBackend && prefixBackend != store) {
                DescriptorBackend prefixWrites = (DescriptorBackend) prefixBackend;
                if (descriptor.exists()) {
                    prefixWrites.putAsync(descriptor);
                } else {
                    prefixWrites.putTombstoneAsync(descriptor);
                }
            }
            recordChange(descriptor);
        });
        // The durable half of deletion, which had a hook in MetadataDeleteIndexService and no registrar, so
        // every gated delete acknowledged with nothing durable behind it.
        //
        // The publisher above is fire-and-forget by necessity: it runs inside cluster state construction,
        // where blocking deadlocks. That is fine for a creation, whose index is in cluster state anyway, and
        // not fine for a tombstone. For a gated index there is no cluster state entry and no graveyard entry
        // standing behind the tombstone, so losing it means a node holding that shard's data can adopt it
        // again on rejoin -- the resurrection IndexGraveyard exists to prevent, reintroduced.
        //
        // This runs after the state is committed and before the client is told the delete succeeded, which
        // is the one window where a write can be both off the cluster state thread and ahead of the
        // acknowledgement. Nothing blocks; the acknowledgement is deferred, not waited on.
        DurableTombstones.register((deleted, whenStored) -> {
            if (deleted.isEmpty()) {
                whenStored.onResponse(null);
                return;
            }
            // Grouped so the acknowledgement waits for all of them and reports the first failure. A delete
            // naming several indices is not durable until the last tombstone is.
            org.opensearch.core.action.ActionListener<Void> perTombstone = new org.opensearch.action.support.GroupedActionListener<>(
                org.opensearch.core.action.ActionListener.wrap(ignored -> whenStored.onResponse(null), whenStored::onFailure),
                deleted.size()
            );
            for (org.opensearch.cluster.metadata.IndexMetadata metadata : deleted) {
                store.putTombstoneAsync(IndexDescriptor.from(metadata).tombstoned(System.currentTimeMillis()), perTombstone);
            }
        });
        // T18. The creator is a separate registration from the publisher because the two have opposite
        // failure semantics, and T17 and T23 are what happened while one stood in for the other. The
        // publisher records an index that already exists in cluster state, so a lost write costs a
        // comparison. The creator writes the only record a gated index will ever have, so it uses
        // op_type=create for atomicity against a competing creation and reports its outcome to the client.
        //
        // Dual-writes for the same reason the publisher does, and this half was missed. Gated creation is
        // the one write that never goes through the publisher, so with only the point half written a gated
        // index created on the blob backend was resolvable by exact name and invisible to every wildcard --
        // permanently, since nothing else ever writes that descriptor again. The prefix write uses put
        // rather than create: the uniqueness gate is the point half's create above, and a second create here
        // would race against it and report a spurious conflict.
        //
        // After the point write rather than beside it, because a name that is not uniquely ours must not
        // appear in the prefix half at all.
        IndexDescriptorPublisher.registerCreator(descriptor -> {
            DescriptorBackedMappingStore.registerDescriptor(descriptor);
            projectMappingForStats(mappingStore, descriptor);
            java.util.concurrent.CompletableFuture<Boolean> created = store.createAsync(descriptor);
            if (prefixBackend instanceof DescriptorBackend == false || prefixBackend == store) {
                return created;
            }
            DescriptorBackend prefixWrites = (DescriptorBackend) prefixBackend;
            // Chained into the future the caller waits on, not fired and forgotten, and that ordering is
            // load-bearing. Creation is acknowledged when this future completes, so a fire-and-forget prefix
            // write can still be in flight when the client issues the delete that follows. The tombstone then
            // lands first and the creation write overwrites it, leaving a deleted index recorded as OPEN in
            // the half that answers wildcards -- a resurrection produced by nothing but write ordering.
            // Observed as exactly that: a tombstoned name reading back OPEN.
            return created.thenCompose(won -> {
                if (Boolean.TRUE.equals(won) == false) {
                    return java.util.concurrent.CompletableFuture.completedFuture(won);
                }
                // Failure here does not fail the creation. The point half is the record of the index; the
                // prefix half is an index over it, and a name missing from it costs a wildcard match rather
                // than the index.
                return prefixWrites.createAsync(descriptor).handle((ok, failure) -> {
                    if (failure != null) {
                        logger.warn(
                            "could not record [{}] in the prefix half; wildcards will not match it: {}",
                            descriptor.name(),
                            failure
                        );
                    }
                    return won;
                });
            });
        });
        IndexDescriptorPublisher.registerUpdater(descriptor -> {
            DescriptorBackedMappingStore.registerDescriptor(descriptor);
            // Also here, not only on creation: a descriptor republished for any other reason carries the
            // current mapping with it, and projecting it is idempotent under external versioning. Cheaper
            // than reasoning about which republish paths can and cannot have changed the fields.
            projectMappingForStats(mappingStore, descriptor);
            java.util.concurrent.CompletableFuture<Boolean> future = new java.util.concurrent.CompletableFuture<>();
            store.putAsync(
                descriptor,
                org.opensearch.core.action.ActionListener.wrap(ignored -> future.complete(true), future::completeExceptionally)
            );
            return future;
        });
        // T58: Mappings for gated indices are stored directly inside IndexDescriptors in Object Storage.
        //
        // Registered from the caller rather than constructed here, and that is a correction. T58 replaced
        // the registration of this parameter with a locally constructed DescriptorBackedMappingStore and
        // left the parameter itself declared and unread, so the store the plugin builds -- and the
        // MappingIndexWatcher it registers a cluster state listener for -- was assembled and discarded. The
        // composition of "authoritative descriptor plus stats projection" belongs at the wiring site where
        // both halves are visible, not hidden behind an ignored argument. See StatsProjectingMappingStore
        // for what the discarded half turned out to be load-bearing for.
        MappingGenerationStore.register(mappingStore);
        INSTALLED_MAPPING_STORE.set(mappingStore);
        // Cluster stats. H19 measured that _cluster/stats reports a plausible wrong number for a gated
        // population, and H20 built a seam through which per-index iteration cannot be expressed. This is
        // the aggregate it was waiting for: one query whose cost is set by the number of field types
        // rather than by the number of indices.
        GatedMappingStatsAggregator.register(statsAggregator);
        // The switch that turns the feature on, and it is deliberately last. Until this is registered,
        // DescriptorOnlyCreation.skipsClusterState answers false for every index, so nothing is gated and
        // every seam above serves a population of zero. Installing it before the seams existed would have
        // created indices that nothing could resolve, which is why the order matters rather than being
        // incidental.
        //
        // Gating is a strict subset of computed placement, and the direction matters. A gated index has no
        // cluster state entry, so it can have no published routing table, so its placement must be derived:
        // gating without computed placement would leave an index nothing can route to.
        //
        // The subset is narrower than ownsIndex because a descriptor cannot carry everything metadata can.
        // T7 found that an alias filter, alias routing and the write index flag are all dropped in silence
        // by IndexDescriptor.from, and an alias filter restricts which documents a query may see, so gating
        // such an index widens a restricted view rather than breaking it. DescriptorRepresentable names
        // every such reason in one place. An index it rejects keeps its cluster state entry and still gets
        // computed placement, which is exactly the arrangement that existed before W12 turned gating on.
        // The refresher W14's trigger consults. Registered here rather than at the trigger so it shares
        // this gate's lifetime: an unregistered refresher makes the trigger a null check, which is the
        // pre-W15 behaviour and correct for a cluster with no gated indices.
        UnknownFieldRefresh.register(fieldRefresher);

        DescriptorOnlyCreation.register(DescriptorGate::gatable);
        // And the road that reaches it. Registered after the gate rather than before, for the same reason the
        // gate is registered last: this only chooses which path a creation takes, and a path installed ahead
        // of the gate it is meant to reach would pull requests off the cluster state thread only to have them
        // find nothing gating them and fall back.
        DescriptorOnlyCreation.registerAdmissionCheck(DescriptorGate::worthAdmittingOffThread);
        nodeInstalled();
        logger.info("descriptor resolution installed against the object store");
    }

    /**
     * Whether this index may skip its cluster state entry.
     *
     * <p>Two conditions, and they are separate questions. The plugin has to own the index at all, and the
     * descriptor has to be able to carry everything the index declares. Before T7 only the first was asked,
     * so an index with a filtered alias was gated and its filter went nowhere.
     *
     * <p>Logged at info rather than silently declined, because an operator who asked for a gated index and
     * got a cluster state entry needs to know which feature kept it there. A silent decline here would be
     * the same failure this whole area is about, one layer up.
     */
    private static boolean gatable(org.opensearch.cluster.metadata.IndexMetadata indexMetadata) {
        if (ComputedPlacementGate.ownsIndex(indexMetadata) == false) {
            return false;
        }
        String reason = org.opensearch.cluster.metadata.DescriptorRepresentable.whyNotRepresentable(indexMetadata);
        if (reason != null) {
            logger.info("index [{}] keeps its cluster state entry: {}", indexMetadata.getIndex().getName(), reason);
            return false;
        }
        return true;
    }

    /**
     * Whether a creation request is worth admitting off the cluster state update thread.
     *
     * <p>Only the first of {@link #gatable}'s two conditions, and only the part of it that can be read from
     * the request itself. {@code ownsIndex} reads exactly this setting off the finished metadata, so a request
     * that carries it will almost always turn out gatable; the second condition needs resolved aliases, which
     * do not exist yet here, and is left to the real gate.
     *
     * <p><b>This reads whatever settings it is given, and since T49 those have templates merged into them.</b>
     * It used to be handed the request's own settings only, on the reasoning that an index gated by a template
     * takes the ordinary path and is gated at the bottom exactly as before: correct, and no faster. That was
     * wrong. Being gated at the bottom means writing the declared mapping to a store whose writes block, from
     * the state update thread, by a path that had already concluded the index was not gated. The resolution
     * happens in {@code MetadataCreateIndexService} rather than here, so this predicate and the gate at the
     * bottom read the same settings rather than two computations of the same idea.
     */
    private static boolean worthAdmittingOffThread(org.opensearch.common.settings.Settings requestSettings) {
        return requestSettings.getAsBoolean("index.serverless_storage.enabled", false);
    }

    /** Clears both registrations, which a node shutting down must do. */
    /**
     * The change log, which had nowhere to be called from until the publisher above called it.
     *
     * <p>What the log carries is cache invalidation and shard release on other nodes, applied by
     * {@code DescriptorChangeTailer}. It once also fed an in-memory name index on every node; that index
     * was removed because no request path read it and wildcards are answered by a prefix search over the
     * descriptor store instead, which is one shared structure rather than a copy per node.
     *
     * <p>Optional on purpose. A deployment with no object store configured still publishes descriptors,
     * and a null log simply means nothing is recorded, which is what the setter's absence already meant.
     */
    private static final AtomicReference<BlobDescriptorChangeLog> CHANGE_LOG = new AtomicReference<>();

    /**
     * The mapping store this node installed, kept only so it can be drained.
     *
     * <p>{@link MappingGenerationStore} deliberately exposes no getter -- it is a registration seam, not a
     * registry to read back -- and the one thing a caller genuinely needs from the installed store is to wait
     * for its write-behind projections before the node underneath them goes away.
     */
    private static final AtomicReference<MappingGenerationStore.Store> INSTALLED_MAPPING_STORE = new AtomicReference<>();

    /** The installed mapping store, for tests that need to wait for its projections. Null when nothing is installed. */
    public static MappingGenerationStore.Store installedMappingStore() {
        return INSTALLED_MAPPING_STORE.get();
    }

    /** Installs the change log. Passing null clears it, matching every other registration here. */
    public static void setChangeFeed(BlobDescriptorChangeLog changeLog) {
        CHANGE_LOG.set(changeLog);
    }

    /**
     * Records one descriptor write to the log, so other nodes learn of it.
     *
     * <p>Nothing is applied locally: this node wrote the descriptor, so its own cache is already correct,
     * and the log exists to carry the change to nodes that were not party to the write. Those consume it
     * through {@code DescriptorChangeTailer}, which invalidates their caches and releases shards of a
     * deleted gated index.
     *
     * <p>Never throws. A descriptor write that succeeded must not be undone by a derived feed failing,
     * which is the same reasoning {@code append} already applies inside itself.
     */
    private static void recordChange(IndexDescriptor descriptor) {
        DescriptorChange change = new DescriptorChange(
            descriptor.name(),
            descriptor.uuid(),
            descriptor.exists() ? DescriptorChange.Kind.UPDATED : DescriptorChange.Kind.DELETED,
            System.currentTimeMillis()
        );
        try {
            BlobDescriptorChangeLog changeLog = CHANGE_LOG.get();
            if (changeLog != null) {
                changeLog.append(change);
            }
        } catch (RuntimeException e) {
            logger.warn("could not record the descriptor change for [{}]; the feed will need a rebuild: {}", descriptor.name(), e);
        }
    }

    /**
     * How many nodes in this JVM currently hold the gate installed.
     *
     * <p>One in production, where a JVM hosts one node and this is decoration. More than one under
     * {@code InternalTestCluster}, which is where its absence was a defect: every registry this class
     * touches is static, so the first node to close called {@link #uninstall} and disarmed gating for every
     * other node still running in the same process.
     *
     * <p>The symptom was a suite failing nondeterministically -- a different test each run, green on a rerun
     * of identical code. {@code GatedCreationSwitchIT} asserting an unregistered gate answers false is one
     * shape; a resolution that suddenly returns nothing is another. It was treated as test-only for a while,
     * and it is not: {@code IndicesClusterStateService.heldOnDemand} reads
     * {@code AbsentIndexDescriptorSuppliers.isRegistered}, so an unbalanced uninstall can make a node stop
     * recognising gated indices it is currently holding open.
     */
    private static final java.util.concurrent.atomic.AtomicInteger INSTALLED_NODES = new java.util.concurrent.atomic.AtomicInteger();

    /** Records that a node has installed the gate. Called at the end of a successful {@link #install}. */
    private static void nodeInstalled() {
        INSTALLED_NODES.incrementAndGet();
    }

    /**
     * Releases one node's claim, clearing the registries only once the last node has gone.
     *
     * <p>What a node's {@code close()} calls. {@link #uninstall} is the unconditional reset, which is what a
     * test wants between methods and what a node emphatically does not want while its neighbours are still
     * serving requests through the registries it would be clearing.
     */
    public static void uninstallOneNode() {
        if (INSTALLED_NODES.get() <= 0 || INSTALLED_NODES.decrementAndGet() > 0) {
            return;
        }
        uninstall();
    }

    public static void uninstall() {
        // Back to zero rather than decremented: this is the unconditional reset, so whatever nodes thought
        // they had claims no longer do. A test calling this between methods must not leave a count behind
        // that makes the next install look like a second node.
        INSTALLED_NODES.set(0);
        // Cleared in the reverse order, so the supplier is gone before the store it reads.
        AbsentIndexDescriptorSuppliers.register(null);
        AbsentIndexDescriptorSuppliers.registerCached(null);
        AbsentIndexDescriptorSuppliers.registerExpander(null);
        DescriptorPrefetch.register(null);
        setChangeFeed(null);
        // Reset rather than leave, since the registries are static and a limit set by one test would
        // otherwise decide the behaviour of every suite that ran after it in the same JVM.
        WILDCARD_EXPANSION_LIMIT.set(DEFAULT_WILDCARD_EXPANSION_LIMIT);
        IndexDescriptorPublisher.register(null);
        DurableTombstones.register(null);
        IndexDescriptorPublisher.registerCreator(null);
        IndexDescriptorPublisher.registerUpdater(null);
        // Before the registration is cleared, and before the node this belongs to finishes closing: a
        // projection still on the executor has a client that is about to be shut under it, and abandoning it
        // leaves the field type counts for whatever it was writing under-reporting until that index's next
        // mapping change. The wait is bounded because a close must finish either way.
        MappingGenerationStore.Store draining = INSTALLED_MAPPING_STORE.getAndSet(null);
        if (draining instanceof StatsProjectingMappingStore projecting && projecting.awaitQuiescence(10_000) == false) {
            logger.warn("mapping stats projections were still running at shutdown; gated field type counts may under-report");
        }
        MappingGenerationStore.register(null);
        GatedMappingStatsAggregator.register(null);
        DescriptorOnlyCreation.registerAdmissionCheck(null);
        DescriptorOnlyCreation.register(null);
        UnknownFieldRefresh.register(null);
        STORE.set(null);
    }

}
