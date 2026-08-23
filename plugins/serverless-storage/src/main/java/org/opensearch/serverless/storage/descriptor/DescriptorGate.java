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
import org.opensearch.cluster.metadata.DescriptorPrefetch;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexDescriptorPublisher;
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
        // Where change log appends go. Taken from the store rather than passed in, so this cannot end up
        // pointing at a different pool from the descriptor writes it accompanies.
        APPEND_EXECUTOR.set(store == null ? null : store.writeExecutor());
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
            // Deliberately no change log entry, which is a change and needs stating.
            //
            // This hook is called from Metadata.Builder.put, which means it runs on the cluster manager's
            // state update thread *and* on every other node's applier thread as that state's diff is
            // applied. Appending here was therefore N nodes each writing the same entry, and each of those
            // appends was an inline blocking writeBlob on a thread that must not do I/O -- the constraint
            // this hook's own comment above records having found the hard way, honoured for the descriptor
            // write beside it and not for the append.
            //
            // Nothing is lost by dropping it. An index reaching this hook is an index that is *in* cluster
            // state, so every node learns of the change through the state it is applying at this very
            // moment; the change log exists for indices cluster state never mentions. Those are written by
            // the creator, the updater, the mapping compare-and-swap and the tombstone writer, and all four
            // record.
        });
        // The durable half of deletion is not registered here any more. It had a hook in
        // MetadataDeleteIndexService and no registrar, so every gated delete acknowledged with nothing
        // durable behind it; that is fixed, and the fix now lives in DescriptorBackedIndexLifecycle, which
        // the plugin hands core through ClusterPlugin#getClaimedIndexLifecycle() rather than through a
        // static registry. Its body -- the grouped tombstone writes, the change log entry, and the
        // stored-mapping prune that used to sit in core after the acknowledgement -- moved verbatim.
        //
        // Nothing to install and nothing to clear: it reads installedStore() live, so it is armed exactly
        // when this gate is, which is the same lifetime the registration gave it.

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
            java.util.concurrent.CompletableFuture<Boolean> created = store.createAsync(descriptor).thenApply(won -> {
                // The creation's change log entry, which this path never wrote.
                //
                // A gated creation is the one descriptor write that goes nowhere near the publisher, so no
                // node other than this one learned that the name now exists. Mostly that costs nothing --
                // absence is not cached, so a cold node reads the store and finds it -- except after a
                // delete: readFromStore answers a deleted name with its tombstone, the cache admits it, and
                // a node that read the name while it was deleted goes on answering "deleted" for the whole
                // freshness window after it is recreated. The local cache is handled by create() itself;
                // this is how the other nodes hear.
                //
                // Only on a win. Recording a creation that lost the race would invalidate every other node's
                // cache on behalf of a name this call does not hold.
                if (Boolean.TRUE.equals(won)) {
                    recordChange(descriptor, DescriptorChange.Kind.CREATED);
                }
                return won;
            });
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
        // The update path, which is a read-modify-write and is now shaped like one.
        //
        // It used to take a finished descriptor and put it, unconditionally. Every caller built that
        // descriptor by resolving the current one -- from the cache, and on the cluster state thread from
        // the cache or not at all -- changing one field and handing it back, so the write reverted anything
        // else that had changed inside the freshness window. A close issued while a dynamic field was being
        // added rolled that field out of the mapping and acknowledged.
        //
        // Taking the mutation instead lets the read happen here, past the cache, and the write happen
        // conditionally on the version that read observed. See IndexDescriptorPublisher.DescriptorMutator.
        IndexDescriptorPublisher.registerUpdater(
            (name, mutation) -> java.util.concurrent.CompletableFuture.supplyAsync(
                // On the store's own executor, because the caller may be the cluster state thread -- the
                // alias path is -- and this both reads and writes the object store.
                () -> applyToDescriptor(store, mappingStore, name, mutation),
                writeExecutorOf(store)
            )
        );
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
        nodeInstalled();
        logger.info("descriptor resolution installed against the object store");
    }

    /** How many times a losing conditional write is re-based before the update is reported as contention. */
    private static final int MAX_UPDATE_ATTEMPTS = 16;

    /**
     * The pool a backend's writes run on, falling back to the calling thread.
     *
     * <p>The fallback is for a backend that has no pool of its own -- an in-memory one, or a mock, whose
     * writes are not I/O and for which an executor hop would be pure ceremony. It must never be reached by
     * the object store backend, which overrides {@link DescriptorBackend#writeExecutor()} precisely because
     * running its writes inline on a cluster state thread hangs the node.
     */
    private static java.util.concurrent.Executor writeExecutorOf(DescriptorBackend store) {
        java.util.concurrent.Executor executor = store == null ? null : store.writeExecutor();
        return executor == null ? Runnable::run : executor;
    }

    /**
     * Applies one change to a stored descriptor: fresh read, mutate, conditional write, retry if it lost.
     *
     * <p>The same loop {@code MappingGenerationStore.updateMapping} runs one level up, and for the same
     * reason. Re-basing on the value that won is what makes concurrent changes to one descriptor compose --
     * an alias added while a mapping is being extended has to leave both, and the only way to be sure of
     * that is to apply the change to what the winner wrote rather than to what this caller last saw.
     *
     * <p>A mutation that returns its argument unchanged writes nothing and reports success. That is how an
     * already-satisfied request -- adding an alias that is there, closing an index that is closed -- costs
     * no write rather than merely doing no harm.
     *
     * <p>Bounded rather than unbounded, because a livelock here would hang the request instead of failing
     * it. Sixteen matches the mapping loop above it.
     */
    private static boolean applyToDescriptor(
        DescriptorBackend store,
        MappingGenerationStore.Store mappingStore,
        String name,
        java.util.function.UnaryOperator<IndexDescriptor> mutation
    ) {
        for (int attempt = 0; attempt < MAX_UPDATE_ATTEMPTS; attempt++) {
            DescriptorBackend.VersionedDescriptor current = store.getForUpdate(name);
            IndexDescriptor descriptor = current == null ? null : current.descriptor();
            if (descriptor == null || descriptor.exists() == false) {
                // Absent or tombstoned. Failing rather than writing is the point: recreating a deleted
                // index by way of an alias change is exactly the resurrection tombstones exist to stop.
                throw new org.opensearch.index.IndexNotFoundException(name);
            }
            IndexDescriptor updated = mutation.apply(descriptor);
            if (updated == null || updated == descriptor) {
                DescriptorBackedMappingStore.registerDescriptor(descriptor);
                return true;
            }
            if (store.compareAndSwap(updated, current.storeVersion())) {
                DescriptorBackedMappingStore.registerDescriptor(updated);
                // Also here, not only on creation: a descriptor written for any other reason carries the
                // current mapping with it, and projecting it is idempotent under external versioning.
                // Cheaper than reasoning about which paths can and cannot have changed the fields.
                projectMappingForStats(mappingStore, updated);
                // The entry this path has always had to write, and the reason it exists as a kind of its
                // own. A close goes through here, and before the log heard about it no other node learned
                // that a gated index had closed: it kept the shard, kept renewing the writer lease, and a
                // restore-in-place -- which refuses while a lease is held -- was unreachable.
                recordChange(updated);
                return true;
            }
            logger.debug("descriptor update for [{}] lost the race on attempt {}; re-reading and re-applying", name, attempt + 1);
        }
        throw new IllegalStateException(
            "the descriptor for ["
                + name
                + "] could not be updated after "
                + MAX_UPDATE_ATTEMPTS
                + " attempts, which means sustained contention"
        );
    }

    /**
     * Whether this index may skip its cluster state entry.
     *
     * <p>Three conditions, and they are separate questions. The name has to be in the serverless namespace,
     * the plugin has to own the index at all, and the descriptor has to be able to carry everything the index
     * declares. Before T7 only the second was asked, so an index with a filtered alias was gated and its
     * filter went nowhere.
     *
     * <p><b>The name is first and it is the one that closes the collision.</b> Gating used to follow the
     * setting, which meant a name could be claimed in either plane -- {@code GatedAndOrdinaryNameCollisionIT}
     * measured a gated {@code x} and an ordinary {@code x} both being granted, sequentially, because neither
     * creation path consults the other's authority. Requiring the namespace here makes it impossible for a
     * name outside it to be held by a descriptor *alone*, and {@code MetadataCreateIndexService#
     * clusterStateCreateIndex} makes a cluster state entry impossible for a name inside it. (An ordinary
     * index still has a descriptor -- {@code Metadata.Builder} publishes one on every incremental change --
     * but it is a projection of an index cluster state holds, and resolution reads metadata first.) The two claims cannot meet, so there is nothing
     * left to reconcile and no window to reconcile it in.
     *
     * <p>The setting keeps its other meaning. It still selects serverless storage, the lazy directory and
     * computed placement, for a data stream backing index or an alias-bearing index that wants those and
     * cannot be gated anyway. What it no longer does is decide whether an index has a cluster state entry.
     *
     * <p>Logged at info rather than silently declined, because an operator who asked for a gated index and
     * got a cluster state entry needs to know which feature kept it there. A silent decline here would be
     * the same failure this whole area is about, one layer up.
     */
    private static boolean gatable(org.opensearch.cluster.metadata.IndexMetadata indexMetadata) {
        if (DescriptorOnlyCreation.namesAServerlessIndex(indexMetadata.getIndex().getName()) == false) {
            return false;
        }
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

    /** Clears the registration, which a node shutting down must do. */
    /**
     * The change log, written by every path above that changes a descriptor a gated index depends on.
     *
     * <p>Four of them: the creator, the updater, the tombstone writer, and {@link
     * DescriptorBackedMappingStore}'s compare-and-swap. Deliberately <em>not</em> the publisher, which
     * records indices that are in cluster state and would therefore have every node in the cluster append
     * the same entry from its applier thread. The set matters as a set: {@code BlobDescriptorBackend}
     * justifies a sixty second freshness window on the grounds that this feed carries every change, so a
     * fifth write path that does not record here silently makes that window the only mechanism rather than
     * the backstop.
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

    /**
     * The installed descriptor store, for the operations in this package that write through it without being
     * registered into a static seam of their own. Null when nothing is installed, which is how {@link
     * DescriptorBackedIndexLifecycle} tells "the feature is off" from "there is a store to tombstone into" --
     * read live on every call, never captured, for the reason {@code WILDCARD_EXPANSION_LIMIT} states above.
     */
    static DescriptorBackend installedStore() {
        return STORE.get();
    }

    /** Installs the change log. Passing null clears it, matching every other registration here. */
    public static void setChangeFeed(BlobDescriptorChangeLog changeLog) {
        CHANGE_LOG.set(changeLog);
    }

    /**
     * Where change log appends run, taken from the store so this does not need its own pool.
     *
     * <p>An append is a blob write, and the paths that record one reach here from threads that must not do
     * I/O: the alias path calls the updater from the cluster manager's state update thread, and a tombstone
     * completes on whatever thread the store's write finished on. The store already owns a pool chosen for
     * exactly this -- {@code GENERIC}, passed to {@code BlobDescriptorBackend} by the plugin with the
     * comment "which pool is the caller's decision precisely because getting it wrong hangs a node rather
     * than slowing one down" -- so the append shares it rather than inventing a second answer.
     *
     * <p>Same-thread when nothing is installed, which is a test and an in-memory backend.
     */
    private static final AtomicReference<java.util.concurrent.Executor> APPEND_EXECUTOR = new AtomicReference<>();

    /**
     * Records one descriptor write to the log, so other nodes learn of it.
     *
     * <p>The kind is derived from the descriptor's own state rather than from the operation, so a write that
     * leaves the descriptor closed is reported as a close whatever produced it. A mapping write against an
     * already-closed index cannot then quietly report UPDATED and leave a node that missed the original
     * close still holding the shard.
     */
    private static void recordChange(IndexDescriptor descriptor) {
        DescriptorChange.Kind kind;
        if (descriptor.exists() == false) {
            kind = DescriptorChange.Kind.DELETED;
        } else if (descriptor.state() == org.opensearch.cluster.metadata.IndexDescriptor.State.CLOSE) {
            kind = DescriptorChange.Kind.CLOSED;
        } else {
            kind = DescriptorChange.Kind.UPDATED;
        }
        recordChange(descriptor, kind);
    }

    /**
     * Records one descriptor write to the log under a kind the caller names.
     *
     * <p>Nothing is applied locally: this node wrote the descriptor, so its own cache is already correct,
     * and the log exists to carry the change to nodes that were not party to the write. Those consume it
     * through {@code DescriptorChangeTailer}, which invalidates their caches and releases shards of a
     * deleted or closed gated index.
     *
     * <p><b>Off the calling thread.</b> The append was an inline blocking {@code writeBlob}, and the callers
     * are cluster state hooks and completion callbacks. The descriptor write beside it was made
     * asynchronous for precisely this reason and the append was left behind, which is the shape of omission
     * this file has produced before: the reasoning was written down, applied to one of the two writes, and
     * not to the other.
     *
     * <p>Never throws, and a rejected execution is not an error either. A descriptor write that succeeded
     * must not be undone by a derived feed failing, which is the same reasoning {@code append} already
     * applies inside itself.
     */
    private static void recordChange(IndexDescriptor descriptor, DescriptorChange.Kind kind) {
        BlobDescriptorChangeLog changeLog = CHANGE_LOG.get();
        if (changeLog == null) {
            return;
        }
        DescriptorChange change = new DescriptorChange(descriptor.name(), descriptor.uuid(), kind, System.currentTimeMillis());
        java.util.concurrent.Executor executor = APPEND_EXECUTOR.get();
        try {
            if (executor == null) {
                changeLog.append(change);
            } else {
                executor.execute(() -> changeLog.append(change));
            }
        } catch (RuntimeException e) {
            // Including a rejected execution, which is a busy or closing node rather than a fault. The cost
            // is a stale cache entry elsewhere until its window expires, which is what the window is for.
            logger.warn("could not record the descriptor change for [{}]; the feed will need a rebuild: {}", descriptor.name(), e);
        }
    }

    /**
     * Records a descriptor another component in this package has just written.
     *
     * <p>Here rather than there because the change log is this class's to own -- it is what {@link
     * #setChangeFeed} installs and what {@link #uninstall} clears -- and a second holder of it would be a
     * second thing to keep in step. The callers outside this class are
     * {@link DescriptorBackedMappingStore}, whose compare-and-swap is the fifth descriptor write path and
     * the last one that recorded nothing: a dynamic field added on one node left every other node's cached
     * descriptor claiming the older mapping generation for the whole freshness window, which is what
     * {@code StoreBackedFieldRefresher} then has to discover the hard way; and {@link
     * DescriptorBackedIndexLifecycle}, which records a deletion the moment its tombstone is durable and
     * which reached this same code through a lambda registered here until deletion became one operation.
     */
    static void recordWrite(IndexDescriptor descriptor) {
        recordChange(descriptor);
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
     * and it is not: {@code GatedIndexResidency.heldOnDemand} (core's residency bookkeeping for on-demand-opened indices) reads
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
        DescriptorOnlyCreation.register(null);
        UnknownFieldRefresh.register(null);
        STORE.set(null);
        // With the store gone there is nowhere for an append to run, and holding a reference to a closing
        // node's pool is the leak this method exists to prevent.
        APPEND_EXECUTOR.set(null);
    }

}
