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
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexDescriptorPublisher;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.index.mapper.UnknownFieldRefresh;
import org.opensearch.serverless.storage.nameindex.NameIndexService;
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
     * Resolves a name through the store, except the store's own index.
     *
     * <p><b>Without this the first resolution miss on a fresh cluster recurses until the stack overflows.</b>
     * The store reads descriptors by issuing a get against the descriptor index, and that get resolves an
     * index name, and resolution consults this supplier, which reads the descriptor index. The loop closes
     * only when the descriptor index is absent from cluster state, which is exactly the state a cluster is
     * in before its first gated index exists.
     *
     * <p>It was found by the control asserting that a missing name still raises, not by the test asserting
     * the feature works: the feature test had already created a descriptor, so the descriptor index was in
     * cluster state and resolution never reached the supplier. A gate installed on a fresh node would have
     * killed the first request that named anything unknown.
     *
     * <p>The guard is a name comparison rather than a re-entrancy flag because the recursion is not
     * incidental. The descriptor index is the one index whose metadata cannot come from descriptors, and
     * saying so once is clearer than detecting it dynamically.
     */
    private static IndexDescriptor supplyExceptForTheStoreItself(String name) {
        if (DescriptorStore.DESCRIPTOR_INDEX.equals(name)) {
            return null;
        }
        DescriptorBackend store = STORE.get();
        return store == null ? null : store.get(name);
    }

    /**
     * Installs resolution and pagination against {@code store}, or does nothing when disabled.
     *
     * @param enabled whether gated indices are in use at all, so an ordinary cluster is untouched
     */
    /**
     * The unswitched form: one store answers both point reads and prefix searches.
     *
     * <p>Which is what every deployment did before the backend became selectable, and what most callers
     * mean. Kept as an overload rather than making them pass the same object twice, since a caller that
     * has to repeat itself eventually passes two different things by accident.
     */
    public static void install(
        DescriptorStore store,
        MappingGenerationStore.Store mappingStore,
        GatedMappingStatsAggregator.Aggregator statsAggregator,
        UnknownFieldRefresh.Refresher fieldRefresher,
        boolean enabled
    ) {
        install(store, store, mappingStore, statsAggregator, fieldRefresher, enabled);
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
        AbsentIndexDescriptorSuppliers.register(DescriptorGate::supplyExceptForTheStoreItself);
        AbsentIndexDescriptorSuppliers.registerPager(pagerFor(prefixBackend));
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
            if (recordsItself(descriptor)) {
                if (descriptor.exists() == false) {
                    // The store's own index has just been deleted, and the store is the last thing to find
                    // out. Both bootstrap latches are one-way, so without this the next write skips creation,
                    // is auto-created instead, and comes back with dynamic mappings: name becomes a text
                    // field, and every prefix query then fails with "Text fields are not optimised for
                    // operations that require per-document field data". That state is unrecoverable rather
                    // than merely broken -- a put-mapping cannot change a field's type, so repairing it needs
                    // the index dropped and rebuilt.
                    //
                    // Told through the publisher because it already observes every index leaving cluster
                    // state, including this one. The alternative was giving the store a ClusterService to
                    // consult on each write, which is a dependency and a per-write check for a fact that
                    // arrives here as an event.
                    forgetStoreIndex(store);
                    forgetStoreIndex(prefixBackend);
                }
                return;
            }
            // Which name is being written and whether it is a tombstone. Descriptor writes are asynchronous,
            // so they are hard to attribute after the fact: a write that lands after a test has finished
            // shows up only as the descriptor index reappearing, with nothing saying what wrote to it.
            logger.debug("publishing descriptor for [{}], exists [{}]", descriptor.name(), descriptor.exists());
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
                        logger.warn("could not record [{}] in the prefix half; wildcards will not match it", descriptor.name(), failure);
                    }
                    return won;
                });
            });
        });
        // Mappings, which H4c required to leave cluster state and which have had no backing store since
        // H6a proved the swap converges. Blocking is safe here, unlike the publish hook above: a mapping
        // update runs on a transport thread handling a put-mapping or a dynamic field inference, not on
        // the cluster state thread where W4 found that a blocking write deadlocks.
        MappingGenerationStore.register(mappingStore);
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
        logger.info("descriptor resolution installed against [{}]", DescriptorStore.DESCRIPTOR_INDEX);
    }

    /**
     * Whether this descriptor is about the store that would hold it.
     *
     * <p>The publisher fires for every index that enters or leaves cluster state, and the descriptor system
     * index is one of those, so without this the store records its own lifecycle into itself. That is
     * circular on its face, and it has a concrete consequence: deleting the descriptor index issues a write
     * to the descriptor index, and auto-creation puts it straight back. The index cannot be dropped, and a
     * delete reports success while leaving a live index behind.
     *
     * <p>Found from a shard lock left held at test teardown. The wipe deleted the store, the in-flight write
     * about that deletion failed with "no such index", auto-creation recreated it under a new uuid, and the
     * recreated shards were open and locked when the assertion ran. The lock was three steps downstream of
     * the actual fault, which is why this is worth a name rather than a condition.
     *
     * <p>Deliberately narrow. Other system indices still get descriptors, because the dual write H2b
     * introduced is what lets the two representations be compared during migration, and only this one index
     * is its own subject.
     */
    private static boolean recordsItself(org.opensearch.cluster.metadata.IndexDescriptor descriptor) {
        return DescriptorStore.DESCRIPTOR_INDEX.equals(descriptor.name());
    }

    /**
     * Makes a backend re-bootstrap its index, for whichever half is backed by one.
     *
     * <p>Reaches through {@link CompositeDescriptorBackend} because either half may be the system index and
     * only that half has anything to forget. A blob-backed half has no index to bootstrap and is skipped.
     */
    private static void forgetStoreIndex(Object backend) {
        if (backend instanceof CompositeDescriptorBackend composite) {
            forgetStoreIndex(composite.prefixBackend());
            return;
        }
        if (backend instanceof DescriptorStore indexBacked) {
            indexBacked.forgetIndex();
        }
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

    /** Clears both registrations, which a node shutting down must do. */
    /**
     * The change feed, which had nowhere to be called from until the publisher above called it.
     *
     * <p>Both halves were built and neither was reachable: {@code BlobDescriptorChangeLog} had no
     * appender and {@code NameIndexService.apply} had no caller. A feed nothing writes and an index
     * nothing feeds look exactly like a quiet cluster, which is why this needed wiring rather than more
     * building.
     *
     * <p>Optional on purpose. A deployment with no object store configured still publishes descriptors,
     * and a null feed simply means nothing is recorded, which is what the setter's absence already meant.
     */
    private static final AtomicReference<BlobDescriptorChangeLog> CHANGE_LOG = new AtomicReference<>();
    private static final AtomicReference<NameIndexService> NAME_INDEX = new AtomicReference<>();

    /** Installs the change feed. Passing nulls clears it, matching every other registration here. */
    public static void setChangeFeed(BlobDescriptorChangeLog changeLog, NameIndexService nameIndexService) {
        CHANGE_LOG.set(changeLog);
        NAME_INDEX.set(nameIndexService);
    }

    /**
     * Records one descriptor write to the log and applies it to this node's name index.
     *
     * <p>The local apply is immediate rather than tailed, because this node already knows what it just
     * wrote and waiting to read it back would make its own index the last to know. The log is what carries
     * the same change to every other node, and the tailer that consumes it on those nodes is the piece
     * still missing: today a remote node learns of the change only through a rebuild. That is a real gap
     * and it is stated rather than papered over, because a feed that is half-wired reads as a working one.
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
            NameIndexService nameIndexService = NAME_INDEX.get();
            if (nameIndexService != null) {
                nameIndexService.apply(java.util.List.of(change));
            }
        } catch (RuntimeException e) {
            logger.warn("could not record the descriptor change for [{}]; the feed will need a rebuild", descriptor.name(), e);
        }
    }

    public static void uninstall() {
        // Cleared in the reverse order, so the supplier is gone before the store it reads.
        AbsentIndexDescriptorSuppliers.register(null);
        AbsentIndexDescriptorSuppliers.registerPager(null);
        AbsentIndexDescriptorSuppliers.registerExpander(null);
        DescriptorPrefetch.register(null);
        setChangeFeed(null, null);
        // Reset rather than leave, since the registries are static and a limit set by one test would
        // otherwise decide the behaviour of every suite that ran after it in the same JVM.
        WILDCARD_EXPANSION_LIMIT.set(DEFAULT_WILDCARD_EXPANSION_LIMIT);
        IndexDescriptorPublisher.register(null);
        IndexDescriptorPublisher.registerCreator(null);
        MappingGenerationStore.register(null);
        GatedMappingStatsAggregator.register(null);
        DescriptorOnlyCreation.register(null);
        UnknownFieldRefresh.register(null);
        STORE.set(null);
    }

    /**
     * A pager over the descriptor index.
     *
     * <p>The {@code size} argument is the point of the interface rather than a convenience: H16 measured
     * that pagination sorted the whole population to make one page, so a pager that fetched everything and
     * then trimmed would satisfy the type and reintroduce the cost. This asks the index for exactly the page.
     *
     * <p>It is a method reference rather than a lambda because every argument now goes through unchanged.
     * The lambda that used to be here dropped {@code afterCreationDate} and served descending by reversing
     * an ascending page, and T27 measured what that cost: a descending walk returned the same names as an
     * ascending one, and pages were selected in name order while the caller merged them in creation-date
     * order. A pager that quietly reinterprets its arguments is worse than one that cannot express them.
     */
    private static AbsentIndexDescriptorSuppliers.DescriptorPager pagerFor(DescriptorPrefixBackend store) {
        // Names and creation dates only. T5 measured that decoding the full descriptor for every hit is a
        // quarter to a third of what a page costs, and pagination reads exactly these two fields.
        return store::findNamesForPage;
    }
}
