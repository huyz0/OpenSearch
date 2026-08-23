/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Node-level hook for supplying a routing entry for an index that has none published.
 *
 * <p>An earlier change made an index legal in metadata and absent from the routing table, and taught the
 * request paths to degrade rather than throw. That degradation is deliberately pessimistic: no routing entry
 * means no shard available. This hook lets a plugin answer the same question differently, by
 * <em>computing</em> the entry instead of reporting its absence.
 *
 * <p><b>Why a hook rather than a lazy routing table.</b> The obvious way to make routing derived is to
 * change {@link RoutingTable}'s internal map to hold suppliers, mirroring what was done for
 * {@code Metadata}. That map has around forty internal references, and among them the diff, the
 * serializer and the verifiable serializer. {@code Metadata}'s holders stayed contained because they
 * resolve before serialization; {@link RoutingTable} is diffed and serialized on <em>every</em> cluster
 * state publication, so a lazy entry there has to define what a diff of an unresolved entry means, and
 * getting that wrong means either no saving or two nodes reading one state differently.
 *
 * <p>The question dissolves once you notice it does not need asking. An entry computed identically on
 * every node from inputs every node already has does not need publishing at all. So a computed-placement
 * index publishes no routing entry, there is nothing to diff or serialize, and each node fills the gap
 * locally. The earlier absent-routing change is what makes the gap legal, which is why it was a
 * prerequisite rather than merely adjacent work.
 *
 * <p>A single static reference rather than an injected service, following
 * {@code EngineNativeSnapshotReleasers}: the call site is deep inside routing resolution, reached from
 * paths that have no plugin context to thread a dependency through.
 *
 * <p>Unset by default, so behaviour is exactly the pessimistic no-shard-available default until a plugin
 * opts in.
 *
 * <p><b>Settled architecture: one authority per plane.</b> This static registry is the node-level
 * <em>authority</em> for computed routing -- the single place a plugin's answers live, and the seam the
 * few core internals that ask node-level questions consult directly. The {@code ClusterPlugin} SPI
 * ({@link org.opensearch.cluster.metadata.IndexCatalog}, implemented for this registry by {@link
 * org.opensearch.cluster.metadata.SupplierBackedIndexCatalog}) is the registration <em>front door</em>:
 * how a plugin installs into this authority, not a second authority. The catalog registered on the node
 * is the <em>read path</em> ({@code ClusterState#getIndexRoutingTable},
 * {@code ClusterState#resolveShard}, {@code ClusterState#allShards}, {@code
 * RoutingTable#shouldPublishRouting}), and it answers by forwarding here. Its metadata-plane counterpart,
 * {@code org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers}, follows exactly the same shape.
 *
 * <p><b>What is no longer a direct static call site.</b> Four core call sites used to consult {@link
 * #isRegistered()} here because they needed "is the feature currently active on this node" and the
 * resolver SPI of the day, being attached per cluster state, could not model it. {@link
 * org.opensearch.cluster.metadata.IndexCatalog#isActive()} now does, and all four have migrated onto it --
 * {@code SupplierBackedIndexCatalog#isActive} forwards straight back here, so the answers are unchanged.
 */
public final class AbsentIndexRoutingSuppliers {

    private static final AtomicReference<BiFunction<ClusterState, IndexMetadata, IndexRoutingTable>> SUPPLIER = new AtomicReference<>();

    /**
     * Indices whose routing must <em>not</em> be published, because it will be supplied instead.
     *
     * <p>Separate from the supplier, and the separation is the whole point. A supplier only ever runs
     * when an index has no published routing entry, and index creation publishes one for every index it
     * creates. Without this predicate the supplier is installed and never invoked -- the mechanism looks
     * wired and is dead. That was missed on the first pass through this area and found only by asking
     * what an integration test would actually exercise.
     */
    private static final AtomicReference<Predicate<IndexMetadata>> UNPUBLISHED = new AtomicReference<>();

    /**
     * Notified when a supplier is installed. Copy-on-write because installation is rare and the list is
     * read from whichever thread happens to register.
     */
    private static final List<Runnable> REGISTRATION_LISTENERS = new CopyOnWriteArrayList<>();

    /**
     * Which shards of an index are asleep, by index uuid. Registering null clears it, and with nothing
     * registered no shard is suspended, so an unconfigured cluster behaves exactly as before.
     *
     * <p>Suspension has to be expressed here rather than by a decider. {@code
     * SuspendedShardAllocationDecider} works by telling the allocator to refuse a shard, and a computed
     * index never reaches the allocator: this registry derives its placement instead. So the only way a computed
     * shard can be asleep is for placement not to place it, which means the filter belongs at the point
     * where placement is read.
     *
     * <p>It is applied in {@link #supply} rather than left to each supplier because every routing read for
     * a computed index funnels through there. A supplier that forgets the check would place a sleeping
     * shard and wake it, and that is precisely the class of mistake this registry was built to make
     * impossible: an earlier pass hooked resolution, left the write path open-coded, and the two disagreed.
     *
     * <p>Keyed by uuid rather than by name for two reasons: it is what scale-to-zero already carries, so
     * no lookup is needed to record a suspension, and it does not survive a delete-and-recreate, so a new
     * index reusing a name cannot inherit the old one's sleeping shards. Holding only the shards that
     * <em>are</em> asleep keeps the hot-path read a hash lookup against a small map rather than a
     * descriptor fetch. The durable copy lives on {@link
     * org.opensearch.cluster.metadata.IndexDescriptor}, which is what survives a restart; this is the
     * cached view placement reads on every request.
     */
    private static final AtomicReference<Function<String, Set<Integer>>> SUSPENDED_SHARDS = new AtomicReference<>();

    /**
     * Which shards of an index have only their <em>reader</em> copies asleep, by index uuid.
     *
     * <p>Separate from {@link #SUSPENDED_SHARDS} because the two mean different things and merging them
     * broke shards. A shard in {@link #SUSPENDED_SHARDS} is not placed at all; a shard here keeps its
     * writer copy and loses only its search-only copies. Scale-to-zero suspends the two roles on
     * unrelated schedules, so a reader that had been idle long enough to sleep was, with one role-blind
     * set, unplacing the writer of an index being actively written to -- an outage produced by an
     * optimisation, on an index nobody had stopped using.
     *
     * <p>Unset by default, so a registry that only knows about whole-shard suspension behaves exactly as
     * it did before this existed.
     */
    private static final AtomicReference<Function<String, Set<Integer>>> SUSPENDED_READER_SHARDS = new AtomicReference<>();

    private AbsentIndexRoutingSuppliers() {}

    /**
     * Declares which indices skip routing publication. Registering null clears it.
     *
     * <p>Deliberately not derived from the supplier. A supplier that declines still leaves the index
     * with published routing, which is correct; an index that skips publication and has no supplier
     * would have no routing at all, which is not. Keeping them separate makes the second case a
     * configuration error rather than a silent outage.
     */
    public static void registerUnpublished(Predicate<IndexMetadata> unpublished) {
        UNPUBLISHED.set(unpublished);
    }

    /**
     * Whether an index should have its routing entry published at creation.
     *
     * <p>Defaults to true, so an unconfigured cluster behaves exactly as it always has. A predicate that
     * throws is treated as "publish", because publishing routing that is then ignored is recoverable and
     * not publishing routing that is then needed is not.
     */
    public static boolean shouldPublishRouting(IndexMetadata indexMetadata) {
        Predicate<IndexMetadata> unpublished = UNPUBLISHED.get();
        if (unpublished == null || indexMetadata == null) {
            return true;
        }
        try {
            return unpublished.test(indexMetadata) == false;
        } catch (Exception e) {
            return true;
        }
    }

    /**
     * Installs the supplier. Last registration wins, and a null clears it, which is what lets a test
     * restore the default rather than leaking a supplier into unrelated cases.
     */
    public static void register(BiFunction<ClusterState, IndexMetadata, IndexRoutingTable> supplier) {
        SUPPLIER.set(supplier);
        // Any change to either input invalidates every memoised placement.
        MEMOS.clear();
        if (supplier != null) {
            for (Runnable listener : REGISTRATION_LISTENERS) {
                try {
                    listener.run();
                } catch (Exception e) {
                    // A listener that fails must not prevent registration, for the same reason a supplier
                    // that throws is treated as declining: this is installation, not a request path.
                }
            }
        }
    }

    /**
     * Runs when a supplier is installed.
     *
     * <p>Exists because anything that reacts to placement being enabled would otherwise have to wait for
     * an unrelated cluster state change to notice. The membership maintainer hit exactly that: it
     * publishes on cluster state changes, so a cluster that installed a supplier and then went idle kept
     * computing placement against the live node list, which is the input this area exists to stop using.
     *
     * <p>Listeners are not removed, matching the registry's own lifetime, and a listener that throws is
     * swallowed rather than allowed to break installation.
     */
    public static void addRegistrationListener(Runnable listener) {
        REGISTRATION_LISTENERS.add(listener);
    }

    /**
     * Stops notifying a listener.
     *
     * <p>Needed because a listener outlives the node that added it otherwise. This registry is static, so
     * in a test JVM every node of every suite would accumulate here, each holding its {@code
     * ClusterService} alive and each running against a cluster that has already closed.
     */
    public static void removeRegistrationListener(Runnable listener) {
        REGISTRATION_LISTENERS.remove(listener);
    }

    /**
     * The computed entry for an index with none published, or null to fall back to the default
     * no-shard-available behaviour.
     *
     * <p>A supplier that throws is treated as having no answer rather than being allowed to fail the
     * request. This is a degradation path already; turning it into an error would make a plugin bug
     * worse than the absence it was installed to handle.
     */
    /**
     * Placement for a named index, falling back to the descriptor when cluster state has no metadata.
     *
     * <p>Measurement showed why this overload exists. Gating removes the cluster state entry, so a gated
     * index reaches placement as a null and {@code ownsIndex} answers false for null, leaving the index with
     * no routing at all and nothing thrown. The name is what makes the descriptor reachable, and the two
     * resolution paths that lose it are the only ones that ever see a null.
     *
     * <p>The metadata is synthesised rather than the seam widened. Passing a descriptor through would
     * change {@code supply}'s signature and the supplier interface behind it, which the resolution hook and
     * every routing caller depend on; synthesising keeps every existing supplier working unchanged and
     * confines the fix to the one place the information was lost.
     */
    public static IndexRoutingTable supply(ClusterState state, String indexName, IndexMetadata indexMetadata) {
        if (indexMetadata != null) {
            return supply(state, indexMetadata);
        }
        IndexMetadata synthesised = synthesisedMetadata(indexName);
        return synthesised == null ? supply(state, (IndexMetadata) null) : supply(state, synthesised);
    }

    /**
     * The metadata synthesised from a gated index's descriptor.
     *
     * <p>Delegated rather than kept here, because the write path synthesises the same metadata for the same
     * index on the same request and the memo below keys on identity. Two caches would hand the two paths
     * two equal instances that are not the same object, and this memo would then miss on every write.
     */
    private static IndexMetadata synthesisedMetadata(String indexName) {
        return org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.synthesisedMetadata(indexName);
    }

    public static IndexRoutingTable supply(ClusterState state, IndexMetadata indexMetadata) {
        BiFunction<ClusterState, IndexMetadata, IndexRoutingTable> supplier = SUPPLIER.get();
        if (supplier == null) {
            return null;
        }
        try {
            if (indexMetadata == null) {
                // No metadata means no memo key and, for every supplier that exists, no table either. Kept
                // ahead of the cache so the null case stays a single call rather than a map lookup first.
                return withoutSuspendedShards(supplier.apply(state, indexMetadata));
            }

            String uuid = indexMetadata.getIndexUUID();
            Set<Integer> suspended = suspendedShards(uuid);
            Set<Integer> suspendedReaders = suspendedReaderShards(uuid);
            Metadata metadata = state == null ? null : state.metadata();
            DiscoveryNodes nodes = state == null ? null : state.nodes();
            Memo memo = MEMOS.get(uuid);
            // Identity on the metadata, equality on the suspension sets, and the asymmetry is deliberate.
            //
            // IndexMetadata is immutable, large, and replaced on every cluster state change that touches
            // the index, so identity is exactly the invalidation signal and costs one reference compare.
            //
            // The suspension sets are small and their source is a registered function this class does not
            // control. Identity there looked equivalent and was not: a source that builds a fresh set per
            // call, which the obvious lambda does, makes the memo miss every time. That is not a slow memo,
            // it is a negative one, because every resolution then pays the rebuild plus a map write. It
            // measured 159,499 ns against 8,728 ns without the memo at all. Equality on a set of a few
            // shard ids costs almost nothing and cannot be defeated by how a caller allocates.
            //
            // The state's own metadata and node list are part of the key, and leaving them out was a bug
            // that no amount of local reasoning about this class would have found. A computed placement is
            // a function of more than the index: the plugin that computes it reads the cluster's member
            // list, which lives in a Metadata.Custom, and falls back to the live DiscoveryNodes while no
            // member list is published. Neither of those touches the index's own IndexMetadata -- and a
            // gated index's synthesised metadata is deliberately identity-cached, so it does not change at
            // all -- so a node that joined, or one that was decommissioned, left every memo on this node
            // answering from the member list of an earlier epoch, indefinitely. Two nodes then computed
            // different placement for the same shard from the same published state, which is precisely the
            // split view computed placement exists to make impossible: a coordinator derives one allocation
            // id, the data node derives another, and the write fails with "expected allocation id [x] but
            // found [y]".
            //
            // Keyed by identity rather than by version or epoch on purpose. An epoch has to be published by
            // whoever changes it and read by whoever caches it, and a missed publication is a memo that is
            // silently stale; two reference compares against the very state the answer was computed from
            // cannot be missed, because there is nothing to remember to do. It costs one recomputation per
            // index per metadata or node-list change, which is once per cluster state that changes either,
            // not once per request.
            if (memo != null
                && memo.indexMetadata == indexMetadata
                && memo.computedFrom(metadata, nodes)
                && memo.suspended.equals(suspended)
                && memo.suspendedReaders.equals(suspendedReaders)) {
                return memo.result;
            }

            IndexRoutingTable result = withoutSuspendedShards(supplier.apply(state, indexMetadata));
            MEMOS.put(
                uuid,
                new Memo(
                    new java.lang.ref.WeakReference<>(metadata),
                    new java.lang.ref.WeakReference<>(nodes),
                    indexMetadata,
                    suspended,
                    suspendedReaders,
                    result,
                    MEMO_CLOCK.incrementAndGet()
                )
            );
            evictIfOverCapacity();
            return result;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * One memoised placement per index.
     *
     * <p>Measurement showed why. {@code ComputedRoutingTable.build} allocates a fresh table on every resolution
     * and the suspension filter then rebuilds it again when anything is asleep, so a hundred-shard index
     * with one cold shard cost about nine microseconds per request, linear in shard count, on a path taken
     * once per index per request.
     *
     * <p><b>Bounded by capacity, and the claim it used to make instead was the unbounded quantity itself.</b>
     * This javadoc said the map was "bounded by the number of computed indices this node resolves, which is
     * the same bound the routing table itself has". For a published routing table that is a real bound; here
     * it is not one at all. A computed index has no routing entry to hold it, and the whole reason this area
     * exists is that a node resolves indices it does not hold -- a coordinator that touches a million
     * distinct gated indices over a week had a million synthesised {@link IndexMetadata} instances and a
     * million routing tables pinned here, none of which anything ever removed. Entries being replaced per
     * uuid bounds how often one index appears, not how many indices do.
     *
     * <p>Eviction costs a recomputation of one index's placement and nothing else: the entry is a memo, not
     * a record, so an evicted one is rebuilt from inputs that are still there. That is what makes bounding
     * it free of the argument {@code GatedShardSuspensionRegistry} has to make about evicting a suspension.
     *
     * <p><b>Bounded without an access-ordered cache, deliberately, and {@link org.opensearch.common.cache.Cache}
     * is the tempting wrong answer.</b> That one is an LRU: every {@code get} promotes the entry under a
     * single lock, and this map is read on every computed routing resolution -- every search and every
     * write. Measurement on this exact shape put an access-ordered structure at 9M reads per second across
     * sixteen threads against 1,084M for a concurrent map, throughput going backwards with concurrency,
     * which is a lock convoy. So recency is stamped on write and never reordered on read, and a sweep drops
     * the stalest tenth when the map goes over capacity -- the identical shape, and for the identical
     * reason, that {@code GatedShardSuspensionRegistry} and {@code DescriptorCache} already use.
     */
    private static final int MEMO_CAPACITY = 50_000;

    private static final java.util.concurrent.ConcurrentHashMap<String, Memo> MEMOS = new java.util.concurrent.ConcurrentHashMap<>();

    /** Stamps memos in write order, so eviction can find the stalest without a read ever mutating anything. */
    private static final java.util.concurrent.atomic.AtomicLong MEMO_CLOCK = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Drops the stalest entries once the map is over capacity.
     *
     * <p>A batch rather than one at a time, because finding the stalest costs a scan and doing that on every
     * write past the bound would cost more than the memo saves. Amortised, the scan runs once per ten
     * percent of capacity written. Writes here are already rare relative to reads: one per index per change
     * to that index's inputs, not one per request.
     */
    private static void evictIfOverCapacity() {
        if (MEMOS.size() <= MEMO_CAPACITY) {
            return;
        }
        int toEvict = Math.max(1, MEMO_CAPACITY / 10);
        long[] stamps = MEMOS.values().stream().mapToLong(Memo::stamp).sorted().toArray();
        if (stamps.length == 0) {
            return;
        }
        long threshold = stamps[Math.min(toEvict, stamps.length - 1)];
        MEMOS.entrySet().removeIf(entry -> entry.getValue().stamp() < threshold);
    }

    /** How many indices this node currently holds a memoised placement for, which is what the capacity bounds. */
    public static int memoisedIndexCountForTesting() {
        return MEMOS.size();
    }

    /**
     * One memoised placement, together with every input it was computed from.
     *
     * <p>{@code metadata} and {@code nodes} are the cluster-state inputs a computed placement may read
     * beside the index itself -- see {@link #supply(ClusterState, IndexMetadata)} for why they are in the
     * key.
     *
     * <p><b>Weakly, and that is not caution about a hypothetical.</b> A memo is replaced when its index is
     * resolved again, so an index resolved once and then never again keeps whatever it was computed from
     * until it is evicted. Held strongly, a few thousand such entries would pin a few thousand distinct
     * {@code Metadata} generations, and at the index counts this area exists for one of those is the
     * largest object in the process. Weak references make a stale memo cost a recomputation instead: the
     * identity compare below simply fails once the generation it was computed from is gone, which is
     * exactly what should happen to a memo whose inputs no longer exist. The compare cannot spuriously fail
     * for a memo that is still current, because the caller is holding the very instance being compared.
     */
    private record Memo(java.lang.ref.WeakReference<Metadata> metadata, java.lang.ref.WeakReference<DiscoveryNodes> nodes,
        IndexMetadata indexMetadata, Set<Integer> suspended, Set<Integer> suspendedReaders, IndexRoutingTable result, long stamp) {
        boolean computedFrom(Metadata currentMetadata, DiscoveryNodes currentNodes) {
            return metadata.get() == currentMetadata && nodes.get() == currentNodes;
        }
    }

    /** Drops every memoised placement, which a test must do because this registry is static. */
    public static void clearMemos() {
        MEMOS.clear();
        org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.clearSynthesised();
    }

    /**
     * Installs the source of truth for which shards are asleep. Registering null clears it.
     *
     * <p>A function rather than a map so the caller keeps ownership of the lifetime: this registry is
     * static and outlives any one node, and a map handed over here would be a leak across test suites in
     * the same JVM.
     */
    public static void registerSuspendedShards(Function<String, Set<Integer>> suspendedShards) {
        SUSPENDED_SHARDS.set(suspendedShards);
        // Any change to either input invalidates every memoised placement.
        MEMOS.clear();
        org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.clearSynthesised();
    }

    /**
     * Installs the source of truth for which shards have only their reader copies asleep. Registering null
     * clears it, and with nothing registered no shard's readers are suspended, so a caller that only knows
     * about whole-shard suspension is unaffected.
     *
     * <p>See {@link #SUSPENDED_READER_SHARDS} for why this is a second source rather than a flag on the
     * first one.
     */
    public static void registerSuspendedReaderShards(Function<String, Set<Integer>> suspendedReaderShards) {
        SUSPENDED_READER_SHARDS.set(suspendedReaderShards);
        MEMOS.clear();
        org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.clearSynthesised();
    }

    /** The shards of this index that are asleep, empty when nothing is registered or the source fails. */
    public static Set<Integer> suspendedShards(String indexUuid) {
        return read(SUSPENDED_SHARDS.get(), indexUuid);
    }

    /**
     * The shards of this index whose reader copies are asleep while the shard itself stays placed, empty
     * when nothing is registered or the source fails.
     */
    public static Set<Integer> suspendedReaderShards(String indexUuid) {
        return read(SUSPENDED_READER_SHARDS.get(), indexUuid);
    }

    private static Set<Integer> read(Function<String, Set<Integer>> source, String indexUuid) {
        if (source == null) {
            return Set.of();
        }
        try {
            Set<Integer> suspended = source.apply(indexUuid);
            return suspended == null ? Set.of() : suspended;
        } catch (Exception e) {
            // Treated as nothing suspended, matching how a throwing supplier is treated as no answer. A
            // failing source must not take an index's routing away: a shard wrongly awake is a served
            // request, a shard wrongly asleep is an outage.
            return Set.of();
        }
    }

    /**
     * The same routing entry with sleeping shards removed, or the entry unchanged when none are.
     *
     * <p>Returns the original instance when nothing is suspended, which is the overwhelmingly common case,
     * so a cluster that never scales to zero pays two hash lookups and no allocation.
     *
     * <p><b>Two kinds of asleep, because collapsing them broke live indices.</b> A shard in the whole-shard
     * set is dropped entirely: nothing places it, which is what "this shard is asleep" means for an index
     * whose routing is computed rather than allocated. A shard in the reader set keeps its writer copy and
     * loses only its search-only copies, because scale-to-zero decides the two roles on unrelated
     * schedules: a reader idle long enough to sleep says nothing about whether the index is being written,
     * and treating the two alike unplaced the writer of an actively written index.
     */
    private static IndexRoutingTable withoutSuspendedShards(IndexRoutingTable computed) {
        if (computed == null) {
            return null;
        }
        String uuid = computed.getIndex().getUUID();
        Set<Integer> suspended = suspendedShards(uuid);
        Set<Integer> suspendedReaders = suspendedReaderShards(uuid);
        if (suspended.isEmpty() && suspendedReaders.isEmpty()) {
            return computed;
        }
        IndexRoutingTable.Builder awake = IndexRoutingTable.builder(computed.getIndex());
        boolean changedAny = false;
        for (IndexShardRoutingTable shard : computed) {
            if (suspended.contains(shard.shardId().id())) {
                changedAny = true;
                continue;
            }
            if (suspendedReaders.contains(shard.shardId().id())) {
                IndexShardRoutingTable withoutReaders = withoutSearchOnlyCopies(shard);
                if (withoutReaders != shard) {
                    changedAny = true;
                    // A shard whose every copy was a search-only one is, with those asleep, a shard with
                    // nothing to place: dropped rather than published as an entry serving nothing.
                    if (withoutReaders.size() > 0) {
                        awake.addIndexShard(withoutReaders);
                    }
                    continue;
                }
            }
            awake.addIndexShard(shard);
        }
        return changedAny ? awake.build() : computed;
    }

    /** The same shard entry without its search-only copies, or the same instance when it has none. */
    private static IndexShardRoutingTable withoutSearchOnlyCopies(IndexShardRoutingTable shard) {
        boolean hasSearchOnly = false;
        for (ShardRouting routing : shard) {
            if (routing.isSearchOnly()) {
                hasSearchOnly = true;
                break;
            }
        }
        if (hasSearchOnly == false) {
            return shard;
        }
        IndexShardRoutingTable.Builder awake = new IndexShardRoutingTable.Builder(shard.shardId());
        for (ShardRouting routing : shard) {
            if (routing.isSearchOnly() == false) {
                awake.addShard(routing);
            }
        }
        return awake.build();
    }

    /** Whether a supplier is installed. Exposed so callers can skip work they would otherwise discard. */
    public static boolean isRegistered() {
        return SUPPLIER.get() != null;
    }
}
