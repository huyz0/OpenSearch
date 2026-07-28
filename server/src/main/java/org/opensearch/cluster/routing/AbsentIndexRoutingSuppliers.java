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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardNotFoundException;

import java.util.ArrayList;
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
 * <p>Phase A made an index legal in metadata and absent from the routing table, and taught the request
 * paths to degrade rather than throw. That degradation is deliberately pessimistic: no routing entry
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
 * every node from inputs every node already has does not need publishing at all. So a serverless index
 * publishes no routing entry, there is nothing to diff or serialize, and each node fills the gap
 * locally. Phase A is what makes the gap legal, which is why it was a prerequisite rather than merely
 * adjacent work.
 *
 * <p>A single static reference rather than an injected service, following
 * {@code EngineNativeSnapshotReleasers}: the call site is deep inside routing resolution, reached from
 * paths that have no plugin context to thread a dependency through.
 *
 * <p>Unset by default, so behaviour is exactly what Phase A shipped until a plugin opts in.
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
     * index never reaches the allocator: Area C derives its placement instead. So the only way a computed
     * shard can be asleep is for placement not to place it, which means the filter belongs at the point
     * where placement is read.
     *
     * <p>It is applied in {@link #supply} rather than left to each supplier because every routing read for
     * a computed index funnels through there. A supplier that forgets the check would place a sleeping
     * shard and wake it, and that is precisely the class of mistake this registry was built to make
     * impossible: C3 hooked resolution, left the write path open-coded, and the two disagreed.
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
     * The computed entry for an index with none published, or null to fall back to Phase A's
     * no-shard-available behaviour.
     *
     * <p>A supplier that throws is treated as having no answer rather than being allowed to fail the
     * request. This is a degradation path already; turning it into an error would make a plugin bug
     * worse than the absence it was installed to handle.
     */
    /**
     * Placement for a named index, falling back to the descriptor when cluster state has no metadata.
     *
     * <p>P6 measured why this overload exists. Gating removes the cluster state entry, so a gated index
     * reaches placement as a null and {@code ownsIndex} answers false for null, leaving the index with no
     * routing at all and nothing thrown. The name is what makes the descriptor reachable, and the two
     * resolution paths that lose it are the only ones that ever see a null.
     *
     * <p>The metadata is synthesised rather than the seam widened. Passing a descriptor through would
     * change {@code supply}'s signature and the supplier interface behind it, which C3 and every routing
     * caller depend on; synthesising keeps every existing supplier working unchanged and confines the fix
     * to the one place the information was lost.
     */
    public static IndexRoutingTable supply(ClusterState state, String indexName, IndexMetadata indexMetadata) {
        if (indexMetadata != null) {
            return supply(state, indexMetadata);
        }
        IndexMetadata synthesised = synthesisedMetadata(indexName);
        return synthesised == null ? supply(state, (IndexMetadata) null) : supply(state, synthesised);
    }

    /**
     * The metadata synthesised from a gated index's descriptor, cached so it is stable.
     *
     * <p>Stability is the point, not just the saving. P5's memo keys on metadata identity, so synthesising
     * a fresh instance per resolution would make that memo miss every time, which P5 measured as an 18x
     * regression rather than a slow path. Keyed by descriptor identity so a descriptor change invalidates
     * it.
     */
    private static IndexMetadata synthesisedMetadata(String indexName) {
        org.opensearch.cluster.metadata.IndexDescriptor descriptor = org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.supply(
            indexName
        );
        if (descriptor == null || descriptor.exists() == false) {
            return null;
        }
        SynthesisedMetadata cached = SYNTHESISED.get(indexName);
        if (cached != null && cached.descriptor == descriptor) {
            return cached.metadata;
        }
        IndexMetadata metadata = descriptor.toIndexMetadata();
        SYNTHESISED.put(indexName, new SynthesisedMetadata(descriptor, metadata));
        return metadata;
    }

    private record SynthesisedMetadata(org.opensearch.cluster.metadata.IndexDescriptor descriptor, IndexMetadata metadata) {
    }

    private static final java.util.concurrent.ConcurrentHashMap<String, SynthesisedMetadata> SYNTHESISED =
        new java.util.concurrent.ConcurrentHashMap<>();

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
            Memo memo = MEMOS.get(uuid);
            // Identity on the metadata, equality on the suspension set, and the asymmetry is deliberate.
            //
            // IndexMetadata is immutable, large, and replaced on every cluster state change that touches
            // the index, so identity is exactly the invalidation signal and costs one reference compare.
            //
            // The suspension set is small and its source is a registered function this class does not
            // control. Identity there looked equivalent and was not: a source that builds a fresh set per
            // call, which the obvious lambda does, makes the memo miss every time. That is not a slow memo,
            // it is a negative one, because every resolution then pays the rebuild plus a map write. It
            // measured 159,499 ns against 8,728 ns without the memo at all. Equality on a set of a few
            // shard ids costs almost nothing and cannot be defeated by how a caller allocates.
            if (memo != null && memo.metadata == indexMetadata && memo.suspended.equals(suspended)) {
                return memo.result;
            }

            IndexRoutingTable result = withoutSuspendedShards(supplier.apply(state, indexMetadata));
            MEMOS.put(uuid, new Memo(indexMetadata, suspended, result));
            return result;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * One memoised placement per index.
     *
     * <p>P4 measured why. {@code ComputedRoutingTable.build} allocates a fresh table on every resolution
     * and the suspension filter then rebuilds it again when anything is asleep, so a hundred-shard index
     * with one cold shard cost about nine microseconds per request, linear in shard count, on a path taken
     * once per index per request.
     *
     * <p>Bounded by the number of computed indices this node resolves, which is the same bound the routing
     * table itself has, so this cannot grow past what the node already holds. Entries are replaced rather
     * than accumulated: the key is the index uuid and each new cluster state for that index overwrites it.
     *
     * <p>A {@code ConcurrentHashMap} with no access ordering, deliberately. P1 measured an access-ordered
     * synchronized map on this same path at 9M reads per second across sixteen threads against 1,084M for a
     * concurrent one, because access ordering makes a read mutate shared structure. Repeating that here
     * would give back more than the memo saves.
     */
    private static final java.util.concurrent.ConcurrentHashMap<String, Memo> MEMOS = new java.util.concurrent.ConcurrentHashMap<>();

    private record Memo(IndexMetadata metadata, Set<Integer> suspended, IndexRoutingTable result) {
    }

    /** Drops every memoised placement, which a test must do because this registry is static. */
    public static void clearMemos() {
        MEMOS.clear();
        SYNTHESISED.clear();
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
        SYNTHESISED.clear();
    }

    /** The shards of this index that are asleep, empty when nothing is registered or the source fails. */
    public static Set<Integer> suspendedShards(String indexUuid) {
        Function<String, Set<Integer>> source = SUSPENDED_SHARDS.get();
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
     * so a cluster that never scales to zero pays one hash lookup and no allocation.
     */
    private static IndexRoutingTable withoutSuspendedShards(IndexRoutingTable computed) {
        if (computed == null) {
            return null;
        }
        Set<Integer> suspended = suspendedShards(computed.getIndex().getUUID());
        if (suspended.isEmpty()) {
            return computed;
        }
        IndexRoutingTable.Builder awake = IndexRoutingTable.builder(computed.getIndex());
        boolean removedAny = false;
        for (IndexShardRoutingTable shard : computed) {
            if (suspended.contains(shard.shardId().id())) {
                removedAny = true;
                continue;
            }
            awake.addIndexShard(shard);
        }
        return removedAny ? awake.build() : computed;
    }

    /**
     * The routing entry for an index: the published one if there is one, otherwise the computed one, or
     * null if neither exists.
     *
     * <p>Every caller that reads a routing entry and has to handle its absence should go through this
     * rather than pairing {@code routingTable().index(name)} with its own call to {@link #supply}. C3
     * hooked resolution and left the write path unhooked, so a computed index answered searches and
     * failed writes; that divergence was possible because the pairing was open-coded at each site. One
     * function is what makes the next site hard to get wrong.
     *
     * <p>Returns null rather than throwing for an index in neither table, because the callers disagree
     * about what that means: routing treats it as no shard available, health skips it, and creation
     * treats it as already deleted.
     */
    public static IndexRoutingTable resolve(ClusterState state, String indexName) {
        IndexRoutingTable published = state.routingTable().index(indexName);
        if (published != null) {
            return published;
        }
        if (isRegistered() == false) {
            // The metadata lookup is skipped rather than made and discarded. This runs once per index per
            // health call, and the index count this area exists for is in the millions.
            return null;
        }
        return supply(state, indexName, state.metadata().index(indexName));
    }

    /**
     * One shard's routing table, from the published entry or the computed one, or null if neither has
     * it.
     *
     * <p>The shard-level counterpart of {@link #resolve}, and here for the same reason: the request
     * paths that ask this question are the ones that were missed last time. C3 hooked search and left
     * the write path reading the table directly, and after that was fixed in {@code OperationRouting}
     * the replication path was still reading it directly somewhere else. Each of those was one open-coded
     * lookup that nobody thought of as a routing decision.
     *
     * <p>Delegates to {@link RoutingTable#shardRoutingTableOrNull} for the published case rather than
     * reimplementing it, because the two absences it distinguishes are not the same absence. An index
     * with no entry is a maybe-computed index and returns null; an index that <em>has</em> an entry
     * without this shard is a caller asking for a shard that does not exist, and that must keep throwing
     * {@link ShardNotFoundException}. Collapsing the two turned a hard error into a retry loop, which is
     * what {@code TransportReplicationActionTests.testUnknownIndexOrShardOnReroute} noticed.
     */
    public static IndexShardRoutingTable resolveShard(ClusterState state, ShardId shardId) {
        IndexShardRoutingTable published = state.routingTable().shardRoutingTableOrNull(shardId);
        if (published != null) {
            return published;
        }
        IndexRoutingTable computed = supplyIfRegistered(state, shardId.getIndex().getName());
        return computed == null ? null : computed.shard(shardId.id());
    }

    /**
     * Every shard of the named indices, resolving each index rather than looking it up.
     *
     * <p>The bulk counterpart of {@link #resolve}, and the reason it lives here rather than on
     * {@link RoutingTable} is a constraint rather than a preference. Those accessors receive a routing
     * table and nothing else, while supplying an entry needs the {@link ClusterState} and the index
     * metadata; and the no-argument forms take their index list from the routing table's own key set,
     * which a computed index is not in. So a computed index cannot be resolved there, and cannot even be
     * named there.
     *
     * <p>What this replaces failed by succeeding. Stats reported an index with no shards, segments
     * reported no segments, recovery reported nothing recovering, and a force merge reported success
     * having merged nothing. None of them threw, so the caller had no way to tell an unreachable index
     * from an empty one, which is the same signature as the refresh in C21 and the field mappings in C22.
     *
     * <p>Missing indices are skipped rather than raising, matching
     * {@link RoutingTable#allShardsSatisfyingPredicate} exactly, because these callers already depend on
     * that for indices that disappear mid-request.
     *
     * @param includeRelocationTargets whether to add the target of a relocating shard, as recovery needs
     */
    public static ShardsIterator allShards(
        ClusterState state,
        String[] concreteIndices,
        Predicate<ShardRouting> predicate,
        boolean includeRelocationTargets
    ) {
        if (isRegistered() == false) {
            // Untouched behaviour when nothing is installed, delegating to the exact method each caller
            // used to call rather than to an equivalent one. The distinction is not pedantry: routing
            // through allShardsSatisfyingPredicate where the caller used allShards is behaviourally
            // identical and still wrong, because callers and tests bind to the method rather than to the
            // behaviour. TransportRemoteStoreStatsActionTests stubs allShards(String[]) on a spy, and the
            // equivalent-but-different call slipped straight past it.
            if (includeRelocationTargets) {
                return state.routingTable().allShardsIncludingRelocationTargets(concreteIndices);
            }
            if (predicate == ALL_SHARDS) {
                return state.routingTable().allShards(concreteIndices);
            }
            return state.routingTable().allShardsSatisfyingPredicate(concreteIndices, predicate);
        }
        // A list rather than a set, because these callers rely on shard identity being preserved.
        List<ShardRouting> shards = new ArrayList<>();
        for (String index : concreteIndices) {
            IndexRoutingTable indexRoutingTable = resolve(state, index);
            if (indexRoutingTable == null) {
                continue;
            }
            for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : shardRoutingTable) {
                    if (predicate.test(shardRouting) == false) {
                        continue;
                    }
                    shards.add(shardRouting);
                    if (includeRelocationTargets && shardRouting.relocating()) {
                        shards.add(shardRouting.getTargetRelocatingShard());
                    }
                }
            }
        }
        return new PlainShardsIterator(shards);
    }

    /**
     * The "no filter" predicate, held as a constant so the fast path can recognise it by identity and
     * delegate to {@link RoutingTable#allShards(String[])} itself rather than to an equivalent method.
     */
    private static final Predicate<ShardRouting> ALL_SHARDS = shardRouting -> true;

    /**
     * Every shard in the cluster, including those of indices that publish no routing entry.
     *
     * <p>For the callers that name no indices at all, {@code _cat/shards} and {@code _cat/allocation}.
     * They cannot use the array form because they have no list to pass, and
     * {@link RoutingTable#allShards()} takes its list from the routing table's own key set, which a
     * computed index is not in. So the index list has to come from metadata, which is the one place every
     * index appears whether its routing is published or not.
     *
     * <p><b>Why enumerating metadata is acceptable here specifically.</b> This area exists to avoid
     * walking millions of indices, so the cost deserves an argument rather than a shrug. These callers
     * already produce one row per shard, so they are inherently linear in the number of shards, and the
     * index count is bounded by the shard count. The walk adds no asymptotic cost to a caller that is
     * already paying more. It would not be acceptable on a request path, which is why this is separate
     * from {@link #resolve} rather than folded into it.
     *
     * <p>The paginated {@code _cat/shards} path already enumerates metadata for its own ordering, which
     * is both precedent and a warning: it did that and then looked routing up unguarded, so a computed
     * index was a NullPointerException there rather than a missing row.
     */
    public static List<ShardRouting> allShards(ClusterState state) {
        if (isRegistered() == false) {
            // Identical to what the caller used to do, by calling exactly that method.
            return state.routingTable().allShards();
        }
        List<ShardRouting> shards = new ArrayList<>(state.routingTable().allShards());
        for (IndexMetadata indexMetadata : state.metadata()) {
            if (shouldPublishRouting(indexMetadata)) {
                // Published, so allShards() above already returned it.
                continue;
            }
            IndexRoutingTable computed = supply(state, indexMetadata);
            if (computed == null) {
                continue;
            }
            for (IndexShardRoutingTable shardRoutingTable : computed) {
                for (ShardRouting shardRouting : shardRoutingTable) {
                    shards.add(shardRouting);
                }
            }
        }
        return shards;
    }

    /** Every shard of the named indices, the common case. */
    public static ShardsIterator allShards(ClusterState state, String[] concreteIndices) {
        return allShards(state, concreteIndices, ALL_SHARDS, false);
    }

    /** Every shard of the named indices, plus the targets of any that are relocating. */
    public static ShardsIterator allShardsIncludingRelocationTargets(ClusterState state, String[] concreteIndices) {
        return allShards(state, concreteIndices, ALL_SHARDS, true);
    }

    /** The computed entry for an index, skipping the metadata lookup when no supplier is installed. */
    private static IndexRoutingTable supplyIfRegistered(ClusterState state, String indexName) {
        if (isRegistered() == false) {
            return null;
        }
        return supply(state, indexName, state.metadata().index(indexName));
    }

    /**
     * Computed shards assigned to one node, for indices with no published routing entry.
     *
     * <p>Separate from {@link #register} because it answers the inverse question. A supplier is asked
     * "where does index X live", which is what a coordinator needs; a data node needs "which shards live
     * here", and deriving that from the supplier means enumerating every index in the cluster on every
     * applied cluster state. At the index counts this area exists for that enumeration is the cost the
     * area was built to avoid, so the inverse is a registration of its own and the plugin decides how to
     * answer it.
     */
    private static final AtomicReference<BiFunction<ClusterState, String, List<ShardRouting>>> LOCAL_SHARDS = new AtomicReference<>();

    /** Installs the inverse lookup. Registering null clears it. */
    public static void registerLocalShards(BiFunction<ClusterState, String, List<ShardRouting>> localShards) {
        LOCAL_SHARDS.set(localShards);
    }

    /**
     * The computed shards this node should host, or empty when nothing is installed.
     *
     * <p>Empty rather than null, and a throwing implementation reads as empty, for the same reason
     * {@link #supply} declines rather than fails: a plugin bug must not stop a node from applying
     * cluster state.
     */
    public static List<ShardRouting> localShards(ClusterState state, String nodeId) {
        BiFunction<ClusterState, String, List<ShardRouting>> localShards = LOCAL_SHARDS.get();
        if (localShards == null) {
            return List.of();
        }
        try {
            List<ShardRouting> shards = localShards.apply(state, nodeId);
            return shards == null ? List.of() : shards;
        } catch (Exception e) {
            return List.of();
        }
    }

    /** Whether an inverse lookup is installed. Lets a caller skip work it would otherwise discard. */
    public static boolean hasLocalShards() {
        return LOCAL_SHARDS.get() != null;
    }

    /** Whether a supplier is installed. Exposed so callers can skip work they would otherwise discard. */
    public static boolean isRegistered() {
        return SUPPLIER.get() != null;
    }
}
