/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero;

import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Which shards of a gated index are asleep.
 *
 * <p>An ordinary index records suspension by rewriting its {@code IndexMetadata} and publishing, and pays
 * a cluster state update per suspension for it. A gated index has no {@code IndexMetadata} to rewrite, and
 * at the index counts this area exists for it could not afford the publication anyway: scale-to-zero
 * suspends shards continuously, so a cluster with a hundred million indices would be publishing constantly
 * and doing nothing else.
 *
 * <p>So the record is kept here instead, off cluster state entirely, and read by placement rather than by
 * the allocator. H9c is why placement rather than the allocator: a computed index never reaches the
 * allocator, so a decider refusing the shard would never run.
 *
 * <p><b>Bounded, and the first attempt at bounding it was backwards.</b> H9d argued that holding only the
 * shards currently asleep was enough, since that is fewer than the index count. Under scale-to-zero it is
 * not fewer: the steady state is that <em>most</em> indices are asleep, so a map of every sleeping shard
 * approaches one entry per index, which is precisely the residency ceiling Area H exists to remove. The
 * reasoning was inverted by the very property that makes the feature worth having.
 *
 * <p>So it is a fixed-capacity cache holding the active working set rather than the whole sleeping
 * population.
 *
 * <p><b>Deliberately volatile, which H15 settled by measurement rather than by preference.</b> A durable
 * record on the descriptor was designed and then removed. The question was whether a restart would
 * materialize every sleeping shard before the first tick could suspend them, which at a hundred million
 * indices decides the whole design. H15 measured that computed placement opens no engine before a request
 * addresses the shard, so a restart rebuilds a large routing view and nothing else: the shards stay cold
 * until something asks for them, and the first idle tick puts them formally back to sleep. A suspension is
 * therefore a derived decision that scale-to-zero recomputes, not a fact only this map holds, and
 * persisting it would have been dead weight in a wire format. An index with nothing asleep is still removed rather than left holding an empty
 * set, which keeps the common case out of the cache entirely.
 *
 * <p><b>Why eviction is safe here, which is not the argument H11 could make.</b> Evicting a mapping costs
 * a refetch, so that cache cannot be wrong. Evicting a suspension does change behaviour: the shard is
 * placed again, and it wakes. That is tolerable for one reason only, and it is the same reason a failing
 * suspension source leaves every shard placed: a shard wrongly awake serves requests, a shard wrongly
 * asleep is an outage. The next scale-to-zero tick observes it idle and suspends it again, so the error
 * is self-correcting and costs one tick of an idle shard being resident.
 *
 * <p><b>Recency is stamped on write, not reordered on read, and that is a correction to H12.</b> H12 made
 * this an access-ordered {@code LinkedHashMap} so a sweep could not evict the actively maintained
 * suspensions, and access ordering means a read mutates the recency list, so every lookup had to take the
 * same monitor. This map is read from {@code AbsentIndexRoutingSuppliers.supply} on every computed routing
 * resolution, so that was one global lock on the path of every search and every write. P1 measured the
 * result: 57.8M reads per second on one thread, falling to 7.9M on sixteen, against 1,084M for a concurrent
 * map. Throughput going backwards with concurrency is a lock convoy.
 *
 * <p>Read ordering was never needed for H12's argument. This map holds only indices that <em>are</em>
 * suspended, so reading an unsuspended one is a miss that inserts nothing: a sweep of reads cannot evict
 * anything. Only {@link #suspend} and {@link #reactivate} mutate it, and the coordinator re-suspends
 * already-suspended shards on every tick, so an actively maintained suspension keeps its stamp fresh
 * without any read needing to touch the structure.
 *
 * <p>Node-local by design. Every node computes the same placement from the same inputs, and suspension is
 * one of those inputs, so it has to reach every node. What makes that safe rather than divergent is that a
 * disagreement is self-correcting in the direction that serves requests: a node that has not yet learned
 * of a suspension places the shard and serves reads from it, which is stale-but-correct, while the node
 * that suspended it stops sending work there. The durable record is on the descriptor, so a restart
 * recovers the truth rather than waking everything.
 *
 * <p><b>What node-local actually costs today, stated rather than implied.</b> Only the elected
 * cluster-manager runs {@code ShardSuspensionCoordinator}, so in practice only that node's registry is
 * ever written. A gated suspension is therefore a decision one node takes about its own placement view: it
 * stops routing work to the shard from requests that node coordinates, and frees nothing anywhere, because
 * the eviction that would unassign copies has no routing entry to act on for a gated index. That is a
 * narrower feature than the name suggests, and it is only tolerable because waking is now cheap and
 * reachable from every path that needs it -- {@link #reactivateAll} on the node holding the record, which
 * {@code ShardReactivationActionFilter} calls locally and {@code TransportReactivateShardsAction} calls on
 * the cluster manager. Before that existed, a gated suspension was permanent: {@code reactivate} had no
 * caller at all, and both reactivation paths resolved indices through {@code state.metadata().index(name)},
 * which a gated index does not have by definition.
 *
 * <p><b>Roles are tracked separately, because merging them broke live indices.</b> Scale-to-zero decides a
 * shard's writer and reader roles on unrelated schedules. With one role-blind set, a reader that had been
 * idle long enough to sleep removed the whole shard from placement -- the writer included -- on an index
 * that was being actively written to. A reader suspension now removes only the shard's search-only copies
 * (see {@code AbsentIndexRoutingSuppliers#registerSuspendedReaderShards}); only a writer suspension takes
 * the shard out of placement entirely.
 */
public final class GatedShardSuspensionRegistry {

    /**
     * How many indices one node tracks suspensions for. A working-set size rather than a correctness
     * parameter: too small wakes idle shards, which the next tick puts back to sleep. It costs residency
     * and a re-suspension, not data, because a suspension is a derived decision that scale-to-zero
     * recomputes rather than a fact only this map knows.
     */
    public static final int DEFAULT_CAPACITY = 50_000;

    /**
     * One index's sleeping shards, per role, and when the entry was last written so eviction can pick the
     * stalest.
     *
     * <p>Both sets in one entry rather than two maps, so the two roles of one index cannot be evicted
     * independently and leave a shard half-asleep in a way no tick reconciles.
     */
    private record Stamped(Set<Integer> writerShards, Set<Integer> readerShards, long stamp) {
        Set<Integer> forRole(boolean reader) {
            return reader ? readerShards : writerShards;
        }

        boolean isEmpty() {
            return writerShards.isEmpty() && readerShards.isEmpty();
        }
    }

    private final ConcurrentHashMap<String, Stamped> suspendedByIndexUuid = new ConcurrentHashMap<>();
    private final AtomicLong evictions = new AtomicLong();
    private final AtomicLong clock = new AtomicLong();
    private final int capacity;

    /**
     * The registry currently installed on this node, so a request path can wake a gated shard without a
     * dependency-injected reference it cannot get.
     *
     * <p>Needed because the two things that have to reach it -- the action filter that runs on whichever
     * node receives a request, and the transport action that runs on the cluster manager -- are both
     * constructed by machinery this registry is not part of. It follows the same last-install-wins,
     * uninstall-on-close discipline as the routing registry this class already installs into, which is the
     * lifetime rule that keeps a closed node's state from answering for whatever runs next in the same JVM.
     */
    private static final java.util.concurrent.atomic.AtomicReference<GatedShardSuspensionRegistry> INSTALLED =
        new java.util.concurrent.atomic.AtomicReference<>();

    /** The installed registry, or null when this node has none -- see {@link #INSTALLED}. */
    public static GatedShardSuspensionRegistry installed() {
        return INSTALLED.get();
    }

    public GatedShardSuspensionRegistry() {
        this(DEFAULT_CAPACITY);
    }

    public GatedShardSuspensionRegistry(int capacity) {
        this.capacity = capacity;
    }

    /** Installs this registry as the source placement consults, for both roles. */
    public void install() {
        AbsentIndexRoutingSuppliers.registerSuspendedShards(this::suspendedShards);
        AbsentIndexRoutingSuppliers.registerSuspendedReaderShards(this::suspendedReaderShards);
        INSTALLED.set(this);
    }

    /**
     * Uninstalls it, which a node shutting down must do.
     *
     * <p>The routing registry is static and outlives any one node, so a registry left installed keeps a
     * closed node's suspensions applying to whatever runs next. In a test JVM that is every subsequent
     * suite; in production it is a node restart inheriting its own stale state.
     */
    public void uninstall() {
        AbsentIndexRoutingSuppliers.registerSuspendedShards(null);
        AbsentIndexRoutingSuppliers.registerSuspendedReaderShards(null);
        INSTALLED.compareAndSet(this, null);
        suspendedByIndexUuid.clear();
    }

    /**
     * The shards of this index that are asleep outright -- writer role, so nothing places them.
     *
     * <p>A plain concurrent get that touches no shared mutable state, which is the point: this runs on
     * every routing resolution, and the previous access-ordered structure made it take a monitor.
     */
    public Set<Integer> suspendedShards(String indexUuid) {
        Stamped stamped = suspendedByIndexUuid.get(indexUuid);
        return stamped == null ? Set.of() : stamped.writerShards();
    }

    /** The shards of this index whose reader copies are asleep while the shard itself stays placed. */
    public Set<Integer> suspendedReaderShards(String indexUuid) {
        Stamped stamped = suspendedByIndexUuid.get(indexUuid);
        return stamped == null ? Set.of() : stamped.readerShards();
    }

    /** Whether this shard's writer role is asleep, which is what takes it out of placement entirely. */
    public boolean isSuspended(String indexUuid, int shardId) {
        return suspendedShards(indexUuid).contains(shardId);
    }

    /** Whether this shard's reader copies are asleep. */
    public boolean isReaderSuspended(String indexUuid, int shardId) {
        return suspendedReaderShards(indexUuid).contains(shardId);
    }

    /** Records a shard's writer role as asleep -- see {@link #suspend(String, int, boolean)}. */
    public boolean suspend(String indexUuid, int shardId) {
        return suspend(indexUuid, shardId, false);
    }

    /**
     * Records a shard's copy of one role as asleep, and reports whether that changed anything.
     *
     * <p>The return value is what lets the caller skip the work that follows a real suspension. Suspension
     * is attempted once per candidate shard on every tick, so in steady state almost every call is a
     * repeat, and treating a repeat as a change would evict an already-evicted shard on every tick.
     *
     * <p>The role is not a label on the record, it decides what suspension <em>means</em> for the shard:
     * see this class's own javadoc on why a reader suspension must not unplace a writer.
     */
    public boolean suspend(String indexUuid, int shardId, boolean reader) {
        boolean[] added = new boolean[1];
        suspendedByIndexUuid.compute(indexUuid, (uuid, current) -> {
            // Restamped even when the shard is already suspended, because the coordinator calls this on
            // every tick for already-suspended shards. That repeat is what keeps an actively maintained
            // suspension fresh now that recency is not refreshed by reads.
            if (current == null) {
                added[0] = true;
                return stamped(reader ? Set.of() : Set.of(shardId), reader ? Set.of(shardId) : Set.of());
            }
            if (current.forRole(reader).contains(shardId)) {
                return stamped(current.writerShards(), current.readerShards());
            }
            added[0] = true;
            Set<Integer> next = new HashSet<>(current.forRole(reader));
            next.add(shardId);
            return reader ? stamped(current.writerShards(), Set.copyOf(next)) : stamped(Set.copyOf(next), current.readerShards());
        });
        evictIfOverCapacity();
        return added[0];
    }

    /** Wakes a shard's writer role -- see {@link #reactivate(String, int, boolean)}. */
    public boolean reactivate(String indexUuid, int shardId) {
        return reactivate(indexUuid, shardId, false);
    }

    /** Wakes one role of one shard, and reports whether it had been asleep. */
    public boolean reactivate(String indexUuid, int shardId, boolean reader) {
        boolean[] removed = new boolean[1];
        suspendedByIndexUuid.compute(indexUuid, (uuid, current) -> {
            if (current == null || current.forRole(reader).contains(shardId) == false) {
                return current;
            }
            removed[0] = true;
            Set<Integer> next = new HashSet<>(current.forRole(reader));
            next.remove(shardId);
            Stamped updated = reader
                ? stamped(current.writerShards(), Set.copyOf(next))
                : stamped(Set.copyOf(next), current.readerShards());
            // Removed rather than left empty, so the map is bounded by shards actually asleep. An entry
            // per index that has ever slept is the residency ceiling in another data structure.
            return updated.isEmpty() ? null : updated;
        });
        return removed[0];
    }

    /**
     * Wakes every sleeping shard of one index in one role, and reports whether anything was asleep.
     *
     * <p>Index-granular rather than shard-granular for the same reason {@code
     * ShardReactivationActionFilter} is: at the point a request is seen, which shard it will touch has not
     * been resolved yet, so the only safe answer is to wake the index. Waking a shard that was already
     * awake costs nothing, and leaving the one shard the request needed asleep costs the request.
     *
     * <p>This is the call that had no production caller at all. Suspension recorded, placement dropped the
     * shard, and nothing anywhere put it back -- so a gated shard that went to sleep stayed asleep until
     * the entry was evicted or the node restarted.
     */
    public boolean reactivateAll(String indexUuid, boolean reader) {
        boolean[] removed = new boolean[1];
        suspendedByIndexUuid.compute(indexUuid, (uuid, current) -> {
            if (current == null || current.forRole(reader).isEmpty()) {
                return current;
            }
            removed[0] = true;
            Stamped updated = reader ? stamped(current.writerShards(), Set.of()) : stamped(Set.of(), current.readerShards());
            return updated.isEmpty() ? null : updated;
        });
        return removed[0];
    }

    private Stamped stamped(Set<Integer> writerShards, Set<Integer> readerShards) {
        return new Stamped(writerShards, readerShards, clock.incrementAndGet());
    }

    /** How many indices this node tracks suspensions for, which is what the capacity bounds. */
    public int trackedIndexCount() {
        return suspendedByIndexUuid.size();
    }

    /**
     * How many indices have been evicted, each of which woke shards that were asleep.
     *
     * <p>Exposed because a node evicting continuously has a capacity below its active working set, and the
     * symptom is shards that will not stay asleep. Without a counter that looks like scale-to-zero being
     * broken rather than being under-provisioned.
     */
    public long evictionCount() {
        return evictions.get();
    }

    /**
     * Drops the stalest entries once the map is over capacity.
     *
     * <p>A batch rather than one at a time, because finding the stalest costs a scan and doing that on
     * every write past the bound would cost more than the lock this change removes. Amortised, the scan
     * runs once per ten percent of capacity written.
     */
    private void evictIfOverCapacity() {
        if (suspendedByIndexUuid.size() <= capacity) {
            return;
        }
        int toEvict = Math.max(1, capacity / 10);
        long[] stamps = suspendedByIndexUuid.values().stream().mapToLong(Stamped::stamp).sorted().toArray();
        if (stamps.length == 0) {
            return;
        }
        // A threshold rather than a sorted eviction list, so this is one pass over the entries.
        long threshold = stamps[Math.min(toEvict, stamps.length - 1)];
        suspendedByIndexUuid.entrySet().removeIf(entry -> {
            if (entry.getValue().stamp() < threshold) {
                evictions.incrementAndGet();
                return true;
            }
            return false;
        });
    }

}
