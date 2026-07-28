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
 */
public final class GatedShardSuspensionRegistry {

    /**
     * How many indices one node tracks suspensions for. A working-set size rather than a correctness
     * parameter: too small wakes idle shards, which the next tick puts back to sleep. It costs residency
     * and a re-suspension, not data, because a suspension is a derived decision that scale-to-zero
     * recomputes rather than a fact only this map knows.
     */
    public static final int DEFAULT_CAPACITY = 50_000;

    /** A suspended shard set and when it was last written, so eviction can pick the stalest. */
    private record Stamped(Set<Integer> shards, long stamp) {
    }

    private final ConcurrentHashMap<String, Stamped> suspendedByIndexUuid = new ConcurrentHashMap<>();
    private final AtomicLong evictions = new AtomicLong();
    private final AtomicLong clock = new AtomicLong();
    private final int capacity;

    public GatedShardSuspensionRegistry() {
        this(DEFAULT_CAPACITY);
    }

    public GatedShardSuspensionRegistry(int capacity) {
        this.capacity = capacity;
    }

    /** Installs this registry as the source placement consults. */
    public void install() {
        AbsentIndexRoutingSuppliers.registerSuspendedShards(this::suspendedShards);
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
        suspendedByIndexUuid.clear();
    }

    /**
     * The shards of this index that are asleep.
     *
     * <p>A plain concurrent get that touches no shared mutable state, which is the point: this runs on
     * every routing resolution, and the previous access-ordered structure made it take a monitor.
     */
    public Set<Integer> suspendedShards(String indexUuid) {
        Stamped stamped = suspendedByIndexUuid.get(indexUuid);
        return stamped == null ? Set.of() : stamped.shards();
    }

    /** Whether this shard is asleep. */
    public boolean isSuspended(String indexUuid, int shardId) {
        return suspendedShards(indexUuid).contains(shardId);
    }

    /**
     * Records a shard as asleep, and reports whether that changed anything.
     *
     * <p>The return value is what lets the caller skip the work that follows a real suspension. Suspension
     * is attempted once per candidate shard on every tick, so in steady state almost every call is a
     * repeat, and treating a repeat as a change would evict an already-evicted shard on every tick.
     */
    public boolean suspend(String indexUuid, int shardId) {
        boolean[] added = new boolean[1];
        suspendedByIndexUuid.compute(indexUuid, (uuid, current) -> {
            // Restamped even when the shard is already suspended, because the coordinator calls this on
            // every tick for already-suspended shards. That repeat is what keeps an actively maintained
            // suspension fresh now that recency is not refreshed by reads.
            if (current != null && current.shards().contains(shardId)) {
                return new Stamped(current.shards(), clock.incrementAndGet());
            }
            added[0] = true;
            if (current == null) {
                return new Stamped(Set.of(shardId), clock.incrementAndGet());
            }
            Set<Integer> next = new HashSet<>(current.shards());
            next.add(shardId);
            return new Stamped(Set.copyOf(next), clock.incrementAndGet());
        });
        evictIfOverCapacity();
        return added[0];
    }

    /** Wakes a shard, and reports whether it had been asleep. */
    public boolean reactivate(String indexUuid, int shardId) {
        boolean[] removed = new boolean[1];
        suspendedByIndexUuid.compute(indexUuid, (uuid, current) -> {
            if (current == null || current.shards().contains(shardId) == false) {
                return current;
            }
            removed[0] = true;
            Set<Integer> next = new HashSet<>(current.shards());
            next.remove(shardId);
            // Removed rather than left empty, so the map is bounded by shards actually asleep. An entry
            // per index that has ever slept is the residency ceiling in another data structure.
            return next.isEmpty() ? null : new Stamped(Set.copyOf(next), clock.incrementAndGet());
        });
        return removed[0];
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
