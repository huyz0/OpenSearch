/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero;

import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
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
 * <p>So it is a fixed-capacity cache over the durable record on {@link
 * org.opensearch.cluster.metadata.IndexDescriptor}, holding the active working set rather than the whole
 * sleeping population. An index with nothing asleep is still removed rather than left holding an empty
 * set, which keeps the common case out of the cache entirely.
 *
 * <p><b>Why eviction is safe here, which is not the argument H11 could make.</b> Evicting a mapping costs
 * a refetch, so that cache cannot be wrong. Evicting a suspension does change behaviour: the shard is
 * placed again, and it wakes. That is tolerable for one reason only, and it is the same reason a failing
 * suspension source leaves every shard placed: a shard wrongly awake serves requests, a shard wrongly
 * asleep is an outage. The next scale-to-zero tick observes it idle and suspends it again, so the error
 * is self-correcting and costs one tick of an idle shard being resident.
 *
 * <p>Access-ordered, so a sweep over cold indices cannot evict the suspensions that are being actively
 * maintained. The same argument as H11, and it applies more strongly here, because the population being
 * swept is the sleeping majority.
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
     * parameter: too small wakes idle shards a tick early and costs residency, it cannot lose data,
     * because the descriptor holds the durable record.
     */
    public static final int DEFAULT_CAPACITY = 50_000;

    private final Map<String, Set<Integer>> suspendedByIndexUuid;

    private final AtomicLong evictions = new AtomicLong();

    public GatedShardSuspensionRegistry() {
        this(DEFAULT_CAPACITY);
    }

    public GatedShardSuspensionRegistry(int capacity) {
        this.suspendedByIndexUuid = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Set<Integer>> eldest) {
                if (size() > capacity) {
                    evictions.incrementAndGet();
                    return true;
                }
                return false;
            }
        });
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

    /** The shards of this index that are asleep. */
    public Set<Integer> suspendedShards(String indexUuid) {
        return suspendedByIndexUuid.getOrDefault(indexUuid, Set.of());
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
            if (current != null && current.contains(shardId)) {
                return current;
            }
            added[0] = true;
            if (current == null) {
                return Set.of(shardId);
            }
            Set<Integer> next = new HashSet<>(current);
            next.add(shardId);
            return Set.copyOf(next);
        });
        return added[0];
    }

    /** Wakes a shard, and reports whether it had been asleep. */
    public boolean reactivate(String indexUuid, int shardId) {
        boolean[] removed = new boolean[1];
        suspendedByIndexUuid.compute(indexUuid, (uuid, current) -> {
            if (current == null || current.contains(shardId) == false) {
                return current;
            }
            removed[0] = true;
            Set<Integer> next = new HashSet<>(current);
            next.remove(shardId);
            // Removed rather than left empty, so the map is bounded by shards actually asleep. An entry
            // per index that has ever slept is the residency ceiling in another data structure.
            return next.isEmpty() ? null : Set.copyOf(next);
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
}
