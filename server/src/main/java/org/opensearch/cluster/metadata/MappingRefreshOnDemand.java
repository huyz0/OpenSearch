/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The read half of H.7: a shard catches up with a mapping change instead of being told about one.
 *
 * <p>Today a mapping update is published to every node, which is why informing a hundred shards of a
 * hundred million indices is impossible. The observation that removes the broadcast is that a shard does
 * not need to be told; it needs to be <em>current at the moment it serves a request</em>. So the
 * coordinator stamps the request with the generation it planned against, and a shard behind that
 * generation reads the mapping object before executing.
 *
 * <p><b>Pull on demand, not push on change.</b> A mapping change costs one compare-and-swap and one
 * descriptor write, independent of how many shards the index has, because nothing fans out. A shard that
 * never serves a request never learns, and never needed to.
 *
 * <p><b>The cost that would make this worse than a broadcast</b> is a refresh on every request. A shard
 * already at or ahead of the stamped generation must do no read at all, which is why
 * {@link #ensureCurrent} compares before it fetches and why the test counts fetches rather than trusting
 * the comparison.
 *
 * <p>Being ahead is normal rather than an error. The shard that inferred the field swapped the mapping
 * itself, so it is at generation N while a coordinator that planned a moment earlier stamps N-1.
 *
 * <p><b>T9: nothing calls this.</b> Every reference outside this file is a test or a comment. H6c's design
 * was for a coordinator to stamp each request with the generation it planned against, and W14 then chose a
 * different trigger: a shard refreshes when it meets a field its mapping does not have, which
 * {@code StoreBackedFieldRefresher} implements and which is what ships. This class is the earlier half of
 * that idea, complete and tested and never wired.
 *
 * <p>That makes it the fourth mechanism in this area found correct and unreachable, after H7a, H8a and the
 * W series, so the note is here rather than in a commit message: a reader arriving at this file should not
 * have to discover by search that it does not run.
 *
 * <p>It is also why the {@code synchronizedMap} below is left alone. It is the same access-ordered
 * structure P1 measured at 57.4M reads per second on one thread falling to 9.1M on sixteen, and T9 replaced
 * the equivalent in {@code StoreBackedFieldRefresher} for that reason. Replacing it here would be optimising
 * a path that never executes, which is the P10 lesson stated as a rule: a change with no measurable effect
 * on a path that does not run is not an improvement.
 *
 * <p><b>The fix is not simply the one T9 applied</b>, and that is worth recording before someone reaches for
 * it. T9 stamps recency on write, which suits a map whose hot entries are written often. Here the hot
 * entries are <em>read</em> often and written only when a mapping generation advances, so write-stamping
 * would let a sweep over cold indices take the freshest stamps and evict the working set, which is exactly
 * what access ordering was chosen to prevent. A read-cheap structure that keeps recency, such as the SIEVE
 * algorithm TiDB uses for the same read-hot and write-rare shape, is the shape of the answer if this is
 * ever wired.
 */
public final class MappingRefreshOnDemand {

    /** Where a shard reads a mapping it has fallen behind on. */
    public interface MappingLoader {
        /** The mapping at or after {@code atLeastGeneration}, or null when it cannot be loaded. */
        MappingGenerationStore.MappingGeneration load(String indexUuid, long atLeastGeneration);
    }

    /**
     * How many indices one node keeps mappings for.
     *
     * <p>A cap rather than no cap is the whole point. This map was unbounded, which meant a node
     * accumulated an entry holding a full field map for every index it had ever served, and that grows
     * with the number of indices <em>touched</em> rather than the number currently active. At the index
     * counts this area exists for, that is the residency ceiling Area H removed from cluster state,
     * rebuilt on the data nodes in a different data structure and holding whole mappings rather than
     * 698 B.
     *
     * <p>The value is a working-set size, not a correctness parameter. Eviction is always safe, because
     * the store is the source of truth and an evicted index is refetched on its next request. Too small
     * costs fetches; it cannot cost correctness.
     */
    public static final int DEFAULT_CAPACITY = 10_000;

    private final MappingLoader loader;
    private final Map<String, MappingGenerationStore.MappingGeneration> local;
    private final AtomicLong fetches = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();

    public MappingRefreshOnDemand(MappingLoader loader) {
        this(loader, DEFAULT_CAPACITY);
    }

    /**
     * Creates a refresher holding at most {@code capacity} indices' mappings.
     *
     * <p>Access-ordered rather than insertion-ordered, so a sweep over cold indices does not evict the hot
     * ones. That distinction is what makes the cap survivable: scale-to-zero means most indices are cold,
     * and an insertion-ordered cache would let a background walk of them flush the working set on every
     * pass, turning a bounded cache into a guaranteed miss.
     *
     * <p>Synchronized rather than concurrent because access-order maintenance is a write on every read, so
     * a {@code ConcurrentHashMap} could not maintain it. The critical sections are map operations, and the
     * loader is deliberately called outside them: holding the lock across a fetch would serialize every
     * shard on the node behind one slow read.
     */
    public MappingRefreshOnDemand(MappingLoader loader, int capacity) {
        this.loader = loader;
        this.local = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, MappingGenerationStore.MappingGeneration> eldest) {
                if (size() > capacity) {
                    evictions.incrementAndGet();
                    return true;
                }
                return false;
            }
        });
    }

    /**
     * Ensures this shard is at least at {@code requiredGeneration} before it serves a request.
     *
     * @return the mapping the shard should execute against, or null when nothing is known and nothing
     *         could be loaded, which the caller must treat as "cannot serve" rather than "no fields"
     */
    public MappingGenerationStore.MappingGeneration ensureCurrent(String indexUuid, long requiredGeneration) {
        MappingGenerationStore.MappingGeneration current = local.get(indexUuid);
        if (current != null && current.generation() >= requiredGeneration) {
            // Already current, or ahead because this shard is the one that inferred the field. No read.
            return current;
        }
        fetches.incrementAndGet();
        MappingGenerationStore.MappingGeneration loaded = loader.load(indexUuid, requiredGeneration);
        if (loaded == null) {
            return current;
        }
        // merge() rather than put(), because a concurrent refresh may have landed a newer generation while
        // this one was in flight, and taking the older of the two would undo it.
        return local.merge(indexUuid, loaded, (existing, fresh) -> fresh.generation() > existing.generation() ? fresh : existing);
    }

    /** What this shard currently holds, without triggering a fetch. */
    public MappingGenerationStore.MappingGeneration localMapping(String indexUuid) {
        return local.get(indexUuid);
    }

    /** Seeds the local view, for the shard that performed the swap itself. */
    public void record(String indexUuid, MappingGenerationStore.MappingGeneration mapping) {
        local.merge(indexUuid, mapping, (existing, fresh) -> fresh.generation() > existing.generation() ? fresh : existing);
    }

    /**
     * How many times this shard has gone to the store.
     *
     * <p>Exposed because it is the number that says whether this is cheaper than a broadcast. A design
     * that refreshed on every request would be correct and useless, and only a count distinguishes them.
     */
    public long fetchCount() {
        return fetches.get();
    }

    /** How many indices this node currently holds a mapping for, which is what the cap bounds. */
    public int trackedIndexCount() {
        return local.size();
    }

    /**
     * How many mappings have been evicted.
     *
     * <p>Worth exposing for the same reason as {@link #fetchCount()}: a cache sized well below the working
     * set is correct and slow, and only a count tells the two apart. A node evicting continuously is one
     * whose capacity is wrong, not one that is broken.
     */
    public long evictionCount() {
        return evictions.get();
    }
}
