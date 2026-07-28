/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
 */
public final class MappingRefreshOnDemand {

    /** Where a shard reads a mapping it has fallen behind on. */
    public interface MappingLoader {
        /** The mapping at or after {@code atLeastGeneration}, or null when it cannot be loaded. */
        MappingGenerationStore.MappingGeneration load(String indexUuid, long atLeastGeneration);
    }

    private final MappingLoader loader;
    private final Map<String, MappingGenerationStore.MappingGeneration> local = new ConcurrentHashMap<>();
    private final AtomicLong fetches = new AtomicLong();

    public MappingRefreshOnDemand(MappingLoader loader) {
        this.loader = loader;
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
}
