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
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.UnknownFieldRefresh;

import java.util.HashMap;
import java.util.Map;

/**
 * Makes a field another shard inferred usable by this one.
 *
 * <p>W14 put the trigger where a shard meets a field its mapping does not have. This is what happens then:
 * read the index's mapping from the store, and if it has the field, merge it into this shard's
 * {@link MapperService} so the document can be parsed. A field present in the store but absent from the
 * in-memory mapping cannot parse anything, which is why fetching alone is not enough.
 *
 * <p><b>Why a shard can be behind at all.</b> For a gated index the mapping lives outside cluster state and
 * nothing broadcasts a change, which is the whole of H6c: a mapping update costs one compare-and-swap
 * independent of how many shards the index has, because nothing fans out. The price is that a shard learns
 * only when it needs to, and this is that moment.
 *
 * <p><b>The cost this must not have.</b> H6c's claim was that pulling beats broadcasting, and a refresher
 * that read the store on every unknown field would refute it: a document with ten new fields would cost ten
 * reads, and a shard behind on a field it will never see would still pay. So a read is skipped when the
 * mapping this shard already holds is at or ahead of the store's generation, and the tests count fetches
 * rather than trusting that comparison.
 *
 * <p>Returning false means the field is not being supplied from the store, and the caller then rejects or
 * dynamically infers it exactly as it would have without any of this. Since T43 that covers two cases
 * rather than one: the field is genuinely new, or the store could not be read. They are deliberately not
 * distinguished here, because the caller's next move is the same either way and the write path it takes
 * next meets the same store.
 *
 * <h2>The other half of this idea, and why its cache would not be shaped like this one's</h2>
 *
 * H6c's original design had a coordinator stamp each request with the generation it planned against, and a
 * core class, {@code MappingRefreshOnDemand}, implemented it. W14 chose this trigger instead: a shard
 * refreshes when it meets a field its mapping does not have. That other half was complete, tested, and
 * called by nothing, so it was removed from core rather than left as one more mechanism a reader has to
 * discover by search does not run.
 *
 * <p>One conclusion from it is worth keeping, because it applies to anyone changing the cache below. T9
 * replaced an access-ordered {@code synchronizedMap} here with recency stamped on write, after P1 measured
 * that structure going from 57.4M reads per second on one thread to 9.1M on sixteen. That suits this class,
 * whose hot entries are written often. It would be the wrong move for a generation-stamped cache, where
 * entries are read often and written only when a mapping generation advances: stamping on write lets a
 * sweep over cold indices take the freshest stamps and evict the working set, which is precisely what
 * access ordering was there to prevent. A read-cheap structure that still keeps recency, such as the SIEVE
 * algorithm TiDB uses for the same read-hot write-rare shape, is the answer if that path is built again.
 */
public final class StoreBackedFieldRefresher implements UnknownFieldRefresh.Refresher {

    private static final Logger logger = LogManager.getLogger(StoreBackedFieldRefresher.class);

    /**
     * The generation this node last merged, per index.
     *
     * <p>Bounded by the same argument as H11's mapping cache and with the same eviction consequence: an
     * evicted entry costs one extra read, never a wrong answer, because the store remains the source of
     * truth and a re-read simply re-merges what is already there.
     */
    /**
     * What this node merged for an index, when it last asked the store, and when it was written.
     *
     * <p>{@code checkedAtNanos} answers the recheck window and {@code stamp} orders eviction. They are
     * separate for the reason T4 found in the descriptor cache: entries written within one clock tick
     * share a timestamp, and an eviction threshold taken from equal stamps evicts nothing.
     */
    private record Checked(long generation, long checkedAtNanos, long stamp) {
    }

    /**
     * Written on every merge and read on every unknown field, never reordered on read.
     *
     * <p><b>This was a {@code synchronizedMap} around an access-ordered {@code LinkedHashMap}.</b> P1
     * measured exactly that structure at 57.4M reads per second on one thread falling to 9.1M on sixteen,
     * because access ordering mutates the recency list on read and so requires the monitor on every lookup.
     * P2 replaced it in {@code GatedShardSuspensionRegistry} and this one was left behind: P3's title said
     * the mapping cache had the same read-mutating structure, but P3's fix moved the guard in front of the
     * store read, which is a different problem. The titled one stayed open until the review after T8.
     *
     * <p>Recency is now stamped on write, so a read is a plain concurrent get that touches no shared
     * mutable state. Eviction quality is approximate rather than exact LRU, which is the trade P1 measured
     * as costing about six percent against an unbounded concurrent map.
     */
    private final java.util.concurrent.ConcurrentHashMap<String, Checked> mergedGeneration = new java.util.concurrent.ConcurrentHashMap<>();

    private final java.util.concurrent.atomic.AtomicLong stamps = new java.util.concurrent.atomic.AtomicLong();
    private final int capacity;

    /**
     * How long a shard trusts its last look at the store before asking again.
     *
     * <p>Without this the cache cannot do its job. Knowing whether the store has moved requires asking it,
     * so a generation check alone cannot avoid the read it is meant to avoid, and every unknown field cost
     * a get against the mapping index even when this shard was already current. A document with ten new
     * fields cost ten reads, which is precisely the cost H6c argued pulling avoids.
     *
     * <p>The window bounds reads to one per index per interval and bounds staleness by the same interval:
     * a field another shard inferred becomes visible here within it. One second is short against how long
     * a mapping change takes to matter and long against how fast documents arrive.
     */
    static final long RECHECK_WINDOW_NANOS = java.util.concurrent.TimeUnit.SECONDS.toNanos(1);

    private final java.util.function.LongSupplier clock;

    public StoreBackedFieldRefresher() {
        this(10_000, System::nanoTime);
    }

    public StoreBackedFieldRefresher(int capacity) {
        this(capacity, System::nanoTime);
    }

    /** Test seam for the clock, so the recheck window can be exercised without sleeping. */
    StoreBackedFieldRefresher(int capacity, java.util.function.LongSupplier clock) {
        this.clock = clock;
        this.capacity = capacity;
    }

    /** Records what this node merged, evicting the stalest tenth if that puts the map over its bound. */
    private void record(String indexUuid, long generation, long now) {
        mergedGeneration.put(indexUuid, new Checked(generation, now, stamps.incrementAndGet()));
        if (mergedGeneration.size() <= capacity) {
            return;
        }
        int toEvict = Math.max(1, capacity / 10);
        long[] ordered = mergedGeneration.values().stream().mapToLong(Checked::stamp).sorted().toArray();
        if (ordered.length == 0) {
            return;
        }
        // A threshold from one pass rather than a sorted eviction list, matching what
        // GatedShardSuspensionRegistry and DescriptorCache both do, so there is one eviction shape here
        // rather than three.
        long threshold = ordered[Math.min(toEvict, ordered.length - 1)];
        mergedGeneration.entrySet().removeIf(entry -> entry.getValue().stamp() < threshold);
    }

    @Override
    public boolean refresh(MapperService mapperService, String indexUuid, String fieldName) {
        if (mapperService == null || indexUuid == null) {
            return false;
        }
        // The guard comes before the read, which is the whole point of having it. Asking the store whether
        // it has moved is itself the expensive operation, so a check performed afterwards prevents nothing:
        // the first version of this method read the mapping index on every unknown field, and a document
        // with ten new fields cost ten reads.
        Checked checked = mergedGeneration.get(indexUuid);
        long now = clock.getAsLong();
        if (checked != null && now - checked.checkedAtNanos() < RECHECK_WINDOW_NANOS) {
            // Looked recently. The field is treated as genuinely new, and the caller infers or rejects it
            // exactly as it would have. A field another shard inferred becomes visible here once the window
            // passes, which bounds staleness by the window rather than leaving it unbounded.
            return false;
        }

        MappingGenerationStore.MappingGeneration stored;
        try {
            stored = MappingGenerationStore.currentMapping(indexUuid, expectedGeneration(mapperService, indexUuid));
        } catch (MappingGenerationStore.MissingMappingException e) {
            // T59. The descriptor this node resolved for the index says it declared fields; the store says
            // it has none. That is the window between T47's deletion-time prune and the moment this node's
            // descriptor cache catches up, and letting it through here would merge nothing, report the field
            // absent, and send the caller to infer it fresh -- silently replacing whatever generation the
            // descriptor claims with whatever this one document happens to carry. Unlike the catch below,
            // this must not degrade: it names a real inconsistency rather than a store this node cannot
            // reach.
            throw e;
        } catch (Exception e) {
            // T43 stopped the store reporting an unreadable mapping as an absent one, so this is now the
            // place that decides what to do about it, and the answer here is the same as everywhere else in
            // this method: leave the caller to infer or reject the field.
            //
            // This does not make the document safe from the same failure, and it would be wrong to read it
            // that way. The caller's next move on false is to infer the field, which sends an auto
            // put-mapping, which reaches MappingGenerationStore.updateMapping and the same unreadable store.
            // What this buys is that the failure surfaces once, from the path whose job is writing mappings,
            // rather than twice from a path whose job is reading one.
            logger.debug("could not read the stored mapping for [{}]: {}", indexUuid, e);
            return false;
        }
        if (stored == null) {
            return false;
        }

        Map<String, Object> definition = stored.definitionOf(fieldName);
        if (definition == null) {
            // The store does not have it either. Recording the check is what stops the next unknown field
            // on this shard re-reading a mapping that has not moved.
            record(indexUuid, stored.generation(), now);
            return false;
        }

        try {
            // The definition as stored, not a type rebuilt from it. Before T15 the store held only a type
            // name, so this could only ever merge {"type": t} -- and a field declared with a date format or
            // an analyzer came back without it, on the shard that had to re-learn it. The store carries the
            // whole definition now, so what is merged here is what was declared.
            Map<String, Object> fragment = new HashMap<>();
            fragment.put("properties", Map.of(fieldName, definition));
            mapperService.merge(MapperService.SINGLE_MAPPING_NAME, fragment, MapperService.MergeReason.MAPPING_UPDATE);
            record(indexUuid, stored.generation(), now);
            return mapperService.documentMapper() != null && mapperService.documentMapper().mappers().getMapper(fieldName) != null;
        } catch (Exception e) {
            // A merge that fails leaves the caller to reject or infer, which is a correct outcome. Failing
            // the document instead would turn a mapping hiccup into a rejected write.
            logger.debug("failed to merge field [{}] for index [{}]: {}", fieldName, indexUuid, e);
            return false;
        }
    }

    /** How many indices this node holds a merged generation for, which the capacity bounds. */
    public int trackedIndexCount() {
        return mergedGeneration.size();
    }

    /**
     * The generation this node's own descriptor resolution says {@code indexUuid} is at, or 0 when there is
     * nothing to check against.
     *
     * <p>T59. Zero rather than throwing whenever resolution cannot confirm the uuid, matching the "a null
     * answer is not treated as gone" contract {@code MetadataMappingService#refuseIfDescriptorShowsTheIndexIsGone}
     * settled for the write side in T50: a resolver that is not registered, cannot answer, or resolves the
     * name to a different uuid than this shard's gives no evidence either way, and forcing a failure from
     * that would fail an ordinary unreadable-resolver case rather than only the one this closes.
     */
    private static long expectedGeneration(MapperService mapperService, String indexUuid) {
        if (AbsentIndexDescriptorSuppliers.isRegistered() == false) {
            return 0L;
        }
        org.opensearch.core.index.Index index = mapperService.index();
        if (index == null) {
            return 0L;
        }
        IndexDescriptor current = AbsentIndexDescriptorSuppliers.supply(index.getName());
        if (current == null || current.exists() == false || current.uuid().equals(indexUuid) == false) {
            return 0L;
        }
        return current.mappingGeneration();
    }
}
