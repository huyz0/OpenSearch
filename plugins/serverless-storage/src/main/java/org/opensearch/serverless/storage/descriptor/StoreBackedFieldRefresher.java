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
 * mapping this shard already holds is at or ahead of the store's generation, which is the same comparison
 * {@code MappingRefreshOnDemand} makes and the reason it counts fetches rather than trusting itself.
 *
 * <p>Returning false is a complete answer, not a failure: it means the field is genuinely new, and the
 * caller then rejects or dynamically infers it exactly as it would have without any of this.
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
    /** What this node merged for an index, and when it last asked the store. */
    private record Checked(long generation, long checkedAtNanos) {
    }

    private final Map<String, Checked> mergedGeneration;
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
        this.mergedGeneration = java.util.Collections.synchronizedMap(new java.util.LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Checked> eldest) {
                return size() > StoreBackedFieldRefresher.this.capacity;
            }
        });
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

        MappingGenerationStore.MappingGeneration stored = MappingGenerationStore.currentMapping(indexUuid);
        if (stored == null) {
            return false;
        }

        String type = stored.fields().get(fieldName);
        if (type == null) {
            // The store does not have it either. Recording the check is what stops the next unknown field
            // on this shard re-reading a mapping that has not moved.
            mergedGeneration.put(indexUuid, new Checked(stored.generation(), now));
            return false;
        }

        try {
            Map<String, Object> fragment = new HashMap<>();
            fragment.put("properties", Map.of(fieldName, Map.of("type", type)));
            mapperService.merge(MapperService.SINGLE_MAPPING_NAME, fragment, MapperService.MergeReason.MAPPING_UPDATE);
            mergedGeneration.put(indexUuid, new Checked(stored.generation(), now));
            return mapperService.documentMapper() != null && mapperService.documentMapper().mappers().getMapper(fieldName) != null;
        } catch (Exception e) {
            // A merge that fails leaves the caller to reject or infer, which is a correct outcome. Failing
            // the document instead would turn a mapping hiccup into a rejected write.
            logger.debug("failed to merge field [{}] for index [{}]", fieldName, indexUuid, e);
            return false;
        }
    }

    /** How many indices this node holds a merged generation for, which the capacity bounds. */
    public int trackedIndexCount() {
        return mergedGeneration.size();
    }
}
