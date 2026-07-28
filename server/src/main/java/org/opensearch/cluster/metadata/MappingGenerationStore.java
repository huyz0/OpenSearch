/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The seam a mapping update goes through when the index has no cluster state entry.
 *
 * <p>Area H's H.7 in one interface. Today a document carrying a new field builds a
 * {@code PutMappingRequest} and submits it through {@code MetadataMappingService}, which resolves the
 * index with {@code Metadata#getIndexSafe} and therefore throws for a gated index. The same path serves
 * the explicit {@code PUT _mapping} API, so the constraint being removed is not "no dynamic fields" but
 * "the mapping is immutable after creation".
 *
 * <p><b>Why this is a compare-and-swap rather than a broadcast.</b> The broadcast exists because cluster
 * state was the only distribution mechanism, not because every shard has to be told. A shard receiving
 * {@code age: 30} infers {@code long}, which is what every other shard would infer from the same data, so
 * it does not need the answer pushed to it. It needs its inference not to conflict. A generation counter
 * with a retry loop gives exactly that, at one swap per new field rather than per document:
 *
 * <pre>
 *   shard A: read gen 5, add age:long,     CAS 5 -&gt; 6  ok
 *   shard B: read gen 5, add city:keyword, CAS 5 -&gt; 6  conflict
 *            re-read 6, merge city onto it, CAS 6 -&gt; 7  ok
 * </pre>
 *
 * <p><b>The retry must merge.</b> Re-reading and then overwriting would drop shard A's field, and the
 * document that introduced it would be silently unqueryable on that field: a wrong answer that looks like
 * a correct one, which is the failure shape this area has hit repeatedly. {@link #updateMapping} therefore
 * takes the fields to add rather than a whole mapping, so a caller cannot express "replace".
 *
 * <p><b>A genuine type conflict must still fail.</b> {@code age:long} against {@code age:float} is an
 * error today, raised by the cluster manager because it serialises the two updates. Here it surfaces at
 * the second shard's merge instead. Same error, different place, and it must not be resolved by picking
 * one.
 *
 * <p>Unset by default, so an ordinary cluster's mapping updates take exactly the path they always have.
 */
public final class MappingGenerationStore {

    /**
     * What a store must implement. Deliberately narrow: read a generation, and attempt one swap. Anything
     * richer would let a caller express an overwrite, which is the one thing that must be impossible.
     */
    public interface Store {
        /** The current mapping and its generation, or null when the index has no mapping object yet. */
        MappingGeneration read(String indexUuid);

        /**
         * Attempts to replace {@code expectedGeneration} with {@code updated}.
         *
         * @return true when the swap took, false when someone else got there first and the caller must
         *         re-read and merge rather than retry with the same value
         */
        boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGeneration updated);
    }

    /** A mapping and the generation it was read at, which is what makes the swap safe. */
    public record MappingGeneration(long generation, Map<String, String> fields) {
        public MappingGeneration {
            fields = Map.copyOf(fields);
        }

        public static MappingGeneration empty() {
            return new MappingGeneration(0L, Map.of());
        }
    }

    /** Raised when two inferences disagree about a field's type, which no retry can reconcile. */
    public static class MappingConflictException extends IllegalArgumentException {
        public MappingConflictException(String field, String existing, String proposed) {
            super("field [" + field + "] is mapped as [" + existing + "] and cannot be changed to [" + proposed + "]");
        }
    }

    private static final AtomicReference<Store> STORE = new AtomicReference<>();

    /** Bounded because a livelock here would hang the write path rather than fail it. */
    private static final int MAX_ATTEMPTS = 16;

    private MappingGenerationStore() {}

    public static void register(Store store) {
        STORE.set(store);
    }

    public static boolean isRegistered() {
        return STORE.get() != null;
    }

    /**
     * Adds fields to an index's mapping, retrying against concurrent writers until the swap takes.
     *
     * @param newFields the fields this caller inferred, which are merged onto whatever is current rather
     *                  than replacing it
     * @return the generation the mapping reached, or -1 when no store is installed
     * @throws MappingConflictException when a field already exists with a different type
     */
    public static long updateMapping(String indexUuid, Map<String, String> newFields) {
        Store store = STORE.get();
        if (store == null) {
            return -1L;
        }
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            MappingGeneration current = store.read(indexUuid);
            if (current == null) {
                current = MappingGeneration.empty();
            }

            // Merge onto what is there. Conflicts are raised before the swap is attempted, so a genuine
            // disagreement fails rather than being retried until it wins.
            java.util.Map<String, String> merged = new java.util.HashMap<>(current.fields());
            boolean changed = false;
            for (Map.Entry<String, String> field : newFields.entrySet()) {
                String existing = merged.get(field.getKey());
                if (existing == null) {
                    merged.put(field.getKey(), field.getValue());
                    changed = true;
                } else if (existing.equals(field.getValue()) == false) {
                    throw new MappingConflictException(field.getKey(), existing, field.getValue());
                }
            }
            if (changed == false) {
                // Someone else already added exactly these fields. Converged, so nothing to swap: this is
                // the common case when two shards infer the same field from the same shape of document.
                return current.generation();
            }

            MappingGeneration updated = new MappingGeneration(current.generation() + 1, merged);
            if (store.compareAndSwap(indexUuid, current.generation(), updated)) {
                return updated.generation();
            }
        }
        throw new IllegalStateException(
            "mapping update for [" + indexUuid + "] did not converge after " + MAX_ATTEMPTS + " attempts, which means sustained contention"
        );
    }

    /**
     * The current mapping and its generation, or null when nothing is stored or no store is registered.
     *
     * <p>Added for W15, which needs the fields rather than only the generation: a shard that discovers it is
     * behind has to merge the field it was missing, and a generation alone cannot tell it what type that
     * field is. {@link #currentGeneration} remains for callers that only need to know whether they are
     * stale.
     */
    public static MappingGeneration currentMapping(String indexUuid) {
        Store store = STORE.get();
        return store == null ? null : store.read(indexUuid);
    }

    /** The current generation, for a caller deciding whether its cached mapping is stale. */
    public static long currentGeneration(String indexUuid) {
        Store store = STORE.get();
        if (store == null) {
            return -1L;
        }
        MappingGeneration current = store.read(indexUuid);
        return current == null ? 0L : current.generation();
    }
}
