/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.index.mapper.UnknownFieldRefresh;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The seam a mapping update goes through when the index has no cluster state entry.
 *
 * <p>Today a document carrying a new field builds a
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
     * What a store must implement. Deliberately narrow: read a generation, attempt one swap, and remove a
     * mapping whose index is gone. Anything richer would let a caller express an overwrite, which is the one
     * thing that must be impossible -- the delete is not that, because it names an index that no longer
     * exists rather than a mapping to replace.
     */
    public interface Store {
        /**
         * The current mapping and its generation, or null when the index has no mapping object yet.
         *
         * <p><b>Null means absent, and only absent.</b> An implementation that cannot find out must throw,
         * not answer null: a caller that reads "could not find out" as "has no fields" merges onto empty,
         * and exactly that was removed from the index-backed implementation. Callers that would rather
         * degrade than fail decide so themselves.
         */
        MappingGeneration read(String indexUuid);

        /**
         * Attempts to replace {@code expectedGeneration} with {@code updated}.
         *
         * @return true when the swap took, false when someone else got there first and the caller must
         *         re-read and merge rather than retry with the same value
         */
        boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGeneration updated);

        /**
         * Removes an index's mapping, for an index that is being deleted.
         *
         * <p>Deliberately not a default no-op. This interface originally had no delete at all, so nothing
         * ever removed a mapping and the store grew with every index that had ever existed rather than with
         * the live population -- the residency problem this whole area exists to remove, one level down. A
         * default would let the next implementation inherit that silently.
         *
         * <p>Removing a mapping that is not there is not an error. The caller cannot know whether the index
         * declared one, and asking first would cost a round trip to learn something the delete already
         * handles.
         */
        void delete(String indexUuid);
    }

    /**
     * A mapping and the generation it was read at, which is what makes the swap safe.
     *
     * <p><b>Each value is a field's whole definition, not its type.</b> It originally held the type alone,
     * and that shape was the reason gating had to refuse most real mappings: a definition naming a type and
     * carrying anything else -- {@code format}, {@code analyzer}, {@code ignore_above} -- had nowhere to put
     * the rest, and an object field had nowhere to put its properties. Those refusals were made honest after
     * they were found silently dropping what they could not hold. Widening the value is what makes them
     * narrow, because the limitation was always this map rather than either caller.
     *
     * <p>A plain string value is still accepted and read as {@code {"type": value}}. That covers documents
     * written before the widening and callers that only ever mean a type, and {@link #definitionOf} is the
     * one place that difference is resolved.
     */
    public record MappingGeneration(long generation, Map<String, Object> fields) {
        public MappingGeneration {
            fields = Map.copyOf(fields);
        }

        public static MappingGeneration empty() {
            return new MappingGeneration(0L, Map.of());
        }

        /** A field's definition as a map, normalising the bare-type form. Null when the field is absent. */
        public Map<String, Object> definitionOf(String field) {
            return definition(fields.get(field));
        }
    }

    /** Normalises a stored value to a definition map: a bare type string becomes {@code {"type": it}}. */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> definition(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return Map.of("type", String.valueOf(value));
    }

    /** A stored value's declared type, whichever form it is in. Null when it declares none. */
    public static String typeOf(Object value) {
        Map<String, Object> definition = definition(value);
        Object type = definition == null ? null : definition.get("type");
        return type == null ? null : String.valueOf(type);
    }

    /** Raised when two inferences disagree about a field's type, which no retry can reconcile. */
    public static class MappingConflictException extends IllegalArgumentException {
        public MappingConflictException(String field, Object existing, Object proposed) {
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
     * Writes the fields an index declared at creation, for an index that cannot yet have any.
     *
     * <p>A creation used to reach {@link #updateMapping}, whose first act is to read the current mapping so
     * it has something to merge onto. For a creation there is nothing to merge onto and there cannot be: the
     * UUID was generated moments earlier and has never been given to anyone, so the read is a blocking round
     * trip whose answer is known to be "absent" before it is issued. Measurement put the index-backed store
     * at at least about 80% of what a declared mapping costs a creation, and this is one of its two round
     * trips.
     *
     * <p>So the swap is attempted first, at generation 1, and the read-and-merge loop is kept as the
     * fallback for when the swap is refused.
     *
     * <p><b>What makes the fallback reachable is not a retried creation.</b> That was the first answer and
     * it is wrong: {@code aggregateIndexSettings} puts a fresh {@code UUIDs.randomBase64UUID} into every
     * attempt, so a re-executed creation is a different UUID and finds nothing. What can refuse the swap is
     * the store's own write being retried underneath it -- a primary relocating or failing over after the
     * write landed, so the replication layer reissues it and the second attempt comes back as a version
     * conflict. The fallback then reads, finds the fields already there, and returns the stored generation
     * without writing again. Removing it would fail those creations for having already succeeded.
     *
     * @return the generation the mapping reached, or -1 when no store is installed, or 0 when there is
     *         nothing to declare
     * @throws MappingConflictException when a field already exists with a different type
     */
    public static long createMapping(String indexUuid, Map<String, ?> declaredFields) {
        Store store = STORE.get();
        if (store == null) {
            return -1L;
        }
        if (declaredFields.isEmpty()) {
            // Matching updateMapping, which takes its "nothing changed" exit here and writes nothing. Without
            // this the two entry points disagree on the same input, and this one leaves a document holding an
            // empty mapping in a shared index that nothing ever deletes from.
            return 0L;
        }
        if (store.compareAndSwap(indexUuid, 0L, new MappingGeneration(1L, new java.util.HashMap<>(declaredFields)))) {
            return 1L;
        }
        // Something is already stored under this UUID. Merge rather than overwrite: this path cannot tell
        // what put it there, and the one thing that must never happen is dropping fields someone is relying
        // on.
        return updateMapping(indexUuid, declaredFields);
    }

    /**
     * Adds fields to an index's mapping, retrying against concurrent writers until the swap takes.
     *
     * @param newFields the fields this caller inferred, which are merged onto whatever is current rather
     *                  than replacing it
     * @return the generation the mapping reached, or -1 when no store is installed
     * @throws MappingConflictException when a field already exists with a different type
     */
    public static long updateMapping(String indexUuid, Map<String, ?> newFields) {
        Store store = STORE.get();
        if (store == null) {
            return -1L;
        }
        RuntimeException lastReadFailure = null;
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            MappingGeneration current;
            try {
                current = store.read(indexUuid);
                lastReadFailure = null;
            } catch (RuntimeException e) {
                // The store no longer answers null for a read it could not perform, which means this
                // loop now sees the failures it used to be lied to about. Retrying them is what keeps the
                // change from being a regression: a mapping index shard relocating fails a read where the
                // write path underneath would have retried, and the old code got its retries by accident --
                // read null, merge onto empty, have the swap rejected by the stored generation, go round.
                // Same number of attempts, an honest reason, and the last failure is raised rather than
                // reported as contention.
                lastReadFailure = e;
                continue;
            }
            if (current == null) {
                current = MappingGeneration.empty();
            }

            // Merge onto what is there. Conflicts are raised before the swap is attempted, so a genuine
            // disagreement fails rather than being retried until it wins.
            java.util.Map<String, Object> merged = new java.util.HashMap<>(current.fields());
            boolean changed = false;
            for (Map.Entry<String, ?> field : newFields.entrySet()) {
                Object existing = merged.get(field.getKey());
                if (existing == null) {
                    merged.put(field.getKey(), field.getValue());
                    changed = true;
                    continue;
                }
                // Compared on the declared type rather than the whole definition, which matters once the
                // value carries parameters. A field declared at creation as {"type":"keyword",
                // "ignore_above":256} and later seen by a shard that only knows it is a keyword must
                // converge, not conflict: the shard's view is less specific, not contradictory. Comparing
                // definitions whole would raise a conflict there and fail the write.
                //
                // So the stored definition wins when the types agree. It is the declared one, and the
                // incoming one is at best equally specific.
                String existingType = typeOf(existing);
                String proposedType = typeOf(field.getValue());
                if (java.util.Objects.equals(existingType, proposedType) == false) {
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
        if (lastReadFailure != null) {
            throw lastReadFailure;
        }
        throw new IllegalStateException(
            "mapping update for [" + indexUuid + "] did not converge after " + MAX_ATTEMPTS + " attempts, which means sustained contention"
        );
    }

    /**
     * The current mapping and its generation, or null when nothing is stored or no store is registered.
     *
     * <p>Added for stale-shard refresh, which needs the fields rather than only the generation: a shard that discovers it is
     * behind has to merge the field it was missing, and a generation alone cannot tell it what type that
     * field is. {@link #currentGeneration} remains for callers that only need to know whether they are
     * stale.
     */
    public static MappingGeneration currentMapping(String indexUuid) {
        Store store = STORE.get();
        return store == null ? null : store.read(indexUuid);
    }

    /**
     * The current mapping, refusing to answer "no fields" when a descriptor says otherwise.
     *
     * <p>{@code Store.read} was taught to tell absent from unreadable and absent from lost, but neither
     * distinction covers a third case: the index resolves, the store answers "no document", and a caller
     * with no other information reports that as an index declaring no fields. That is exactly what a reader
     * sees in the window between the deletion-time prune and the moment a stale-cache node stops resolving
     * the deleted name -- the gated put-mapping refusal closed the write half of that window; this is the
     * read half.
     *
     * <p>The fix needs no new record. {@link org.opensearch.cluster.metadata.IndexDescriptor#mappingGeneration()} already carries what
     * settles it: a caller that resolved a descriptor to reach this point, and found it claiming generation
     * {@code expectedGeneration > 0}, knows the index declared fields. A store answering null then is not
     * "never written", it is "written and now missing", and only the caller holding that descriptor can tell
     * the two apart -- the store itself cannot, which is why this lives here rather than in {@code
     * Store.read}.
     *
     * <p><b>Why a null answer is not the only shape of this.</b> The first version of this guard checked
     * only for null, which was
     * exhaustive while the registered store was the index-backed one: it had a document or it did not.
     * Making the descriptor itself the store changed that: a descriptor that resolves always answers -- with
     * its own generation and its own fields, which for an index whose mapping is missing is generation 0 and
     * no fields. That is the identical wrong answer arriving as a value rather than as a null, so the guard
     * has to be stated over the generation rather than over the reference; written the narrow way it
     * survived that change as one more mechanism that is correct, tested, and no longer reachable by the
     * path it was built for.
     * A store *behind* what the caller's descriptor claims is the same inconsistency for the same
     * reason: the caller resolved a descriptor at generation N, and a mapping at anything below N cannot
     * contain what generation N declared.
     *
     * <p>Only {@code StoreBackedFieldRefresher} passes a non-zero generation, and its alternative on a wrong
     * answer is to report the field absent and let the caller infer it fresh -- which is precisely the
     * silent overwrite this exists to prevent, so failing the read is the better of the two.
     *
     * @param expectedGeneration the calling descriptor's {@code mappingGeneration}, or 0 when there is none
     *                           to check against, in which case this behaves exactly like {@link
     *                           #currentMapping(String)}
     * @throws MissingMappingException when the store cannot answer at the generation the descriptor claims
     */
    public static MappingGeneration currentMapping(String indexUuid, long expectedGeneration) {
        if (STORE.get() == null) {
            // No store means the whole seam is off, the same as every other entry point here degrading to
            // its no-op answer. A generation the caller cannot have gotten from a live descriptor plane
            // proves nothing about this store, which does not exist.
            return null;
        }
        MappingGeneration current = currentMapping(indexUuid);
        if (expectedGeneration > 0 && (current == null || current.generation() < expectedGeneration)) {
            throw new MissingMappingException(indexUuid, expectedGeneration, current == null ? -1L : current.generation());
        }
        return current;
    }

    /**
     * Raised by {@link #currentMapping(String, long)} when a descriptor's generation says an index has a
     * mapping and the store holds nothing for it.
     *
     * <p>Deliberately not swallowed anywhere this is thrown from: an earlier fix removed exactly this shape of failure
     * being caught and reported as "no fields", and a caller here would be reintroducing it one level up if
     * it caught this and returned null or false.
     *
     * <p><b>Extends {@link UnknownFieldRefresh.MappingUnavailableException}, which is how that guarantee
     * survives core no longer knowing this class exists.</b> {@code UnknownFieldRefresh#refreshed} degrades
     * everything a refresher throws to "field not found" except this, and it used to catch this class by
     * name -- a core file importing one plugin's storage exception so it could name the one failure it must
     * not swallow. Core now declares the contract and this satisfies it, which is the same behaviour with
     * the dependency pointing the other way.
     */
    public static final class MissingMappingException extends UnknownFieldRefresh.MappingUnavailableException {

        /** @param storedGeneration what the store answered with, or -1 when it answered nothing at all. */
        public MissingMappingException(String indexUuid, long expectedGeneration, long storedGeneration) {
            super(message(indexUuid, expectedGeneration, storedGeneration));
        }

        private static String message(String indexUuid, long expectedGeneration, long storedGeneration) {
            return "index ["
                + indexUuid
                + "]'s descriptor claims mapping generation ["
                + expectedGeneration
                + "] but the mapping store "
                + (storedGeneration < 0 ? "holds no document for it" : "is at generation [" + storedGeneration + "]")
                + ", so the mapping is missing rather than empty; answering here would report an index "
                + "with declared fields as one with none";
        }
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
