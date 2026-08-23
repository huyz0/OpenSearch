/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import java.util.HashMap;
import java.util.Map;

/**
 * Every reason an index must keep its cluster state entry, in one place.
 *
 * <p>A gated index has no cluster state entry at all, so anything {@link IndexDescriptor} cannot carry is
 * not degraded when an index is gated, it is gone. Before this class nothing checked that: {@code ownsIndex}
 * answered
 * on the single setting {@code index.serverless_storage.enabled}, and an index with configuration the
 * descriptor has no field for was gated anyway.
 *
 * <p><b>The list being closed and in one file is the point.</b> This is TiDB's
 * {@code checkAttributesInOrder} ({@code meta/meta.go:1286}), which names every attribute that stops a table
 * being lazily loaded, so adding a feature that needs residency forces an edit to that list. Ours was spread
 * across whatever happened to notice, which is why two of these gaps were found separately rather than
 * together.
 *
 * <p><b>What is deliberately not here.</b> In-sync allocation IDs, index blocks and rollover info are
 * runtime state rather than declared configuration, and treating them as reasons would make almost every
 * index ineligible while fixing nothing. The entries below are all user-declared configuration that a
 * descriptor silently drops.
 *
 * <p>This gates cluster state residency only, not computed placement. An index that stays in cluster state
 * still gets its placement computed, which is the arrangement that existed before gating was switched on and
 * is known to work. The combination that cannot be served is the reverse, a gated index with a published
 * routing entry, and that remains impossible because gating is a strict subset of placement ownership.
 */
public final class DescriptorRepresentable {

    private DescriptorRepresentable() {}

    /**
     * Why this index cannot be represented by a descriptor, or null if it can.
     *
     * <p>Returns the reason rather than a boolean so a caller can log which feature kept an index resident.
     * An operator asking why one index costs cluster state and its neighbour does not needs the answer, and
     * a boolean makes that question unanswerable.
     */
    public static String whyNotRepresentable(IndexMetadata indexMetadata) {
        if (indexMetadata == null) {
            return "there is no metadata to represent";
        }
        // This rule was widened from four alias properties to every alias, and the reason is not that a
        // descriptor cannot carry the name. It carries it already. The reason is that nothing can ever
        // change it.
        //
        // Every alias operation is a cluster state update that looks the index up in Metadata and rewrites
        // its IndexMetadata. A gated index is not in Metadata, so there is nothing to rewrite: adding an
        // alias to one does not fail cleanly, it times out with ClusterManagerNotDiscoveredException.
        // An alias could therefore only ever be set at creation and never added, removed or repointed,
        // which is the opposite of what an alias is for. A client is given an alias precisely so the index
        // behind it can change.
        //
        // The widening also measured that resolution never reads the field: naming the alias of a gated index
        // resolved to nothing, silently under the options most clients use. Building that resolution was
        // the alternative to this rule and was rejected, because it would have served a set-once alias
        // with a refresh-bound visibility window, which is a partial feature that invites exactly the
        // usage it cannot support.
        //
        // The four narrower rules this replaces (filter, routing, write index, hidden) are now subsumed.
        // They are worth remembering rather than deleting from history: the filter one mattered most,
        // because losing an alias filter widens a restricted view rather than breaking it, which is a
        // security-shaped failure rather than an availability-shaped one.
        if (indexMetadata.getAliases().isEmpty() == false) {
            return "index declares aliases "
                + indexMetadata.getAliases().keySet()
                + ", and an alias on an index held outside cluster state could never be changed afterwards";
        }
        if (indexMetadata.getCustomData().isEmpty() == false) {
            return "index carries custom metadata " + indexMetadata.getCustomData().keySet() + ", which a descriptor cannot carry";
        }
        // Hidden was already only half-honoured, which is what makes refusing it a correction rather than a
        // narrowing. IndexDescriptor carries the flag and wildcard expansion is the single line that reads
        // it, but IndexDescriptor#toIndexMetadata does not put index.hidden back, so the metadata a node
        // synthesises to open the shard says the index is not hidden. One of the two answers was always
        // going to be wrong, and an index that hides from wildcards while presenting itself as visible
        // everywhere else is the worse of them.
        //
        // Refusing also keeps expansion honest against an object store, where the answer has to be derivable
        // from a listing. A key under the descriptor prefix carries a name and nothing else, so serving
        // hidden from a bucket needs either a GET for every match -- a hundred round trips for a wildcard
        // the cap exists to keep cheap -- or a second keyspace written on create and deleted on delete,
        // which is a consistency problem bought for a flag that has one reader.
        if (indexMetadata.isHidden()) {
            return "index is hidden, and a descriptor's hidden flag is read by wildcard expansion while "
                + "IndexDescriptor#toIndexMetadata drops it, so the index would be hidden from wildcards and "
                + "visible to everything else";
        }
        // A descriptor carries a mappingGeneration, not a mapping. The fields live in
        // MappingGenerationStore, and originally nothing wrote them there at creation, so an index created
        // with an explicit mapping had it parsed, validated, built into this very IndexMetadata, and then
        // dropped when the descriptor was written -- acknowledged, with the first document to arrive
        // inferring the fields again from its own values.
        //
        // First the fields were carried, then the carrying stopped being lossy, then the store was widened
        // to hold a
        // field's whole definition rather than its type. What is left to refuse is small: a property whose
        // definition is not an object at all. Object and nested fields, and every field parameter, now
        // round-trip.
        if (indexMetadata.mapping() != null && fieldDefinitionsOrNull(indexMetadata) == null) {
            return "index declares a mapping with a property whose definition is not an object, which the "
                + "mapping store has no representation for";
        }
        return null;
    }

    /**
     * The mapping's top-level fields as name to definition, or null if any of it would not round-trip.
     *
     * <p>A descriptor carries a mapping generation and the fields live in {@code MappingGenerationStore},
     * so gating an index with a mapping means writing those fields there at creation.
     *
     * <p><b>What this refuses shrank twice, and the order matters.</b> It first required a declared type and
     * no sub-properties, which reads like a completeness check and was not one: a definition may name a type
     * and carry six other things, and only the type was stored, so {@code format}, {@code analyzer},
     * {@code ignore_above} and multi-{@code fields} passed the guard and were dropped. The rule was then made
     * to match what was kept -- the type and nothing beside it -- which was honest and refused most real
     * mappings. The store was then widened to hold the definition itself, so the rule could shrink to what
     * genuinely has no representation.
     *
     * <p><b>Null rather than a partial map, throughout.</b> Returning the fields that do round-trip and
     * dropping the rest is the silent loss this check exists to prevent, one level further in. An index that
     * cannot be carried completely is not gated at all, keeps its cluster state entry, and works normally.
     */
    static Map<String, Object> fieldDefinitionsOrNull(IndexMetadata indexMetadata) {
        MappingMetadata mapping = indexMetadata.mapping();
        if (mapping == null) {
            return Map.of();
        }
        return fieldDefinitionsOrNull(mapping.sourceAsMap().get("properties"));
    }

    /**
     * The one extraction, shared by both paths that write a gated mapping.
     *
     * <p>{@link #fieldDefinitionsOrNull} calls it for a create-time mapping, and a {@link
     * ClaimedIndexLifecycle} implementation calls it for a put-mapping. They used to be two implementations
     * described as mirroring each other, and they did not: this one refused a partial result while that one
     * skipped whatever it could not read and reported success, so an object field was refused at creation
     * and silently dropped on update. One function is what makes the claim true.
     *
     * <p><b>Public, unlike its {@link IndexMetadata}-taking sibling, because the two callers now sit on
     * opposite sides of the SPI.</b> The put-mapping half moved out of core with the rest of the mapping
     * protocol, and the alternative to exposing this was for it to grow its own extraction again -- which is
     * precisely the divergence the previous paragraph describes, reintroduced at a module boundary where it
     * would be harder to notice rather than easier.
     *
     * @param properties the mapping's {@code properties} object, however the caller obtained it
     * @return name to definition for every field, or null if any of it would not round-trip
     */
    public static Map<String, Object> fieldDefinitionsOrNull(Object properties) {
        if (properties instanceof Map == false) {
            // A mapping with no properties at all carries nothing, so there is nothing to lose.
            return Map.of();
        }
        Map<String, Object> fields = new HashMap<>();
        for (Map.Entry<?, ?> property : ((Map<?, ?>) properties).entrySet()) {
            Object definition = property.getValue();
            if (definition instanceof Map == false) {
                // A definition that is not an object at all -- a bare string shorthand, say. The store
                // holds definitions, so there is nothing sensible to store for this, and guessing is how
                // the losses this guard exists for happened in the first place.
                return null;
            }
            fields.put(String.valueOf(property.getKey()), definition);
        }
        return fields;
    }

    /** Whether gating this index would lose nothing. */
    public static boolean isRepresentable(IndexMetadata indexMetadata) {
        return whyNotRepresentable(indexMetadata) == null;
    }
}
