/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.serverless.cluster.IndexDescriptor;

/**
 * Which shard a document id belongs to.
 *
 * <p>This mirrors {@code OperationRouting.calculateScaledShardId}, which is private, and it must keep
 * mirroring it: a serverless node that hashed documents differently from the data plane would put a
 * document in one shard and look for it in another, and the symptom would be a search that returns
 * nothing rather than an error.
 *
 * <p>An index created with a shard mode routes through its unit layout instead: the 64-bit hash of the
 * id, looked up in the layout's cut points. Its answer is a unit, which is the physical shard.
 *
 * <p>A document with custom routing is placed by that value instead of its id; see {@link #shardFor(IndexDescriptor,
 * String, String)}.
 *
 * <p><b>Core's split indices are not supported here.</b> The real function consults
 * {@code getSplitShardsMetadata()} after the modulo; this does not, because nothing in this shell
 * splits an index. If splitting ever lands, this is one of the places that has to learn about it, and
 * failing to would be silent.
 */
public final class DocumentRouting {

    private DocumentRouting() {}

    /**
     * Returns the shard a document id routes to, for a document with no custom routing.
     *
     * @param descriptor the index
     * @param id the document id, which is also the routing value
     * @return the shard number; for an index with a unit layout, the unit id
     */
    public static int shardFor(IndexDescriptor descriptor, String id) {
        return shardFor(descriptor, id, null);
    }

    /**
     * Returns the shard a document routes to.
     *
     * <p><b>Custom routing.</b> A routing value replaces the id as the value hashed, exactly as core does it: in a
     * legacy index through core's own function, and in an index with a shard mode by the routing value's 64-bit
     * hash in the unit layout. A declared shard is a slice of that hash space, so in a fixed index the routing
     * value picks one declared shard, and in an adaptive index it picks the unit that holds the value's hash. The
     * routing value is stored with the document and logged with it, so replay and unit splits keep the document
     * where routed reads look for it.
     *
     * <p>An index whose mapping says {@code _routing.required} refuses a document without routing, as core does.
     *
     * @param descriptor the index
     * @param id the document id
     * @param routing the custom routing value, or null to place the document by its id
     * @return the shard number; for an index with a unit layout, the unit id
     * @throws org.opensearch.action.RoutingMissingException if the mapping requires routing and none was given
     */
    public static int shardFor(IndexDescriptor descriptor, String id, String routing) {
        if (routing == null) {
            requireRoutingUnlessOptional(descriptor, id);
        }
        if (descriptor.layout() != null) {
            return descriptor.layout().unitForRouting(routing != null ? routing : id).id();
        }
        try {
            return org.opensearch.cluster.routing.OperationRouting.generateShardId(descriptor.routingMetadata(), id, routing);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException("could not render [" + descriptor.name() + "] for routing", e);
        }
    }

    /**
     * Returns the shards a search with routing values has to visit: the shard each value routes to.
     *
     * @param descriptor the index
     * @param routings the routing values, at least one
     * @return the shard numbers, ascending; for an index with a unit layout, unit ids; null when every shard has to be
     *     searched, which is a legacy index with a routing partition
     */
    public static java.util.SortedSet<Integer> shardsForSearch(IndexDescriptor descriptor, java.util.Collection<String> routings) {
        final java.util.SortedSet<Integer> shards = new java.util.TreeSet<>();
        for (String routing : routings) {
            if (descriptor.layout() != null) {
                shards.add(descriptor.layout().unitForRouting(routing).id());
                continue;
            }
            try {
                if (descriptor.routingMetadata().getRoutingPartitionSize() != 1) {
                    return null; // a routing value spreads over a partition of shards; searched whole rather than guessed
                }
                shards.add(org.opensearch.cluster.routing.OperationRouting.generateShardId(descriptor.routingMetadata(), null, routing));
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException("could not render [" + descriptor.name() + "] for routing", e);
            }
        }
        return shards;
    }

    /**
     * Normalises a routing parameter the way core's requests do: an empty value means none.
     *
     * @param routing the parameter as given
     * @return the routing value, or null
     */
    public static String normalize(String routing) {
        return routing == null || routing.isEmpty() ? null : routing;
    }

    /**
     * Splits a search's comma-separated routing parameter into its values, as core does.
     *
     * @param routing the parameter, or null
     * @return the distinct non-empty values, in order; empty when there are none
     */
    public static java.util.Set<String> searchRoutings(String routing) {
        final java.util.Set<String> values = new java.util.LinkedHashSet<>();
        if (routing != null) {
            for (String value : org.opensearch.core.common.Strings.splitStringByCommaToArray(routing)) {
                if (value.isEmpty() == false) {
                    values.add(value);
                }
            }
        }
        return values;
    }

    private static void requireRoutingUnlessOptional(IndexDescriptor descriptor, String id) {
        if (descriptor.mapping() == null || descriptor.mapping().contains("_routing") == false) {
            return; // the common case, decided without rendering anything
        }
        final org.opensearch.cluster.metadata.MappingMetadata mapping;
        try {
            mapping = descriptor.routingMetadata().mapping();
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException("could not render [" + descriptor.name() + "] for routing", e);
        }
        if (mapping != null && mapping.routingRequired()) {
            throw new org.opensearch.action.RoutingMissingException(descriptor.name(), id);
        }
    }

}
