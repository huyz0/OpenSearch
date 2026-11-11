/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

/**
 * Grows an index's mapping for {@link ServerlessNode} when a write brings fields it does not have yet.
 */
final class MappingGrowthPath {

    private final ServerlessNode node;

    MappingGrowthPath(ServerlessNode node) {
        this.node = node;
    }

    static final class MappingGrowth {
        private final java.util.Queue<org.opensearch.index.mapper.Mapping> pending = new java.util.concurrent.ConcurrentLinkedQueue<>();
        // Guarded by mappingGrowth.compute, never by this object's monitor.
        private int users;
    }

    /**
     * Adds what a document needed to the index's mapping, durably, and applies it here.
     *
     * <p>The descriptor is the mapping's home, so this is the same compare-and-swap
     * {@code PUT /{index}/_mapping} performs — deliberately, because two ways of changing a mapping would be
     * two ways for it to drift. A lost swap re-reads and retries, so two writers introducing different fields
     * at the same time both keep theirs.
     *
     * @param shardId the shard whose write needed the field
     * @param addition what core says the mapping is missing
     * @throws java.io.IOException if the mapping cannot be stored
     */
    void growMapping(org.opensearch.core.index.shard.ShardId shardId, org.opensearch.index.mapper.Mapping addition)
        throws java.io.IOException {
        final var plane = node.metadataPlane;
        if (plane == null || addition == null) {
            return;
        }
        final var index = shardId.getIndex();
        // Coalesced, per index, on this node. This is the one register write a data path performs, and
        // it used to be one compare-and-swap per document that named a new field: a bulk of a thousand
        // such documents across the write pool was a thousand swaps on one register, of which one won
        // per round and the rest re-read and retried, four times each, and then failed the write. Now
        // every thread queues what it needs and the first to take the index's monitor swaps once for
        // all of them; a thread that finds its addition already drained by that leader has nothing left
        // to do. The rate is bounded by the field limit, which is the argument for allowing the write
        // here at all.
        final MappingGrowth growth = node.mappingGrowth.compute(index, (key, existing) -> {
            final MappingGrowth retained = existing == null ? new MappingGrowth() : existing;
            retained.users++;
            return retained;
        });
        try {
            growth.pending.add(addition);
            synchronized (growth) {
                final java.util.List<org.opensearch.index.mapper.Mapping> batch = new java.util.ArrayList<>();
                org.opensearch.index.mapper.Mapping next;
                while ((next = growth.pending.poll()) != null) {
                    batch.add(next);
                }
                if (batch.isEmpty()) {
                    // Another participant already committed and applied this caller's addition.
                    return;
                }
                try {
                    growMappingBy(plane, index, batch);
                } catch (MappingConflictException e) {
                    // An incompatible addition must not poison every future growth.
                    throw e;
                } catch (Exception e) {
                    // Waiting participants must retry the whole uncommitted batch.
                    growth.pending.addAll(batch);
                    throw e;
                }
            }
        } finally {
            node.mappingGrowth.compute(index, (key, current) -> {
                assert current == growth;
                // Evict only once no caller can still enqueue or wait on this monitor. Any additions
                // left then belong to failed requests; keeping them retains every departed index UUID.
                return --growth.users == 0 ? null : growth;
            });
        }
    }

    /** One index's mapping grown by a batch of additions: read, merge all of them, swap once, retry with backoff. */
    void growMappingBy(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        org.opensearch.core.index.Index index,
        java.util.List<org.opensearch.index.mapper.Mapping> additions
    ) throws java.io.IOException {
        final String indexName = index.getName();
        for (int attempt = 0; attempt < ServerlessNode.MAPPING_GROWTH_ATTEMPTS; attempt++) {
            if (attempt > 0) {
                // Exponential, jittered, capped: the register's other writers are other nodes doing the
                // same thing, and four immediate retries by every loser was a stampede that mostly lost.
                final long upper = Math.min(500L, 20L << Math.min(attempt, 6));
                try {
                    Thread.sleep(java.util.concurrent.ThreadLocalRandom.current().nextLong(1L, upper + 1L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new java.io.IOException("interrupted while growing the mapping of " + indexName, e);
                }
            }
            final long generation = plane.descriptorGeneration(indexName);
            final var current = plane.describe(indexName);
            if (current.isEmpty() || index.getUUID().equals(current.get().uuid()) == false) {
                throw new java.io.IOException("cannot grow mapping for " + index + ": the index was deleted or recreated");
            }
            final String merged;
            final var mapperService = node.indicesService.createIndexMapperService(current.get().toIndexMetadata(java.util.Map.of()));
            try {
                if (current.get().mapping() != null) {
                    mapperService.merge(
                        org.opensearch.index.mapper.MapperService.SINGLE_MAPPING_NAME,
                        new org.opensearch.common.compress.CompressedXContent(current.get().mapping()),
                        org.opensearch.index.mapper.MapperService.MergeReason.MAPPING_RECOVERY
                    );
                }
                org.opensearch.index.mapper.DocumentMapper grown = null;
                for (org.opensearch.index.mapper.Mapping addition : additions) {
                    grown = mapperService.merge(
                        org.opensearch.index.mapper.MapperService.SINGLE_MAPPING_NAME,
                        new org.opensearch.common.compress.CompressedXContent(addition.toString()),
                        org.opensearch.index.mapper.MapperService.MergeReason.MAPPING_UPDATE
                    );
                }
                merged = unwrapMappingType(grown.mappingSource().string());
            } catch (Exception e) {
                // Most likely this shard is behind the descriptor -- it mapped the field dynamically because a
                // mapping declaring it had not reached it yet. Brought up to it, the client's retry indexes the
                // document with the declared mapping instead of failing the same way.
                try {
                    node.descriptorRefresh.applyDescriptor(indexName, current.get());
                } catch (Exception nested) {
                    e.addSuppressed(nested);
                }
                throw new MappingConflictException("could not grow the mapping of " + indexName + " to fit the document", e);
            } finally {
                mapperService.close();
            }
            if (merged.equals(current.get().mapping())) {
                // Another node already grew it. The descriptor is right; this node's open shard is not,
                // and without this refresh every write carrying the field failed here until the shard
                // reopened.
                node.reconciler.refreshMapping(indexName, current.get());
                return;
            }
            final var updated = current.get().withMapping(merged);
            if (plane.updateDescriptor(updated, generation).isPresent()) {
                node.hosted.computeIfPresent(indexName, (k, v) -> v.uuid().equals(index.getUUID()) ? updated : v);
                node.reconciler.refreshMapping(indexName, updated);
                return;
            }
        }
        throw new java.io.IOException("the mapping of " + indexName + " is being changed concurrently; retry the write");
    }

    /** A document's dynamic mapping addition that the index's mapping refuses: the write fails, and retrying it as is cannot help. */
    static final class MappingConflictException extends java.io.IOException {
        MappingConflictException(String message, Exception cause) {
            super(message, cause);
        }
    }

    /** Strips the single mapping type core wraps a merged mapping in, as MappingUpdateHandler does. */
    static String unwrapMappingType(String source) throws java.io.IOException {
        final java.util.Map<String, Object> parsed = org.opensearch.common.xcontent.XContentHelper.convertToMap(
            new org.opensearch.core.common.bytes.BytesArray(source),
            false,
            org.opensearch.common.xcontent.XContentType.JSON
        ).v2();
        final Object inner = parsed.get(org.opensearch.index.mapper.MapperService.SINGLE_MAPPING_NAME);
        if (parsed.size() != 1 || inner instanceof java.util.Map == false) {
            return source;
        }
        try (var builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            @SuppressWarnings("unchecked")
            final java.util.Map<String, Object> body = (java.util.Map<String, Object>) inner;
            builder.map(body);
            return builder.toString();
        }
    }
}
