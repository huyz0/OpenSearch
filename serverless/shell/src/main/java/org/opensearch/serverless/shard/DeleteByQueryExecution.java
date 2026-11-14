/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;

/**
 * The walk behind {@link ShardOperations#deleteByQuery}: page through each shard's matches as a search sees them, and delete
 * each one through the ordinary write path, conditional on the sequence identity the page saw.
 */
final class DeleteByQueryExecution {

    private final ServerlessNode node;
    private final MetadataPlane plane;
    private final boolean refuseSystemIndices;
    private final RequestRouting routing;
    private final DocumentWrites writes;

    DeleteByQueryExecution(
        ServerlessNode node,
        MetadataPlane plane,
        boolean refuseSystemIndices,
        RequestRouting routing,
        DocumentWrites writes
    ) {
        this.node = node;
        this.plane = plane;
        this.refuseSystemIndices = refuseSystemIndices;
        this.routing = routing;
        this.writes = writes;
    }

    /** How many matches are fetched per page, per shard. */
    static final int DELETE_BY_QUERY_BATCH_SIZE = 1000;

    /** What a walk does to one matched document: true if it changed it, false if it left it as it was. */
    @FunctionalInterface
    interface HitChange {
        boolean apply(String id, long seqNo, long primaryTerm, boolean refresh) throws java.io.IOException;
    }

    /** What a walk did: documents visited, changed and left alone, conflicts, and the conflict that stopped it, if one did. */
    record Walk(long visited, long changed, long noops, long conflicts, String abortedOn,
        org.opensearch.index.engine.VersionConflictEngineException conflict, int batches) {
    }

    DeleteByQueryOutcome doDeleteByQuery(
        String index,
        org.opensearch.index.query.QueryBuilder query,
        long maxDocs,
        boolean refresh,
        boolean abortOnConflict
    ) throws Exception {
        final Walk walk = walk(
            index,
            query,
            maxDocs,
            refresh,
            abortOnConflict,
            (id, seqNo, primaryTerm, refreshNow) -> writes.doDeleteIfUnchanged(index, id, refreshNow, seqNo, primaryTerm)
        );
        return new DeleteByQueryOutcome(
            walk.visited(),
            walk.changed(),
            walk.conflicts(),
            walk.abortedOn(),
            null,
            walk.conflict(),
            walk.batches()
        );
    }

    /**
     * Pages through each shard's matches as a search sees them now, and applies a change to each one, conditional on the
     * sequence identity the page saw: a document rewritten after it was read is a conflict, never overwritten.
     *
     * <p>Core's by-query works through a snapshot of what each shard had refreshed when it started. The walk here gets the
     * same set without holding a reader open: each shard's highest visible sequence number is read first, and the shard is
     * then paged in sequence-number order up to it. A document written or changed after that has a higher number and is not
     * matched -- the documents this operation changes among them, so it never visits its own writes -- and pages after one
     * another stay in order while the shard changes. Each page is an ordinary search of one shard, answered where the shard is
     * written (or forwarded there), so a document refreshed but not yet published is matched, as in core.
     */
    Walk walk(
        String index,
        org.opensearch.index.query.QueryBuilder query,
        long maxDocs,
        boolean refresh,
        boolean abortOnConflict,
        HitChange change
    ) throws Exception {
        if (refuseSystemIndices && node.isSystemIndex(index)) {
            throw new SystemIndexException(index);
        }
        final IndexDescriptor descriptor = routing.describeOnce(index).orElseThrow(() -> new NoSuchIndexException(index));
        final java.util.Map<String, IndexDescriptor> indices = java.util.Map.of(index, descriptor);
        long visited = 0;
        long deleted = 0;
        long noops = 0;
        long conflicts = 0;
        int batches = 0;
        for (int shard : descriptor.unitIds()) {
            if (visited >= maxDocs) {
                break;
            }
            final var onlyThisShard = org.opensearch.serverless.rest.SearchFanout.Options.DEFAULT.withOnlyShards(java.util.Set.of(shard));
            final var newest = searchOne(
                indices,
                new org.opensearch.search.builder.SearchSourceBuilder().query(query)
                    .sort(new org.opensearch.search.sort.FieldSortBuilder(SEQ_NO).order(org.opensearch.search.sort.SortOrder.DESC))
                    .size(1)
                    .trackTotalHits(false)
                    .fetchSource(false),
                onlyThisShard
            );
            if (newest.isEmpty()) {
                // Nothing on this shard matches now.
                continue;
            }
            final long upTo = (Long) newest.get(0).getSortValues()[0];
            final var bounded = org.opensearch.index.query.QueryBuilders.boolQuery()
                .filter(query)
                .filter(org.opensearch.index.query.QueryBuilders.rangeQuery(SEQ_NO).lte(upTo));
            Object[] cursor = null;
            while (visited < maxDocs) {
                final var page = new org.opensearch.search.builder.SearchSourceBuilder().query(bounded)
                    .sort(new org.opensearch.search.sort.FieldSortBuilder(SEQ_NO).order(org.opensearch.search.sort.SortOrder.ASC))
                    .size((int) Math.min(DELETE_BY_QUERY_BATCH_SIZE, maxDocs - visited))
                    .trackTotalHits(false)
                    // The sequence number each change is conditional on.
                    .seqNoAndPrimaryTerm(true)
                    .fetchSource(false);
                if (cursor != null) {
                    page.searchAfter(cursor);
                }
                final var hits = searchOne(indices, page, onlyThisShard);
                if (hits.isEmpty()) {
                    break;
                }
                batches = Math.incrementExact(batches);
                final boolean lastPageOfShard = hits.size() < page.size() || visited + hits.size() >= maxDocs;
                long pageBytes = 0L;
                for (var hit : hits) {
                    pageBytes += 64L + (hit.getId() == null ? 0 : hit.getId().length());
                }
                // A page of changes is a page of writes, and accounted as one.
                try (
                    org.opensearch.common.lease.Releasable inFlight = node.services()
                        .indexingPressure()
                        .markCoordinatingOperationStarted(pageBytes, false)
                ) {
                    for (int i = 0; i < hits.size(); i++) {
                        final var hit = hits.get(i);
                        visited++;
                        // The refresh -- when asked for -- rides the last change of the shard, not every one.
                        final boolean isLastOfShard = refresh && lastPageOfShard && i == hits.size() - 1;
                        try {
                            if (change.apply(hit.getId(), hit.getSeqNo(), hit.getPrimaryTerm(), isLastOfShard)) {
                                deleted++;
                            } else {
                                noops++;
                            }
                        } catch (org.opensearch.index.engine.VersionConflictEngineException e) {
                            // Rewritten -- or already removed -- after the page was read. Left alone, and counted.
                            conflicts++;
                            if (abortOnConflict) {
                                return new Walk(visited, deleted, noops, conflicts, hit.getId(), e, batches);
                            }
                        }
                    }
                }
                cursor = hits.get(hits.size() - 1).getSortValues();
                if (lastPageOfShard) {
                    break;
                }
            }
        }
        return new Walk(visited, deleted, noops, conflicts, null, null, batches);
    }

    private static final String SEQ_NO = org.opensearch.index.mapper.SeqNoFieldMapper.NAME;

    /** One page from one shard, as a search sees it; a shard that cannot answer fails the operation rather than being skipped. */
    private java.util.List<org.opensearch.search.SearchHit> searchOne(
        java.util.Map<String, IndexDescriptor> indices,
        org.opensearch.search.builder.SearchSourceBuilder page,
        org.opensearch.serverless.rest.SearchFanout.Options options
    ) throws Exception {
        final var outcome = org.opensearch.serverless.rest.SearchFanout.run(node, plane, indices, page, options);
        if (outcome.failures().isEmpty() == false) {
            final Throwable cause = outcome.failures().get(0).getCause();
            if (cause instanceof Exception failure) {
                throw failure;
            }
            throw new java.io.IOException("a shard could not be searched", cause);
        }
        return outcome.hits();
    }
}
