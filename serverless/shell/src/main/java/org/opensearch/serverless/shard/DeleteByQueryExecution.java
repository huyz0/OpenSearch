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
 * The walk behind {@link ShardOperations#deleteByQuery}: freeze a view, page through each shard's matches, and delete
 * each one through the ordinary write path, conditional on the sequence identity the view saw.
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

    /** How many matches are fetched per page, per shard, while walking a frozen view. */
    static final int DELETE_BY_QUERY_BATCH_SIZE = 1000;

    /** How long the frozen view a delete-by-query takes is held. Bounded, not indefinite. */
    static final long DELETE_BY_QUERY_KEEP_ALIVE_MILLIS = 3_600_000L;

    DeleteByQueryOutcome doDeleteByQuery(
        String index,
        org.opensearch.index.query.QueryBuilder query,
        long maxDocs,
        boolean refresh,
        boolean abortOnConflict
    ) throws Exception {
        if (refuseSystemIndices && node.isSystemIndex(index)) {
            throw new SystemIndexException(index);
        }
        final IndexDescriptor descriptor = routing.describeOnce(index).orElseThrow(() -> new NoSuchIndexException(index));

        // A frozen view, exactly as _pit takes one -- see the class javadoc for why the query is decided
        // once rather than re-evaluated as the walk goes.
        // The capture marker before the first manifest read, so the commits this is about to freeze
        // cannot be swept while it is reading them. See PointInTime#isCapturing.
        final String pitId = org.opensearch.common.UUIDs.randomBase64UUID();
        final long expiresAt = plane.clock().getAsLong() + DELETE_BY_QUERY_KEEP_ALIVE_MILLIS;
        plane.beginPointInTime(
            new org.opensearch.serverless.metadata.PointInTime(pitId, index, descriptor.uuid(), expiresAt, java.util.Map.of(), true)
        );

        final java.util.Map<Integer, org.opensearch.serverless.store.CommitManifest> shards = new java.util.LinkedHashMap<>();
        for (int shard = 0; shard < descriptor.numberOfShards(); shard++) {
            final var manifest = plane.segmentPublisher(index, descriptor.uuid(), shard).readManifest();
            if (manifest.isEmpty()) {
                // Nothing published on this shard: nothing to match, and nothing silently skipped either
                // -- an unpublished shard has no documents a search could have found.
                continue;
            }
            shards.put(shard, manifest.get());
        }
        if (shards.isEmpty()) {
            // Nothing to freeze, so the marker must not be left pinning the index's sweep.
            plane.releasePointInTime(pitId);
            return new DeleteByQueryOutcome(0, 0);
        }

        final var pit = new org.opensearch.serverless.metadata.PointInTime(pitId, index, descriptor.uuid(), expiresAt, shards);
        plane.finishPointInTime(pit);
        try {
            long visited = 0;
            long deleted = 0;
            long conflicts = 0;
            int batches = 0;
            for (Integer shard : shards.keySet()) {
                final var frozenShardId = node.openFrozenView(plane, pit, shard);
                Object[] cursor = null;
                while (visited < maxDocs) {
                    final var page = new org.opensearch.search.builder.SearchSourceBuilder().query(query)
                        .sort(new org.opensearch.search.sort.FieldSortBuilder(org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME))
                        .size((int) Math.min(DELETE_BY_QUERY_BATCH_SIZE, maxDocs - visited))
                        .trackTotalHits(false)
                        // The sequence number each delete is conditional on.
                        .seqNoAndPrimaryTerm(true)
                        .fetchSource(false);
                    if (cursor != null) {
                        page.searchAfter(cursor);
                    }
                    final var result = org.opensearch.serverless.shard.ShardQuery.execute(node.searchService(), frozenShardId, page);
                    final var hits = result.hits();
                    if (hits.isEmpty()) {
                        break;
                    }
                    batches = Math.incrementExact(batches);
                    final boolean lastPageOfShard = hits.size() < page.size() || visited + hits.size() >= maxDocs;
                    long pageBytes = 0L;
                    for (var hit : hits) {
                        pageBytes += 64L + (hit.getId() == null ? 0 : hit.getId().length());
                    }
                    // A page of deletes is a page of writes, and accounted as one: this operation used to
                    // be the only writer on the node that indexing pressure could not see.
                    try (
                        org.opensearch.common.lease.Releasable inFlight = node.indexingPressure()
                            .markCoordinatingOperationStarted(pageBytes, false)
                    ) {
                        for (int i = 0; i < hits.size(); i++) {
                            final var hit = hits.get(i);
                            visited++;
                            // The refresh -- when asked for -- rides the last delete of the shard, not every
                            // one: refreshing per document would refresh the shard once per document instead
                            // of once for the whole operation.
                            final boolean isLastOfShard = refresh && lastPageOfShard && i == hits.size() - 1;
                            try {
                                if (writes.doDeleteIfUnchanged(index, hit.getId(), isLastOfShard, hit.getSeqNo(), hit.getPrimaryTerm())) {
                                    deleted++;
                                }
                            } catch (org.opensearch.index.engine.VersionConflictEngineException e) {
                                // Rewritten -- or already removed -- after the view was taken. Left alone,
                                // and counted, rather than deleted regardless.
                                conflicts++;
                                if (abortOnConflict) {
                                    return new DeleteByQueryOutcome(visited, deleted, conflicts, hit.getId(), null, e, batches);
                                }
                            }
                        }
                    }
                    cursor = hits.get(hits.size() - 1).getSortValues();
                    if (lastPageOfShard) {
                        // Fewer than a full page, or the cap was reached: this shard has nothing left to
                        // give (or nothing more is wanted), and asking again would cost a round trip to
                        // learn what this already knows.
                        break;
                    }
                }
                // A shard with no match never entered the loop above, so it never issued a delete and
                // never needed a refresh -- correctly, since nothing changed on it to make visible.
            }
            return new DeleteByQueryOutcome(visited, deleted, conflicts, null, null, null, batches);
        } finally {
            // Local shards first, then the record -- the same order _pit's own release does, so this node
            // is never left holding open frozen shards for a view no longer recorded anywhere.
            node.reconciler().closeFrozenReader(pit.id());
            plane.releasePointInTime(pit.id());
        }
    }
}
