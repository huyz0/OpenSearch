/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

/**
 * Reads documents for {@link ServerlessNode}: get and explain against a shard held here.
 */
public final class NodeDocumentReads {

    private final ServerlessNode node;

    NodeDocumentReads(ServerlessNode node) {
        this.node = node;
    }

    /**
     * Explains a document using this incarnation's current mapping and records the shard as in use.
     *
     * @param shardId the shard open on this node
     * @param id the document id
     * @param query the query to explain
     * @return whether the document exists and Lucene's explanation
     * @throws java.io.IOException if the shard or its descriptor cannot be read
     */
    public org.opensearch.serverless.shard.ShardExplain.Outcome explain(
        org.opensearch.core.index.shard.ShardId shardId,
        String id,
        org.opensearch.index.query.QueryBuilder query
    ) throws java.io.IOException {
        node.ensureStarted();
        if (node.reconciler.shard(shardId) == null) {
            throw new java.io.IOException("cannot explain from " + shardId + ": not open on " + node.nodeName);
        }
        node.shards().markUsed(shardId);
        node.fences().ensureIncarnationLive(shardId);
        final var outcome = org.opensearch.serverless.shard.ShardExplain.execute(node.searchService(), shardId, id, query);
        node.fences().ensureIncarnationLive(shardId);
        return outcome;
    }

    /**
     * Reads one document by id, from this node's own copy of the shard.
     *
     * <p><b>Realtime, and that is the whole reason this exists separately from search.</b> A write is
     * applied to the engine and durable in the log before it is acknowledged, but it is not searchable
     * until a refresh and not in the object store until a publish. A get that went through the search
     * path would therefore fail to find a document the caller had just been told was written — which
     * reads as data loss and is not. This reads the live version map, so it sees the write.
     *
     * <p>Whether the answer is realtime depends on which copy this is, and the caller is the one that
     * knows: a writer's copy includes everything acknowledged, a reader's copy includes only what was
     * published. This method does not choose between them — {@code GetHandler} does, and says which it
     * used in the response.
     *
     * @param shardId the shard, open on this node
     * @param id the document id
     * @return what was found, which may be nothing
     * @throws java.io.IOException if the shard is not open here
     */
    public Document get(org.opensearch.core.index.shard.ShardId shardId, String id) throws java.io.IOException {
        node.ensureStarted();
        final var shard = node.reconciler.shard(shardId);
        if (shard == null) {
            throw new java.io.IOException("cannot read from " + shardId + ": not open on " + node.nodeName);
        }
        node.shards().markUsed(shardId);
        node.fences().ensureIncarnationLive(shardId);
        final var result = shard.getService()
            .get(
                id,
                null,
                true,
                org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
                org.opensearch.index.VersionType.INTERNAL,
                org.opensearch.search.fetch.subphase.FetchSourceContext.FETCH_SOURCE
            );
        // No searcher to release here: ShardGetService acquires and closes its own, and GetResult is a
        // value rather than a handle.
        // Before answering, found or not: a document from a deleted incarnation and a 404 from one are both
        // answers about an index that is gone. See INCARNATION_FENCE_MILLIS.
        node.fences().ensureIncarnationLive(shardId);
        if (result.isExists() == false) {
            return Document.absent(id);
        }
        // The sequence identity comes back with the document, and always did -- ShardGetService reads it
        // out of the same doc-values lookup that finds the source. It was simply dropped here, which is
        // why a caller could never obtain the token it needed to send back as if_seq_no.
        final org.opensearch.common.document.DocumentField routing = result.field(org.opensearch.index.mapper.RoutingFieldMapper.NAME);
        return new Document(id, true, result.sourceAsString(), result.getSeqNo(), result.getPrimaryTerm(), result.getVersion()).withRouting(
            routing == null ? null : routing.getValue()
        );
    }
}
