/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.transport;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.shard.ShardExplain;
import org.opensearch.serverless.shard.ShardQuery;
import org.opensearch.transport.TransportChannel;

/**
 * Answers the work other nodes forward to this one through {@link ShardRouter}: checks each request's MAC and
 * origin, confirms this node still owns the shard, and runs it.
 */
final class ForwardedWorkHandlers {

    private final ShardRouter router;

    ForwardedWorkHandlers(ShardRouter router) {
        this.router = router;
    }

    void handleStats(ForwardedStatsRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        router.requireMac(ForwardedStatsRequest.ACTION, request);
        final ForwardedStatsResponse response = router.node.services()
            .actionGate()
            .run(
                org.opensearch.action.admin.cluster.node.stats.NodesStatsAction.NAME,
                new org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest(router.node.localNode().getId()).all(),
                admitted -> {
                    if ((admitted instanceof org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest) == false
                        || java.util.Arrays.equals(
                            ((org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest) admitted).nodesIds(),
                            new String[] { router.node.localNode().getId() }
                        ) == false) {
                        throw new UnsupportedOperationException("a forwarded stats request can only describe its receiving node");
                    }
                    // The same document as the REST endpoint, authorized by this receiving node's filters.
                    try (var builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
                        builder.startObject();
                        org.opensearch.serverless.rest.StatsHandler.describe(builder, router.node, router.plane.get());
                        builder.endObject();
                        return new ForwardedStatsResponse(
                            router.node.localNode().getId(),
                            org.opensearch.core.common.bytes.BytesReference.bytes(builder).utf8ToString()
                        );
                    }
                },
                org.opensearch.serverless.shell.ActionGate.opaque()
            );
        channel.sendResponse(response);
    }

    void handleIndex(ForwardedIndexRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        router.requireMac(ForwardedIndexRequest.ACTION, request);
        router.requirePluginOriginForSystemIndex(request.index());
        ShardId shardId = router.writerShard(request.index(), request.shard(), request.indexUuid());
        if (shardId == null && request.isSteered()) {
            // Sent here to be taken, by a sender nearly at its cap: take it, as for a write that arrived here.
            shardId = takeForSteeredWrite(request);
        }
        // Accounted on the owner as a primary operation, the way core accounts a write that arrived from
        // a coordinating node: a flood of forwarded writes used to be invisible to this node's pressure.
        try (
            org.opensearch.common.lease.Releasable primary = router.node.services()
                .indexingPressure()
                .markPrimaryOperationStarted(org.opensearch.serverless.store.WalRecord.contentBytes(request.id(), request.source()), false)
        ) {
            handleIndexAccounted(request, channel, shardId);
        }
    }

    /** Takes the shard a steered write is for, if nobody else has; null if it was not taken here. */
    ShardId takeForSteeredWrite(ForwardedIndexRequest request) {
        try {
            router.node.signals()
                .activate(request.index(), request.shard())
                .get(ShardRouter.STEERED_ACTIVATION_WAIT_MILLIS, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            // Refused below as not held here: the sender takes it itself.
            return null;
        }
        return router.writerShard(request.index(), request.shard(), request.indexUuid());
    }

    void handleIndexAccounted(ForwardedIndexRequest request, TransportChannel channel, ShardId shardId) throws Exception {
        if (shardId == null) {
            // Ownership moved between the sender reading the head and this arriving. Failing is right:
            // the sender re-reads and retries, and accepting a write for a shard we do not own is the
            // one outcome that would be unsafe. Typed, so the sender can tell this refusal from any other
            // failure without reading prose. Matched as a writer and by uuid: this used to match by name
            // alone, so a shard held here only as a search reader took the write and refused it with a
            // message the sender did not recognise, and a shard of a deleted index of the same name took
            // the write and acknowledged it.
            throw new NotShardOwnerException(
                request.index(),
                request.shard(),
                router.whyNotHeld(request.index(), request.shard(), request.indexUuid())
            );
        }
        router.requireLiveOwnership(shardId);
        final org.opensearch.serverless.shell.WriteOutcome outcome;
        if (request.deletion()) {
            outcome = router.node.writes().delete(shardId, request.id(), request.ifSeqNo(), request.ifPrimaryTerm());
        } else {
            // requireAbsent travels on the wire and used to be dropped here, so a forwarded _create was an
            // overwrite -- and a data-stream write, which is create-only, was one too.
            outcome = router.node.writes()
                .index(shardId, request.id(), request.source(), request.ifSeqNo(), request.ifPrimaryTerm(), request.requireAbsent());
        }
        if (request.refresh()) {
            router.node.reconciler().shard(shardId).refresh("serverless-forwarded-refresh");
        }
        channel.sendResponse(
            new ForwardedIndexResponse(
                router.node.localNode().getId(),
                outcome.seqNo(),
                outcome.primaryTerm(),
                outcome.version(),
                outcome.created(),
                outcome.found()
            )
        );
    }

    void handleBulk(ForwardedBulkRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        router.requireMac(ForwardedBulkRequest.ACTION, request);
        router.requirePluginOriginForSystemIndex(request.index());
        final ShardId shardId = router.writerShard(request.index(), request.shard(), request.indexUuid());
        // Counted by the request as it was built, not by re-encoding every record here to measure it.
        try (
            org.opensearch.common.lease.Releasable primary = router.node.services()
                .indexingPressure()
                .markPrimaryOperationStarted(request.bytes(), false)
        ) {
            handleBulkAccounted(request, channel, shardId);
        }
    }

    void handleBulkAccounted(ForwardedBulkRequest request, TransportChannel channel, ShardId shardId) throws Exception {
        if (shardId == null) {
            // Same refusal as a forwarded single write, and for the same reason: ownership moved between
            // the sender reading the head and this arriving. Refusing the whole batch is right -- every
            // item in it routes to this one shard, so there is no partial answer to give.
            throw new NotShardOwnerException(
                request.index(),
                request.shard(),
                router.whyNotHeld(request.index(), request.shard(), request.indexUuid())
            );
        }
        router.requireLiveOwnership(shardId);
        final var outcomes = router.node.writes().bulkOperations(shardId, request.batch());
        if (request.refresh()) {
            router.node.reconciler().shard(shardId).refresh("serverless-forwarded-refresh");
        }
        channel.sendResponse(new ForwardedBulkResponse(router.node.localNode().getId(), outcomes));
    }

    void handleGet(ForwardedGetRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        router.requireMac(ForwardedGetRequest.ACTION, request);
        final ShardId shardId = router.writerShard(request.index(), request.shard(), request.indexUuid());
        if (shardId == null) {
            // Refuse rather than open it as a reader, which is what handleSearch does here and is right
            // there. A get was forwarded to this node precisely because the shard-head named it the
            // owner; if the shard is not open as a writer, that head is stale, and answering from a
            // published commit would return a stale document to a caller that asked for a fresh one.
            // Failing sends the caller back to re-read the head, which is the only thing that fixes it.
            throw new NotShardOwnerException(
                request.index(),
                request.shard(),
                router.whyNotHeld(request.index(), request.shard(), request.indexUuid())
            );
        }
        channel.sendResponse(new ForwardedGetResponse(router.node.localNode().getId(), router.node.reads().get(shardId, request.id())));
    }

    void handleExplain(ForwardedExplainRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        router.requireMac(ForwardedExplainRequest.ACTION, request);
        final ShardId shardId = router.writerShard(request.index(), request.shard(), request.indexUuid());
        if (shardId == null) {
            // Refused rather than opened as a reader, following handleGet and not handleSearch. An explain
            // was forwarded here because the shard-head named this node the owner; if the shard is not open
            // as a writer that head is stale, and explaining from a published commit would score against
            // segment statistics that are missing every unpublished write. The number would look right.
            throw new NotShardOwnerException(
                request.index(),
                request.shard(),
                router.whyNotHeld(request.index(), request.shard(), request.indexUuid())
            );
        }
        final ShardExplain.Outcome outcome = router.node.reads().explain(shardId, request.id(), request.query());
        channel.sendResponse(new ForwardedExplainResponse(router.node.localNode().getId(), outcome.exists(), outcome.explanation()));
    }

    void handleSearch(ForwardedSearchRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        router.requireMac(ForwardedSearchRequest.ACTION, request);
        ShardId shardId = router.localShard(request.index(), request.shard(), request.indexUuid());
        final boolean openedForThis = shardId == null;
        if (shardId == null) {
            // Open it. This is the property that makes placement a hint rather than a requirement: a
            // node asked for a shard it does not have fetches the manifest and serves, so a stale or
            // unlucky routing decision costs a cold read and never a wrong answer.
            shardId = router.node.readers().serveAsReader(router.plane.get(), request.index(), request.shard(), request.indexUuid());
        } else {
            // Open already, perhaps as a reader whose writer has since died: checked like a fresh one.
            router.node.readers().ensureReaderCurrent(router.plane.get(), request.index(), request.shard(), shardId);
        }
        if (openedForThis) {
            // Opened now, so used now; one already held is used only if it matches, below -- see SearchFanout.
            router.node.shards().markUsed(shardId);
        }
        // Before the query too, so it runs on the mapping in force now; see WriteFences#ensureIncarnationLive.
        router.node.fences().ensureIncarnationLive(shardId);
        // The coordinator's instant for "now", so this shard scores against the same clock as every other
        // shard of the same search. Entered and exited around the query so the reconciler cannot release
        // the reader from under a running query.
        router.node.reconciler().enter(shardId);
        final ShardQuery.Result result;
        try {
            result = ShardQuery.execute(router.node.searchService(), shardId, request.source(), request.nowInMillis());
        } finally {
            router.node.reconciler().exit(shardId);
        }
        router.node.fences().ensureIncarnationLive(shardId);
        if (result.total() > 0) {
            router.node.shards().markUsed(shardId);
        }
        channel.sendResponse(new ForwardedSearchResponse(result));
    }

    void handleFrozenSearch(ForwardedFrozenSearchRequest request, TransportChannel channel, org.opensearch.tasks.Task task)
        throws Exception {
        router.requireMac(ForwardedFrozenSearchRequest.ACTION, request);
        // No "not held" refusal here, unlike handleIndex/handleGet: a view has no owner to be stale about,
        // and openFrozenView is idempotent -- opening it here for the first time is exactly what placement
        // being a hint means. plane.get() rather than the request: the view travelled the wire, but which
        // object store to open its files from is this node's own configuration, never the sender's.
        final ShardId shardId = router.node.activation().openFrozenView(router.plane.get(), request.pit(), request.shard());
        router.node.reconciler().enter(shardId);
        final ShardQuery.Result result;
        try {
            result = ShardQuery.execute(router.node.searchService(), shardId, request.source(), request.nowInMillis());
        } finally {
            router.node.reconciler().exit(shardId);
        }
        channel.sendResponse(new ForwardedSearchResponse(result));
    }
}
