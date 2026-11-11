/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.shell.ServerlessNode;

import java.io.IOException;

/**
 * Single-document writes for {@link ShardOperations}: an index or a delete applied here, or forwarded to the node that
 * owns the shard.
 */
final class DocumentWrites {

    private final ServerlessNode node;
    private final RequestRouting routing;

    DocumentWrites(ServerlessNode node, RequestRouting routing) {
        this.node = node;
        this.routing = routing;
    }

    record NativeIndexResult(org.opensearch.action.index.IndexResponse response, String servedBy) {
    }

    /** Executes an admitted native index request without running its filters a second time. */
    NativeIndexResult executeNativeIndex(org.opensearch.action.index.IndexRequest accepted) throws IOException {
        final var invalid = accepted.validate();
        if (invalid != null) {
            throw invalid;
        }
        final boolean create = accepted.opType() == org.opensearch.action.DocWriteRequest.OpType.CREATE;
        validateNativeWrite(accepted.routing(), accepted.version(), accepted.versionType(), accepted.waitForActiveShards(), create);
        if (accepted.isRequireAlias()) {
            throw new UnsupportedOperationException("require_alias is not supported through the native Client");
        }
        final var descriptor = routing.describeOnce(accepted.index()).orElseThrow(() -> new NoSuchIndexException(accepted.index()));
        if (org.opensearch.serverless.ingest.IngestPipelines.pipelinesFor(accepted.getPipeline(), descriptor.extraSettings())
            .isEmpty() == false
            || (accepted.getFinalPipeline() != null && "_none".equals(accepted.getFinalPipeline()) == false)) {
            throw new UnsupportedOperationException("ingest pipelines require the REST write API; the native Client cannot apply them");
        }
        final String id = accepted.id() == null ? org.opensearch.common.UUIDs.base64UUID() : accepted.id();
        accepted.id(id);
        final String source = org.opensearch.common.xcontent.XContentHelper.convertToJson(
            accepted.source(),
            false,
            accepted.getContentType()
        );
        final boolean refresh = accepted.getRefreshPolicy() != org.opensearch.action.support.WriteRequest.RefreshPolicy.NONE;
        try (
            var ignored = node.indexingPressure()
                .markCoordinatingOperationStarted(org.opensearch.serverless.store.WalRecord.contentBytes(id, source), false)
        ) {
            final Written written = written(
                accepted.index(),
                id,
                source,
                refresh,
                false,
                accepted.ifSeqNo(),
                accepted.ifPrimaryTerm(),
                create
            );
            final var outcome = written.outcome();
            final var response = new org.opensearch.action.index.IndexResponse(
                written.shardId,
                id,
                outcome.seqNo(),
                outcome.primaryTerm(),
                outcome.version(),
                outcome.created()
            );
            completeNativeWriteResponse(response, refresh);
            return new NativeIndexResult(response, written.servedBy());
        }
    }

    static void validateNativeWrite(
        String routing,
        long version,
        org.opensearch.index.VersionType versionType,
        org.opensearch.action.support.ActiveShardCount activeShards,
        boolean create
    ) {
        if (routing != null) {
            throw new UnsupportedOperationException("routing is not supported: documents are placed by their id");
        }
        if (versionType != org.opensearch.index.VersionType.INTERNAL
            || (version != org.opensearch.common.lucene.uid.Versions.MATCH_ANY
                && (create == false || version != org.opensearch.common.lucene.uid.Versions.MATCH_DELETED))) {
            throw new UnsupportedOperationException("version/version_type is not supported; use if_seq_no and if_primary_term");
        }
        if (activeShards.validate(0) == false) {
            throw new UnsupportedOperationException("wait_for_active_shards cannot exceed one: shards have no replicas");
        }
    }

    static void completeNativeWriteResponse(org.opensearch.action.DocWriteResponse response, boolean refresh) {
        response.setForcedRefresh(refresh);
        response.setShardInfo(new org.opensearch.action.support.replication.ReplicationResponse.ShardInfo(1, 1));
    }

    boolean doDelete(String index, String id, boolean refresh) throws IOException {
        return written(index, id, "", refresh, true, org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO, 0L).outcome().found();
    }

    /** Deletes against the sequence identity a frozen view recorded, when it recorded one. */
    boolean doDeleteIfUnchanged(String index, String id, boolean refresh, long seqNo, long primaryTerm) throws IOException {
        if (seqNo < 0 || primaryTerm <= 0) {
            return doDelete(index, id, refresh);
        }
        return written(index, id, "", refresh, true, seqNo, primaryTerm).outcome().found();
    }

    /** Who served a write, and what the engine assigned it. */
    static final class Written {
        private final String servedBy;
        private final org.opensearch.serverless.shell.ServerlessNode.WriteOutcome outcome;
        final ShardId shardId;

        Written(String servedBy, org.opensearch.serverless.shell.ServerlessNode.WriteOutcome outcome, ShardId shardId) {
            this.servedBy = servedBy;
            this.outcome = outcome;
            this.shardId = shardId;
        }

        String servedBy() {
            return servedBy;
        }

        org.opensearch.serverless.shell.ServerlessNode.WriteOutcome outcome() {
            return outcome;
        }
    }

    Written written(String index, String id, String source, boolean refresh, boolean deletion, long ifSeqNo, long ifPrimaryTerm)
        throws IOException {
        return written(index, id, source, refresh, deletion, ifSeqNo, ifPrimaryTerm, false);
    }

    Written written(
        String index,
        String id,
        String source,
        boolean refresh,
        boolean deletion,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent
    ) throws IOException {
        final Placement placement = routing.place(index, id);
        if (placement.local() != null) {
            final var outcome = deletion
                ? node.delete(placement.local(), id, ifSeqNo, ifPrimaryTerm)
                : node.index(placement.local(), id, source, ifSeqNo, ifPrimaryTerm, requireAbsent);
            if (refresh) {
                node.reconciler().shard(placement.local()).refresh("serverless-ops-refresh");
            }
            return new Written(node.localNode().getId(), outcome, placement.local());
        }
        return forwardWritten(index, id, source, refresh, deletion, placement, ifSeqNo, ifPrimaryTerm, requireAbsent);
    }

    String forward(String index, String id, String source, boolean refresh, boolean deletion, Placement placement) throws IOException {
        return forwardWritten(
            index,
            id,
            source,
            refresh,
            deletion,
            placement,
            org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO,
            0L
        ).servedBy();
    }

    Written forwardWritten(
        String index,
        String id,
        String source,
        boolean refresh,
        boolean deletion,
        Placement placement,
        long ifSeqNo,
        long ifPrimaryTerm
    ) throws IOException {
        return forwardWritten(index, id, source, refresh, deletion, placement, ifSeqNo, ifPrimaryTerm, false);
    }

    Written forwardWritten(
        String index,
        String id,
        String source,
        boolean refresh,
        boolean deletion,
        Placement placement,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent
    ) throws IOException {
        return forwardWritten(index, id, source, refresh, deletion, placement, ifSeqNo, ifPrimaryTerm, requireAbsent, true);
    }

    Written forwardWritten(
        String index,
        String id,
        String source,
        boolean refresh,
        boolean deletion,
        Placement placement,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent,
        boolean mayRetry
    ) throws IOException {
        if (placement.owner() == null || placement.owner().equals(node.localNode().getId())) {
            throw routing.notHere(index, placement);
        }
        try {
            final var peer = node.router().peer(placement.owner());
            if (peer.isEmpty()) {
                throw new org.opensearch.serverless.transport.OwnerUnreachableException(
                    "shard " + placement.shard() + " of " + index + " is owned by " + placement.owner() + ", which has no reachable lease"
                );
            }
            final var ack = node.router()
                .forwardIndex(
                    peer.get(),
                    new org.opensearch.serverless.transport.ForwardedIndexRequest(
                        index,
                        placement.uuid(),
                        placement.shard(),
                        id,
                        source,
                        refresh,
                        deletion,
                        ifSeqNo,
                        ifPrimaryTerm,
                        requireAbsent
                    )
                );
            return new Written(
                ack.ownerNodeId(),
                new org.opensearch.serverless.shell.ServerlessNode.WriteOutcome(
                    ack.seqNo(),
                    ack.primaryTerm(),
                    ack.version(),
                    ack.created(),
                    ack.found()
                ),
                new ShardId(new org.opensearch.core.index.Index(index, placement.uuid()), placement.shard())
            );
        } catch (NotHereException e) {
            throw e;
        } catch (Exception e) {
            // Sorted by afterForwardFailure: a lost condition is rethrown as itself, an owner's own answer
            // keeps its status, a deadline is not retried, and a refusal or an unreachable owner costs one
            // register read and one more forward.
            return forwardWritten(
                index,
                id,
                source,
                refresh,
                deletion,
                routing.afterForwardFailure(index, placement, e, mayRetry),
                ifSeqNo,
                ifPrimaryTerm,
                requireAbsent,
                false
            );
        }
    }
}
