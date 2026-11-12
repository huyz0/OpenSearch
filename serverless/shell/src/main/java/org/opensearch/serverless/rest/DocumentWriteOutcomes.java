/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.shell.WriteOutcome;

/**
 * What a single-document write produced or why it was refused: the result with its routing facts, the typed
 * refusals, how a failed forward is classified, and the options a write may not carry. Split out of
 * {@link DocumentHandler}.
 */
final class DocumentWriteOutcomes {

    private DocumentWriteOutcomes() {}

    static void validateWriteOptions(
        long version,
        org.opensearch.index.VersionType versionType,
        org.opensearch.action.support.ActiveShardCount activeShards,
        boolean create
    ) {
        if (versionType != org.opensearch.index.VersionType.INTERNAL
            || version != org.opensearch.common.lucene.uid.Versions.MATCH_ANY
                && (create == false || version != org.opensearch.common.lucene.uid.Versions.MATCH_DELETED)) {
            throw new WriteRefusal(
                RestStatus.NOT_IMPLEMENTED,
                "unsupported_write",
                "version/version_type is not supported; use sequence conditions"
            );
        }
        if (activeShards.validate(0) == false) {
            throw new WriteRefusal(
                RestStatus.NOT_IMPLEMENTED,
                "unsupported_write",
                "wait_for_active_shards cannot exceed one: shards have no replicas"
            );
        }
    }

    /** The result and the routing facts that belong only to an actual execution. */
    static final class WriteResult {
        final ShardId shardId;
        final String id;
        final String owner;
        final boolean deletion;
        final WriteOutcome outcome;
        final boolean refreshed;
        final String droppedBy;
        final boolean requireAbsent;
        org.opensearch.action.DocWriteResponse response;

        WriteResult(
            ShardId shardId,
            String id,
            String owner,
            boolean deletion,
            WriteOutcome outcome,
            boolean refreshed,
            String droppedBy,
            boolean requireAbsent
        ) {
            this.shardId = shardId;
            this.id = id;
            this.owner = owner;
            this.deletion = deletion;
            this.outcome = outcome;
            this.refreshed = refreshed;
            this.droppedBy = droppedBy;
            this.requireAbsent = requireAbsent;
        }

        static WriteResult written(ShardId shardId, String id, String owner, boolean deletion, WriteOutcome outcome, boolean refreshed) {
            return new WriteResult(shardId, id, owner, deletion, outcome, refreshed, null, false);
        }

        static WriteResult dropped(ShardId shardId, String id, String pipeline, boolean requireAbsent) {
            return new WriteResult(shardId, id, null, false, null, false, pipeline, requireAbsent);
        }

        static WriteResult supplied(org.opensearch.action.DocWriteResponse response) {
            final var result = new WriteResult(null, null, null, false, null, false, null, false);
            result.response = response;
            return result;
        }

        org.opensearch.action.DocWriteResponse nativeResponse() {
            if (response == null) {
                if (droppedBy != null) {
                    // Core's single-item bulk adapter likewise uses UpdateResponse for an ingest NOOP.
                    response = new org.opensearch.action.update.UpdateResponse(
                        shardId,
                        id,
                        org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO,
                        0L,
                        requireAbsent
                            ? org.opensearch.common.lucene.uid.Versions.MATCH_DELETED
                            : org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
                        org.opensearch.action.DocWriteResponse.Result.NOOP
                    );
                    response.setShardInfo(new org.opensearch.action.support.replication.ReplicationResponse.ShardInfo(0, 0));
                } else {
                    response = deletion
                        ? new org.opensearch.action.delete.DeleteResponse(
                            shardId,
                            id,
                            outcome.seqNo(),
                            outcome.primaryTerm(),
                            outcome.version(),
                            outcome.found()
                        )
                        : new org.opensearch.action.index.IndexResponse(
                            shardId,
                            id,
                            outcome.seqNo(),
                            outcome.primaryTerm(),
                            outcome.version(),
                            outcome.created()
                        );
                    response.setShardInfo(new org.opensearch.action.support.replication.ReplicationResponse.ShardInfo(1, 1));
                    response.setForcedRefresh(refreshed);
                }
            }
            return response;
        }
    }

    static org.opensearch.serverless.shell.ActionGate.ResponseView<WriteResult> writeView(boolean deletion) {
        return new org.opensearch.serverless.shell.ActionGate.ResponseView<>() {
            @Override
            public org.opensearch.core.action.ActionResponse show(WriteResult result) {
                return result.nativeResponse();
            }

            @Override
            public WriteResult read(org.opensearch.core.action.ActionResponse response, WriteResult original) {
                final boolean supported = deletion
                    ? response instanceof org.opensearch.action.delete.DeleteResponse
                    : response instanceof org.opensearch.action.index.IndexResponse
                        || response instanceof org.opensearch.action.update.UpdateResponse update
                            && update.getResult() == org.opensearch.action.DocWriteResponse.Result.NOOP;
                if (supported == false) {
                    throw new IllegalStateException("a write filter supplied an incompatible response: " + response.getClass().getName());
                }
                return original != null && original.response == response
                    ? original
                    : WriteResult.supplied((org.opensearch.action.DocWriteResponse) response);
            }
        };
    }

    static final class WriteRefusal extends org.opensearch.OpenSearchStatusException {
        final String type;

        WriteRefusal(RestStatus status, String type, String reason) {
            super(reason, status);
            this.type = type;
        }
    }

    static WriteRefusal notWriterRefusal(String index, int shard, String owner) {
        final String reason = owner == null
            ? "no node currently owns shard " + shard + " of " + index + "; activate it before writing"
            : "this node does not own shard " + shard + " of " + index + "; it is owned by " + owner;
        return new WriteRefusal(RestStatus.MISDIRECTED_REQUEST, "not_the_writer", reason);
    }

    static WriteRefusal activationRefusal(String index, int shard) {
        return new WriteRefusal(
            RestStatus.SERVICE_UNAVAILABLE,
            "activation_in_progress",
            "this node is acquiring shard " + shard + " of " + index + "; retry"
        );
    }

    static Exception classifyForwardFailure(ServerlessNode serving, String index, int shard, String owner, Throwable cause) {
        final var kind = org.opensearch.serverless.transport.ForwardFailure.classify(cause);
        if (kind.castsDoubtOnOwnership()) {
            serving.signals().ownershipDoubted(index, shard);
        }
        return switch (kind) {
            case CONFLICT -> asException(
                org.opensearch.ExceptionsHelper.unwrap(cause, org.opensearch.index.engine.VersionConflictEngineException.class)
            );
            case REJECTED -> asException(
                org.opensearch.ExceptionsHelper.unwrap(cause, org.opensearch.core.concurrency.OpenSearchRejectedExecutionException.class)
            );
            case REMOTE -> org.opensearch.serverless.transport.ForwardFailure.isClientStatus(cause)
                ? asException(org.opensearch.serverless.transport.ForwardFailure.answer(cause))
                : new WriteRefusal(
                    RestStatus.SERVICE_UNAVAILABLE,
                    "forward_failed",
                    org.opensearch.serverless.transport.ForwardFailure.describe(owner, cause)
                );
            default -> cause instanceof org.opensearch.serverless.transport.OwnerUnreachableException
                ? new WriteRefusal(RestStatus.SERVICE_UNAVAILABLE, "owner_unreachable", cause.getMessage())
                : new WriteRefusal(
                    RestStatus.SERVICE_UNAVAILABLE,
                    "forward_failed",
                    org.opensearch.serverless.transport.ForwardFailure.describe(owner, cause)
                );
        };
    }

    /**
     * A forward that failed, and which node it was sent to.
     *
     * <p>Only transport failures receive this wrapper. A filter refusal occurs outside forwarding and
     * cannot be mistaken for a stale owner hint or an instruction to retry elsewhere.
     */
    static final class ForwardFailed extends RuntimeException {

        final String target;

        ForwardFailed(String target, Throwable cause) {
            super(cause.getMessage(), cause);
            this.target = target;
        }

        String target() {
            return target;
        }
    }

    /**
     * Whether a forward failed because the node it reached does not hold the shard as its writer.
     *
     * <p>Typed, not phrased: the check is {@link org.opensearch.serverless.transport.NotShardOwnerException},
     * which the owner throws and which carries a token no other message does. This used to search every
     * cause's message for "does not own", so a security plugin refusing with "user does not own index
     * alpha" re-routed the write instead of reaching the caller as a 403.
     *
     * @param e what the forward threw
     * @return true for the owner's own refusal
     */
    public static boolean refusedAsNotOwner(Exception e) {
        return org.opensearch.serverless.transport.NotShardOwnerException.describes(e);
    }

    static Exception asException(Throwable failure) {
        return failure instanceof Exception e ? e : new RuntimeException(failure);
    }
}
