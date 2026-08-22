/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Names a real index this operator has already shrunk (rfc-serverless-opensearch.md &sect;16 Phase 5)
 * and now wants retired -- {@link RetireShrinkSourceAction}'s own explicit, opt-in second half of
 * {@code ShardShrinker#shrink}'s own "no source shard is touched, deleted, or otherwise modified"
 * contract. {@link #targetIndexUuid}/{@link #targetShardId} are the safety proof: the caller states
 * which shrink target it believes {@link #sourceIndexName} was merged into, and {@link
 * TransportRetireShrinkSourceAction} verifies that target genuinely has a published manifest before
 * ever deleting anything -- deliberately not a bare {@code DELETE /source-index}, the same "never
 * auto-deletes anything it didn't itself just create" caution {@code ShardCloner#deleteClone}
 * already applies, applied here as "never deletes anything without first confirming its own
 * replacement genuinely exists."
 *
 * <p>This same action is also documented (see {@link
 * org.opensearch.serverless.storage.resharding.SourceSplitFenceMetadata}, {@link
 * TransportOrchestrateShardSplitAction}) as the eventual deleter of a resharding-by-copy split's
 * source, once {@link FenceSplitSourceAction} has fenced it. But nothing about this request lets
 * {@link TransportRetireShrinkSourceAction} tell a genuine, never-split shrink source apart from a
 * split source that simply hasn't been fenced yet -- both look identical (no distinguishing
 * {@code IndexMetadata} state exists before fencing). {@link #acknowledgeUnfencedSource} closes
 * that gap: retiring a source that isn't currently fenced requires this flag to be explicitly
 * {@code true}, forcing the same "explicit, separate operator decision" this class already demands
 * for the deletion itself, rather than silently allowing an automated or mistaken call to delete a
 * split source that's still receiving direct writes.
 */
public class RetireShrinkSourceRequest extends ActionRequest {

    private final String sourceIndexName;
    private final String targetIndexUuid;
    private final int targetShardId;
    private final boolean acknowledgeUnfencedSource;

    /**
     * Creates a request.
     *
     * @param sourceIndexName the real index name to delete, once verified safe.
     * @param targetIndexUuid the shrink target's index UUID the caller believes {@code sourceIndexName} was merged into.
     * @param targetShardId the shard number within {@code targetIndexUuid}.
     * @param acknowledgeUnfencedSource must be {@code true} if {@code sourceIndexName} is not currently fenced (see
     *                {@link org.opensearch.serverless.storage.resharding.SourceSplitFenceMetadata}) -- required so an
     *                automated or mistaken retirement of a not-yet-fenced split source fails loudly instead of silently
     *                deleting data that may still be written directly. Genuine shrink sources (never part of a split)
     *                are never fenced, so this must always be set for them once the caller has independently confirmed
     *                writes have stopped.
     */
    public RetireShrinkSourceRequest(String sourceIndexName, String targetIndexUuid, int targetShardId, boolean acknowledgeUnfencedSource) {
        this.sourceIndexName = sourceIndexName;
        this.targetIndexUuid = targetIndexUuid;
        this.targetShardId = targetShardId;
        this.acknowledgeUnfencedSource = acknowledgeUnfencedSource;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link RetireShrinkSourceRequest}.
     */
    public RetireShrinkSourceRequest(StreamInput in) throws IOException {
        super(in);
        this.sourceIndexName = in.readString();
        this.targetIndexUuid = in.readString();
        this.targetShardId = in.readVInt();
        this.acknowledgeUnfencedSource = in.readBoolean();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sourceIndexName);
        out.writeString(targetIndexUuid);
        out.writeVInt(targetShardId);
        out.writeBoolean(acknowledgeUnfencedSource);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sourceIndexName == null || sourceIndexName.isEmpty()) {
            validationException = addValidationError("sourceIndexName is required", validationException);
        }
        if (targetIndexUuid == null || targetIndexUuid.isEmpty()) {
            validationException = addValidationError("targetIndexUuid is required", validationException);
        }
        if (targetShardId < 0) {
            validationException = addValidationError("targetShardId must be >= 0", validationException);
        }
        return validationException;
    }

    /** The real index name to delete, once verified safe. */
    public String sourceIndexName() {
        return sourceIndexName;
    }

    /** The shrink target's index UUID the caller believes {@link #sourceIndexName()} was merged into. */
    public String targetIndexUuid() {
        return targetIndexUuid;
    }

    /** The shard number within {@link #targetIndexUuid()}. */
    public int targetShardId() {
        return targetShardId;
    }

    /** Whether the caller has explicitly acknowledged that {@link #sourceIndexName()} is not currently fenced. */
    public boolean acknowledgeUnfencedSource() {
        return acknowledgeUnfencedSource;
    }
}
