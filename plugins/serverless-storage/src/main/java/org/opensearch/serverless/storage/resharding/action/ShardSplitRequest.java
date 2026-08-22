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
 * Names a source and target shard by (indexUuid, shardId), plus which partition (of how many)
 * the target should serve, for {@link ShardSplitAction} -- one call per target, the same "one
 * call per target" shape {@code ShardCloneRequest} already has for plain clones.
 */
public class ShardSplitRequest extends ActionRequest {

    private final String sourceIndexUuid;
    private final int sourceShardId;
    private final String targetIndexUuid;
    private final int targetShardId;
    private final int partitionIndex;
    private final int numPartitions;

    /**
     * Creates a request.
     *
     * @param sourceIndexUuid the index being split from.
     * @param sourceShardId the shard number within {@code sourceIndexUuid}.
     * @param targetIndexUuid the brand-new index this split target creates.
     * @param targetShardId the shard number within {@code targetIndexUuid}.
     * @param partitionIndex which of {@code numPartitions} partitions this target serves.
     * @param numPartitions how many partitions the source shard's document space is being split into.
     */
    public ShardSplitRequest(
        String sourceIndexUuid,
        int sourceShardId,
        String targetIndexUuid,
        int targetShardId,
        int partitionIndex,
        int numPartitions
    ) {
        this.sourceIndexUuid = sourceIndexUuid;
        this.sourceShardId = sourceShardId;
        this.targetIndexUuid = targetIndexUuid;
        this.targetShardId = targetShardId;
        this.partitionIndex = partitionIndex;
        this.numPartitions = numPartitions;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardSplitRequest}.
     */
    public ShardSplitRequest(StreamInput in) throws IOException {
        super(in);
        this.sourceIndexUuid = in.readString();
        this.sourceShardId = in.readVInt();
        this.targetIndexUuid = in.readString();
        this.targetShardId = in.readVInt();
        this.partitionIndex = in.readVInt();
        this.numPartitions = in.readVInt();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sourceIndexUuid);
        out.writeVInt(sourceShardId);
        out.writeString(targetIndexUuid);
        out.writeVInt(targetShardId);
        out.writeVInt(partitionIndex);
        out.writeVInt(numPartitions);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sourceIndexUuid == null || sourceIndexUuid.isEmpty()) {
            validationException = addValidationError("source indexUuid is required", validationException);
        }
        if (targetIndexUuid == null || targetIndexUuid.isEmpty()) {
            validationException = addValidationError("target indexUuid is required", validationException);
        }
        if (sourceShardId < 0) {
            validationException = addValidationError("source shardId must be >= 0", validationException);
        }
        if (targetShardId < 0) {
            validationException = addValidationError("target shardId must be >= 0", validationException);
        }
        if (numPartitions < 2) {
            validationException = addValidationError("numPartitions must be >= 2", validationException);
        }
        if (partitionIndex < 0 || (numPartitions >= 2 && partitionIndex >= numPartitions)) {
            validationException = addValidationError("partitionIndex must be in [0, numPartitions)", validationException);
        }
        return validationException;
    }

    /** The index being split from. */
    public String sourceIndexUuid() {
        return sourceIndexUuid;
    }

    /** The shard number within {@link #sourceIndexUuid()}. */
    public int sourceShardId() {
        return sourceShardId;
    }

    /** The brand-new index this split target creates. */
    public String targetIndexUuid() {
        return targetIndexUuid;
    }

    /** The shard number within {@link #targetIndexUuid()}. */
    public int targetShardId() {
        return targetShardId;
    }

    /** Which of {@link #numPartitions()} partitions this target serves. */
    public int partitionIndex() {
        return partitionIndex;
    }

    /** How many partitions the source shard's document space is being split into. */
    public int numPartitions() {
        return numPartitions;
    }
}
