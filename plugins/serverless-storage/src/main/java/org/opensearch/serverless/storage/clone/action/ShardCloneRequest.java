/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/** Names a source and target shard by (indexUuid, shardId) for {@link ShardCloneAction}. */
public class ShardCloneRequest extends ActionRequest {

    private final String sourceIndexUuid;
    private final int sourceShardId;
    private final String targetIndexUuid;
    private final int targetShardId;

    public ShardCloneRequest(String sourceIndexUuid, int sourceShardId, String targetIndexUuid, int targetShardId) {
        this.sourceIndexUuid = sourceIndexUuid;
        this.sourceShardId = sourceShardId;
        this.targetIndexUuid = targetIndexUuid;
        this.targetShardId = targetShardId;
    }

    public ShardCloneRequest(StreamInput in) throws IOException {
        super(in);
        this.sourceIndexUuid = in.readString();
        this.sourceShardId = in.readVInt();
        this.targetIndexUuid = in.readString();
        this.targetShardId = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sourceIndexUuid);
        out.writeVInt(sourceShardId);
        out.writeString(targetIndexUuid);
        out.writeVInt(targetShardId);
    }

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
        return validationException;
    }

    public String sourceIndexUuid() {
        return sourceIndexUuid;
    }

    public int sourceShardId() {
        return sourceShardId;
    }

    public String targetIndexUuid() {
        return targetIndexUuid;
    }

    public int targetShardId() {
        return targetShardId;
    }
}
