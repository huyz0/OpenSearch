/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Names the parent shard whose in-progress in-place split should be abandoned -- see {@link
 * CancelInPlaceSplitAction}'s own javadoc for the design rationale.
 */
public class CancelInPlaceSplitRequest extends ClusterManagerNodeRequest<CancelInPlaceSplitRequest> {

    private final String indexName;
    private final int shardId;

    /**
     * Creates a request.
     *
     * @param indexName the index whose split should be cancelled; must be non-empty.
     * @param shardId the parent shard id of the in-progress split; must be non-negative.
     */
    public CancelInPlaceSplitRequest(String indexName, int shardId) {
        this.indexName = indexName;
        this.shardId = shardId;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link CancelInPlaceSplitRequest}.
     */
    public CancelInPlaceSplitRequest(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
        this.shardId = in.readVInt();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
        out.writeVInt(shardId);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indexName == null || indexName.isEmpty()) {
            validationException = addValidationError("indexName must be non-empty", validationException);
        }
        if (shardId < 0) {
            validationException = addValidationError("shardId must be non-negative", validationException);
        }
        return validationException;
    }

    /** The index whose split should be cancelled. */
    public String indexName() {
        return indexName;
    }

    /** The parent shard id of the in-progress split. */
    public int shardId() {
        return shardId;
    }
}
