/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/** Names a shard by (indexUuid, shardId) and a document id for {@link RealtimeGetAction}. */
public class RealtimeGetRequest extends ActionRequest {

    private final String indexUuid;
    private final int shardId;
    private final String id;

    /**
     * Creates a request.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param id the document id to look up.
     */
    public RealtimeGetRequest(String indexUuid, int shardId, String id) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.id = id;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link RealtimeGetRequest}.
     */
    public RealtimeGetRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUuid = in.readString();
        this.shardId = in.readVInt();
        this.id = in.readString();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeString(id);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indexUuid == null || indexUuid.isEmpty()) {
            validationException = addValidationError("indexUuid is required", validationException);
        }
        if (shardId < 0) {
            validationException = addValidationError("shardId must be >= 0", validationException);
        }
        if (id == null || id.isEmpty()) {
            validationException = addValidationError("id is required", validationException);
        }
        return validationException;
    }

    /** UUID of the index the shard belongs to. */
    public String indexUuid() {
        return indexUuid;
    }

    /** The shard number within {@link #indexUuid()}. */
    public int shardId() {
        return shardId;
    }

    /** The document id to look up. */
    public String id() {
        return id;
    }
}
