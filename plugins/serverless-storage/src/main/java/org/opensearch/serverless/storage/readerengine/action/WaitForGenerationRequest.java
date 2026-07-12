/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/** Names a shard, a minimum manifest generation, and a timeout for {@link WaitForGenerationAction}. */
public class WaitForGenerationRequest extends ActionRequest {

    private final String indexUuid;
    private final int shardId;
    private final long minGeneration;
    private final TimeValue timeout;

    /**
     * Creates a request.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param minGeneration the manifest generation the reader engine on the receiving node must reach.
     * @param timeout how long the receiving node should wait before giving up.
     */
    public WaitForGenerationRequest(String indexUuid, int shardId, long minGeneration, TimeValue timeout) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.minGeneration = minGeneration;
        this.timeout = timeout;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link WaitForGenerationRequest}.
     */
    public WaitForGenerationRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUuid = in.readString();
        this.shardId = in.readVInt();
        this.minGeneration = in.readVLong();
        this.timeout = in.readTimeValue();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeVLong(minGeneration);
        out.writeTimeValue(timeout);
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
        if (minGeneration < 0) {
            validationException = addValidationError("minGeneration must be >= 0", validationException);
        }
        if (timeout == null || timeout.millis() < 0) {
            validationException = addValidationError("timeout must be >= 0", validationException);
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

    /** The manifest generation the reader engine on the receiving node must reach. */
    public long minGeneration() {
        return minGeneration;
    }

    /** How long the receiving node should wait before giving up. */
    public TimeValue timeout() {
        return timeout;
    }
}
