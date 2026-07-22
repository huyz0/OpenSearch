/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/** Requests a warmup mark start ({@link #warming()} {@code true}) or clear ({@code false}) for one node. */
public class NodeWarmupRequest extends ActionRequest {

    private final String nodeId;
    private final boolean warming;

    /**
     * Creates a request.
     *
     * @param nodeId the id of the node to mark or clear as warming.
     * @param warming {@code true} to mark warming, {@code false} to clear it.
     */
    public NodeWarmupRequest(String nodeId, boolean warming) {
        this.nodeId = nodeId;
        this.warming = warming;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeWarmupRequest}.
     */
    public NodeWarmupRequest(StreamInput in) throws IOException {
        super(in);
        this.nodeId = in.readString();
        this.warming = in.readBoolean();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeId);
        out.writeBoolean(warming);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (nodeId == null || nodeId.isEmpty()) {
            validationException = addValidationError("node_id is required", null);
        }
        return validationException;
    }

    /** The id of the node to mark or clear as warming. */
    public String nodeId() {
        return nodeId;
    }

    /** {@code true} to mark warming, {@code false} to clear it. */
    public boolean warming() {
        return warming;
    }
}
