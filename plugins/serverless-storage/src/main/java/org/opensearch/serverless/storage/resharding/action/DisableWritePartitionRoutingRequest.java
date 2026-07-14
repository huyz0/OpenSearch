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
import java.util.List;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Clears the write-routing assignment from every named target index -- see {@link
 * DisableWritePartitionRoutingAction}'s own javadoc for the full design rationale. A {@link
 * ClusterManagerNodeRequest}, the same shape {@link EnableWritePartitionRoutingRequest} uses.
 */
public class DisableWritePartitionRoutingRequest extends ClusterManagerNodeRequest<DisableWritePartitionRoutingRequest> {

    private final List<String> targetIndexNames;

    /**
     * Creates a request.
     *
     * @param targetIndexNames the target indices to clear the write-routing assignment from; must be non-empty.
     */
    public DisableWritePartitionRoutingRequest(List<String> targetIndexNames) {
        this.targetIndexNames = targetIndexNames;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link DisableWritePartitionRoutingRequest}.
     */
    public DisableWritePartitionRoutingRequest(StreamInput in) throws IOException {
        super(in);
        this.targetIndexNames = in.readStringList();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(targetIndexNames);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (targetIndexNames == null || targetIndexNames.isEmpty()) {
            validationException = addValidationError("targetIndexNames must be non-empty", validationException);
        }
        return validationException;
    }

    /** The target indices to clear the write-routing assignment from. */
    public List<String> targetIndexNames() {
        return targetIndexNames;
    }
}
