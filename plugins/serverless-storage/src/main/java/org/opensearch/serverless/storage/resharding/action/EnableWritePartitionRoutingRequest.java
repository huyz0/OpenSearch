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
 * Assigns {@code targetIndexNames}, in list order, as partitions {@code 0..targetIndexNames.size()-1}
 * of {@code aliasName}'s write routing -- see {@link EnableWritePartitionRoutingAction}'s own
 * javadoc for the full design rationale. A {@link ClusterManagerNodeRequest}, the same shape {@code
 * ReactivateShardsRequest} uses, since this mutates cluster state and must run on the elected
 * cluster-manager node.
 */
public class EnableWritePartitionRoutingRequest extends ClusterManagerNodeRequest<EnableWritePartitionRoutingRequest> {

    private final String aliasName;
    private final List<String> targetIndexNames;

    /**
     * Creates a request.
     *
     * @param aliasName the write-routing alias to assign.
     * @param targetIndexNames the real split-target indices, in partition order; must be non-empty.
     */
    public EnableWritePartitionRoutingRequest(String aliasName, List<String> targetIndexNames) {
        this.aliasName = aliasName;
        this.targetIndexNames = targetIndexNames;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link EnableWritePartitionRoutingRequest}.
     */
    public EnableWritePartitionRoutingRequest(StreamInput in) throws IOException {
        super(in);
        this.aliasName = in.readString();
        this.targetIndexNames = in.readStringList();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(aliasName);
        out.writeStringCollection(targetIndexNames);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (aliasName == null || aliasName.isEmpty()) {
            validationException = addValidationError("aliasName is required", validationException);
        }
        if (targetIndexNames == null || targetIndexNames.size() < 2) {
            validationException = addValidationError("targetIndexNames must name at least 2 partitions", validationException);
        }
        return validationException;
    }

    /** The write-routing alias to assign. */
    public String aliasName() {
        return aliasName;
    }

    /** The real split-target indices, in partition order. */
    public List<String> targetIndexNames() {
        return targetIndexNames;
    }
}
