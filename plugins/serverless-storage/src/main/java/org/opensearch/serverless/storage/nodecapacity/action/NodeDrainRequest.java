/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Requests a drain start ({@link #drain()} {@code true}) or cancel ({@code false}) for one node. A
 * {@link ClusterManagerNodeRequest} because {@code DrainCoordinator} mutates cluster state via a
 * plain {@code ClusterService#submitStateUpdateTask} call, which throws {@code
 * NotClusterManagerException} when invoked from a node that isn't currently the cluster-manager --
 * {@link org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction} is what
 * transparently forwards this request to whichever node actually is, rather than every caller
 * needing to know or care (mirrors {@code ReactivateShardsRequest}'s identical reasoning).
 */
public class NodeDrainRequest extends ClusterManagerNodeRequest<NodeDrainRequest> {

    private final String nodeId;
    private final boolean drain;

    /**
     * Creates a request.
     *
     * @param nodeId the id of the node to drain or cancel the drain of.
     * @param drain {@code true} to start a drain, {@code false} to cancel one.
     */
    public NodeDrainRequest(String nodeId, boolean drain) {
        this.nodeId = nodeId;
        this.drain = drain;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeDrainRequest}.
     */
    public NodeDrainRequest(StreamInput in) throws IOException {
        super(in);
        this.nodeId = in.readString();
        this.drain = in.readBoolean();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeId);
        out.writeBoolean(drain);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (nodeId == null || nodeId.isEmpty()) {
            validationException = addValidationError("node_id is required", null);
        }
        return validationException;
    }

    /** The id of the node to drain or cancel the drain of. */
    public String nodeId() {
        return nodeId;
    }

    /** {@code true} to start a drain, {@code false} to cancel one. */
    public boolean drain() {
        return drain;
    }
}
