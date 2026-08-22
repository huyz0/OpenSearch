/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/** Requests the cluster-manager's current {@code NodeCapacitySignal}; carries no fields of its own. */
public class NodeCapacityRequest extends ClusterManagerNodeReadRequest<NodeCapacityRequest> {

    /** Creates a request. */
    public NodeCapacityRequest() {}

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeCapacityRequest}.
     */
    public NodeCapacityRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
