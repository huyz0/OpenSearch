/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;

/** Acknowledgement that the owning node made a forwarded write durable. */
public final class ForwardedIndexResponse extends TransportResponse {

    private final String ownerNodeId;

    /**
     * Creates a response.
     *
     * @param ownerNodeId the node that actually performed the write
     */
    public ForwardedIndexResponse(String ownerNodeId) {
        this.ownerNodeId = ownerNodeId;
    }

    /**
     * Reads a response off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public ForwardedIndexResponse(StreamInput in) throws IOException {
        this.ownerNodeId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(ownerNodeId);
    }

    /**
     * Returns the node that performed the write, so a caller can see where its document went.
     *
     * @return the owning node id
     */
    public String ownerNodeId() {
        return ownerNodeId;
    }
}
