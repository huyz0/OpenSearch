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
import org.opensearch.serverless.shell.ServerlessNode;

import java.io.IOException;

/** What the owning node found, and the fact that it was the owner that looked. */
public final class ForwardedGetResponse extends TransportResponse {

    private final String ownerNodeId;
    private final ServerlessNode.Document document;

    /**
     * Creates a response.
     *
     * @param ownerNodeId the node that performed the read
     * @param document what it found, which may be nothing
     */
    public ForwardedGetResponse(String ownerNodeId, ServerlessNode.Document document) {
        this.ownerNodeId = ownerNodeId;
        this.document = document;
    }

    /**
     * Reads a response off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public ForwardedGetResponse(StreamInput in) throws IOException {
        this.ownerNodeId = in.readString();
        final String id = in.readString();
        final boolean found = in.readBoolean();
        final String source = in.readOptionalString();
        this.document = new ServerlessNode.Document(id, found, source);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(ownerNodeId);
        out.writeString(document.id());
        out.writeBoolean(document.found());
        out.writeOptionalString(document.source());
    }

    /**
     * Returns the node that performed the read.
     *
     * @return the owning node id
     */
    public String ownerNodeId() {
        return ownerNodeId;
    }

    /**
     * Returns what was found.
     *
     * @return the document, which may be absent
     */
    public ServerlessNode.Document document() {
        return document;
    }
}
