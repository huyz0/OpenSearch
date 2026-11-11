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
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Asks a search peer to take over a draining node's share of its readers before it stops: copy their warm blocks from
 * it and open them. The answer, a {@link HandOffResponse}, comes once the peer has.
 */
public final class HandOffRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/cache/hand_off";

    private final String drainingId;

    /**
     * Creates a request.
     *
     * @param drainingId the draining node, which the peer copies from
     */
    public HandOffRequest(String drainingId) {
        this.drainingId = drainingId;
    }

    /**
     * Reads a request off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public HandOffRequest(StreamInput in) throws IOException {
        super(in);
        this.drainingId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(drainingId);
    }

    /**
     * Returns the draining node.
     *
     * @return its id
     */
    public String drainingId() {
        return drainingId;
    }
}
