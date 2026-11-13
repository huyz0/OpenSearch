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

/** A search node's answer to a commit push: whether it has the shard open as a reader; see {@link CommitPush}. */
public final class CommitPushResponse extends TransportResponse {

    private final boolean open;

    /**
     * Creates a response.
     *
     * @param open whether the shard is open here as a reader
     */
    public CommitPushResponse(boolean open) {
        this.open = open;
    }

    /**
     * Reads a response off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public CommitPushResponse(StreamInput in) throws IOException {
        this.open = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(open);
    }

    /** @return whether the shard is open on the search node as a reader */
    public boolean open() {
        return open;
    }
}
