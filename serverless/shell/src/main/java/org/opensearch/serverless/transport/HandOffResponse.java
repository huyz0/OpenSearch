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

/** What a peer took over from a draining node: the shards it opened and the blocks it copied. */
public final class HandOffResponse extends TransportResponse {

    private final int opened;
    private final long blocksCopied;

    /**
     * Creates a response.
     *
     * @param opened shards opened
     * @param blocksCopied blocks copied
     */
    public HandOffResponse(int opened, long blocksCopied) {
        this.opened = opened;
        this.blocksCopied = blocksCopied;
    }

    /**
     * Reads a response off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public HandOffResponse(StreamInput in) throws IOException {
        this.opened = in.readVInt();
        this.blocksCopied = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(opened);
        out.writeVLong(blocksCopied);
    }

    /**
     * Returns the shards opened.
     *
     * @return the count
     */
    public int opened() {
        return opened;
    }

    /**
     * Returns the blocks copied.
     *
     * @return the count
     */
    public long blocksCopied() {
        return blocksCopied;
    }
}
