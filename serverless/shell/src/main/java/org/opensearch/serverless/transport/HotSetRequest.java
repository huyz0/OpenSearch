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
 * Asks a search peer which of the shards it reads the sender will serve once ready, and which of their blocks it holds.
 *
 * <p>Sent by a node joining to warm, to each search peer. The answer is a {@link HotSetResponse}.
 */
public final class HotSetRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/cache/hot_set";

    private final String joinerId;
    private final long maxBytes;
    private final long recentMillis;

    /**
     * Creates a request for every block held for the joiner's shards.
     *
     * @param joinerId the joining node, which the peer ranks as a search member
     */
    public HotSetRequest(String joinerId) {
        this(joinerId, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    /**
     * Creates a request.
     *
     * @param joinerId the joining node, which the peer ranks as a search member
     * @param maxBytes the most bytes of blocks to name
     * @param recentMillis how long ago, at most, a block was last read for it to count as hot
     */
    public HotSetRequest(String joinerId, long maxBytes, long recentMillis) {
        this.joinerId = joinerId;
        this.maxBytes = maxBytes;
        this.recentMillis = recentMillis;
    }

    /**
     * Reads a request off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public HotSetRequest(StreamInput in) throws IOException {
        super(in);
        this.joinerId = in.readString();
        this.maxBytes = in.readVLong();
        this.recentMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(joinerId);
        out.writeVLong(maxBytes);
        out.writeVLong(recentMillis);
    }

    /**
     * Returns the most bytes of blocks to name.
     *
     * @return the budget
     */
    public long maxBytes() {
        return maxBytes;
    }

    /**
     * Returns how long ago, at most, a block was last read for it to count as hot.
     *
     * @return the window in milliseconds
     */
    public long recentMillis() {
        return recentMillis;
    }

    /**
     * Returns the joining node.
     *
     * @return its id
     */
    public String joinerId() {
        return joinerId;
    }
}
