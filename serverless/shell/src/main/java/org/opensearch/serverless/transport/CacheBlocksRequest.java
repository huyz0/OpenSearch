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
import java.util.List;

/** Asks a peer for the bytes of some cached blocks. The answer is a {@link CacheBlocksResponse}. */
public final class CacheBlocksRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/cache/blocks";

    private final List<String> keys;

    /**
     * Creates a request.
     *
     * @param keys the blocks wanted
     */
    public CacheBlocksRequest(List<String> keys) {
        this.keys = List.copyOf(keys);
    }

    /**
     * Reads a request off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public CacheBlocksRequest(StreamInput in) throws IOException {
        super(in);
        this.keys = in.readStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(keys);
    }

    /**
     * Returns the blocks wanted.
     *
     * @return their keys
     */
    public List<String> keys() {
        return keys;
    }
}
