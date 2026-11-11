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
import java.util.LinkedHashMap;
import java.util.Map;

/** The blocks a peer still held of those asked for; any it has evicted since are simply absent. */
public final class CacheBlocksResponse extends TransportResponse {

    private final Map<String, byte[]> blocks;

    /**
     * Creates a response.
     *
     * @param blocks the blocks, by key
     */
    public CacheBlocksResponse(Map<String, byte[]> blocks) {
        this.blocks = blocks;
    }

    /**
     * Reads a response off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public CacheBlocksResponse(StreamInput in) throws IOException {
        final int count = in.readVInt();
        this.blocks = new LinkedHashMap<>(count);
        for (int i = 0; i < count; i++) {
            blocks.put(in.readString(), in.readByteArray());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(blocks.size());
        for (Map.Entry<String, byte[]> block : blocks.entrySet()) {
            out.writeString(block.getKey());
            out.writeByteArray(block.getValue());
        }
    }

    /**
     * Returns the blocks.
     *
     * @return the blocks held, by key, in the order asked
     */
    public Map<String, byte[]> blocks() {
        return blocks;
    }
}
