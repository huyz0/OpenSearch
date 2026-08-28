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
import java.util.List;

/** One shard's answer: how many matched, and the hits themselves. */
public final class ForwardedSearchResponse extends TransportResponse {

    private final long total;
    private final List<String> ids;
    private final List<String> sources;

    /**
     * Creates a response.
     *
     * @param total how many documents matched in this shard
     * @param ids the ids of the returned hits
     * @param sources the sources of the returned hits, positionally matching the ids
     */
    public ForwardedSearchResponse(long total, List<String> ids, List<String> sources) {
        this.total = total;
        this.ids = List.copyOf(ids);
        this.sources = List.copyOf(sources);
    }

    /**
     * Reads a response off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public ForwardedSearchResponse(StreamInput in) throws IOException {
        this.total = in.readVLong();
        this.ids = in.readStringList();
        this.sources = in.readStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(total);
        out.writeStringCollection(ids);
        out.writeStringCollection(sources);
    }

    /**
     * Returns the shard's total match count.
     *
     * @return the total
     */
    public long total() {
        return total;
    }

    /**
     * Returns the hit ids.
     *
     * @return the ids
     */
    public List<String> ids() {
        return ids;
    }

    /**
     * Returns the hit sources.
     *
     * @return the sources
     */
    public List<String> sources() {
        return sources;
    }
}
