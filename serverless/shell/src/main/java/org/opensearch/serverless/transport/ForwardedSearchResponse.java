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
import org.opensearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * One shard's answer: how many matched, and the hits themselves.
 *
 * <p><b>Whole hits, not parallel lists of ids and sources.</b> The earlier shape carried an id list and a
 * source list and nothing else, which meant a score never crossed the network — so the coordinating node
 * had nothing to merge on and concatenated shards in whatever order it visited them. A {@link SearchHit}
 * is already {@code Writeable} and already carries the score, the source and the sort values, so sending
 * it is both less code and the only version that can be merged correctly.
 */
public final class ForwardedSearchResponse extends TransportResponse {

    private final long total;
    private final List<SearchHit> hits;
    private final org.opensearch.search.aggregations.InternalAggregations aggregations;

    /**
     * Creates a response.
     *
     * @param total how many documents matched in this shard
     * @param hits the returned hits, in this shard's own order
     */
    public ForwardedSearchResponse(long total, List<SearchHit> hits) {
        this(total, hits, null);
    }

    /**
     * Creates a response carrying this shard's unreduced aggregations.
     *
     * <p><b>Unreduced, and that is what makes them worth sending.</b> A shard's terms aggregation holds
     * that shard's counts; combining them is the coordinating node's job, and a shard that reduced its own
     * would be sending an answer computed over a fraction of the index. {@code InternalAggregations} is
     * {@code Writeable} and its concrete types are in the node's named-writeable registry, which the
     * transport was already given, so this costs a field rather than a serialisation format.
     *
     * @param total how many documents matched in this shard
     * @param hits the returned hits, in this shard's own order
     * @param aggregations this shard's aggregations, or null if none were asked for
     */
    public ForwardedSearchResponse(long total, List<SearchHit> hits, org.opensearch.search.aggregations.InternalAggregations aggregations) {
        this.total = total;
        this.hits = List.copyOf(hits);
        this.aggregations = aggregations;
    }

    /**
     * Reads a response off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public ForwardedSearchResponse(StreamInput in) throws IOException {
        this.total = in.readVLong();
        final int count = in.readVInt();
        final List<SearchHit> read = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            read.add(new SearchHit(in));
        }
        this.hits = List.copyOf(read);
        this.aggregations = in.readBoolean() ? org.opensearch.search.aggregations.InternalAggregations.readFrom(in) : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(total);
        out.writeVInt(hits.size());
        for (SearchHit hit : hits) {
            hit.writeTo(out);
        }
        out.writeBoolean(aggregations != null);
        if (aggregations != null) {
            aggregations.writeTo(out);
        }
    }

    /**
     * Returns this shard's unreduced aggregations.
     *
     * @return the aggregations, or null
     */
    public org.opensearch.search.aggregations.InternalAggregations aggregations() {
        return aggregations;
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
     * Returns the shard's hits.
     *
     * @return the hits
     */
    public List<SearchHit> hits() {
        return hits;
    }
}
