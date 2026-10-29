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
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.metadata.PointInTime;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * A query against one shard of a frozen view, handed to a node placement prefers.
 *
 * <p>{@link ForwardedSearchRequest}'s sibling, for {@code runFrozen} rather than the live fan-out. The
 * shape it answers with is identical -- a shard's total, hits and unreduced aggregations owe nothing to
 * whether the commit they came from is the live one or a frozen one -- so this reuses
 * {@link ForwardedSearchResponse} rather than a parallel type that would only ever hold the same fields.
 *
 * <p>Only the selected shard's manifest travels whole. Other shards carry their terms so opening this
 * shard cannot reset another frozen shard's primary term. Sending every file map to every shard would
 * multiply manifest metadata by the shard count for each frozen search.
 */
public final class ForwardedFrozenSearchRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/search/frozen_shard";

    private final PointInTime pit;
    private final int shard;
    private final SearchSourceBuilder source;
    private final long nowInMillis;

    /**
     * Creates a request.
     *
     * @param pit the frozen view
     * @param shard the shard to query
     * @param source the query as the client sent it
     */
    public ForwardedFrozenSearchRequest(PointInTime pit, int shard, SearchSourceBuilder source) {
        this(pit, shard, source, System.currentTimeMillis());
    }

    /**
     * Creates a request that evaluates {@code now} at the instant the coordinator chose, for the reason
     * {@link ForwardedSearchRequest} gives: every shard of one search must score against one clock.
     *
     * @param pit the frozen view
     * @param shard the shard to query
     * @param source the query as the client sent it
     * @param nowInMillis the instant {@code now} means for this whole search
     */
    public ForwardedFrozenSearchRequest(PointInTime pit, int shard, SearchSourceBuilder source, long nowInMillis) {
        final java.util.Map<Integer, org.opensearch.serverless.store.CommitManifest> needed = new java.util.LinkedHashMap<>();
        pit.shards()
            .forEach(
                (number, manifest) -> needed.put(
                    number,
                    number == shard ? manifest : new org.opensearch.serverless.store.CommitManifest(manifest.term(), java.util.Map.of())
                )
            );
        this.pit = new PointInTime(pit.id(), pit.index(), pit.indexUuid(), pit.expiresAtMillis(), needed);
        this.shard = shard;
        this.source = source;
        this.nowInMillis = nowInMillis;
    }

    /**
     * Reads a request off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public ForwardedFrozenSearchRequest(StreamInput in) throws IOException {
        super(in);
        this.pit = new PointInTime(in);
        this.shard = in.readVInt();
        this.source = new SearchSourceBuilder(in);
        this.nowInMillis = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        pit.writeTo(out);
        out.writeVInt(shard);
        source.writeTo(out);
        out.writeLong(nowInMillis);
    }

    /**
     * Returns the instant {@code now} means for this search, chosen once by the coordinator.
     *
     * @return epoch milliseconds
     */
    public long nowInMillis() {
        return nowInMillis;
    }

    /**
     * Returns the selected shard's frozen manifest and every shard's term.
     *
     * @return the view
     */
    public PointInTime pit() {
        return pit;
    }

    /**
     * Returns the shard.
     *
     * @return the shard number
     */
    public int shard() {
        return shard;
    }

    /**
     * Returns the search source.
     *
     * @return the source
     */
    public SearchSourceBuilder source() {
        return source;
    }
}
