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
 * <p><b>Carries the whole view, not a single shard's manifest.</b> Opening one shard of a view needs every
 * shard's term (see {@code ServerlessNode#openFrozenView}), so a request that carried only the shard being
 * asked for would leave the receiving node unable to call that method at all without a second
 * implementation of what it does. Sending the {@link PointInTime} whole means the node this is forwarded
 * to opens the view exactly as the coordinator would have, locally.
 */
public final class ForwardedFrozenSearchRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/search/frozen_shard";

    private final PointInTime pit;
    private final int shard;
    private final SearchSourceBuilder source;

    /**
     * Creates a request.
     *
     * @param pit the frozen view
     * @param shard the shard to query
     * @param source the query as the client sent it
     */
    public ForwardedFrozenSearchRequest(PointInTime pit, int shard, SearchSourceBuilder source) {
        this.pit = pit;
        this.shard = shard;
        this.source = source;
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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        pit.writeTo(out);
        out.writeVInt(shard);
        source.writeTo(out);
    }

    /**
     * Returns the frozen view.
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
