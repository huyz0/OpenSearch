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
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * A query handed to a node that can serve one shard.
 *
 * <p><b>Carries the whole search source</b> rather than a field, a value and a size. Those three were
 * enough for the one query the shell could express; they are not enough for a query language, and
 * unpacking a body into them on the coordinating node and re-packing it on the far side would mean the
 * remote shard answering a different question from the local one.
 */
public final class ForwardedSearchRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/search/shard";

    private final String index;
    private final int shard;
    private final SearchSourceBuilder source;
    private final String indexUuid;
    private final long nowInMillis;

    /**
     * Creates a request.
     *
     * @param index the index
     * @param shard the shard to query
     * @param source the query as the client sent it
     */
    public ForwardedSearchRequest(String index, int shard, SearchSourceBuilder source) {
        this(index, shard, source, null);
    }

    /**
     * Creates a request for one shard of one incarnation of an index.
     *
     * @param index the index
     * @param shard the shard to query
     * @param source the query as the client sent it
     * @param indexUuid the uuid the coordinator resolved, so the peer does not answer from a recreated index
     */
    public ForwardedSearchRequest(String index, int shard, SearchSourceBuilder source, String indexUuid) {
        this(index, shard, source, indexUuid, System.currentTimeMillis());
    }

    /**
     * Creates a request that evaluates {@code now} at the instant the coordinator chose.
     *
     * <p><b>One clock reading per search, not one per shard.</b> A range on {@code now-1h} evaluated on
     * each shard at its own arrival time is a different query per shard, and a document on the boundary
     * is counted by one shard and not another. The coordinator reads the clock once and every shard,
     * local or forwarded, scores against that instant.
     *
     * @param index the index
     * @param shard the shard to query
     * @param source the query as the client sent it
     * @param indexUuid the uuid the coordinator resolved, so the peer does not answer from a recreated index
     * @param nowInMillis the instant {@code now} means for this whole search
     */
    public ForwardedSearchRequest(String index, int shard, SearchSourceBuilder source, String indexUuid, long nowInMillis) {
        this.index = index;
        this.shard = shard;
        this.source = source;
        this.indexUuid = indexUuid;
        this.nowInMillis = nowInMillis;
    }

    /**
     * Reads a request off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public ForwardedSearchRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.shard = in.readVInt();
        this.source = new SearchSourceBuilder(in);
        this.indexUuid = in.readOptionalString();
        this.nowInMillis = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeVInt(shard);
        source.writeTo(out);
        out.writeOptionalString(indexUuid);
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
     * Returns the uuid the coordinator resolved the index to.
     *
     * @return the uuid, or null from an older coordinator
     */
    public String indexUuid() {
        return indexUuid;
    }

    /**
     * Returns the index.
     *
     * @return the index name
     */
    public String index() {
        return index;
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
