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
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * An explain handed to the node that owns the document's shard.
 *
 * <p>Carries the query as a {@link QueryBuilder} rather than as the JSON it arrived in, so the far side
 * scores exactly the query this node parsed. Re-parsing on arrival would let a parser difference between two
 * nodes turn into a different explanation for the same request, which is the one thing an explain must not do.
 */
public final class ForwardedExplainRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/explain/shard";

    private final String index;
    private final int shard;
    private final String id;
    private final QueryBuilder query;

    /**
     * Creates a request.
     *
     * @param index the index
     * @param shard the shard holding the document
     * @param id the document id
     * @param query the query to score it against
     */
    public ForwardedExplainRequest(String index, int shard, String id, QueryBuilder query) {
        this.index = index;
        this.shard = shard;
        this.id = id;
        this.query = query;
    }

    /**
     * Reads a request off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public ForwardedExplainRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.shard = in.readVInt();
        this.id = in.readString();
        this.query = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeVInt(shard);
        out.writeString(id);
        out.writeNamedWriteable(query);
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
     * Returns the document id.
     *
     * @return the id
     */
    public String id() {
        return id;
    }

    /**
     * Returns the query.
     *
     * @return the query
     */
    public QueryBuilder query() {
        return query;
    }
}
