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

/** A query for one shard, sent to the node holding it. */
public final class ForwardedSearchRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/search/shard";

    private final String index;
    private final int shard;
    private final String field;
    private final String value;
    private final int size;

    /**
     * Creates a request.
     *
     * @param index the index
     * @param shard the shard to query
     * @param field the field to match
     * @param value the text to match
     * @param size the maximum hits to return
     */
    public ForwardedSearchRequest(String index, int shard, String field, String value, int size) {
        this.index = index;
        this.shard = shard;
        this.field = field;
        this.value = value;
        this.size = size;
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
        this.field = in.readString();
        this.value = in.readString();
        this.size = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeVInt(shard);
        out.writeString(field);
        out.writeString(value);
        out.writeVInt(size);
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
     * Returns the field to match.
     *
     * @return the field
     */
    public String field() {
        return field;
    }

    /**
     * Returns the value to match.
     *
     * @return the value
     */
    public String value() {
        return value;
    }

    /**
     * Returns the maximum hits requested.
     *
     * @return the size
     */
    public int size() {
        return size;
    }
}
