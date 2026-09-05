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
 * A read by id, handed to the node that owns the shard.
 *
 * <p><b>Sent to the owner rather than to a reader, which is the opposite of how a search is routed.</b>
 * A search fans out to readers because it answers from published commits and placement is a hint. A get
 * must see writes that are acknowledged but not yet published, and the only copy that has them is the
 * writer's.
 */
public final class ForwardedGetRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/document/get";

    private final String index;
    private final int shard;
    private final String id;

    /**
     * Creates a request.
     *
     * @param index the index
     * @param shard the shard the document routes to
     * @param id the document id
     */
    public ForwardedGetRequest(String index, int shard, String id) {
        this.index = index;
        this.shard = shard;
        this.id = id;
    }

    /**
     * Reads a request off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public ForwardedGetRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.shard = in.readVInt();
        this.id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeVInt(shard);
        out.writeString(id);
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
}
