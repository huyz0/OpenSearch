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

/** Asks a shard's writer for the newest commit it holds, staged or published. */
public final class NewestCommitRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/commit/newest";

    private final String indexName;
    private final String indexUuid;
    private final int shard;

    /**
     * Creates a request.
     *
     * @param indexName the index
     * @param indexUuid the index's uuid
     * @param shard the shard number
     */
    public NewestCommitRequest(String indexName, String indexUuid, int shard) {
        this.indexName = indexName;
        this.indexUuid = indexUuid;
        this.shard = shard;
    }

    /**
     * Reads a request off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public NewestCommitRequest(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
        this.indexUuid = in.readString();
        this.shard = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
        out.writeString(indexUuid);
        out.writeVInt(shard);
    }

    /** @return the index */
    public String indexName() {
        return indexName;
    }

    /** @return the index's uuid */
    public String indexUuid() {
        return indexUuid;
    }

    /** @return the shard number */
    public int shard() {
        return shard;
    }
}
