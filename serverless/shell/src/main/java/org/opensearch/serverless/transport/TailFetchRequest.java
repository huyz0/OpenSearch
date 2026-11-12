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
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * A reader that has just opened a shard asking its writer for the acknowledged writes its commit lacks, and to be pushed
 * the shard's next ones.
 */
public final class TailFetchRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/tail/fetch";

    private final ShardId shardId;
    private final long afterSeqNo;
    private final String subscriber;

    /**
     * Creates a request.
     *
     * @param shardId the shard
     * @param afterSeqNo the commit's local checkpoint: only records above it are wanted
     * @param subscriber the asking node, which the writer pushes the shard's next writes to
     */
    public TailFetchRequest(ShardId shardId, long afterSeqNo, String subscriber) {
        this.shardId = shardId;
        this.afterSeqNo = afterSeqNo;
        this.subscriber = subscriber;
    }

    /**
     * Reads a request off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public TailFetchRequest(StreamInput in) throws IOException {
        super(in);
        this.shardId = new ShardId(new Index(in.readString(), in.readString()), in.readVInt());
        this.afterSeqNo = in.readZLong();
        this.subscriber = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(shardId.getIndexName());
        out.writeString(shardId.getIndex().getUUID());
        out.writeVInt(shardId.id());
        out.writeZLong(afterSeqNo);
        out.writeString(subscriber);
    }

    /** @return the shard */
    public ShardId shardId() {
        return shardId;
    }

    /** @return the asking node */
    public String subscriber() {
        return subscriber;
    }

    /** @return the sequence number above which records are wanted */
    public long afterSeqNo() {
        return afterSeqNo;
    }
}
