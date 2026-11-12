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
 * the shard's next ones; or a reader asking whether it is still pushed to, which renews its grant; or a reader asking
 * only how far the writer has answered refreshing writes. See {@link TailPush}.
 */
public final class TailFetchRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/tail/fetch";

    private final ShardId shardId;
    private final long afterSeqNo;
    private final String subscriber;
    private final long subscription;

    /** The {@link #subscription()} of a reader that wants to be pushed to anew, with what is kept past its commit. */
    public static final long NEW = -1L;

    /** The {@link #subscription()} of a reader that asks only how far refreshing writes have been answered. */
    public static final long CHECK = -2L;

    /**
     * Creates a request.
     *
     * @param shardId the shard
     * @param afterSeqNo the commit's local checkpoint: only records above it are wanted
     * @param subscriber the asking node, which the writer pushes the shard's next writes to
     */
    public TailFetchRequest(ShardId shardId, long afterSeqNo, String subscriber) {
        this(shardId, afterSeqNo, subscriber, NEW);
    }

    /**
     * Creates a request.
     *
     * @param shardId the shard
     * @param afterSeqNo the commit's local checkpoint: only records above it are wanted
     * @param subscriber the asking node
     * @param subscription the subscription the node holds and wants renewed; {@link #NEW} or {@link #CHECK}
     */
    public TailFetchRequest(ShardId shardId, long afterSeqNo, String subscriber, long subscription) {
        this.shardId = shardId;
        this.afterSeqNo = afterSeqNo;
        this.subscriber = subscriber;
        this.subscription = subscription;
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
        this.subscription = in.readZLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(shardId.getIndexName());
        out.writeString(shardId.getIndex().getUUID());
        out.writeVInt(shardId.id());
        out.writeZLong(afterSeqNo);
        out.writeString(subscriber);
        out.writeZLong(subscription);
    }

    /** @return the subscription to renew, {@link #NEW}, or {@link #CHECK} */
    public long subscription() {
        return subscription;
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
