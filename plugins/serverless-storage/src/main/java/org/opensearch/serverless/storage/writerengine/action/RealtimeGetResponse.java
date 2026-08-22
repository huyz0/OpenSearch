/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.serverless.storage.writerengine.RealtimeGetResult;

import java.io.IOException;

/** The result of a {@link RealtimeGetAction} request. */
public class RealtimeGetResponse extends ActionResponse implements ToXContentObject {

    private final boolean shardTracked;
    private final boolean exists;
    private final long version;
    private final long seqNo;
    private final long primaryTerm;

    /**
     * Creates a response.
     *
     * @param shardTracked {@code false} if no writer engine for the requested shard is currently
     *                     tracked on the receiving node at all -- distinct from {@code
     *                     exists=false}, which means the shard exists but the document doesn't.
     * @param result the real-time get result, ignored (fields default to absent) when {@code shardTracked} is {@code false}.
     */
    public RealtimeGetResponse(boolean shardTracked, RealtimeGetResult result) {
        this.shardTracked = shardTracked;
        this.exists = shardTracked && result.exists();
        this.version = shardTracked ? result.version() : -1;
        this.seqNo = shardTracked ? result.seqNo() : -1;
        this.primaryTerm = shardTracked ? result.primaryTerm() : -1;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link RealtimeGetResponse}.
     */
    public RealtimeGetResponse(StreamInput in) throws IOException {
        super(in);
        this.shardTracked = in.readBoolean();
        this.exists = in.readBoolean();
        this.version = in.readLong();
        this.seqNo = in.readLong();
        this.primaryTerm = in.readLong();
    }

    /** @param out stream to write this response's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(shardTracked);
        out.writeBoolean(exists);
        out.writeLong(version);
        out.writeLong(seqNo);
        out.writeLong(primaryTerm);
    }

    /** {@code false} if no writer engine for the requested shard is currently tracked on the receiving node at all. */
    public boolean shardTracked() {
        return shardTracked;
    }

    /** Whether the document currently exists per the writer's live version map. */
    public boolean exists() {
        return exists;
    }

    /** The document's current version, meaningless if {@link #exists()} is {@code false}. */
    public long version() {
        return version;
    }

    /** The document's current sequence number, meaningless if {@link #exists()} is {@code false}. */
    public long seqNo() {
        return seqNo;
    }

    /** The document's current primary term, meaningless if {@link #exists()} is {@code false}. */
    public long primaryTerm() {
        return primaryTerm;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("shard_tracked", shardTracked)
            .field("exists", exists)
            .field("version", version)
            .field("seq_no", seqNo)
            .field("primary_term", primaryTerm)
            .endObject();
    }
}
