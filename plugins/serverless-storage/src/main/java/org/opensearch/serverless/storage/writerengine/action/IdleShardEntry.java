/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine.action;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/** One shard's idle time, as reported by {@link NodeIdleShardsAction} for a single node. */
public final class IdleShardEntry implements Writeable, ToXContentObject {

    private final String indexUuid;
    private final int shardId;
    private final long millisSinceLastActivity;

    /**
     * Creates an entry.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param millisSinceLastActivity how long it's been since this shard's writer engine last saw
     *                                a real client write, on the node that reported this entry.
     */
    public IdleShardEntry(String indexUuid, int shardId, long millisSinceLastActivity) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.millisSinceLastActivity = millisSinceLastActivity;
    }

    /**
     * Deserializes an entry.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link IdleShardEntry}.
     */
    public IdleShardEntry(StreamInput in) throws IOException {
        this.indexUuid = in.readString();
        this.shardId = in.readVInt();
        this.millisSinceLastActivity = in.readZLong();
    }

    /** @param out stream to write this entry's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeZLong(millisSinceLastActivity);
    }

    /** UUID of the index the shard belongs to. */
    public String indexUuid() {
        return indexUuid;
    }

    /** The shard number within {@link #indexUuid()}. */
    public int shardId() {
        return shardId;
    }

    /** How long it's been since this shard's writer engine last saw a real client write. */
    public long millisSinceLastActivity() {
        return millisSinceLastActivity;
    }

    /**
     * @param builder the builder to append this entry's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("index_uuid", indexUuid)
            .field("shard_id", shardId)
            .field("millis_since_last_activity", millisSinceLastActivity)
            .endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IdleShardEntry that = (IdleShardEntry) o;
        return shardId == that.shardId
            && millisSinceLastActivity == that.millisSinceLastActivity
            && Objects.equals(indexUuid, that.indexUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexUuid, shardId, millisSinceLastActivity);
    }
}
