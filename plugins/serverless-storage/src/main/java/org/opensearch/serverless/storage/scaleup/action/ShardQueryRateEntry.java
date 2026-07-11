/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaleup.action;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * One shard's raw queries-per-minute estimate, as reported by a single node -- the scale-up
 * counterpart to {@code org.opensearch.serverless.storage.writerengine.action.IdleShardEntry},
 * carrying {@link org.opensearch.serverless.storage.readerengine.ObjectStoreReaderEngine#queriesPerMinute()}
 * instead of an idle duration.
 */
public final class ShardQueryRateEntry implements Writeable, ToXContentObject {

    private final String indexUuid;
    private final int shardId;
    private final long queriesPerMinute;

    /**
     * Creates an entry.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param queriesPerMinute this shard's reader engine's own {@code queriesPerMinute()} estimate,
     *                         on the node that reported this entry.
     */
    public ShardQueryRateEntry(String indexUuid, int shardId, long queriesPerMinute) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.queriesPerMinute = queriesPerMinute;
    }

    /**
     * Deserializes an entry.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardQueryRateEntry}.
     */
    public ShardQueryRateEntry(StreamInput in) throws IOException {
        this.indexUuid = in.readString();
        this.shardId = in.readVInt();
        this.queriesPerMinute = in.readZLong();
    }

    /** @param out stream to write this entry's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeZLong(queriesPerMinute);
    }

    /** UUID of the index the shard belongs to. */
    public String indexUuid() {
        return indexUuid;
    }

    /** The shard number within {@link #indexUuid()}. */
    public int shardId() {
        return shardId;
    }

    /** This shard's reader engine's own queries-per-minute estimate. */
    public long queriesPerMinute() {
        return queriesPerMinute;
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
            .field("queries_per_minute", queriesPerMinute)
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
        ShardQueryRateEntry that = (ShardQueryRateEntry) o;
        return shardId == that.shardId && queriesPerMinute == that.queriesPerMinute && Objects.equals(indexUuid, that.indexUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexUuid, shardId, queriesPerMinute);
    }
}
