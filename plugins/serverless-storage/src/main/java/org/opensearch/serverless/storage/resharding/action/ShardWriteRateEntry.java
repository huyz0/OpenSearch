/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * One shard's raw writes-per-minute estimate, as reported by a single node -- the split-candidate
 * counterpart to {@code org.opensearch.serverless.storage.scaleup.action.ShardQueryRateEntry},
 * carrying {@code ObjectStoreWriterEngine#writesPerMinute()} instead of a query rate.
 */
public final class ShardWriteRateEntry implements Writeable, ToXContentObject {

    private final String indexUuid;
    private final int shardId;
    private final long writesPerMinute;

    /**
     * Creates an entry.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param writesPerMinute this shard's writer engine's own {@code writesPerMinute()} estimate,
     *                        on the node that reported this entry.
     */
    public ShardWriteRateEntry(String indexUuid, int shardId, long writesPerMinute) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.writesPerMinute = writesPerMinute;
    }

    /**
     * Deserializes an entry.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardWriteRateEntry}.
     */
    public ShardWriteRateEntry(StreamInput in) throws IOException {
        this.indexUuid = in.readString();
        this.shardId = in.readVInt();
        this.writesPerMinute = in.readZLong();
    }

    /** @param out stream to write this entry's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeZLong(writesPerMinute);
    }

    /** UUID of the index the shard belongs to. */
    public String indexUuid() {
        return indexUuid;
    }

    /** The shard number within {@link #indexUuid()}. */
    public int shardId() {
        return shardId;
    }

    /** This shard's writer engine's own writes-per-minute estimate. */
    public long writesPerMinute() {
        return writesPerMinute;
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
            .field("writes_per_minute", writesPerMinute)
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
        ShardWriteRateEntry that = (ShardWriteRateEntry) o;
        return shardId == that.shardId && writesPerMinute == that.writesPerMinute && Objects.equals(indexUuid, that.indexUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexUuid, shardId, writesPerMinute);
    }
}
