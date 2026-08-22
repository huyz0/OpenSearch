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
 * One shard's raw split-candidate signals, as reported by a single node -- the split-candidate
 * counterpart to {@code org.opensearch.serverless.storage.scaleup.action.ShardQueryRateEntry},
 * carrying both {@code ObjectStoreWriterEngine#writesPerMinute()} (split-for-heat) and {@code
 * ObjectStoreWriterEngine#shardSizeInBytes()} (split-for-size, dynamic-partitioning-plan.md Phase 1
 * item 1.1) instead of a single query rate.
 */
public final class ShardWriteRateEntry implements Writeable, ToXContentObject {

    private final String indexUuid;
    private final int shardId;
    private final long writesPerMinute;
    private final long shardSizeInBytes;

    /**
     * Creates an entry.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param writesPerMinute this shard's writer engine's own {@code writesPerMinute()} estimate,
     *                        on the node that reported this entry.
     * @param shardSizeInBytes this shard's writer engine's own {@code shardSizeInBytes()}, on the
     *                         node that reported this entry.
     */
    public ShardWriteRateEntry(String indexUuid, int shardId, long writesPerMinute, long shardSizeInBytes) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.writesPerMinute = writesPerMinute;
        this.shardSizeInBytes = shardSizeInBytes;
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
        this.shardSizeInBytes = in.readZLong();
    }

    /** @param out stream to write this entry's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeZLong(writesPerMinute);
        out.writeZLong(shardSizeInBytes);
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

    /** This shard's writer engine's own size-in-bytes estimate. */
    public long shardSizeInBytes() {
        return shardSizeInBytes;
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
            .field("shard_size_in_bytes", shardSizeInBytes)
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
        return shardId == that.shardId
            && writesPerMinute == that.writesPerMinute
            && shardSizeInBytes == that.shardSizeInBytes
            && Objects.equals(indexUuid, that.indexUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexUuid, shardId, writesPerMinute, shardSizeInBytes);
    }
}
