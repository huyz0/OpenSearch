/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format.action;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/** One reader shard's local disk cache stats, as reported by {@link NodeCacheStatsAction} for a single node. */
public final class ShardCacheStatsEntry implements Writeable, ToXContentObject {

    private final String indexUuid;
    private final int shardId;
    private final long hitCount;
    private final long missCount;
    private final long averageColdReadLatencyMillis;

    /**
     * Creates an entry.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param hitCount reads served from this shard's local disk cache so far.
     * @param missCount reads that missed this shard's local disk cache so far.
     * @param averageColdReadLatencyMillis average wall-clock time spent fetching from the delegate
     *                                     on a cache miss, 0 if there have been no misses yet.
     */
    public ShardCacheStatsEntry(String indexUuid, int shardId, long hitCount, long missCount, long averageColdReadLatencyMillis) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.averageColdReadLatencyMillis = averageColdReadLatencyMillis;
    }

    /**
     * Deserializes an entry.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardCacheStatsEntry}.
     */
    public ShardCacheStatsEntry(StreamInput in) throws IOException {
        this.indexUuid = in.readString();
        this.shardId = in.readVInt();
        this.hitCount = in.readVLong();
        this.missCount = in.readVLong();
        this.averageColdReadLatencyMillis = in.readVLong();
    }

    /** @param out stream to write this entry's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeVLong(hitCount);
        out.writeVLong(missCount);
        out.writeVLong(averageColdReadLatencyMillis);
    }

    /** UUID of the index the shard belongs to. */
    public String indexUuid() {
        return indexUuid;
    }

    /** The shard number within {@link #indexUuid()}. */
    public int shardId() {
        return shardId;
    }

    /** Reads served from this shard's local disk cache so far. */
    public long hitCount() {
        return hitCount;
    }

    /** Reads that missed this shard's local disk cache so far. */
    public long missCount() {
        return missCount;
    }

    /** Average wall-clock time, in milliseconds, spent fetching from the delegate on a cache miss, 0 if there have been no misses yet. */
    public long averageColdReadLatencyMillis() {
        return averageColdReadLatencyMillis;
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
            .field("hit_count", hitCount)
            .field("miss_count", missCount)
            .field("average_cold_read_latency_millis", averageColdReadLatencyMillis)
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
        ShardCacheStatsEntry that = (ShardCacheStatsEntry) o;
        return shardId == that.shardId
            && hitCount == that.hitCount
            && missCount == that.missCount
            && averageColdReadLatencyMillis == that.averageColdReadLatencyMillis
            && Objects.equals(indexUuid, that.indexUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexUuid, shardId, hitCount, missCount, averageColdReadLatencyMillis);
    }
}
