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
    private final long millisSinceLastQuery;

    /**
     * Creates an entry.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param queriesPerMinute this shard's reader engine's own {@code queriesPerMinute()} estimate,
     *                         on the node that reported this entry.
     */
    public ShardQueryRateEntry(String indexUuid, int shardId, long queriesPerMinute) {
        this(indexUuid, shardId, queriesPerMinute, UNKNOWN_IDLE);
    }

    /** Sentinel for {@link #millisSinceLastQuery()} when the reporting node had no idleness reading for this shard. */
    public static final long UNKNOWN_IDLE = -1L;

    /**
     * Creates an entry carrying both signals.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param queriesPerMinute this shard's reader engine's own {@code queriesPerMinute()} estimate.
     * @param millisSinceLastQuery how long ago that reader copy last served a query, or {@link
     *                             #UNKNOWN_IDLE} -- see {@link #millisSinceLastQuery()}.
     */
    public ShardQueryRateEntry(String indexUuid, int shardId, long queriesPerMinute, long millisSinceLastQuery) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.queriesPerMinute = queriesPerMinute;
        this.millisSinceLastQuery = millisSinceLastQuery;
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
        this.millisSinceLastQuery = in.readZLong();
    }

    /** @param out stream to write this entry's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeZLong(queriesPerMinute);
        out.writeZLong(millisSinceLastQuery);
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
     * How long ago this reader copy last served a query, in millis, or {@link #UNKNOWN_IDLE}.
     *
     * <p><b>Finding S-1: why the rate alone is not a usable signal.</b> {@code
     * ObjectStoreReaderEngine#recordQueryForRateCounter} rolls its 60-second window over only on the
     * <em>next</em> query. When traffic stops there is no next query, so the completed window's count
     * freezes and {@code queriesPerMinute()} reports the last busy value indefinitely. The engine's
     * javadoc acknowledges this and excuses it with "an idle shard has nothing to scale up for
     * regardless" -- which is false for this signal's actual consumer, whose candidate predicate
     * tested only the rate. A shard that sustained 5,000 qpm for an hour and then went to zero was
     * flagged a candidate on every subsequent tick, ratcheted one step per hysteresis window all the
     * way to {@code max_search_replicas}, and stayed there: nothing in this repository ever reduces
     * {@code index.number_of_search_replicas}. The cluster permanently carried five idle reader
     * copies -- each of which must be materialised from the object store -- of a shard receiving no
     * traffic at all.
     *
     * <p>Carrying idleness alongside the rate lets the predicate cross-check them, which also
     * resolves finding S-3: scale-up and scale-to-zero were reading two signals that disagree once
     * traffic stops, with no arbiter, so the same shard could be expanded for load and suspended for
     * idleness concurrently, on independent schedules.
     */
    public long millisSinceLastQuery() {
        return millisSinceLastQuery;
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
            .field("millis_since_last_query", millisSinceLastQuery)
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
        return shardId == that.shardId
            && queriesPerMinute == that.queriesPerMinute
            && millisSinceLastQuery == that.millisSinceLastQuery
            && Objects.equals(indexUuid, that.indexUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexUuid, shardId, queriesPerMinute, millisSinceLastQuery);
    }
}
