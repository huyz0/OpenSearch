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
 * One shard's merged split-candidate policy view: the cluster-wide join of every node's {@code
 * ObjectStoreWriterEngine#writesPerMinute()} for the same {@code (indexUuid, shardId)}, plus the
 * threshold evaluation a real split controller would otherwise have to reimplement itself.
 *
 * <p>Mirrors {@code org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidateEntry}'s
 * shape, but signals a write-throughput split candidate rather than a query-rate replica-expansion
 * candidate, and -- unlike that class -- is deliberately not paired with any mechanism half.
 * {@link org.opensearch.serverless.storage.resharding.ShardSplitter#split} only re-points an
 * already-provisioned target shard identity; it does not create new indices/shards, allocate them,
 * or cut over routing from the source shard to the split targets. None of that orchestration exists
 * in this plugin yet, so there is no automated action this signal could safely drive today.
 * {@link #candidate()} is purely advisory, surfaced for an operator or a future controller to act
 * on manually via the existing {@code ShardSplitAction}, not something this plugin ever acts on
 * itself.
 */
public final class ShardSplitCandidateEntry implements Writeable, ToXContentObject {

    /** Sentinel used for {@link #writesPerMinute()} when no node reported that signal. */
    public static final long UNKNOWN = -1L;

    private final String indexUuid;
    private final int shardId;
    private final String indexName;
    private final long writesPerMinute;
    private final boolean candidate;

    /**
     * Creates an entry.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param indexName the index's current name, for readability and for any future caller that
     *                   would need to address the index by name.
     * @param writesPerMinute the highest writes-per-minute estimate any writer copy of this shard
     *                        reported across the cluster, or {@link #UNKNOWN} if no node reported one.
     * @param candidate whether this plugin's policy considers this shard's sustained write rate
     *                  high enough to be worth an operator's attention as a possible split target.
     */
    public ShardSplitCandidateEntry(String indexUuid, int shardId, String indexName, long writesPerMinute, boolean candidate) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.indexName = indexName;
        this.writesPerMinute = writesPerMinute;
        this.candidate = candidate;
    }

    /**
     * Deserializes an entry.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardSplitCandidateEntry}.
     */
    public ShardSplitCandidateEntry(StreamInput in) throws IOException {
        this.indexUuid = in.readString();
        this.shardId = in.readVInt();
        this.indexName = in.readString();
        this.writesPerMinute = in.readZLong();
        this.candidate = in.readBoolean();
    }

    /** @param out stream to write this entry's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeString(indexName);
        out.writeZLong(writesPerMinute);
        out.writeBoolean(candidate);
    }

    /** UUID of the index the shard belongs to. */
    public String indexUuid() {
        return indexUuid;
    }

    /** The shard number within {@link #indexUuid()}. */
    public int shardId() {
        return shardId;
    }

    /** The index's current name. */
    public String indexName() {
        return indexName;
    }

    /** The highest writes-per-minute estimate any writer copy of this shard reported across the cluster, or {@link #UNKNOWN}. */
    public long writesPerMinute() {
        return writesPerMinute;
    }

    /** Whether this plugin's policy considers this shard's write rate a split candidate. */
    public boolean candidate() {
        return candidate;
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
            .field("index_name", indexName)
            .field("writes_per_minute", writesPerMinute)
            .field("candidate", candidate)
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
        ShardSplitCandidateEntry that = (ShardSplitCandidateEntry) o;
        return shardId == that.shardId
            && writesPerMinute == that.writesPerMinute
            && candidate == that.candidate
            && Objects.equals(indexUuid, that.indexUuid)
            && Objects.equals(indexName, that.indexName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexUuid, shardId, indexName, writesPerMinute, candidate);
    }
}
