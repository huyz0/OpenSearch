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
 * One shard's merged scale-up policy view (see the RFC's scale-up autoscaling subsection): the
 * cluster-wide join of every node's {@link ShardQueryRateEntry} for the same {@code (indexUuid,
 * shardId)}, plus this index's current {@code index.number_of_search_replicas} (read from cluster
 * metadata, not from any node-local signal -- replica count is index configuration, not a
 * per-node observation) and the threshold evaluation a real expansion controller would otherwise
 * have to reimplement itself.
 *
 * <p>Mirrors {@code org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidateEntry}'s
 * shape, but for the opposite direction: {@link #candidate()} is "should another search-only
 * replica be added" (query rate above threshold, and the index hasn't already hit its configured
 * cap), independent of and unrelated to writer-shard idle/suspend policy entirely.
 *
 * <p>Deliberately read-only, same reasoning as {@code ScaleToZeroCandidateEntry}: this entry only
 * ever reports what the plugin's policy considers this shard to be, never itself expands anything.
 */
public final class ScaleUpCandidateEntry implements Writeable, ToXContentObject {

    private final String indexUuid;
    private final int shardId;
    private final String indexName;
    private final long queriesPerMinute;
    private final int currentSearchReplicaCount;
    private final boolean candidate;

    /**
     * Creates an entry.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param indexName the index's current name -- needed (unlike every other entry in this
     *                  plugin, which is keyed purely by UUID) because expanding replica count
     *                  means submitting a settings update, and that update's API is name-addressed.
     * @param queriesPerMinute the highest queries-per-minute estimate any reader copy of this
     *                         shard reported across the cluster.
     * @param currentSearchReplicaCount this index's current {@code index.number_of_search_replicas},
     *                                  as of the cluster state this evaluation read.
     * @param candidate whether this plugin's policy considers this shard's reader copy busy enough,
     *                  with the index not yet at its configured replica cap, to be worth expanding.
     */
    public ScaleUpCandidateEntry(
        String indexUuid,
        int shardId,
        String indexName,
        long queriesPerMinute,
        int currentSearchReplicaCount,
        boolean candidate
    ) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.indexName = indexName;
        this.queriesPerMinute = queriesPerMinute;
        this.currentSearchReplicaCount = currentSearchReplicaCount;
        this.candidate = candidate;
    }

    /**
     * Deserializes an entry.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ScaleUpCandidateEntry}.
     */
    public ScaleUpCandidateEntry(StreamInput in) throws IOException {
        this.indexUuid = in.readString();
        this.shardId = in.readVInt();
        this.indexName = in.readString();
        this.queriesPerMinute = in.readZLong();
        this.currentSearchReplicaCount = in.readVInt();
        this.candidate = in.readBoolean();
    }

    /** @param out stream to write this entry's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeString(indexName);
        out.writeZLong(queriesPerMinute);
        out.writeVInt(currentSearchReplicaCount);
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

    /** The index's current name, for submitting a settings update. */
    public String indexName() {
        return indexName;
    }

    /** The highest queries-per-minute estimate any reader copy of this shard reported across the cluster. */
    public long queriesPerMinute() {
        return queriesPerMinute;
    }

    /** This index's current {@code index.number_of_search_replicas}, as of the cluster state this evaluation read. */
    public int currentSearchReplicaCount() {
        return currentSearchReplicaCount;
    }

    /** Whether this plugin's policy considers this shard's reader copy a scale-up candidate. */
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
            .field("queries_per_minute", queriesPerMinute)
            .field("current_search_replica_count", currentSearchReplicaCount)
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
        ScaleUpCandidateEntry that = (ScaleUpCandidateEntry) o;
        return shardId == that.shardId
            && queriesPerMinute == that.queriesPerMinute
            && currentSearchReplicaCount == that.currentSearchReplicaCount
            && candidate == that.candidate
            && Objects.equals(indexUuid, that.indexUuid)
            && Objects.equals(indexName, that.indexName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexUuid, shardId, indexName, queriesPerMinute, currentSearchReplicaCount, candidate);
    }
}
