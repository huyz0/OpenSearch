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
 * ObjectStoreWriterEngine#writesPerMinute()}/{@code ObjectStoreWriterEngine#shardSizeInBytes()}
 * for the same {@code (indexUuid, shardId)}, plus the threshold evaluation a real split controller
 * would otherwise have to reimplement itself -- a DynamoDB-style split-for-heat trigger ({@link
 * #writeRateCandidate()}) and split-for-size trigger ({@link #sizeCandidate()}), per
 * dynamic-partitioning-plan.md Phase 1 item 1.1.
 *
 * <p>Mirrors {@code org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidateEntry}'s
 * shape, but signals a split candidate rather than a query-rate replica-expansion candidate, and --
 * unlike that class -- is deliberately not paired with any mechanism half yet, beyond the manual
 * {@code InPlaceSplitShardAction} an operator can already trigger directly. {@link #candidate()} is
 * purely advisory, surfaced for an operator or a future controller (Phase 1 item 1.2's scheduler
 * task) to act on, not something this action ever acts on itself.
 */
public final class ShardSplitCandidateEntry implements Writeable, ToXContentObject {

    /** Sentinel used for {@link #writesPerMinute()}/{@link #shardSizeInBytes()} when no node reported that signal. */
    public static final long UNKNOWN = -1L;

    private final String indexUuid;
    private final int shardId;
    private final String indexName;
    private final long writesPerMinute;
    private final long shardSizeInBytes;
    private final boolean writeRateCandidate;
    private final boolean sizeCandidate;

    /**
     * Creates an entry.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param indexName the index's current name, for readability and for any future caller that
     *                   would need to address the index by name.
     * @param writesPerMinute the highest writes-per-minute estimate any writer copy of this shard
     *                        reported across the cluster, or {@link #UNKNOWN} if no node reported one.
     * @param shardSizeInBytes the highest size-in-bytes estimate any writer copy of this shard
     *                         reported across the cluster, or {@link #UNKNOWN} if no node reported one.
     * @param writeRateCandidate whether this plugin's policy considers this shard's sustained
     *                           write rate high enough to be worth an operator's attention.
     * @param sizeCandidate whether this plugin's policy considers this shard's size large enough
     *                      to be worth an operator's attention.
     */
    public ShardSplitCandidateEntry(
        String indexUuid,
        int shardId,
        String indexName,
        long writesPerMinute,
        long shardSizeInBytes,
        boolean writeRateCandidate,
        boolean sizeCandidate
    ) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.indexName = indexName;
        this.writesPerMinute = writesPerMinute;
        this.shardSizeInBytes = shardSizeInBytes;
        this.writeRateCandidate = writeRateCandidate;
        this.sizeCandidate = sizeCandidate;
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
        this.shardSizeInBytes = in.readZLong();
        this.writeRateCandidate = in.readBoolean();
        this.sizeCandidate = in.readBoolean();
    }

    /** @param out stream to write this entry's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeString(indexName);
        out.writeZLong(writesPerMinute);
        out.writeZLong(shardSizeInBytes);
        out.writeBoolean(writeRateCandidate);
        out.writeBoolean(sizeCandidate);
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

    /** The highest size-in-bytes estimate any writer copy of this shard reported across the cluster, or {@link #UNKNOWN}. */
    public long shardSizeInBytes() {
        return shardSizeInBytes;
    }

    /** Whether this plugin's policy considers this shard's write rate a split-for-heat candidate. */
    public boolean writeRateCandidate() {
        return writeRateCandidate;
    }

    /** Whether this plugin's policy considers this shard's size a split-for-size candidate. */
    public boolean sizeCandidate() {
        return sizeCandidate;
    }

    /** Whether either signal makes this shard a split candidate -- {@link #writeRateCandidate()} or {@link #sizeCandidate()}. */
    public boolean candidate() {
        return writeRateCandidate || sizeCandidate;
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
            .field("shard_size_in_bytes", shardSizeInBytes)
            .field("write_rate_candidate", writeRateCandidate)
            .field("size_candidate", sizeCandidate)
            .field("candidate", candidate())
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
            && shardSizeInBytes == that.shardSizeInBytes
            && writeRateCandidate == that.writeRateCandidate
            && sizeCandidate == that.sizeCandidate
            && Objects.equals(indexUuid, that.indexUuid)
            && Objects.equals(indexName, that.indexName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexUuid, shardId, indexName, writesPerMinute, shardSizeInBytes, writeRateCandidate, sizeCandidate);
    }
}
