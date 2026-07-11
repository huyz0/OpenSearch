/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * One shard's merged scale-to-zero policy view (rfc-serverless-opensearch.md &sect;7.3/&sect;10) --
 * the cluster-wide join of {@link org.opensearch.serverless.storage.writerengine.action.NodeIdleShardsAction}'s
 * writer-idle signal and {@link org.opensearch.serverless.storage.readerengine.action.NodeManifestLagAction}'s
 * reader-freshness signal for the same {@code (indexUuid, shardId)}, plus the threshold evaluation
 * a real suspension controller would otherwise have to reimplement itself.
 *
 * <p>Writer and reader candidacy are two entirely independent judgements, reflecting {@link
 * org.opensearch.serverless.storage.allocation.SuspendedShardsMetadata}'s own writer/reader split --
 * {@link #candidate()} is "should the writer copy be suspended" (idle long enough, with every
 * observed reader caught up so suspending the writer doesn't strand a reader with no fresher
 * manifest to poll for); {@link #readerCandidate()} is "should the reader copy be suspended" (no
 * real query traffic for long enough), independent of the writer's own idle/lag state.
 *
 * <p>Deliberately read-only: this entry only ever reports what the plugin's policy considers this
 * shard to be, never suspends or reactivates anything -- see this package's own {@code
 * package-info.java} for why the actual suspend/reactivate mechanism is out of scope for a single
 * node-local plugin action.
 */
public final class ScaleToZeroCandidateEntry implements Writeable, ToXContentObject {

    /**
     * Sentinel used for {@link #millisSinceLastActivity()}/{@link #manifestGenerationLag()}/{@link
     * #readerMillisSinceLastQuery()} when no node reported that signal.
     */
    public static final long UNKNOWN = -1L;

    private final String indexUuid;
    private final int shardId;
    private final long millisSinceLastActivity;
    private final long manifestGenerationLag;
    private final boolean candidate;
    private final long readerMillisSinceLastQuery;
    private final boolean readerCandidate;

    /**
     * Creates an entry with no reader-candidacy signal (equivalent to {@link #UNKNOWN}/{@code
     * false}) -- kept for callers that only ever cared about writer candidacy before reader
     * scale-to-zero existed.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param millisSinceLastActivity the longest idle time any writer copy of this shard reported
     *                                across the cluster, or {@link #UNKNOWN} if no node reported a
     *                                writer copy of this shard.
     * @param manifestGenerationLag the worst (largest) manifest-generation lag any reader copy of
     *                              this shard reported across the cluster, or {@link #UNKNOWN} if
     *                              no node reported a reader copy of this shard.
     * @param candidate whether this plugin's policy considers this shard's writer copy idle long
     *                  enough, with every observed reader caught up, to be worth suspending.
     */
    public ScaleToZeroCandidateEntry(
        String indexUuid,
        int shardId,
        long millisSinceLastActivity,
        long manifestGenerationLag,
        boolean candidate
    ) {
        this(indexUuid, shardId, millisSinceLastActivity, manifestGenerationLag, candidate, UNKNOWN, false);
    }

    /**
     * Creates an entry.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param millisSinceLastActivity the longest idle time any writer copy of this shard reported
     *                                across the cluster, or {@link #UNKNOWN} if no node reported a
     *                                writer copy of this shard.
     * @param manifestGenerationLag the worst (largest) manifest-generation lag any reader copy of
     *                              this shard reported across the cluster, or {@link #UNKNOWN} if
     *                              no node reported a reader copy of this shard.
     * @param candidate whether this plugin's policy considers this shard's writer copy idle long
     *                  enough, with every observed reader caught up, to be worth suspending.
     * @param readerMillisSinceLastQuery the longest query idle time any reader copy of this shard
     *                                   reported across the cluster, or {@link #UNKNOWN} if no node
     *                                   reported a reader copy of this shard.
     * @param readerCandidate whether this plugin's policy considers this shard's reader copy idle
     *                        (no query traffic) long enough to be worth suspending.
     */
    public ScaleToZeroCandidateEntry(
        String indexUuid,
        int shardId,
        long millisSinceLastActivity,
        long manifestGenerationLag,
        boolean candidate,
        long readerMillisSinceLastQuery,
        boolean readerCandidate
    ) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.millisSinceLastActivity = millisSinceLastActivity;
        this.manifestGenerationLag = manifestGenerationLag;
        this.candidate = candidate;
        this.readerMillisSinceLastQuery = readerMillisSinceLastQuery;
        this.readerCandidate = readerCandidate;
    }

    /**
     * Deserializes an entry.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ScaleToZeroCandidateEntry}.
     */
    public ScaleToZeroCandidateEntry(StreamInput in) throws IOException {
        this.indexUuid = in.readString();
        this.shardId = in.readVInt();
        this.millisSinceLastActivity = in.readZLong();
        this.manifestGenerationLag = in.readZLong();
        this.candidate = in.readBoolean();
        this.readerMillisSinceLastQuery = in.readZLong();
        this.readerCandidate = in.readBoolean();
    }

    /** @param out stream to write this entry's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeZLong(millisSinceLastActivity);
        out.writeZLong(manifestGenerationLag);
        out.writeBoolean(candidate);
        out.writeZLong(readerMillisSinceLastQuery);
        out.writeBoolean(readerCandidate);
    }

    /** UUID of the index the shard belongs to. */
    public String indexUuid() {
        return indexUuid;
    }

    /** The shard number within {@link #indexUuid()}. */
    public int shardId() {
        return shardId;
    }

    /** The longest idle time any writer copy of this shard reported across the cluster, or {@link #UNKNOWN}. */
    public long millisSinceLastActivity() {
        return millisSinceLastActivity;
    }

    /** The worst manifest-generation lag any reader copy of this shard reported across the cluster, or {@link #UNKNOWN}. */
    public long manifestGenerationLag() {
        return manifestGenerationLag;
    }

    /** Whether this plugin's policy considers this shard's writer copy a scale-to-zero candidate. */
    public boolean candidate() {
        return candidate;
    }

    /** The longest query idle time any reader copy of this shard reported across the cluster, or {@link #UNKNOWN}. */
    public long readerMillisSinceLastQuery() {
        return readerMillisSinceLastQuery;
    }

    /** Whether this plugin's policy considers this shard's reader copy a scale-to-zero candidate. */
    public boolean readerCandidate() {
        return readerCandidate;
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
            .field("manifest_generation_lag", manifestGenerationLag)
            .field("candidate", candidate)
            .field("reader_millis_since_last_query", readerMillisSinceLastQuery)
            .field("reader_candidate", readerCandidate)
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
        ScaleToZeroCandidateEntry that = (ScaleToZeroCandidateEntry) o;
        return shardId == that.shardId
            && millisSinceLastActivity == that.millisSinceLastActivity
            && manifestGenerationLag == that.manifestGenerationLag
            && candidate == that.candidate
            && readerMillisSinceLastQuery == that.readerMillisSinceLastQuery
            && readerCandidate == that.readerCandidate
            && Objects.equals(indexUuid, that.indexUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            indexUuid,
            shardId,
            millisSinceLastActivity,
            manifestGenerationLag,
            candidate,
            readerMillisSinceLastQuery,
            readerCandidate
        );
    }
}
