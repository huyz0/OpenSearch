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
 * <p>Deliberately read-only: this entry only ever reports what {@link #candidate()} the plugin's
 * policy considers this shard to be, never suspends or reactivates anything -- see this package's
 * own {@code package-info.java} for why the actual suspend/reactivate mechanism is out of scope
 * for a single node-local plugin action.
 */
public final class ScaleToZeroCandidateEntry implements Writeable, ToXContentObject {

    /** Sentinel used for {@link #millisSinceLastActivity()}/{@link #manifestGenerationLag()} when no node reported that signal. */
    public static final long UNKNOWN = -1L;

    private final String indexUuid;
    private final int shardId;
    private final long millisSinceLastActivity;
    private final long manifestGenerationLag;
    private final boolean candidate;

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
     * @param candidate whether this plugin's policy considers this shard idle long enough, with
     *                  every observed reader caught up, to be worth a real controller's attention.
     */
    public ScaleToZeroCandidateEntry(
        String indexUuid,
        int shardId,
        long millisSinceLastActivity,
        long manifestGenerationLag,
        boolean candidate
    ) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.millisSinceLastActivity = millisSinceLastActivity;
        this.manifestGenerationLag = manifestGenerationLag;
        this.candidate = candidate;
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
    }

    /** @param out stream to write this entry's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeZLong(millisSinceLastActivity);
        out.writeZLong(manifestGenerationLag);
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

    /** The longest idle time any writer copy of this shard reported across the cluster, or {@link #UNKNOWN}. */
    public long millisSinceLastActivity() {
        return millisSinceLastActivity;
    }

    /** The worst manifest-generation lag any reader copy of this shard reported across the cluster, or {@link #UNKNOWN}. */
    public long manifestGenerationLag() {
        return manifestGenerationLag;
    }

    /** Whether this plugin's policy considers this shard a scale-to-zero candidate. */
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
            .field("millis_since_last_activity", millisSinceLastActivity)
            .field("manifest_generation_lag", manifestGenerationLag)
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
        ScaleToZeroCandidateEntry that = (ScaleToZeroCandidateEntry) o;
        return shardId == that.shardId
            && millisSinceLastActivity == that.millisSinceLastActivity
            && manifestGenerationLag == that.manifestGenerationLag
            && candidate == that.candidate
            && Objects.equals(indexUuid, that.indexUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexUuid, shardId, millisSinceLastActivity, manifestGenerationLag, candidate);
    }
}
