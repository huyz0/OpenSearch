/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * One manifest generation that just stopped being a shard's head, recorded at the instant it happened.
 *
 * <h2>Why this exists</h2>
 *
 * {@code GcSchedulerTask} discovers deletable manifests by relisting an entire shard's container and
 * recomputing the answer from scratch, on a fixed clock, whether or not anything changed since the last
 * tick. That is expensive in the one dimension that actually costs money: a {@code listBlobsByPrefix} call
 * is priced in S3's write tier, not its read tier, and a shard that has not written anything new still pays
 * for it every tick, forever, as long as it is warm.
 *
 * <p>This record is the alternative: the moment a manifest is superseded is also the moment its identity is
 * completely and unambiguously known, by the one caller in the best position to know it --
 * {@code ObjectStoreCommitHeadPublisher}, which just won the CAS that superseded it. Recording that fact as
 * it happens turns discovery from "relist and diff, always" into "read what was appended, only when
 * something was." See {@link GcCandidateTailer} for the consumer, {@link BlobGcCandidateLog} for where these
 * live, and this package's {@code GcSchedulerTask} for what deliberately keeps running alongside it.
 *
 * <h2>What this deliberately does not cover</h2>
 *
 * Only supersession -- a manifest that <em>was</em> the head and now is not. A manifest written but never
 * published as head at all (the losing side of a CAS race under contention, per
 * {@code ObjectStoreCommitHeadPublisher#publishCommitAsHead}'s own javadoc) never generates one of these,
 * because nothing here ever observed it as current in the first place. {@code GcSchedulerTask}'s own sweep
 * is what remains responsible for that case, and for bundle orphan detection generally -- see this class's
 * own package javadoc note in {@link GcCandidateTailer} for why bundle liveness does not get the same
 * treatment as manifest supersession does here.
 *
 * <h2>What it deliberately does not carry</h2>
 *
 * {@code createdAtMillis}. Carrying it would mean reading the superseded manifest's own header at publish
 * time to populate it, which puts an extra object-store read on the hottest, most latency-sensitive path in
 * this whole system to save a read on a background tailer that can much better afford it. {@link
 * GcCandidateTailer} reads it once, lazily, off this record's identity, exactly where that cost belongs.
 *
 * @param indexUuid  the index this shard belongs to.
 * @param shardId    the shard the superseded manifest belonged to.
 * @param primaryTerm the primary term the superseded manifest was published under.
 * @param generation the superseded manifest's own generation.
 */
public record GcCandidate(String indexUuid, int shardId, long primaryTerm, long generation) implements Writeable {

    public GcCandidate {
        Objects.requireNonNull(indexUuid, "indexUuid is required");
        if (shardId < 0) {
            throw new IllegalArgumentException("shardId must be >= 0, got " + shardId);
        }
        if (primaryTerm < 1) {
            throw new IllegalArgumentException("primaryTerm must be >= 1, got " + primaryTerm);
        }
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be >= 0, got " + generation);
        }
    }

    /** Reads a candidate from a stream. */
    public GcCandidate(StreamInput in) throws IOException {
        this(in.readString(), in.readVInt(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeVLong(primaryTerm);
        out.writeVLong(generation);
    }

    /**
     * A name stable across repeated appends of the same fact, so a retried append (e.g. a caller-level retry
     * outside {@code ObjectStoreCommitHeadPublisher}'s own CAS loop) overwrites an identical entry rather
     * than creating a second one. {@link ManifestId} already exists for exactly this identity but is not
     * reused as the type here: it carries no {@code indexUuid}/{@code shardId}, which this needs to be
     * addressable at all.
     */
    public String entryName() {
        return indexUuid + "-" + shardId + "-" + primaryTerm + "-" + generation;
    }

    /** This candidate's manifest identity, for callers that already work in terms of {@link ManifestId}. */
    public ManifestId manifestId() {
        return new ManifestId(primaryTerm, generation);
    }
}
