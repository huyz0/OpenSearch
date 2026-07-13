/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.manifest;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * The unit of visibility for one Lucene commit stored in object-store-native form
 * (rfc-serverless-opensearch.md &sect;6.3): everything a reader needs to open exactly this
 * commit &mdash; which bundle-relative byte ranges hold which files, how far the write-ahead log
 * has been folded in, and the mapping version and pruning statistics other subsystems depend on.
 *
 * <p>Manifests are immutable and identified by {@code (primaryTerm, generation)}; naming and
 * publication (list-based discovery vs. shard-head CAS) are the concern of the caller, not this
 * class &mdash; see rfc-serverless-opensearch.md &sect;6.3's "discovery vs. authority" note.
 */
public final class CommitManifest implements Writeable {

    private final String indexUuid;
    private final int shardId;
    private final long primaryTerm;
    private final long generation;
    private final String segmentsFileName;
    private final Map<String, FileReference> files;
    private final long maxSeqNo;
    private final long localCheckpoint;
    private final WalPosition walPosition;
    private final long mappingVersion;
    private final PruningStats pruningStats;
    private final long createdAtMillis;
    private final boolean quiescent;
    private final long totalDocCount;
    private final long deletedDocCount;

    /**
     * Creates a non-quiescent manifest for one immutable Lucene commit -- equivalent to the
     * fuller constructor with {@code quiescent = false} and both doc-count fields {@code 0}, kept
     * so every pre-existing caller (the ordinary hot commit path, and every test that never cared
     * about delete-ratio accounting) is unaffected by {@link #quiescent}'s or {@link
     * #deleteRatio()}'s addition. {@code deleteRatio()} on a manifest built this way is always
     * {@code 0.0} -- the same honest "unknown, treated as no deletions" default {@link
     * org.opensearch.serverless.storage.compaction.ManifestSegmentMetrics} itself used to report
     * unconditionally before real doc counts existed to derive it from.
     *
     * @param indexUuid the UUID of the index this shard belongs to
     * @param shardId the shard number
     * @param primaryTerm the primary term this commit was written under
     * @param generation the generation of this commit, unique within {@code primaryTerm}
     * @param segmentsFileName the name of the Lucene segments file for this commit, which must be
     *                         a key of {@code files}
     * @param files the manifest's file map, keyed by file name
     * @param maxSeqNo the maximum sequence number included in this commit
     * @param localCheckpoint the local checkpoint at this commit
     * @param walPosition the write-ahead log position folded into this commit, or {@code null}
     * @param mappingVersion the mapping version in effect at this commit
     * @param pruningStats summary statistics used to decide whether this shard can be pruned
     * @param createdAtMillis the wall-clock time this manifest was created, in epoch millis
     */
    public CommitManifest(
        String indexUuid,
        int shardId,
        long primaryTerm,
        long generation,
        String segmentsFileName,
        Map<String, FileReference> files,
        long maxSeqNo,
        long localCheckpoint,
        WalPosition walPosition,
        long mappingVersion,
        PruningStats pruningStats,
        long createdAtMillis
    ) {
        this(
            indexUuid,
            shardId,
            primaryTerm,
            generation,
            segmentsFileName,
            files,
            maxSeqNo,
            localCheckpoint,
            walPosition,
            mappingVersion,
            pruningStats,
            createdAtMillis,
            false,
            0,
            0
        );
    }

    /**
     * Same as the twelve-argument constructor, additionally marking the published manifest {@link
     * #quiescent()}. Both doc-count fields default to {@code 0} -- see that constructor's own
     * javadoc for what that means for {@link #deleteRatio()}.
     *
     * @param indexUuid the UUID of the index this shard belongs to
     * @param shardId the shard number
     * @param primaryTerm the primary term this commit was written under
     * @param generation the generation of this commit, unique within {@code primaryTerm}
     * @param segmentsFileName the name of the Lucene segments file for this commit, which must be
     *                         a key of {@code files}
     * @param files the manifest's file map, keyed by file name
     * @param maxSeqNo the maximum sequence number included in this commit
     * @param localCheckpoint the local checkpoint at this commit
     * @param walPosition the write-ahead log position folded into this commit, or {@code null}
     * @param mappingVersion the mapping version in effect at this commit
     * @param pruningStats summary statistics used to decide whether this shard can be pruned
     * @param createdAtMillis the wall-clock time this manifest was created, in epoch millis
     * @param quiescent whether this is a writer's deliberate final commit before scale-to-zero
     *                  suspension (rfc-serverless-opensearch.md &sect;7.3) -- {@code true} means no
     *                  further commit is expected from this writer until it is reactivated.
     */
    public CommitManifest(
        String indexUuid,
        int shardId,
        long primaryTerm,
        long generation,
        String segmentsFileName,
        Map<String, FileReference> files,
        long maxSeqNo,
        long localCheckpoint,
        WalPosition walPosition,
        long mappingVersion,
        PruningStats pruningStats,
        long createdAtMillis,
        boolean quiescent
    ) {
        this(
            indexUuid,
            shardId,
            primaryTerm,
            generation,
            segmentsFileName,
            files,
            maxSeqNo,
            localCheckpoint,
            walPosition,
            mappingVersion,
            pruningStats,
            createdAtMillis,
            quiescent,
            0,
            0
        );
    }

    /**
     * Creates a manifest for one immutable Lucene commit.
     *
     * @param indexUuid the UUID of the index this shard belongs to
     * @param shardId the shard number
     * @param primaryTerm the primary term this commit was written under
     * @param generation the generation of this commit, unique within {@code primaryTerm}
     * @param segmentsFileName the name of the Lucene segments file for this commit, which must be
     *                         a key of {@code files}
     * @param files the manifest's file map, keyed by file name
     * @param maxSeqNo the maximum sequence number included in this commit
     * @param localCheckpoint the local checkpoint at this commit
     * @param walPosition the write-ahead log position folded into this commit, or {@code null}
     * @param mappingVersion the mapping version in effect at this commit
     * @param pruningStats summary statistics used to decide whether this shard can be pruned
     * @param createdAtMillis the wall-clock time this manifest was created, in epoch millis
     * @param quiescent whether this is a writer's deliberate final commit before scale-to-zero
     *                  suspension (rfc-serverless-opensearch.md &sect;7.3) -- {@code true} means no
     *                  further commit is expected from this writer until it is reactivated.
     * @param totalDocCount the total document count (live + deleted) across every segment in this
     *                      commit, i.e. the sum of each segment's {@code maxDoc} -- {@code 0} means
     *                      unknown, not "zero documents" (see {@link #deleteRatio()}).
     * @param deletedDocCount the total soft- plus hard-deleted document count across every segment
     *                        in this commit, i.e. the sum of each segment's {@code getSoftDelCount()
     *                        + getDelCount()} -- meaningless on its own without {@code totalDocCount}
     *                        to compare it against, which is why both travel together rather than a
     *                        single precomputed ratio (a future consumer needing the raw counts,
     *                        e.g. an exact reclaimable-bytes estimate, doesn't need a schema change).
     */
    public CommitManifest(
        String indexUuid,
        int shardId,
        long primaryTerm,
        long generation,
        String segmentsFileName,
        Map<String, FileReference> files,
        long maxSeqNo,
        long localCheckpoint,
        WalPosition walPosition,
        long mappingVersion,
        PruningStats pruningStats,
        long createdAtMillis,
        boolean quiescent,
        long totalDocCount,
        long deletedDocCount
    ) {
        this.indexUuid = Objects.requireNonNull(indexUuid, "indexUuid");
        if (shardId < 0) {
            throw new IllegalArgumentException("shardId must be >= 0, got " + shardId);
        }
        if (primaryTerm < 1) {
            throw new IllegalArgumentException("primaryTerm must be >= 1, got " + primaryTerm);
        }
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be >= 0, got " + generation);
        }
        if (totalDocCount < 0) {
            throw new IllegalArgumentException("totalDocCount must be >= 0, got " + totalDocCount);
        }
        if (deletedDocCount < 0 || deletedDocCount > totalDocCount) {
            throw new IllegalArgumentException(
                "deletedDocCount must be in [0, totalDocCount=" + totalDocCount + "], got " + deletedDocCount
            );
        }
        this.shardId = shardId;
        this.primaryTerm = primaryTerm;
        this.generation = generation;
        this.segmentsFileName = Objects.requireNonNull(segmentsFileName, "segmentsFileName");
        this.files = Map.copyOf(Objects.requireNonNull(files, "files"));
        if (!this.files.containsKey(segmentsFileName)) {
            throw new IllegalArgumentException("files map does not contain the segments file '" + segmentsFileName + "'");
        }
        this.maxSeqNo = maxSeqNo;
        this.localCheckpoint = localCheckpoint;
        this.walPosition = walPosition;
        this.mappingVersion = mappingVersion;
        this.pruningStats = Objects.requireNonNull(pruningStats, "pruningStats");
        this.createdAtMillis = createdAtMillis;
        this.quiescent = quiescent;
        this.totalDocCount = totalDocCount;
        this.deletedDocCount = deletedDocCount;
    }

    /**
     * Deserializes a manifest previously written by {@link #writeTo}.
     *
     * @param in the stream to read from
     */
    public CommitManifest(StreamInput in) throws IOException {
        this.indexUuid = in.readString();
        this.shardId = in.readVInt();
        this.primaryTerm = in.readVLong();
        this.generation = in.readVLong();
        this.segmentsFileName = in.readString();
        this.files = in.readMap(StreamInput::readString, FileReference::new);
        this.maxSeqNo = in.readZLong();
        this.localCheckpoint = in.readZLong();
        this.walPosition = in.readOptionalWriteable(WalPosition::new);
        this.mappingVersion = in.readVLong();
        this.pruningStats = new PruningStats(in);
        this.createdAtMillis = in.readVLong();
        this.quiescent = in.readBoolean();
        this.totalDocCount = in.readVLong();
        this.deletedDocCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeVLong(primaryTerm);
        out.writeVLong(generation);
        out.writeString(segmentsFileName);
        out.writeMap(files, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        out.writeZLong(maxSeqNo);
        out.writeZLong(localCheckpoint);
        out.writeOptionalWriteable(walPosition);
        out.writeVLong(mappingVersion);
        pruningStats.writeTo(out);
        out.writeVLong(createdAtMillis);
        out.writeBoolean(quiescent);
        out.writeVLong(totalDocCount);
        out.writeVLong(deletedDocCount);
    }

    /** The UUID of the index this shard belongs to. */
    public String indexUuid() {
        return indexUuid;
    }

    /** The shard number. */
    public int shardId() {
        return shardId;
    }

    /** The primary term this commit was written under. */
    public long primaryTerm() {
        return primaryTerm;
    }

    /** The generation of this commit, unique within {@link #primaryTerm()}. */
    public long generation() {
        return generation;
    }

    /** The name of the Lucene segments file for this commit. */
    public String segmentsFileName() {
        return segmentsFileName;
    }

    /** The manifest's file map, keyed by file name. */
    public Map<String, FileReference> files() {
        return files;
    }

    /** The maximum sequence number included in this commit. */
    public long maxSeqNo() {
        return maxSeqNo;
    }

    /** The local checkpoint at this commit. */
    public long localCheckpoint() {
        return localCheckpoint;
    }

    /** The write-ahead log position folded into this commit, or {@code null}. */
    public WalPosition walPosition() {
        return walPosition;
    }

    /** The mapping version in effect at this commit. */
    public long mappingVersion() {
        return mappingVersion;
    }

    /** Summary statistics used to decide whether this shard can be pruned. */
    public PruningStats pruningStats() {
        return pruningStats;
    }

    /** The wall-clock time this manifest was created, in epoch millis. */
    public long createdAtMillis() {
        return createdAtMillis;
    }

    /**
     * Whether this is a writer's deliberate final commit before scale-to-zero suspension --
     * {@code true} means no further commit is expected from this writer until it is reactivated
     * (rfc-serverless-opensearch.md &sect;7.3).
     */
    public boolean quiescent() {
        return quiescent;
    }

    /**
     * The total document count (live + deleted) across every segment in this commit, i.e. the sum
     * of each segment's {@code maxDoc}. {@code 0} means unknown (a manifest built through a
     * constructor overload that doesn't take doc counts), not literally zero documents -- see
     * {@link #deleteRatio()}.
     */
    public long totalDocCount() {
        return totalDocCount;
    }

    /**
     * The total soft- plus hard-deleted document count across every segment in this commit. Only
     * meaningful together with {@link #totalDocCount()} -- see {@link #deleteRatio()}.
     */
    public long deletedDocCount() {
        return deletedDocCount;
    }

    /**
     * The fraction of this commit's documents that are soft- or hard-deleted, derived from {@link
     * #totalDocCount()}/{@link #deletedDocCount()} rather than stored as a single precomputed
     * field. {@code 0.0} both for a genuinely delete-free commit and for one built through a
     * constructor overload that never recorded doc counts at all (an honest "unknown, treat as no
     * deletions" default, the same one {@link org.opensearch.serverless.storage.compaction.ManifestSegmentMetrics}
     * itself used to report unconditionally before this field existed) -- {@link
     * org.opensearch.serverless.storage.compaction.CompactionRebaseExecutor}'s own safety argument
     * ("at worst, wasted work, never a lost or corrupted update") is exactly why collapsing "unknown"
     * into "zero" here is safe: it only ever means the delete-reclaim compaction trigger doesn't
     * fire when it perhaps should, never the reverse.
     */
    public double deleteRatio() {
        return totalDocCount == 0 ? 0.0 : (double) deletedDocCount / totalDocCount;
    }

    /** Every manifest blob name starts with this -- the prefix {@link org.opensearch.serverless.storage.manifest.BlobContainerManifestStore#listManifests} lists by. */
    public static final String NAME_PREFIX = "manifest-";

    /** The canonical object name for this manifest, per rfc-serverless-opensearch.md &sect;6.3. */
    public String manifestName() {
        return manifestName(primaryTerm, generation);
    }

    /**
     * The canonical object name for the manifest identified by {@code (primaryTerm, generation)},
     * per rfc-serverless-opensearch.md &sect;6.3.
     *
     * @param primaryTerm the primary term of the manifest
     * @param generation the generation of the manifest
     * @return the canonical blob name
     */
    public static String manifestName(long primaryTerm, long generation) {
        return NAME_PREFIX + primaryTerm + "-" + generation;
    }

    /**
     * Whether {@code this} is strictly newer than {@code other} under the total order
     * (primaryTerm, generation) &mdash; term dominates, so a higher generation under an older
     * (stale) term is still older overall (rfc-serverless-opensearch.md &sect;6.3 fencing).
     *
     * @param other the manifest to compare against
     * @return {@code true} if {@code this} is strictly newer than {@code other}
     */
    public boolean isNewerThan(CommitManifest other) {
        if (this.primaryTerm != other.primaryTerm) {
            return this.primaryTerm > other.primaryTerm;
        }
        return this.generation > other.generation;
    }

    /** All distinct bundle names referenced by this manifest's file map. */
    public java.util.Set<String> referencedBundles() {
        java.util.Set<String> bundles = new java.util.LinkedHashSet<>();
        for (FileReference ref : files.values()) {
            bundles.add(ref.bundleName());
        }
        return bundles;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CommitManifest)) return false;
        CommitManifest that = (CommitManifest) o;
        return shardId == that.shardId
            && primaryTerm == that.primaryTerm
            && generation == that.generation
            && maxSeqNo == that.maxSeqNo
            && localCheckpoint == that.localCheckpoint
            && mappingVersion == that.mappingVersion
            && createdAtMillis == that.createdAtMillis
            && quiescent == that.quiescent
            && totalDocCount == that.totalDocCount
            && deletedDocCount == that.deletedDocCount
            && indexUuid.equals(that.indexUuid)
            && segmentsFileName.equals(that.segmentsFileName)
            && files.equals(that.files)
            && Objects.equals(walPosition, that.walPosition)
            && pruningStats.equals(that.pruningStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            indexUuid,
            shardId,
            primaryTerm,
            generation,
            segmentsFileName,
            files,
            maxSeqNo,
            localCheckpoint,
            walPosition,
            mappingVersion,
            pruningStats,
            createdAtMillis,
            quiescent,
            totalDocCount,
            deletedDocCount
        );
    }

    @Override
    public String toString() {
        return "CommitManifest{"
            + "index="
            + indexUuid
            + ", shard="
            + shardId
            + ", term="
            + primaryTerm
            + ", gen="
            + generation
            + ", files="
            + files.size()
            + '}';
    }
}
