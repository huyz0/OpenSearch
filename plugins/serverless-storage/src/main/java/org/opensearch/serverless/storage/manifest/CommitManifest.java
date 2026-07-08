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
    }

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
    }

    public String indexUuid() {
        return indexUuid;
    }

    public int shardId() {
        return shardId;
    }

    public long primaryTerm() {
        return primaryTerm;
    }

    public long generation() {
        return generation;
    }

    public String segmentsFileName() {
        return segmentsFileName;
    }

    public Map<String, FileReference> files() {
        return files;
    }

    public long maxSeqNo() {
        return maxSeqNo;
    }

    public long localCheckpoint() {
        return localCheckpoint;
    }

    public WalPosition walPosition() {
        return walPosition;
    }

    public long mappingVersion() {
        return mappingVersion;
    }

    public PruningStats pruningStats() {
        return pruningStats;
    }

    public long createdAtMillis() {
        return createdAtMillis;
    }

    /** Every manifest blob name starts with this -- the prefix {@link org.opensearch.serverless.storage.manifest.BlobContainerManifestStore#listManifests} lists by. */
    public static final String NAME_PREFIX = "manifest-";

    /** The canonical object name for this manifest, per rfc-serverless-opensearch.md &sect;6.3. */
    public String manifestName() {
        return manifestName(primaryTerm, generation);
    }

    public static String manifestName(long primaryTerm, long generation) {
        return NAME_PREFIX + primaryTerm + "-" + generation;
    }

    /**
     * Whether {@code this} is strictly newer than {@code other} under the total order
     * (primaryTerm, generation) &mdash; term dominates, so a higher generation under an older
     * (stale) term is still older overall (rfc-serverless-opensearch.md &sect;6.3 fencing).
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
            createdAtMillis
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
