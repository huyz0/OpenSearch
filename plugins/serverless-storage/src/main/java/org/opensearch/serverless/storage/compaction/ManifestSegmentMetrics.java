/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction;

import org.apache.lucene.index.IndexFileNames;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;

import java.util.HashMap;
import java.util.Map;

/**
 * Derives the {@link CompactionPolicy#shouldCompact} inputs straight from a {@link CommitManifest}'s
 * file map -- no bundle needs to be opened, matching rfc-serverless-opensearch.md &sect;7.4's
 * "select candidates from manifest metadata alone" requirement, and letting a scheduler evaluate
 * every shard's compaction eligibility from manifest listings alone.
 *
 * <p>Segment identity is recovered via {@link IndexFileNames#parseSegmentName}, the same Lucene
 * convention every file in a commit's file set already follows ({@code "_&lt;segment&gt;..."}),
 * grouping this manifest's files by which segment they belong to without needing to open any of
 * them.
 *
 * <p>Soft-delete ratio is <b>not</b> derivable from the file map alone -- it lives inside a
 * segment's doc-values data, not its file name or size. A per-segment live-docs file (Lucene's
 * {@code .liv}) looked like a plausible manifest-metadata-only proxy for "this segment has
 * deletions," and was confirmed empirically, not just assumed from reading Lucene's source, to
 * <em>not</em> work: a real {@code ObjectStoreWriterEngine}, indexing two documents then
 * soft-deleting one (the default deletion path every OpenSearch index actually uses, {@code
 * index.soft_deletes.enabled=true}) and flushing, produces a manifest with <em>no</em> {@code .liv}
 * file at all -- the soft-delete instead shows up only as a doc-values field-generation update
 * ({@code SegmentCommitInfo#getSoftDelCount()} is 1, {@code hasDeletions()} is {@code false}). A
 * {@code .liv}-presence heuristic would therefore silently do nothing for the dominant real-world
 * deletion path while looking like real logic.
 *
 * <p><b>{@link CommitManifest#deleteRatio()} closes this instead, by carrying the real doc counts
 * in the manifest itself</b> rather than trying to re-derive them from the file map: {@link
 * org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher#publishCommit} computes
 * {@code totalDocCount}/{@code deletedDocCount} directly from the {@code SegmentInfos} it already
 * has in hand at publish time (summing each segment's {@code maxDoc} and {@code getSoftDelCount() +
 * getDelCount()}), so no manifest-metadata-only proxy is needed -- the real Lucene truth is recorded
 * once, at the one point it's cheaply available, instead of reconstructed later from artifacts that
 * don't reliably carry it. A manifest built through an older constructor overload that never
 * recorded doc counts reports {@code deleteRatio() == 0.0}, the same honest "unknown, treat as no
 * deletions" default this class used to return unconditionally -- see {@link
 * CompactionRebaseExecutor}'s own safety argument ("at worst, wasted work, never a lost or corrupted
 * update") for why collapsing "unknown" into "zero" here only ever means the delete-reclaim trigger
 * in {@link CompactionPolicy#shouldCompact} doesn't fire when it perhaps should, never the reverse;
 * the segment-count/size triggers, which don't depend on it, are unaffected either way.
 */
public final class ManifestSegmentMetrics {

    /** Number of distinct Lucene segments found in the manifest's file map. */
    public final int segmentCount;
    /** Total size, across all files in the manifest (including the top-level "segments_N" file). */
    public final long totalBytes;
    /** Size of the single largest segment, 0 if there are no segments. */
    public final long largestSingleSegmentBytes;
    /**
     * {@link CommitManifest#deleteRatio()}, carried through unchanged -- {@code 0.0} both for a
     * genuinely delete-free commit and for a manifest that never recorded doc counts at all (an
     * older manifest, or one built through a doc-count-less constructor overload in a test).
     */
    public final double estimatedDeleteRatio;

    private ManifestSegmentMetrics(int segmentCount, long totalBytes, long largestSingleSegmentBytes, double estimatedDeleteRatio) {
        this.segmentCount = segmentCount;
        this.totalBytes = totalBytes;
        this.largestSingleSegmentBytes = largestSingleSegmentBytes;
        this.estimatedDeleteRatio = estimatedDeleteRatio;
    }

    /**
     * Derives segment-level metrics from a manifest's file map, plus {@code estimatedDeleteRatio}
     * from the manifest's own recorded doc counts ({@link CommitManifest#deleteRatio()}) -- no
     * segment needs to be opened for either.
     *
     * @param manifest the commit manifest to derive metrics from
     * @return the derived metrics
     */
    public static ManifestSegmentMetrics from(CommitManifest manifest) {
        Map<String, Long> bytesBySegment = new HashMap<>();
        long totalBytes = 0;
        for (Map.Entry<String, FileReference> entry : manifest.files().entrySet()) {
            String fileName = entry.getKey();
            long length = entry.getValue().length();
            totalBytes += length;

            if (fileName.equals(manifest.segmentsFileName())) {
                // The top-level "segments_N" file itself -- not part of any one segment, doesn't
                // count toward segment count or any single segment's size.
                continue;
            }
            bytesBySegment.merge(IndexFileNames.parseSegmentName(fileName), length, Long::sum);
        }

        long largest = 0;
        for (long segmentBytes : bytesBySegment.values()) {
            largest = Math.max(largest, segmentBytes);
        }

        return new ManifestSegmentMetrics(bytesBySegment.size(), totalBytes, largest, manifest.deleteRatio());
    }
}
