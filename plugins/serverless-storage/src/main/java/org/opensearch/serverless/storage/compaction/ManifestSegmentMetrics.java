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
 * <p>Soft-delete ratio is <b>not</b> derivable this way -- it lives inside a segment's doc-values
 * data, not its file name or size -- so this always reports {@code estimatedDeleteRatio = 0.0}. Per
 * {@link CompactionRebaseExecutor}'s own safety argument ("at worst, wasted work, never a lost or
 * corrupted update"), underestimating delete ratio only means the delete-reclaim trigger in {@link
 * CompactionPolicy#shouldCompact} never fires from this estimator -- a missed optimization, not a
 * correctness issue; the segment-count/size triggers are unaffected and remain accurate.
 */
public final class ManifestSegmentMetrics {

    /** Number of distinct Lucene segments found in the manifest's file map. */
    public final int segmentCount;
    /** Total size, across all files in the manifest (including the top-level "segments_N" file). */
    public final long totalBytes;
    /** Size of the single largest segment, 0 if there are no segments. */
    public final long largestSingleSegmentBytes;
    /** Always {@code 0.0} -- soft-delete ratio cannot be derived from manifest file metadata alone. */
    public final double estimatedDeleteRatio;

    private ManifestSegmentMetrics(int segmentCount, long totalBytes, long largestSingleSegmentBytes, double estimatedDeleteRatio) {
        this.segmentCount = segmentCount;
        this.totalBytes = totalBytes;
        this.largestSingleSegmentBytes = largestSingleSegmentBytes;
        this.estimatedDeleteRatio = estimatedDeleteRatio;
    }

    /**
     * Derives segment-level metrics from a manifest's file map alone, without opening any segment.
     *
     * @param manifest the commit manifest to derive metrics from
     * @return the derived metrics, with {@code estimatedDeleteRatio} always {@code 0.0}
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

        return new ManifestSegmentMetrics(bytesBySegment.size(), totalBytes, largest, 0.0);
    }
}
