/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction;

/**
 * Decides whether a shard is worth compacting, from manifest-derivable metrics alone &mdash; no
 * bundle needs to be opened to make this decision (rfc-serverless-opensearch.md &sect;7.4:
 * "select candidates from manifest metadata alone (segment count, size skew, delete ratio -- all
 * readable without opening the shard)"). This is the policy half of the compaction service; it is
 * deliberately independent of how those metrics get computed (that requires real Lucene
 * SegmentInfos/segment-attribute access, which belongs to the caller wiring this into an actual
 * engine) and independent of the rebase/publication mechanics (see {@link CompactionRebaseExecutor}).
 */
public final class CompactionPolicy {

    private final int minSegmentCountToCompact;
    private final long maxTargetBundleSizeBytes;
    private final double minDeleteRatioToCompact;

    public CompactionPolicy(int minSegmentCountToCompact, long maxTargetBundleSizeBytes, double minDeleteRatioToCompact) {
        if (minSegmentCountToCompact < 2) {
            throw new IllegalArgumentException("minSegmentCountToCompact must be >= 2, got " + minSegmentCountToCompact);
        }
        if (maxTargetBundleSizeBytes <= 0) {
            throw new IllegalArgumentException("maxTargetBundleSizeBytes must be > 0, got " + maxTargetBundleSizeBytes);
        }
        if (minDeleteRatioToCompact < 0 || minDeleteRatioToCompact > 1) {
            throw new IllegalArgumentException("minDeleteRatioToCompact must be in [0,1], got " + minDeleteRatioToCompact);
        }
        this.minSegmentCountToCompact = minSegmentCountToCompact;
        this.maxTargetBundleSizeBytes = maxTargetBundleSizeBytes;
        this.minDeleteRatioToCompact = minDeleteRatioToCompact;
    }

    /** Reasonable defaults: compact once there are 10+ segments, none of which alone already exceeds 5GB, or once 20%+ of docs are deleted. */
    public static CompactionPolicy withDefaults() {
        return new CompactionPolicy(10, 5L * 1024 * 1024 * 1024, 0.2);
    }

    /**
     * @param segmentCount           number of Lucene segments currently making up the shard
     * @param totalBytes             total size, across all segments, of the data eligible for merging
     * @param largestSingleSegmentBytes the size of the single largest segment (a shard already
     *                                   dominated by one huge segment gains little from merging
     *                                   the small remainder into it again)
     * @param estimatedDeleteRatio   fraction of documents across those segments that are soft-deleted
     * @return whether this shard is a compaction candidate right now
     */
    public boolean shouldCompact(int segmentCount, long totalBytes, long largestSingleSegmentBytes, double estimatedDeleteRatio) {
        if (segmentCount < 2) {
            return false; // nothing to merge
        }
        if (estimatedDeleteRatio >= minDeleteRatioToCompact) {
            return true; // reclaiming deleted-doc space is worth it regardless of segment count
        }
        if (segmentCount >= minSegmentCountToCompact && largestSingleSegmentBytes < maxTargetBundleSizeBytes) {
            return true;
        }
        return false;
    }
}
