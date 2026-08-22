/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.allocation;

import org.opensearch.Version;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;

/**
 * Pure logic tests for {@link ReaderCacheAffinityMetadata}, the {@code IndexMetadata} custom-data
 * accessor {@link ServerlessStorageExistingShardsAllocator} consults for cache-locality
 * hysteresis (rfc-serverless-opensearch.md &sect;10).
 */
public class ReaderCacheAffinityMetadataTests extends OpenSearchAllocationTestCase {

    private static final String INDEX_NAME = "serverless-idx";

    private IndexMetadata newIndexMetadata() {
        return IndexMetadata.builder(INDEX_NAME).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0).build();
    }

    public void testPreferredNodeIdIsNullWhenNeverRecorded() {
        IndexMetadata indexMetadata = newIndexMetadata();
        assertNull(ReaderCacheAffinityMetadata.preferredNodeId(indexMetadata, 0));
        assertFalse(ReaderCacheAffinityMetadata.isAffinityFresh(indexMetadata, 0, 1_000L, 60_000L));
    }

    public void testRecordedAffinityIsReadableAndFreshWithinTtl() {
        IndexMetadata indexMetadata = ReaderCacheAffinityMetadata.withShardCacheAffinity(newIndexMetadata(), 0, "node1", 1_000L);
        assertEquals("node1", ReaderCacheAffinityMetadata.preferredNodeId(indexMetadata, 0));
        assertTrue(ReaderCacheAffinityMetadata.isAffinityFresh(indexMetadata, 0, 1_000L, 60_000L));
        assertTrue(ReaderCacheAffinityMetadata.isAffinityFresh(indexMetadata, 0, 1_000L + 60_000L, 60_000L));
    }

    public void testAffinityGoesStaleAfterTtlElapses() {
        IndexMetadata indexMetadata = ReaderCacheAffinityMetadata.withShardCacheAffinity(newIndexMetadata(), 0, "node1", 1_000L);
        assertFalse(ReaderCacheAffinityMetadata.isAffinityFresh(indexMetadata, 0, 1_000L + 60_001L, 60_000L));
        // Staleness only affects isAffinityFresh -- the underlying record itself is not erased.
        assertEquals("node1", ReaderCacheAffinityMetadata.preferredNodeId(indexMetadata, 0));
    }

    public void testNonPositiveTtlDisablesFreshnessEntirely() {
        IndexMetadata indexMetadata = ReaderCacheAffinityMetadata.withShardCacheAffinity(newIndexMetadata(), 0, "node1", 1_000L);
        assertFalse(ReaderCacheAffinityMetadata.isAffinityFresh(indexMetadata, 0, 1_000L, 0L));
        assertFalse(ReaderCacheAffinityMetadata.isAffinityFresh(indexMetadata, 0, 1_000L, -1L));
    }

    public void testLaterRecordOverwritesEarlierOneForTheSameShard() {
        IndexMetadata indexMetadata = ReaderCacheAffinityMetadata.withShardCacheAffinity(newIndexMetadata(), 0, "node1", 1_000L);
        indexMetadata = ReaderCacheAffinityMetadata.withShardCacheAffinity(indexMetadata, 0, "node2", 2_000L);
        assertEquals("node2", ReaderCacheAffinityMetadata.preferredNodeId(indexMetadata, 0));
        assertTrue(ReaderCacheAffinityMetadata.isAffinityFresh(indexMetadata, 0, 2_000L, 60_000L));
    }

    public void testStaleOverwriteWithAnEarlierTimestampIsANoOp() {
        IndexMetadata indexMetadata = ReaderCacheAffinityMetadata.withShardCacheAffinity(newIndexMetadata(), 0, "node1", 5_000L);
        IndexMetadata unchanged = ReaderCacheAffinityMetadata.withShardCacheAffinity(indexMetadata, 0, "node1", 1_000L);
        assertSame("a same-node, older-or-equal-timestamp record must be a no-op", indexMetadata, unchanged);
    }

    public void testRepeatedSameNodeCallsWithinTheMinRewriteIntervalAreGenuinelyNoOps() {
        // The real bug this test guards against: an earlier version of this guard's ">="
        // comparison was never true for genuine steady-state repeats (a freshly captured "now" is
        // always strictly later than a previously stored timestamp), so every single same-node
        // call rewrote cluster state regardless of how soon after the last one it happened. A real
        // interval-based guard must actually skip a same-node call shortly after the first one.
        IndexMetadata indexMetadata = ReaderCacheAffinityMetadata.withShardCacheAffinity(newIndexMetadata(), 0, "node1", 100_000L);
        IndexMetadata unchanged = ReaderCacheAffinityMetadata.withShardCacheAffinity(indexMetadata, 0, "node1", 100_000L + 30_000L);
        assertSame("a same-node call well within the min-rewrite interval must be a genuine no-op", indexMetadata, unchanged);
        assertEquals(100_000L, Long.parseLong(indexMetadata.getCustomData(ReaderCacheAffinityMetadata.RECORDED_AT_CUSTOM_TYPE).get("0")));
    }

    public void testSameNodeCallAfterTheMinRewriteIntervalElapsesRefreshesTheTimestamp() {
        // A shard that keeps reactivating on the same node over a long period must still have its
        // recorded-at timestamp refreshed occasionally, or a long-lived, continuously-reused
        // affinity record would eventually go TTL-stale even though the node preference is still
        // completely accurate.
        IndexMetadata indexMetadata = ReaderCacheAffinityMetadata.withShardCacheAffinity(newIndexMetadata(), 0, "node1", 100_000L);
        IndexMetadata updated = ReaderCacheAffinityMetadata.withShardCacheAffinity(indexMetadata, 0, "node1", 100_000L + 60_001L);
        assertNotSame("a same-node call past the min-rewrite interval must refresh the timestamp", indexMetadata, updated);
        assertEquals(
            100_000L + 60_001L,
            Long.parseLong(updated.getCustomData(ReaderCacheAffinityMetadata.RECORDED_AT_CUSTOM_TYPE).get("0"))
        );
    }

    public void testDifferentShardsOfTheSameIndexTrackIndependentAffinity() {
        IndexMetadata indexMetadata = IndexMetadata.builder(INDEX_NAME)
            .settings(settings(Version.CURRENT))
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();
        indexMetadata = ReaderCacheAffinityMetadata.withShardCacheAffinity(indexMetadata, 0, "node1", 1_000L);
        indexMetadata = ReaderCacheAffinityMetadata.withShardCacheAffinity(indexMetadata, 1, "node2", 1_000L);
        assertEquals("node1", ReaderCacheAffinityMetadata.preferredNodeId(indexMetadata, 0));
        assertEquals("node2", ReaderCacheAffinityMetadata.preferredNodeId(indexMetadata, 1));
    }
}
