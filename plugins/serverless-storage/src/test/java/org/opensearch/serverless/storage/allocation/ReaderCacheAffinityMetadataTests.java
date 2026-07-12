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
