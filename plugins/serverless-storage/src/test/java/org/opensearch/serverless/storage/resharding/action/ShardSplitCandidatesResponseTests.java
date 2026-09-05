/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Optional;

public class ShardSplitCandidatesResponseTests extends OpenSearchTestCase {

    private static final String INDEX_NAME = "my-index";
    private static final String INDEX_UUID = "idx-uuid";
    private static final long DEFAULT_SIZE_THRESHOLD_BYTES = 1_000_000_000L;

    private DiscoveryNode node(String id) {
        return new DiscoveryNode(id, buildNewFakeTransportAddress(), Version.CURRENT);
    }

    private Metadata metadataWith(String indexName, String indexUuid) {
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        return Metadata.builder().put(indexMetadata, false).build();
    }

    private Optional<ShardSplitCandidateEntry> findEntry(ShardSplitCandidatesResponse response, String indexUuid, int shardId) {
        return response.candidates().stream().filter(e -> e.indexUuid().equals(indexUuid) && e.shardId() == shardId).findFirst();
    }

    /**
     * Index metadata whose shard {@code shardId} is a committed in-place split child covering
     * {@code [start, end]} of the hash space -- what a manifest-cloned child's size signal is read
     * against.
     */
    private Metadata metadataWithSplitChild(String indexName, String indexUuid, int parentShardId, int childShardId) {
        org.opensearch.cluster.metadata.SplitShardsMetadata.Builder splitBuilder =
            new org.opensearch.cluster.metadata.SplitShardsMetadata.Builder(1);
        splitBuilder.splitShard(parentShardId, 2);
        splitBuilder.updateSplitMetadataForChildShards(
            parentShardId,
            java.util.Set.of(childShardId, childShardId + 1),
            System.currentTimeMillis()
        );
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .splitShardsMetadata(splitBuilder.build())
            .build();
        return Metadata.builder().put(indexMetadata, false).build();
    }

    /**
     * Finding R-3. An in-place split child's manifest is a clone of its parent's and references the
     * same file set, so {@code ObjectStoreWriterEngine#shardSizeInBytes()} reports the <em>whole
     * parent's</em> size for each child -- and nothing ever physically rewrites an in-place child, so
     * that number never falls. Comparing it against the size threshold made automatic
     * split-for-size self-perpetuating: split a 10 GiB shard, get two children each reporting
     * 10 GiB, each immediately a size candidate again, 2 -&gt; 4 -&gt; 8, with no terminating
     * condition at all. The candidacy decision is now made against the share of the hash space the
     * child actually owns.
     */
    public void testASplitChildsSizeIsScaledToTheHashRangeItOwns() {
        long threshold = 10_000_000_000L; // 10 GiB
        long reportedSize = 12_000_000_000L; // the whole parent, cloned into the child's manifest
        // splitShard(0, 2) bisects the full hash space, so child shard 1 owns half of it and really
        // holds about 6 GiB -- under the threshold.
        Metadata metadata = metadataWithSplitChild(INDEX_NAME, INDEX_UUID, 0, 1);

        NodeShardSplitCandidatesResponse writerNode = new NodeShardSplitCandidatesResponse(
            node("writer"),
            List.of(new ShardWriteRateEntry(INDEX_UUID, 1, 1L, reportedSize))
        );
        ShardSplitCandidatesResponse response = new ShardSplitCandidatesResponse(
            new ClusterName("test"),
            List.of(writerNode),
            List.of(),
            1_000_000L,
            threshold,
            metadata
        );
        ShardSplitCandidateEntry entry = findEntry(response, INDEX_UUID, 1).orElseThrow();
        assertEquals("the raw measurement is still reported honestly", reportedSize, entry.shardSizeInBytes());
        assertTrue(
            "the owned size must be materially smaller than the reported one for a half-range child, got " + entry.ownedSizeInBytes(),
            entry.ownedSizeInBytes() < reportedSize * 3 / 4
        );
        assertFalse(
            "a child owning roughly half a 12 GiB parent is not a 10 GiB shard, and treating it as one is what made "
                + "split-for-size non-terminating",
            entry.sizeCandidate()
        );
    }

    public void testANeverSplitShardsSizeIsNotScaled() {
        long reportedSize = 12_000_000_000L;
        NodeShardSplitCandidatesResponse writerNode = new NodeShardSplitCandidatesResponse(
            node("writer"),
            List.of(new ShardWriteRateEntry(INDEX_UUID, 0, 1L, reportedSize))
        );
        ShardSplitCandidatesResponse response = new ShardSplitCandidatesResponse(
            new ClusterName("test"),
            List.of(writerNode),
            List.of(),
            1_000_000L,
            10_000_000_000L,
            metadataWith(INDEX_NAME, INDEX_UUID)
        );
        ShardSplitCandidateEntry entry = findEntry(response, INDEX_UUID, 0).orElseThrow();
        assertEquals("a shard covering the whole hash space owns everything it reports", reportedSize, entry.ownedSizeInBytes());
        assertTrue("and is still a size candidate, exactly as before", entry.sizeCandidate());
    }

    public void testAShardOverThresholdIsACandidate() {
        NodeShardSplitCandidatesResponse writerNode = new NodeShardSplitCandidatesResponse(
            node("writer"),
            List.of(new ShardWriteRateEntry(INDEX_UUID, 0, 20_000L, 0L))
        );
        ShardSplitCandidatesResponse response = new ShardSplitCandidatesResponse(
            new ClusterName("test"),
            List.of(writerNode),
            List.of(),
            10_000L,
            DEFAULT_SIZE_THRESHOLD_BYTES,
            metadataWith(INDEX_NAME, INDEX_UUID)
        );
        ShardSplitCandidateEntry entry = findEntry(response, INDEX_UUID, 0).orElseThrow();
        assertTrue(entry.candidate());
        assertTrue(entry.writeRateCandidate());
        assertFalse(entry.sizeCandidate());
        assertEquals(20_000L, entry.writesPerMinute());
        assertEquals(INDEX_NAME, entry.indexName());
    }

    public void testAShardUnderThresholdIsNotACandidate() {
        NodeShardSplitCandidatesResponse writerNode = new NodeShardSplitCandidatesResponse(
            node("writer"),
            List.of(new ShardWriteRateEntry(INDEX_UUID, 0, 5_000L, 0L))
        );
        ShardSplitCandidatesResponse response = new ShardSplitCandidatesResponse(
            new ClusterName("test"),
            List.of(writerNode),
            List.of(),
            10_000L,
            DEFAULT_SIZE_THRESHOLD_BYTES,
            metadataWith(INDEX_NAME, INDEX_UUID)
        );
        ShardSplitCandidateEntry entry = findEntry(response, INDEX_UUID, 0).orElseThrow();
        assertFalse(entry.candidate());
    }

    public void testAShardOverSizeThresholdIsASizeCandidateEvenWithLowWriteRate() {
        NodeShardSplitCandidatesResponse writerNode = new NodeShardSplitCandidatesResponse(
            node("writer"),
            List.of(new ShardWriteRateEntry(INDEX_UUID, 0, 5L, 5_000_000_000L))
        );
        ShardSplitCandidatesResponse response = new ShardSplitCandidatesResponse(
            new ClusterName("test"),
            List.of(writerNode),
            List.of(),
            10_000L,
            DEFAULT_SIZE_THRESHOLD_BYTES,
            metadataWith(INDEX_NAME, INDEX_UUID)
        );
        ShardSplitCandidateEntry entry = findEntry(response, INDEX_UUID, 0).orElseThrow();
        assertTrue(entry.candidate());
        assertFalse(entry.writeRateCandidate());
        assertTrue(entry.sizeCandidate());
        assertEquals(5_000_000_000L, entry.shardSizeInBytes());
    }

    public void testTheHighestSignalAcrossMultipleNodesWinsForTheSameShard() {
        NodeShardSplitCandidatesResponse nodeA = new NodeShardSplitCandidatesResponse(
            node("a"),
            List.of(new ShardWriteRateEntry(INDEX_UUID, 0, 3_000L, 0L))
        );
        NodeShardSplitCandidatesResponse nodeB = new NodeShardSplitCandidatesResponse(
            node("b"),
            List.of(new ShardWriteRateEntry(INDEX_UUID, 0, 15_000L, 0L))
        );
        ShardSplitCandidatesResponse response = new ShardSplitCandidatesResponse(
            new ClusterName("test"),
            List.of(nodeA, nodeB),
            List.of(),
            10_000L,
            DEFAULT_SIZE_THRESHOLD_BYTES,
            metadataWith(INDEX_NAME, INDEX_UUID)
        );
        ShardSplitCandidateEntry entry = findEntry(response, INDEX_UUID, 0).orElseThrow();
        assertEquals(15_000L, entry.writesPerMinute());
        assertTrue(entry.candidate());
    }

    public void testAShardWhoseIndexHasSinceBeenDeletedIsDroppedFromTheMergedList() {
        NodeShardSplitCandidatesResponse writerNode = new NodeShardSplitCandidatesResponse(
            node("writer"),
            List.of(new ShardWriteRateEntry(INDEX_UUID, 0, 20_000L, 0L))
        );
        ShardSplitCandidatesResponse response = new ShardSplitCandidatesResponse(
            new ClusterName("test"),
            List.of(writerNode),
            List.of(),
            10_000L,
            DEFAULT_SIZE_THRESHOLD_BYTES,
            Metadata.builder().build()
        );
        assertTrue(response.candidates().isEmpty());
    }

    public void testFailuresDoNotPreventCandidatesFromNonFailingNodesBeingReported() {
        NodeShardSplitCandidatesResponse writerNode = new NodeShardSplitCandidatesResponse(
            node("writer"),
            List.of(new ShardWriteRateEntry(INDEX_UUID, 0, 20_000L, 0L))
        );
        ShardSplitCandidatesResponse response = new ShardSplitCandidatesResponse(
            new ClusterName("test"),
            List.of(writerNode),
            List.of(new FailedNodeException("other-node", "boom", new RuntimeException("boom"))),
            10_000L,
            DEFAULT_SIZE_THRESHOLD_BYTES,
            metadataWith(INDEX_NAME, INDEX_UUID)
        );
        assertTrue(response.hasFailures());
        assertTrue(findEntry(response, INDEX_UUID, 0).isPresent());
    }
}
