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

    public void testAShardOverThresholdIsACandidate() {
        NodeShardSplitCandidatesResponse writerNode = new NodeShardSplitCandidatesResponse(
            node("writer"),
            List.of(new ShardWriteRateEntry(INDEX_UUID, 0, 20_000L))
        );
        ShardSplitCandidatesResponse response = new ShardSplitCandidatesResponse(
            new ClusterName("test"),
            List.of(writerNode),
            List.of(),
            10_000L,
            metadataWith(INDEX_NAME, INDEX_UUID)
        );
        ShardSplitCandidateEntry entry = findEntry(response, INDEX_UUID, 0).orElseThrow();
        assertTrue(entry.candidate());
        assertEquals(20_000L, entry.writesPerMinute());
        assertEquals(INDEX_NAME, entry.indexName());
    }

    public void testAShardUnderThresholdIsNotACandidate() {
        NodeShardSplitCandidatesResponse writerNode = new NodeShardSplitCandidatesResponse(
            node("writer"),
            List.of(new ShardWriteRateEntry(INDEX_UUID, 0, 5_000L))
        );
        ShardSplitCandidatesResponse response = new ShardSplitCandidatesResponse(
            new ClusterName("test"),
            List.of(writerNode),
            List.of(),
            10_000L,
            metadataWith(INDEX_NAME, INDEX_UUID)
        );
        ShardSplitCandidateEntry entry = findEntry(response, INDEX_UUID, 0).orElseThrow();
        assertFalse(entry.candidate());
    }

    public void testTheHighestSignalAcrossMultipleNodesWinsForTheSameShard() {
        NodeShardSplitCandidatesResponse nodeA = new NodeShardSplitCandidatesResponse(
            node("a"),
            List.of(new ShardWriteRateEntry(INDEX_UUID, 0, 3_000L))
        );
        NodeShardSplitCandidatesResponse nodeB = new NodeShardSplitCandidatesResponse(
            node("b"),
            List.of(new ShardWriteRateEntry(INDEX_UUID, 0, 15_000L))
        );
        ShardSplitCandidatesResponse response = new ShardSplitCandidatesResponse(
            new ClusterName("test"),
            List.of(nodeA, nodeB),
            List.of(),
            10_000L,
            metadataWith(INDEX_NAME, INDEX_UUID)
        );
        ShardSplitCandidateEntry entry = findEntry(response, INDEX_UUID, 0).orElseThrow();
        assertEquals(15_000L, entry.writesPerMinute());
        assertTrue(entry.candidate());
    }

    public void testAShardWhoseIndexHasSinceBeenDeletedIsDroppedFromTheMergedList() {
        NodeShardSplitCandidatesResponse writerNode = new NodeShardSplitCandidatesResponse(
            node("writer"),
            List.of(new ShardWriteRateEntry(INDEX_UUID, 0, 20_000L))
        );
        ShardSplitCandidatesResponse response = new ShardSplitCandidatesResponse(
            new ClusterName("test"),
            List.of(writerNode),
            List.of(),
            10_000L,
            Metadata.builder().build()
        );
        assertTrue(response.candidates().isEmpty());
    }

    public void testFailuresDoNotPreventCandidatesFromNonFailingNodesBeingReported() {
        NodeShardSplitCandidatesResponse writerNode = new NodeShardSplitCandidatesResponse(
            node("writer"),
            List.of(new ShardWriteRateEntry(INDEX_UUID, 0, 20_000L))
        );
        ShardSplitCandidatesResponse response = new ShardSplitCandidatesResponse(
            new ClusterName("test"),
            List.of(writerNode),
            List.of(new FailedNodeException("other-node", "boom", new RuntimeException("boom"))),
            10_000L,
            metadataWith(INDEX_NAME, INDEX_UUID)
        );
        assertTrue(response.hasFailures());
        assertTrue(findEntry(response, INDEX_UUID, 0).isPresent());
    }
}
