/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.serverless.storage.readerengine.action.ShardLagEntry;
import org.opensearch.serverless.storage.writerengine.action.IdleShardEntry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Optional;

public class ScaleToZeroCandidatesResponseTests extends OpenSearchTestCase {

    private DiscoveryNode node(String id) {
        return new DiscoveryNode(id, buildNewFakeTransportAddress(), Version.CURRENT);
    }

    private Optional<ScaleToZeroCandidateEntry> findEntry(ScaleToZeroCandidatesResponse response, String indexUuid, int shardId) {
        return response.candidates().stream().filter(e -> e.indexUuid().equals(indexUuid) && e.shardId() == shardId).findFirst();
    }

    public void testAShardIdlePastThresholdWithNoReaderIsACandidate() {
        NodeScaleToZeroCandidatesResponse writerNode = new NodeScaleToZeroCandidatesResponse(
            node("writer"),
            List.of(new IdleShardEntry("idx-1", 0, 20_000L)),
            List.of(),
            List.of()
        );
        ScaleToZeroCandidatesResponse response = new ScaleToZeroCandidatesResponse(
            new ClusterName("test"),
            List.of(writerNode),
            List.of(),
            10_000L,
            0L
        );
        ScaleToZeroCandidateEntry entry = findEntry(response, "idx-1", 0).orElseThrow();
        assertTrue(entry.candidate());
        assertEquals(20_000L, entry.millisSinceLastActivity());
        assertEquals(ScaleToZeroCandidateEntry.UNKNOWN, entry.manifestGenerationLag());
    }

    public void testAShardIdlePastThresholdButWithALaggingReaderIsNotACandidate() {
        NodeScaleToZeroCandidatesResponse writerNode = new NodeScaleToZeroCandidatesResponse(
            node("writer"),
            List.of(new IdleShardEntry("idx-1", 0, 20_000L)),
            List.of(),
            List.of()
        );
        NodeScaleToZeroCandidatesResponse readerNode = new NodeScaleToZeroCandidatesResponse(
            node("reader"),
            List.of(),
            List.of(new ShardLagEntry("idx-1", 0, 3L)),
            List.of()
        );
        ScaleToZeroCandidatesResponse response = new ScaleToZeroCandidatesResponse(
            new ClusterName("test"),
            List.of(writerNode, readerNode),
            List.of(),
            10_000L,
            0L
        );
        ScaleToZeroCandidateEntry entry = findEntry(response, "idx-1", 0).orElseThrow();
        assertFalse(entry.candidate());
        assertEquals(3L, entry.manifestGenerationLag());
    }

    public void testAShardNotYetIdleEnoughIsNotACandidateEvenWithACaughtUpReader() {
        NodeScaleToZeroCandidatesResponse writerNode = new NodeScaleToZeroCandidatesResponse(
            node("writer"),
            List.of(new IdleShardEntry("idx-1", 0, 1_000L)),
            List.of(),
            List.of()
        );
        NodeScaleToZeroCandidatesResponse readerNode = new NodeScaleToZeroCandidatesResponse(
            node("reader"),
            List.of(),
            List.of(new ShardLagEntry("idx-1", 0, 0L)),
            List.of()
        );
        ScaleToZeroCandidatesResponse response = new ScaleToZeroCandidatesResponse(
            new ClusterName("test"),
            List.of(writerNode, readerNode),
            List.of(),
            10_000L,
            0L
        );
        ScaleToZeroCandidateEntry entry = findEntry(response, "idx-1", 0).orElseThrow();
        assertFalse(entry.candidate());
    }

    public void testTheWorstSignalAcrossMultipleNodesWinsForTheSameShard() {
        // Same (indexUuid, shardId) reported by two nodes with different idle times -- e.g. a
        // primary and a replica writer copy, or a stale vs. fresh report during relocation. The
        // merge must not silently pick whichever happened to be last in the list.
        NodeScaleToZeroCandidatesResponse nodeA = new NodeScaleToZeroCandidatesResponse(
            node("a"),
            List.of(new IdleShardEntry("idx-1", 0, 5_000L)),
            List.of(),
            List.of()
        );
        NodeScaleToZeroCandidatesResponse nodeB = new NodeScaleToZeroCandidatesResponse(
            node("b"),
            List.of(new IdleShardEntry("idx-1", 0, 15_000L)),
            List.of(),
            List.of()
        );
        ScaleToZeroCandidatesResponse response = new ScaleToZeroCandidatesResponse(
            new ClusterName("test"),
            List.of(nodeA, nodeB),
            List.of(),
            10_000L,
            0L
        );
        ScaleToZeroCandidateEntry entry = findEntry(response, "idx-1", 0).orElseThrow();
        assertEquals(15_000L, entry.millisSinceLastActivity());
        assertTrue(entry.candidate());
    }

    public void testAShardWithNoNodeReportsIsAbsentFromTheMergedList() {
        ScaleToZeroCandidatesResponse response = new ScaleToZeroCandidatesResponse(
            new ClusterName("test"),
            List.of(new NodeScaleToZeroCandidatesResponse(node("a"), List.of(), List.of(), List.of())),
            List.of(),
            10_000L,
            0L
        );
        assertTrue(response.candidates().isEmpty());
    }

    public void testFailuresDoNotPreventCandidatesFromNonFailingNodesBeingReported() {
        NodeScaleToZeroCandidatesResponse writerNode = new NodeScaleToZeroCandidatesResponse(
            node("writer"),
            List.of(new IdleShardEntry("idx-1", 0, 20_000L)),
            List.of(),
            List.of()
        );
        ScaleToZeroCandidatesResponse response = new ScaleToZeroCandidatesResponse(
            new ClusterName("test"),
            List.of(writerNode),
            List.of(new FailedNodeException("other-node", "boom", new RuntimeException("boom"))),
            10_000L,
            0L
        );
        assertTrue(response.hasFailures());
        assertTrue(findEntry(response, "idx-1", 0).isPresent());
    }

    public void testReaderCandidacyIsIndependentOfWriterCandidacy() {
        // A shard whose writer is busy (not idle enough) but whose reader has had no query
        // traffic in a long time must still be flagged as a reader candidate.
        NodeScaleToZeroCandidatesResponse writerNode = new NodeScaleToZeroCandidatesResponse(
            node("writer"),
            List.of(new IdleShardEntry("idx-1", 0, 1_000L)),
            List.of(),
            List.of()
        );
        NodeScaleToZeroCandidatesResponse readerNode = new NodeScaleToZeroCandidatesResponse(
            node("reader"),
            List.of(),
            List.of(),
            List.of(new IdleShardEntry("idx-1", 0, 20_000L))
        );
        ScaleToZeroCandidatesResponse response = new ScaleToZeroCandidatesResponse(
            new ClusterName("test"),
            List.of(writerNode, readerNode),
            List.of(),
            10_000L,
            0L
        );
        ScaleToZeroCandidateEntry entry = findEntry(response, "idx-1", 0).orElseThrow();
        assertFalse("the writer is not idle enough, so it must not be a candidate", entry.candidate());
        assertTrue("the reader has had no queries for long enough, so it must be a candidate", entry.readerCandidate());
        assertEquals(20_000L, entry.readerMillisSinceLastQuery());
    }

    public void testReaderNotYetIdleEnoughIsNotAReaderCandidate() {
        NodeScaleToZeroCandidatesResponse readerNode = new NodeScaleToZeroCandidatesResponse(
            node("reader"),
            List.of(),
            List.of(),
            List.of(new IdleShardEntry("idx-1", 0, 1_000L))
        );
        ScaleToZeroCandidatesResponse response = new ScaleToZeroCandidatesResponse(
            new ClusterName("test"),
            List.of(readerNode),
            List.of(),
            10_000L,
            0L
        );
        ScaleToZeroCandidateEntry entry = findEntry(response, "idx-1", 0).orElseThrow();
        assertFalse(entry.readerCandidate());
    }
}
