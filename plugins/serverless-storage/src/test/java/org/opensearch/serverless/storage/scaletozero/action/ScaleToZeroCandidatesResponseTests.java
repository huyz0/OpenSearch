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

    public void testTheMostRecentlyActiveReportAcrossNodesWinsForTheSameShard() {
        // This test used to be named testTheWorstSignalAcrossMultipleNodesWinsForTheSameShard and
        // asserted the opposite: that the *stale* 15s report won and made the shard a candidate. It
        // was pinning the bug, not guarding against it (finding L-2).
        //
        // Its own comment named the scenario it got wrong -- "a stale vs. fresh report during
        // relocation". Neither activity registry has an unregister method, so after shard 0
        // relocates from node A to node B, A keeps reporting an ever-growing idle time for an
        // engine that has been closed for hours while B writes to the shard continuously. Folding
        // with max let A's stale report overrule B's direct evidence of ongoing writes, so the live
        // busy primary was marked suspended and force-cancelled -- and, since the stale entry never
        // expires, again on every cooldown, forever.
        //
        // The shard is active if any copy of it is active, so the most-recently-active report is
        // the authoritative one. Here that is node B at 5s, below the 10s threshold: not a
        // candidate.
        NodeScaleToZeroCandidatesResponse busyNode = new NodeScaleToZeroCandidatesResponse(
            node("b"),
            List.of(new IdleShardEntry("idx-1", 0, 5_000L)),
            List.of(),
            List.of()
        );
        NodeScaleToZeroCandidatesResponse staleNode = new NodeScaleToZeroCandidatesResponse(
            node("a"),
            List.of(new IdleShardEntry("idx-1", 0, 15_000L)),
            List.of(),
            List.of()
        );
        ScaleToZeroCandidatesResponse response = new ScaleToZeroCandidatesResponse(
            new ClusterName("test"),
            List.of(staleNode, busyNode),
            List.of(),
            10_000L,
            0L
        );
        ScaleToZeroCandidateEntry entry = findEntry(response, "idx-1", 0).orElseThrow();
        assertEquals(5_000L, entry.millisSinceLastActivity());
        assertFalse("a shard with a copy written to 5s ago must not be a scale-to-zero candidate", entry.candidate());
    }

    public void testAShardIdleOnEveryNodeIsStillACandidate() {
        // The other half of the L-2 fix: folding idleness with min must not stop a genuinely idle
        // shard from being suspended. Every node reports it idle past the threshold, so the minimum
        // is still past the threshold.
        NodeScaleToZeroCandidatesResponse nodeA = new NodeScaleToZeroCandidatesResponse(
            node("a"),
            List.of(new IdleShardEntry("idx-1", 0, 12_000L)),
            List.of(),
            List.of()
        );
        NodeScaleToZeroCandidatesResponse nodeB = new NodeScaleToZeroCandidatesResponse(
            node("b"),
            List.of(new IdleShardEntry("idx-1", 0, 30_000L)),
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
        assertEquals(12_000L, entry.millisSinceLastActivity());
        assertTrue(entry.candidate());
    }

    public void testAReaderShardQueriedRecentlyOnAnyNodeIsNotReaderIdle() {
        // Reader query-idleness folds the same way and for the same reason: a search served by any
        // reader copy means the shard is being queried.
        NodeScaleToZeroCandidatesResponse busyReader = new NodeScaleToZeroCandidatesResponse(
            node("r1"),
            List.of(),
            List.of(),
            List.of(new IdleShardEntry("idx-1", 0, 1_000L))
        );
        NodeScaleToZeroCandidatesResponse staleReader = new NodeScaleToZeroCandidatesResponse(
            node("r2"),
            List.of(),
            List.of(),
            List.of(new IdleShardEntry("idx-1", 0, 900_000L))
        );
        ScaleToZeroCandidatesResponse response = new ScaleToZeroCandidatesResponse(
            new ClusterName("test"),
            List.of(staleReader, busyReader),
            List.of(),
            10_000L,
            0L
        );
        ScaleToZeroCandidateEntry entry = findEntry(response, "idx-1", 0).orElseThrow();
        assertEquals(1_000L, entry.readerMillisSinceLastQuery());
        assertFalse(entry.readerCandidate());
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
