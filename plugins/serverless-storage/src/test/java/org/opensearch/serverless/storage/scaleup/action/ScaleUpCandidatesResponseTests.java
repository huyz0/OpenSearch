/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaleup.action;

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

/**
 * The scale-up candidate predicate, and specifically the idleness cross-check that findings S-1 and
 * S-3 are about.
 *
 * <p>{@code ObjectStoreReaderEngine#recordQueryForRateCounter} rolls its 60-second window over only
 * on the <em>next</em> query, so when traffic stops there is no next query and
 * {@code queriesPerMinute()} reports the last busy window's value indefinitely. The predicate here
 * used to test only that frozen rate, which meant a shard whose load ended an hour ago was flagged a
 * candidate on every tick and ratcheted to {@code max_search_replicas} -- permanently, since nothing
 * in this repository ever reduces {@code index.number_of_search_replicas}. At the same time
 * scale-to-zero, reading the idleness signal, was suspending that same shard. Two loops, opposite
 * conclusions, no arbiter.
 */
public class ScaleUpCandidatesResponseTests extends OpenSearchTestCase {

    private static final String INDEX_NAME = "hot-index";
    private static final String INDEX_UUID = "hot-idx-uuid";

    private DiscoveryNode node(String id) {
        return new DiscoveryNode(id, buildNewFakeTransportAddress(), Version.CURRENT);
    }

    private Metadata metadata(int searchOnlyReplicas) {
        IndexMetadata indexMetadata = IndexMetadata.builder(INDEX_NAME)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, INDEX_UUID)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, searchOnlyReplicas)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        return Metadata.builder().put(indexMetadata, false).build();
    }

    private ScaleUpCandidatesResponse responseFor(List<ShardQueryRateEntry> entries, List<FailedNodeException> failures) {
        return new ScaleUpCandidatesResponse(
            new ClusterName("test"),
            List.of(new NodeScaleUpCandidatesResponse(node("reader-1"), entries)),
            failures,
            1_000L,
            5,
            metadata(1)
        );
    }

    private Optional<ScaleUpCandidateEntry> entry(ScaleUpCandidatesResponse response) {
        return response.candidates().stream().filter(e -> e.shardId() == 0).findFirst();
    }

    public void testABusyShardIsStillACandidate() {
        ScaleUpCandidatesResponse response = responseFor(List.of(new ShardQueryRateEntry(INDEX_UUID, 0, 5_000L, 500L)), List.of());
        assertTrue(
            "a shard queried half a second ago at 5,000 qpm is exactly what scale-up is for",
            entry(response).orElseThrow().candidate()
        );
    }

    public void testAShardWhoseTrafficStoppedIsNotACandidateDespiteAFrozenRate() {
        // The rate is exactly what a shard that sustained 5,000 qpm for an hour reports forever once
        // traffic stops -- the window never rolls over because no query arrives to roll it. The
        // idleness signal is the only thing that distinguishes this from the case above.
        ScaleUpCandidatesResponse response = responseFor(List.of(new ShardQueryRateEntry(INDEX_UUID, 0, 5_000L, 3_600_000L)), List.of());
        assertFalse(
            "a shard not queried for an hour must not be expanded, whatever its frozen rate says -- expanding it "
                + "ratchets idle reader copies to the cap with no path back down",
            entry(response).orElseThrow().candidate()
        );
    }

    public void testUnknownIdlenessFallsBackToTheRateOnlyBehaviour() {
        // A node that cannot supply the idleness signal must degrade to the previous semantics rather
        // than silently disabling scale-up for that shard.
        ScaleUpCandidatesResponse response = responseFor(
            List.of(new ShardQueryRateEntry(INDEX_UUID, 0, 5_000L, ShardQueryRateEntry.UNKNOWN_IDLE)),
            List.of()
        );
        assertTrue(entry(response).orElseThrow().candidate());
    }

    public void testIdlenessFoldsAcrossNodesByMostRecentlyQueried() {
        // Two reader copies of the same shard: one queried a moment ago, one not for an hour. The
        // shard is being queried, so the recent report is the authoritative one.
        ScaleUpCandidatesResponse response = new ScaleUpCandidatesResponse(
            new ClusterName("test"),
            List.of(
                new NodeScaleUpCandidatesResponse(node("quiet"), List.of(new ShardQueryRateEntry(INDEX_UUID, 0, 5_000L, 3_600_000L))),
                new NodeScaleUpCandidatesResponse(node("busy"), List.of(new ShardQueryRateEntry(INDEX_UUID, 0, 5_000L, 100L)))
            ),
            List.of(),
            1_000L,
            5,
            metadata(1)
        );
        assertTrue(entry(response).orElseThrow().candidate());
    }

    /**
     * Finding S-5: {@code merge} never read the failures list, so a partial fan-out was reported as a
     * complete picture with nothing marking it partial.
     */
    public void testNodeFailuresAreSurfaced() {
        ScaleUpCandidatesResponse clean = responseFor(List.of(new ShardQueryRateEntry(INDEX_UUID, 0, 5_000L, 100L)), List.of());
        assertFalse(clean.hasNodeFailures());

        ScaleUpCandidatesResponse partial = responseFor(
            List.of(new ShardQueryRateEntry(INDEX_UUID, 0, 5_000L, 100L)),
            List.of(new FailedNodeException("busiest-node", "did not answer", new RuntimeException("boom")))
        );
        assertTrue(
            "a partial view must say so: the node that failed to answer may be the one holding the busiest copy",
            partial.hasNodeFailures()
        );
    }
}
