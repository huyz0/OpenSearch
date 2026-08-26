/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.spike;

import org.apache.lucene.search.TotalHits;
import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * S1 — the two-node probe that S0 could not be. Closes RFC §11 Q4 and R1.
 *
 * <p>Three questions, each with an assertion on a number that is zero when the seam is broken:
 * <ol>
 *   <li>Do two nodes holding <em>different</em> node-local views both serve? (§5, never shown at N&gt;1)</li>
 *   <li>Does a projected view that omits a hosted index endanger the shard? (R1)</li>
 *   <li>Are per-node state versions safe to feed to {@code updateShardState}? (Q4, §5.3)</li>
 * </ol>
 */
public class S1TwoNodeTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private static IndexMetadata index(String name, String uuid, long shardHeadTerm) throws IOException {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
                    .build()
            )
            .putMapping(MAPPING)
            .primaryTerm(0, shardHeadTerm)   // S0/F4: the data plane refuses term 0
            .build();
    }

    /**
     * S1-A. The core §5 claim at two nodes: each node computes its own view, containing only what it
     * hosts, and neither is aware of the other's indices. Both serve.
     */
    public void testTwoNodesWithDisjointLocalViewsBothServe() throws Exception {
        try (S1Node a = new S1Node("s1-a", createTempDir()); S1Node b = new S1Node("s1-b", createTempDir())) {
            final IndexMetadata alpha = index("alpha", "uuid-alpha-000000", 1L);
            final IndexMetadata beta = index("beta", "uuid-beta-0000000", 1L);

            // Each node's projected view contains ONLY its own index. Nothing agrees on a global state.
            a.apply(a.project(1L, alpha));
            b.apply(b.project(1L, beta));

            final IndexShard shardA = a.openAndStart(alpha, 1L);
            final IndexShard shardB = b.openAndStart(beta, 1L);

            indexDoc(shardA, "1", "{\"msg\":\"alpha doc\",\"n\":1}");
            indexDoc(shardA, "2", "{\"msg\":\"alpha doc\",\"n\":2}");
            indexDoc(shardB, "1", "{\"msg\":\"beta doc\",\"n\":1}");
            shardA.refresh("s1");
            shardB.refresh("s1");

            assertEquals("node A served no documents", 2L, hits(a, shardA.shardId(), "alpha", "doc"));
            assertEquals("node B served no documents", 1L, hits(b, shardB.shardId(), "beta", "doc"));

            // Neither node's view mentions the other's index. This is the design, not a defect.
            assertFalse("node A's view leaked beta", a.clusterService.state().metadata().hasIndex("beta"));
            assertFalse("node B's view leaked alpha", b.clusterService.state().metadata().hasIndex("alpha"));

            // ...and the two nodes' state versions are equal here purely by coincidence of construction.
            // S1-C is about what happens when they are not.
            logger.info("S1-A passed: disjoint local views, both nodes serving");
        }
    }

    /**
     * S1-B / R1. A projected view that omits an index the node still hosts.
     *
     * <p>Measures two things separately: that the shard itself is undisturbed (the shell's reconciler
     * is what closes shards, not the applier), and that a <em>diff-based</em> reconciler would have
     * closed it — which is the hazard §5.2 names, quantified rather than assumed.
     */
    public void testProjectedViewOmittingAHostedIndexDoesNotDisturbTheShard() throws Exception {
        try (S1Node a = new S1Node("s1-partial", createTempDir())) {
            final IndexMetadata alpha = index("alpha", "uuid-alpha-000000", 1L);
            a.apply(a.project(1L, alpha));
            final IndexShard shard = a.openAndStart(alpha, 1L);
            indexDoc(shard, "1", "{\"msg\":\"still here\",\"n\":1}");
            shard.refresh("s1");
            assertEquals(1L, hits(a, shard.shardId(), "still", "here"));

            // The projector emits a view that no longer mentions alpha, while the node still hosts it.
            a.apply(a.project(2L));   // no indices at all
            assertFalse("precondition: the view must omit alpha", a.clusterService.state().metadata().hasIndex("alpha"));

            // The shard is untouched: applying a state does not itself close shards.
            assertEquals("applying a partial view closed the shard", IndexShardState.STARTED, shard.state());
            assertEquals("shard stopped serving after a partial view", 1L, hits(a, shard.shardId(), "still", "here"));

            // But a diff-based reconciler — the shape IndicesClusterStateService uses — would close it.
            final Set<ShardId> locallyOpen = new HashSet<>();
            a.indicesService.forEach(is -> is.forEach(s -> locallyOpen.add(s.shardId())));
            final Set<String> inView = a.clusterService.state().metadata().indices().keySet().stream().collect(Collectors.toSet());
            final Set<ShardId> naiveCloseSet = locallyOpen.stream()
                .filter(sid -> inView.contains(sid.getIndexName()) == false)
                .collect(Collectors.toSet());

            assertEquals(
                "R1 quantified: a diff-based reconciler would close this many live shards under projection",
                locallyOpen,
                naiveCloseSet
            );
            assertFalse("the hazard is only meaningful if a shard was actually open", locallyOpen.isEmpty());

            logger.info("S1-B passed: shard survives a partial view; naive close-set would have been {}", naiveCloseSet);
        }
    }

    /**
     * S1-C / Q4. Per-node state versions are not comparable, and {@code updateShardState} silently
     * ignores an update whose version is not greater than the last applied one.
     */
    public void testPerNodeVersionsAreSilentlyIgnoredWhileShardHeadGenerationsAreNot() throws Exception {
        try (S1Node a = new S1Node("s1-version", createTempDir())) {
            final IndexMetadata alpha = index("alpha", "uuid-alpha-000000", 1L);
            a.apply(a.project(1L, alpha));
            final IndexShard shard = a.openAndStart(alpha, 1L);
            final ShardRouting routing = shard.routingEntry();
            final ShardId shardId = shard.shardId();
            final String allocId = routing.allocationId().getId();

            // Node A has been up a long time: its projection counter is high.
            final org.opensearch.cluster.node.DiscoveryNodes nodes = a.clusterService.state().nodes();
            update(shard, routing, shardId, allocId, 500L, nodes);
            assertEquals(500L, appliedVersion(shard));

            // A relocation target's counter is low — a fresh node that has applied few states.
            // §5.3: PrimaryContext carries the source's version across, and this comparison is `>`.
            update(shard, routing, shardId, allocId, 4L, nodes);
            assertEquals("a lower per-node version was applied — the hazard would be absent", 500L, appliedVersion(shard));

            // Nothing threw. The update simply did not happen. That is the whole problem.
            update(shard, routing, shardId, allocId, 501L, nodes);
            assertEquals("a higher version was ignored", 501L, appliedVersion(shard));

            logger.info("S1-C hazard confirmed: version 4 silently ignored after 500; 501 applied");
        }

        // ---- and now the fix: one monotonic source per shard, shared by every node that touches it --
        try (S1Node b = new S1Node("s1-head-gen", createTempDir())) {
            final IndexMetadata alpha = index("alpha", "uuid-alpha-000000", 1L);
            b.apply(b.project(1L, alpha));
            final IndexShard shard = b.openAndStart(alpha, 1L);
            final ShardRouting routing = shard.routingEntry();
            final ShardId shardId = shard.shardId();
            final String allocId = routing.allocationId().getId();
            final org.opensearch.cluster.node.DiscoveryNodes nodes = b.clusterService.state().nodes();

            // Stands in for the shard-head register's CAS generation (§9.3). There is exactly one of
            // these per shard, so a node cannot have "its own, lower" value — which is the entire fix.
            final java.util.concurrent.atomic.AtomicLong shardHeadGeneration = new java.util.concurrent.atomic.AtomicLong(1L);

            for (int i = 0; i < 5; i++) {
                final long generation = shardHeadGeneration.incrementAndGet();
                update(shard, routing, shardId, allocId, generation, nodes);
                assertEquals("a shard-head generation was ignored — the fix does not hold", generation, appliedVersion(shard));
            }

            // A relocation target reads the SAME register, so its next value is necessarily higher.
            final long afterHandoff = shardHeadGeneration.incrementAndGet();
            update(shard, routing, shardId, allocId, afterHandoff, nodes);
            assertEquals("post-handoff generation was ignored", afterHandoff, appliedVersion(shard));

            logger.info("S1-C fix verified: {} consecutive shard-head generations, none ignored", 6);
        }
    }

    private static void update(
        IndexShard shard,
        ShardRouting routing,
        ShardId shardId,
        String allocId,
        long version,
        org.opensearch.cluster.node.DiscoveryNodes nodes
    ) throws IOException {
        shard.updateShardState(
            routing,
            shard.getOperationPrimaryTerm(),
            null,
            version,
            Set.of(allocId),
            new IndexShardRoutingTable.Builder(shardId).addShard(routing).build(),
            nodes
        );
    }

    @SuppressForbidden(reason = "reads ReplicationTracker's private applied version; the cross-node comparison in RFC §5.3 has no public accessor")
    private static long appliedVersion(IndexShard shard) throws Exception {
        final java.lang.reflect.Field tracker = IndexShard.class.getDeclaredField("replicationTracker");
        tracker.setAccessible(true);
        final Object rt = tracker.get(shard);
        final java.lang.reflect.Field applied = rt.getClass().getDeclaredField("appliedClusterStateVersion");
        applied.setAccessible(true);
        return applied.getLong(rt);
    }

    private static void indexDoc(IndexShard shard, String id, String source) throws IOException {
        final Engine.IndexResult result = shard.applyIndexOperationOnPrimary(
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            new SourceToParse(shard.shardId().getIndexName(), id, new BytesArray(source), XContentType.JSON),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false
        );
        if (result.getResultType() != Engine.Result.Type.SUCCESS) {
            throw new AssertionError("indexing failed with " + result.getResultType() + " (S0/F5: this is a value, not a throw)");
        }
        shard.sync();
    }

    private static long hits(S1Node node, ShardId shardId, String... terms) throws Exception {
        final SearchRequest searchRequest = new SearchRequest(shardId.getIndexName()).allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().query(QueryBuilders.matchQuery("msg", String.join(" ", terms))).trackTotalHits(true));
        final ShardSearchRequest request = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            shardId,
            1,
            AliasFilter.EMPTY,
            1.0f,
            System.currentTimeMillis(),
            null,
            Strings.EMPTY_ARRAY
        );
        final PlainActionFuture<SearchPhaseResult> future = PlainActionFuture.newFuture();
        node.searchService.executeQueryPhase(
            request,
            false,
            new SearchShardTask(0, "s1", "s1", "s1", null, Collections.emptyMap()),
            ActionListener.wrap(future::onResponse, future::onFailure),
            ThreadPool.Names.SEARCH,
            false
        );
        final TotalHits totalHits = future.actionGet().queryResult().topDocs().topDocs.totalHits;
        return totalHits.value();
    }
}
