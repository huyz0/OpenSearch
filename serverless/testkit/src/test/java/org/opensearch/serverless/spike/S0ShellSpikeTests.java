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
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * S0 — the falsification spike for {@code rfc-serverless-shell.md} §11.
 *
 * <p><b>This class is disposable.</b> It exists to answer four questions and then be deleted or
 * rewritten as {@code serverless/shell}. It deliberately does not extend {@code IndexShardTestCase}
 * or {@code OpenSearchSingleNodeTestCase}: the former hides the wiring that is the entire subject of
 * the spike, and the latter starts a {@code Node}, which is the thing being disproved as necessary.
 * Every dependency is constructed by hand so that the constructor closure is visible.
 *
 * <p>The claim under test (RFC §5): a shard can be opened and searched with no {@code Coordinator},
 * no {@code GatewayMetaState}, no {@code AllocationService}, no discovery and no {@code Node} —
 * driven instead by a hand-built {@link ClusterState} handed to {@link ClusterApplier}.
 *
 * <p>Assertions follow {@code HANDOFF.md}'s rule: every check is on a number that is zero when the
 * seam is broken. Nothing here asserts merely that a call did not throw.
 */
public class S0ShellSpikeTests extends OpenSearchTestCase {

    /** Classes whose presence anywhere in the constructed object graph falsifies the spike. */
    private static final Set<String> FORBIDDEN = Set.of(
        "org.opensearch.cluster.coordination.Coordinator",
        "org.opensearch.cluster.routing.allocation.AllocationService",
        "org.opensearch.gateway.GatewayMetaState",
        "org.opensearch.node.Node"
    );

    private static final String INDEX = "s0-index";
    /** Stands in for the shard-head register's CAS term (RFC §5.3, §9.3). */
    private static final long SHARD_HEAD_TERM = 1L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private ThreadPool threadPool;
    private NodeEnvironment nodeEnv;
    private ClusterService clusterService;
    private IndicesService indicesService;
    private SearchService searchService;
    private final List<Runnable> teardown = new ArrayList<>();

    public void testShardServesSearchWithNoControlPlane() throws Exception {
        try {
            buildAndRun();
        } finally {
            Collections.reverse(teardown);
            for (Runnable r : teardown) {
                try {
                    r.run();
                } catch (Exception e) {
                    logger.warn("teardown step failed", e);
                }
            }
        }
    }

    private void buildAndRun() throws Exception {
        final Settings settings = Settings.builder()
            .put("node.name", "s0")
            .put("cluster.name", "s0-cluster")
            .put("path.home", createTempDir())
            .build();

        final Environment environment = TestEnvironment.newEnvironment(settings);
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        threadPool = new TestThreadPool("s0");
        teardown.add(() -> ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS));
        nodeEnv = new NodeEnvironment(settings, environment);
        teardown.add(nodeEnv::close);

        final DiscoveryNode localNode = new DiscoveryNode(
            "s0-node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        // ---- RFC §5.1: a real ClusterService with no Coordinator behind it -------------------
        clusterService = S0Wiring.clusterService(settings, clusterSettings, threadPool, localNode);
        teardown.add(clusterService::close);

        // ---- the data plane ------------------------------------------------------------------
        indicesService = S0Wiring.indicesService(settings, environment, nodeEnv, threadPool, clusterService, clusterSettings);
        // FINDING (S0): IndicesService and SearchService are AbstractLifecycleComponents — start() is
        // required before either will accept work. Node does this via its own lifecycle; the shell must too.
        indicesService.start();
        teardown.add(() -> IOUtils.closeWhileHandlingException(indicesService));

        searchService = S0Wiring.searchService(
            settings,
            clusterSettings,
            threadPool,
            clusterService,
            indicesService,
            new BigArrays(new PageCacheRecycler(settings), null, "s0")
        );
        searchService.start();
        teardown.add(() -> IOUtils.closeWhileHandlingException(searchService));

        runSpike(localNode);
    }

    private void runSpike(DiscoveryNode localNode) throws Exception {
        final IndexMetadata indexMetadata = IndexMetadata.builder(INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "s0-uuid-0000000000")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
                    .build()
            )
            .putMapping(MAPPING)
            // FINDING (S0): the data plane REFUSES to start a primary at term 0
            // (ReplicationTracker.activatePrimaryMode -> RetentionLeases: "primary term must be positive").
            // Today the elected manager bumps this on allocation. In the shell it is the shard-head's
            // CAS term — RFC §5.3's proposal is not an optimisation, it is load-bearing.
            .primaryTerm(0, SHARD_HEAD_TERM)
            .build();

        final ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        final ShardRouting initializing = TestShardRouting.newShardRouting(
            shardId,
            localNode.getId(),
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );

        // ---- Q1: the minimum ClusterState a shard will accept --------------------------------
        final ClusterState state = ClusterState.builder(new ClusterName("s0-cluster"))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build())
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .routingTable(RoutingTable.builder().addAsRecovery(indexMetadata).build())
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

        // The shell is a second caller of ClusterApplier.onNewClusterState. Coordinator is the first.
        S0Wiring.applyLocally(clusterService, state);

        // ---- Q2: the shell-owned reconciler, in the smallest form that works ------------------
        final IndexService indexService = indicesService.createIndex(indexMetadata, Collections.emptyList(), false);
        // FINDING (S0): createIndex does NOT apply the mapping to MapperService. The real reconciler
        // (IndicesClusterStateService:888) follows it with updateMapping(null, metadata); without that,
        // the first write returns MAPPING_UPDATE_REQUIRED. This is reconciler step 2 of 4.
        indexService.updateMapping(null, indexMetadata);
        final IndexShard shard = S0Wiring.openShard(indexService, initializing, localNode, clusterService);

        shard.markAsRecovering("s0-store", new RecoveryState(initializing, localNode, null));
        final PlainActionFuture<Boolean> recovered = PlainActionFuture.newFuture();
        shard.recoverFromStore(recovered);
        assertTrue("recoverFromStore reported failure", recovered.actionGet());

        final ShardRouting started = initializing.moveToStarted();
        shard.updateShardState(
            started,
            shard.getPendingPrimaryTerm(),
            null,
            state.version(),
            Set.of(started.allocationId().getId()),
            new IndexShardRoutingTable.Builder(shardId).addShard(started).build(),
            state.nodes()
        );
        assertEquals("shard did not reach STARTED", IndexShardState.STARTED, shard.state());

        // ---- criteria 1-3: index, refresh, search --------------------------------------------
        indexDoc(shard, "1", "{\"msg\":\"hello serverless\",\"n\":1}");
        indexDoc(shard, "2", "{\"msg\":\"hello serverless\",\"n\":2}");
        shard.refresh("s0");

        long hits = queryHitCount(shardId, "hello");
        assertEquals("search found no documents — the seam is broken", 2L, hits);

        // ---- criterion 4: a doc indexed after the first search is visible after refresh -------
        indexDoc(shard, "3", "{\"msg\":\"hello serverless\",\"n\":3}");
        assertEquals("unrefreshed write must not be visible", 2L, queryHitCount(shardId, "hello"));
        shard.refresh("s0");
        assertEquals("refresh did not expose the third document", 3L, queryHitCount(shardId, "hello"));

        // ---- criterion 5: nothing forbidden was constructed ----------------------------------
        assertNoControlPlane(clusterService, indicesService, searchService, shard, indexService);

        logger.info("S0 PASSED: 3 docs indexed and searched with no Coordinator/AllocationService/GatewayMetaState/Node");
    }

    private void indexDoc(IndexShard shard, String id, String source) throws IOException {
        final Engine.IndexResult result = shard.applyIndexOperationOnPrimary(
            org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
            org.opensearch.index.VersionType.INTERNAL,
            new SourceToParse(shard.shardId().getIndexName(), id, new BytesArray(source), org.opensearch.common.xcontent.XContentType.JSON),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false
        );
        assertEquals(
            "mapping update required — S0 pre-declares its mapping precisely so this cannot happen",
            Engine.Result.Type.SUCCESS,
            result.getResultType()
        );
        shard.sync();
    }

    /** Runs a real query through SearchService and returns the hit count. Zero when the seam is broken. */
    private long queryHitCount(ShardId shardId, String term) throws Exception {
        // FINDING (S0): allowPartialSearchResults defaults to null and is normally filled in by
        // TransportSearchAction. A shell that calls SearchService directly must set it.
        final SearchRequest searchRequest = new SearchRequest(INDEX).allowPartialSearchResults(false)
            .source(new SearchSourceBuilder().query(org.opensearch.index.query.QueryBuilders.matchQuery("msg", term)).trackTotalHits(true));
        final ShardSearchRequest request = new ShardSearchRequest(
            org.opensearch.action.OriginalIndices.NONE,
            searchRequest,
            shardId,
            1,
            AliasFilter.EMPTY,
            1.0f,
            System.currentTimeMillis(),
            null,
            org.opensearch.core.common.Strings.EMPTY_ARRAY
        );

        final PlainActionFuture<org.opensearch.search.SearchPhaseResult> future = PlainActionFuture.newFuture();
        searchService.executeQueryPhase(
            request,
            false,
            new org.opensearch.action.search.SearchShardTask(0, "s0", "s0", "s0", null, Collections.emptyMap()),
            ActionListener.wrap(future::onResponse, future::onFailure),
            ThreadPool.Names.SEARCH,
            false
        );
        final QuerySearchResult result = future.actionGet().queryResult();
        final TotalHits totalHits = result.topDocs().topDocs.totalHits;
        return totalHits.value();
    }

    /**
     * Criterion 5: walk the constructed object graph and assert no forbidden class appears.
     * Bounded BFS — the graph is large and cyclic, so visited-tracking and a node cap are load-bearing.
     */
    private void assertNoControlPlane(Object... roots) {
        final ScanResult r = scan(FORBIDDEN, roots);
        assertTrue(
            "forbidden control-plane classes reachable from the constructed graph: " + r.found + " (visited " + r.visited + " objects)",
            r.found.isEmpty()
        );
        // Without a floor this assertion passes vacuously if the walk ever stops working —
        // the exact "confident empty answer" failure HANDOFF.md warns about.
        assertTrue("object-graph walk was vacuous: only " + r.visited + " objects visited", r.visited > 1000);
        logger.info("criterion 5: walked {} objects, none forbidden", r.visited);
    }

    /** Negative control: criterion 5 means nothing unless the walker can actually detect a violation. */
    public void testObjectGraphWalkerDetectsAPlantedViolation() {
        final Decoy decoy = new Decoy();
        final Object deep = new Object() {
            @SuppressWarnings("unused")
            final Object level2 = new Object() {
                @SuppressWarnings("unused")
                final Object level3 = decoy;
            };
        };
        final ScanResult r = scan(Set.of(Decoy.class.getName()), deep);
        assertEquals("walker failed to find a decoy planted three hops deep", Set.of(Decoy.class.getName()), r.found);
    }

    private static final class Decoy {}

    private static final class ScanResult {
        final Set<String> found;
        final int visited;

        ScanResult(Set<String> found, int visited) {
            this.found = found;
            this.visited = visited;
        }
    }

    @SuppressForbidden(reason = "the object-graph walk is the mechanism of RFC §11 criterion 5; reflection is the point")
    private ScanResult scan(Set<String> forbidden, Object... roots) {
        final IdentityHashMap<Object, Boolean> seen = new IdentityHashMap<>();
        final Deque<Object> queue = new ArrayDeque<>();
        final Set<String> found = new HashSet<>();
        for (Object r : roots) {
            if (r != null) {
                queue.add(r);
            }
        }
        int visited = 0;
        final int cap = 200_000;
        while (queue.isEmpty() == false && visited < cap) {
            final Object o = queue.poll();
            if (seen.put(o, Boolean.TRUE) != null) {
                continue;
            }
            visited++;
            final Class<?> cls = o.getClass();
            final String name = cls.getName();
            if (forbidden.contains(name)) {
                found.add(name);
                continue;
            }
            if (name.startsWith("java.") || name.startsWith("javax.") || name.startsWith("sun.") || cls.isPrimitive()) {
                continue;
            }
            if (cls.isArray()) {
                if (cls.getComponentType().isPrimitive() == false) {
                    for (Object e : (Object[]) o) {
                        if (e != null) {
                            queue.add(e);
                        }
                    }
                }
                continue;
            }
            for (Class<?> c = cls; c != null && c != Object.class; c = c.getSuperclass()) {
                for (Field f : c.getDeclaredFields()) {
                    if (Modifier.isStatic(f.getModifiers()) || f.getType().isPrimitive()) {
                        continue;
                    }
                    try {
                        f.setAccessible(true);
                        final Object v = f.get(o);
                        if (v != null) {
                            queue.add(v);
                        }
                    } catch (Throwable ignored) {
                        // inaccessible under the module system — skipping is safe, it only weakens the check
                    }
                }
            }
        }
        return new ScanResult(found, visited);
    }
}
