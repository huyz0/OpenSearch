/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero;

import org.opensearch.Version;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit coverage for the search-vs-reactivation race the filter must close: a search that arrives
 * after the suspended marker is already cleared but before the reader copy has actually finished
 * recovering (still {@code INITIALIZING}) must still wait, not proceed straight into a {@code
 * NoShardAvailableActionException}.
 */
public class ShardReactivationActionFilterTests extends OpenSearchTestCase {

    private static final String INDEX = "idx";

    private TestThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private static IndexMetadata readerIndexMetadata() {
        return IndexMetadata.builder(INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, INDEX + "-uuid")
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .numberOfSearchReplicas(1)
            .build();
    }

    /** Primary STARTED plus a single search-only replica in the given non-null started/initializing state. */
    private static ClusterState clusterStateWithSearchReplica(IndexMetadata indexMetadata, boolean searchReplicaStarted) {
        Index index = indexMetadata.getIndex();
        ShardId shardId = new ShardId(index, 0);

        ShardRouting primary = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
        ).initialize("node-1", null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE).moveToStarted();

        ShardRouting searchReplica = ShardRouting.newUnassigned(
            shardId,
            false,
            true,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.REPLICA_ADDED, "test")
        ).initialize("node-2", null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        if (searchReplicaStarted) {
            searchReplica = searchReplica.moveToStarted();
        }

        IndexRoutingTable indexRoutingTable = new IndexRoutingTable.Builder(index).addShard(primary).addShard(searchReplica).build();

        return ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();
    }

    private ShardReactivationActionFilter filter(ClusterService clusterService) {
        ShardReactivationActionFilter filter = new ShardReactivationActionFilter();
        filter.setDependencies(clusterService, new NoOpClient(threadPool), threadPool, TimeValue.timeValueSeconds(30));
        return filter;
    }

    private static ActionFilterChain<SearchRequest, org.opensearch.action.search.SearchResponse> recordingChain(AtomicBoolean proceeded) {
        return (task, action, request, listener) -> proceeded.set(true);
    }

    /**
     * The race the fix targets: the reader suspended marker is already CLEARED (nothing in
     * SuspendedShardsMetadata), yet the search-only replica is still INITIALIZING. Before the fix
     * the filter looked only at the marker, found nothing suspended, and proceeded immediately --
     * failing the search. It must instead wait until the replica reaches STARTED.
     */
    public void testSearchWaitsWhenMarkerClearedButReaderReplicaStillInitializing() throws Exception {
        IndexMetadata indexMetadata = readerIndexMetadata(); // no suspended-reader marker
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        try {
            ClusterServiceUtils.setState(clusterService, clusterStateWithSearchReplica(indexMetadata, false));

            AtomicBoolean proceeded = new AtomicBoolean(false);
            SearchRequest request = new SearchRequest(INDEX);
            filter(clusterService).apply(null, SearchAction.NAME, request, null, null, recordingChain(proceeded));

            assertFalse(
                "with the marker cleared but the reader replica still INITIALIZING, the search must be held, not proceeded",
                proceeded.get()
            );

            // Reader copy finishes recovery -> filter's observer must release the held search.
            ClusterServiceUtils.setState(clusterService, clusterStateWithSearchReplica(indexMetadata, true));
            assertBusy(() -> assertTrue("once the reader replica is STARTED the held search must proceed", proceeded.get()));
        } finally {
            clusterService.close();
        }
    }

    /**
     * Control: when nothing is reactivating (marker clear AND the reader replica already STARTED) the
     * filter must not wait -- the hot path proceeds synchronously.
     */
    public void testSearchProceedsImmediatelyWhenReaderReplicaAlreadyStarted() {
        IndexMetadata indexMetadata = readerIndexMetadata();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        try {
            ClusterServiceUtils.setState(clusterService, clusterStateWithSearchReplica(indexMetadata, true));

            AtomicBoolean proceeded = new AtomicBoolean(false);
            SearchRequest request = new SearchRequest(INDEX);
            filter(clusterService).apply(null, SearchAction.NAME, request, null, null, recordingChain(proceeded));

            assertTrue("a fully-started reader index must not hold the search", proceeded.get());
        } finally {
            clusterService.close();
        }
    }

    /**
     * An index present in metadata with no routing entry at all -- the shape Phase A wants a cold
     * tenant to have. Both routing reads in this filter guard against it, but before the fix they
     * gave the wrong answer: {@code readerCopyNotYetStarted} returned "no reactivation needed" and
     * {@code allFullyReactivated} returned "this one is done". The search proceeded against an index
     * with no shard copy of any role, which is a silent failure rather than a loud one.
     */
    public void testSearchWaitsForAnIndexPresentInMetadataAndAbsentFromRouting() throws Exception {
        IndexMetadata indexMetadata = readerIndexMetadata();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        try {
            ClusterServiceUtils.setState(clusterService, clusterStateWithoutRouting(indexMetadata));

            AtomicBoolean proceeded = new AtomicBoolean(false);
            SearchRequest request = new SearchRequest(INDEX);
            filter(clusterService).apply(null, SearchAction.NAME, request, null, null, recordingChain(proceeded));

            assertFalse("an index with no routing entry has nothing to serve the search; it must be held", proceeded.get());

            // Reactivation recreates the routing entry and the reader copy starts.
            ClusterServiceUtils.setState(clusterService, clusterStateWithSearchReplica(indexMetadata, true));
            assertBusy(() -> assertTrue("once routing exists and the reader copy is STARTED the search must proceed", proceeded.get()));
        } finally {
            clusterService.close();
        }
    }

    /**
     * The other reading of the same absence, and the reason the two cases cannot share an answer: an
     * index gone from metadata is never coming back, so the wait must resolve rather than run to its
     * timeout. This is what keeps the fix above from turning a deleted index into a 30-second stall.
     */
    public void testHeldSearchIsReleasedWhenTheIndexIsDeletedOutright() throws Exception {
        IndexMetadata indexMetadata = readerIndexMetadata();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        try {
            ClusterServiceUtils.setState(clusterService, clusterStateWithoutRouting(indexMetadata));

            AtomicBoolean proceeded = new AtomicBoolean(false);
            SearchRequest request = new SearchRequest(INDEX);
            filter(clusterService).apply(null, SearchAction.NAME, request, null, null, recordingChain(proceeded));
            assertFalse(proceeded.get());

            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(new ClusterName("test"))
                    .metadata(Metadata.builder().build())
                    .routingTable(RoutingTable.builder().build())
                    .build()
            );
            assertBusy(() -> assertTrue("a deleted index must release the wait, not stall it until the timeout", proceeded.get()));
        } finally {
            clusterService.close();
        }
    }

    /**
     * <b>The most common way a search names an index, and the filter skipped every one of them.</b> Each
     * requested name was looked up with {@code state.metadata().index(name)}, which matches concrete names
     * only -- so {@code logs-*}, an alias, or a data stream found null, took no reactivation and no wait,
     * and under the default strict search-replica routing the client saw "all shards failed": the exact
     * failure this filter's own javadoc says must never surface.
     */
    public void testAWildcardSearchStillWaitsForReactivation() throws Exception {
        IndexMetadata indexMetadata = readerIndexMetadata();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        try {
            ClusterServiceUtils.setState(clusterService, clusterStateWithSearchReplica(indexMetadata, false));

            AtomicBoolean proceeded = new AtomicBoolean(false);
            SearchRequest request = new SearchRequest("id*");
            filter(clusterService).apply(null, SearchAction.NAME, request, null, null, recordingChain(proceeded));

            assertFalse("a wildcard that resolves to a reactivating index must be held just like its name would be", proceeded.get());

            ClusterServiceUtils.setState(clusterService, clusterStateWithSearchReplica(indexMetadata, true));
            assertBusy(() -> assertTrue("and released once the reader copy is STARTED", proceeded.get()));
        } finally {
            clusterService.close();
        }
    }

    /**
     * <b>The wait that could not end, on a cluster whose steady state is silence.</b> The filter reads the
     * state, decides to wait, and only then builds its observer -- and {@code waitForNextChange} evaluates
     * only states <em>newer</em> than the observed one. A reader copy that reached STARTED in the gap
     * between those two reads satisfied a predicate against a state the observer would never test, so on a
     * scale-to-zero cluster with nothing else happening the search sat for the full wait and then proceeded
     * anyway. Nothing about that looks like a bug from outside: the search succeeds, slowly, every time.
     *
     * <p>The two states are injected through the same {@code ClusterService} the filter reads twice: stale
     * for {@code apply}, current for the observer. That is precisely the race, made deterministic.
     */
    public void testAWaitIsNotTakenWhenTheStateAlreadySatisfiesIt() {
        IndexMetadata indexMetadata = readerIndexMetadata();
        ClusterService real = ClusterServiceUtils.createClusterService(threadPool);
        try {
            ClusterServiceUtils.setState(real, clusterStateWithSearchReplica(indexMetadata, true));
            ClusterState stale = clusterStateWithSearchReplica(indexMetadata, false);

            ClusterService clusterService = org.mockito.Mockito.spy(real);
            java.util.concurrent.atomic.AtomicBoolean firstRead = new java.util.concurrent.atomic.AtomicBoolean(true);
            org.mockito.Mockito.doAnswer(invocation -> firstRead.compareAndSet(true, false) ? stale : real.state())
                .when(clusterService)
                .state();

            AtomicBoolean proceeded = new AtomicBoolean(false);
            SearchRequest request = new SearchRequest(INDEX);
            filter(clusterService).apply(null, SearchAction.NAME, request, null, null, recordingChain(proceeded));

            assertTrue(
                "a search whose reader copy started between the two reads must proceed at once, not wait "
                    + "for a cluster state change that a quiet cluster will never produce",
                proceeded.get()
            );
        } finally {
            real.close();
        }
    }

    /** In metadata, absent from routing: cold. */
    private static ClusterState clusterStateWithoutRouting(IndexMetadata indexMetadata) {
        return ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .routingTable(RoutingTable.builder().build())
            .build();
    }
}
