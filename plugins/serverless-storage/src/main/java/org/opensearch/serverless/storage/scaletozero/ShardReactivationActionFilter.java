/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.serverless.storage.allocation.SuspendedShardsMetadata;
import org.opensearch.serverless.storage.scaletozero.action.ReactivateShardsAction;
import org.opensearch.serverless.storage.scaletozero.action.ReactivateShardsRequest;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.List;

/**
 * The "cold-start reactivation on next write or query" half of scale-to-zero (rfc-serverless-opensearch.md
 * &sect;7.3): the plugin-side trigger every write and search request needs to make progress against a
 * suspended shard.
 *
 * <p>Deliberately index-granular, not shard-granular: at the point an {@link ActionFilter} sees a
 * request, core has not yet resolved which shard(s) it will actually touch (that happens deeper in
 * the same transport action this filter runs in front of), so this clears every suspended shard of
 * every named index on the request rather than trying to predict the one shard that matters. Safe
 * and cheap either way -- reactivating an unrelated already-active shard is a no-op both here ({@link
 * org.opensearch.serverless.storage.scaletozero.action.TransportReactivateShardsAction} only mutates
 * indices that actually have something suspended) and in {@code SuspendedShardAllocationDecider} (an
 * unsuspended shard's decision is unaffected).
 *
 * <p><b>Dispatches via {@link ReactivateShardsAction}, not a direct {@code ClusterService} mutation
 * -- a real correction made during this feature's own integration testing.</b> This filter runs on
 * whichever node happens to receive the triggering request, which is frequently not the elected
 * cluster-manager node; {@code ClusterService#submitStateUpdateTask} only works when called
 * <em>on</em> the cluster-manager node itself and throws {@code NotClusterManagerException}
 * otherwise -- caught by {@code ServerlessStorageShardSuspensionIT} failing with exactly that
 * exception (visible in the data node's log) when this filter's first version called {@code
 * submitStateUpdateTask} directly. {@link ReactivateShardsAction}, a {@code
 * TransportClusterManagerNodeAction}, transparently forwards to whichever node actually is the
 * cluster-manager, the same way any other cluster-state-mutating admin action already does.
 *
 * <p><b>Write requests never wait; search requests do -- a deliberate, researched split, not an
 * oversight.</b> {@code TransportReplicationAction}/{@code TransportSingleShardAction} (the write
 * and get paths) already wait-and-retry via their own {@code ClusterStateObserver} when a shard has
 * no active copy rather than failing fast, so for those this filter only needs to make a <em>new</em>
 * cluster state happen -- {@link #apply} fires the reactivation dispatch and calls {@code
 * chain.proceed} immediately, letting core's own retry loop do the waiting. {@code
 * TransportSearchAction} has no equivalent: {@code IndexShardRoutingTable#searchReplicaActiveInitializingShardIt}
 * (core's own search-only-shard routing) is a bare filter with no retry, and {@code
 * cluster.routing.search_replica.strict} defaults to {@code true}, so a search against a fully-
 * suspended reader shard would otherwise fail immediately with {@code NoShardAvailableActionException}
 * -- confirmed by direct research into core before this was written, and an explicit product
 * decision that scale-to-zero must never surface as a client-visible failure. For {@link
 * SearchAction#NAME} specifically, this filter therefore holds the request itself via its own {@link
 * ClusterStateObserver}, waiting (bounded by {@link
 * org.opensearch.serverless.storage.ServerlessStoragePlugin#SERVERLESS_STORAGE_SCALE_TO_ZERO_SEARCH_REACTIVATION_WAIT_SETTING})
 * for every suspended shard of every targeted index to clear before calling {@code chain.proceed} --
 * on timeout it proceeds anyway (fail open, matching how a persistent, unrecoverable problem
 * eventually surfaces as an ordinary error rather than hanging a request forever, the same
 * philosophy core's own bounded retries already use).
 */
public final class ShardReactivationActionFilter implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(ShardReactivationActionFilter.class);

    private volatile ClusterService clusterService;
    private volatile Client client;
    private volatile ThreadPool threadPool;
    private volatile TimeValue searchReactivationWait = TimeValue.timeValueSeconds(30);

    /** Creates the filter with no dependencies yet -- see {@link #setDependencies}. */
    public ShardReactivationActionFilter() {}

    /**
     * Supplies the dependencies this filter needs, once available -- {@link
     * org.opensearch.plugins.ActionPlugin#getActionFilters()} is called before {@code
     * createComponents} runs, so this filter is constructed with none of these yet and gets them
     * wired in afterward, the same "instantiate early, wire in late" shape a plugin needs whenever a
     * component core asks for earlier than the dependency it needs is available.
     *
     * @param clusterService used to read suspended shards.
     * @param client dispatches {@link ReactivateShardsAction} to trigger reactivation.
     * @param threadPool used to build this filter's own {@link ClusterStateObserver} for the search-wait path.
     * @param searchReactivationWait the maximum time a held search request waits for reactivation before proceeding anyway.
     */
    public void setDependencies(ClusterService clusterService, Client client, ThreadPool threadPool, TimeValue searchReactivationWait) {
        this.clusterService = clusterService;
        this.client = client;
        this.threadPool = threadPool;
        this.searchReactivationWait = searchReactivationWait;
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE; // run first: cheapest possible check, and reactivation should start as early as possible.
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionRequestMetadata<Request, Response> actionRequestMetadata,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        ClusterService currentClusterService = this.clusterService;
        Client currentClient = this.client;
        if (currentClusterService == null || currentClient == null || (request instanceof IndicesRequest) == false) {
            chain.proceed(task, action, request, listener);
            return;
        }
        // Some IndicesRequest implementations (e.g. internal PutMappingRequests issued by
        // MappingUpdatedAction, caught by this filter's own integration test) return null from
        // indices() rather than an empty array -- guard rather than assume every implementation
        // populates it.
        String[] indexNames = ((IndicesRequest) request).indices();
        if (indexNames == null) {
            chain.proceed(task, action, request, listener);
            return;
        }

        boolean isSearch = SearchAction.NAME.equals(action);
        ClusterState state = currentClusterService.state();
        List<PendingReactivation> pending = new ArrayList<>();
        for (String indexName : indexNames) {
            IndexMetadata indexMetadata = state.metadata().index(indexName);
            if (indexMetadata == null) {
                continue;
            }
            String realIndexName = indexMetadata.getIndex().getName();
            boolean writerSuspended = SuspendedShardsMetadata.suspendedShardIds(indexMetadata).isEmpty() == false;
            boolean readerSuspended = SuspendedShardsMetadata.suspendedReaderShardIds(indexMetadata).isEmpty() == false;
            if (writerSuspended) {
                triggerReactivation(currentClient, realIndexName, false);
            }
            if (readerSuspended) {
                triggerReactivation(currentClient, realIndexName, true);
            }
            if (isSearch && (writerSuspended || readerSuspended)) {
                pending.add(new PendingReactivation(realIndexName, writerSuspended, readerSuspended));
            }
        }

        ThreadPool currentThreadPool = this.threadPool;
        if (pending.isEmpty() || currentThreadPool == null) {
            chain.proceed(task, action, request, listener);
            return;
        }
        waitForReactivationThenProceed(currentClusterService, currentThreadPool, pending, task, action, request, listener, chain);
    }

    /**
     * One index this filter is holding a search request open for, and which role(s) of it were
     * suspended -- {@link #allFullyReactivated} needs to know which role's routing to check
     * (writer's primary shard vs. reader's search-only copies), not just "is something started
     * somewhere," since the other role's copy routinely stays started the entire time and would
     * otherwise make the wait resolve immediately without the actually-reactivating role ever
     * finishing recovery. A real bug this filter's own integration test caught: the first version
     * checked "any copy of the shard is STARTED," which the still-active writer primary trivially
     * satisfied the instant the reader's suspended marker cleared, well before the reader copy
     * itself finished recovering -- the search proceeded immediately and still failed with "all
     * shards failed."
     */
    private record PendingReactivation(String indexName, boolean writer, boolean reader) {
    }

    private static void triggerReactivation(Client client, String indexName, boolean reader) {
        client.execute(
            ReactivateShardsAction.INSTANCE,
            new ReactivateShardsRequest(indexName, reader),
            ActionListener.wrap(
                response -> {},
                e -> logger.warn("failed to trigger reactivation of index [" + indexName + "] (reader=" + reader + ")", e)
            )
        );
    }

    private <Request extends ActionRequest, Response extends ActionResponse> void waitForReactivationThenProceed(
        ClusterService clusterService,
        ThreadPool threadPool,
        List<PendingReactivation> pending,
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        ClusterStateObserver observer = new ClusterStateObserver(clusterService, logger, threadPool.getThreadContext());
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                chain.proceed(task, action, request, listener);
            }

            @Override
            public void onClusterServiceClose() {
                chain.proceed(task, action, request, listener);
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                logger.warn(
                    "timed out after "
                        + timeout
                        + " waiting for reactivation of suspended shard(s) of "
                        + pending
                        + "; proceeding with the search anyway"
                );
                chain.proceed(task, action, request, listener);
            }
        }, state -> allFullyReactivated(state, pending), searchReactivationWait);
    }

    private static boolean allFullyReactivated(ClusterState state, List<PendingReactivation> pending) {
        for (PendingReactivation entry : pending) {
            IndexMetadata indexMetadata = state.metadata().index(entry.indexName());
            if (indexMetadata == null) {
                continue;
            }
            if (entry.writer() && SuspendedShardsMetadata.suspendedShardIds(indexMetadata).isEmpty() == false) {
                return false;
            }
            if (entry.reader() && SuspendedShardsMetadata.suspendedReaderShardIds(indexMetadata).isEmpty() == false) {
                return false;
            }
            if (state.routingTable().hasIndex(entry.indexName()) == false) {
                continue;
            }
            // Clearing the suspended marker alone is not enough: the specific role's shard copy
            // still needs to actually finish recovering before it can serve a query. Checking only
            // "is the writer's own suspended flag cleared" (or any-copy-started) is not sufficient
            // either -- a real bug this filter's own integration test caught: the writer's primary
            // copy routinely stays STARTED the entire time a reader copy is suspended, so an
            // any-copy-started check resolved the wait immediately, well before the reactivating
            // reader copy itself finished recovering, and the search still failed with "all shards
            // failed." Checking specifically the role that was actually suspended fixes it.
            for (org.opensearch.cluster.routing.IndexShardRoutingTable shardRoutingTable : state.routingTable().index(entry.indexName())) {
                if (entry.writer()
                    && shardRoutingTable.primaryShard().state() != org.opensearch.cluster.routing.ShardRoutingState.STARTED) {
                    return false;
                }
                if (entry.reader()
                    && shardRoutingTable.searchOnlyReplicas()
                        .stream()
                        .noneMatch(r -> r.state() == org.opensearch.cluster.routing.ShardRoutingState.STARTED)) {
                    return false;
                }
            }
        }
        return true;
    }
}
