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
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.serverless.storage.allocation.SuspendedShardsMetadata;
import org.opensearch.serverless.storage.scaletozero.action.ReactivateShardsAction;
import org.opensearch.serverless.storage.scaletozero.action.ReactivateShardsRequest;
import org.opensearch.tasks.Task;
import org.opensearch.transport.client.Client;

/**
 * The "cold-start reactivation on next write or query" half of scale-to-zero (rfc-serverless-opensearch.md
 * &sect;7.3): the plugin-side trigger that {@code TransportReplicationAction}/{@code
 * TransportSingleShardAction} already need to make progress, confirmed by direct research into core
 * before this was written (see this feature's own commit message) -- those transport actions already
 * wait-and-retry via {@code ClusterStateObserver} when a shard has no active copy rather than failing
 * fast, so reactivation only needs to make a <em>new</em> cluster state happen; core's own existing
 * retry loop picks it up from there without this filter needing to hold the request itself.
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
 * <p>Never blocks or delays the request itself: the suspended check against the already-in-memory
 * {@link ClusterState} is synchronous and cheap, but the {@link ReactivateShardsAction} dispatch
 * this triggers when something *is* suspended is fired off asynchronously, and {@link #apply}
 * always calls {@code chain.proceed} immediately either way -- exactly the same "trigger reroute,
 * let the request's own retry loop do the waiting" split the research this feature is based on
 * recommended.
 */
public final class ShardReactivationActionFilter implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(ShardReactivationActionFilter.class);

    private volatile ClusterService clusterService;
    private volatile Client client;

    /** Creates the filter with no {@link ClusterService}/{@link Client} yet -- see {@link #setClusterServiceAndClient}. */
    public ShardReactivationActionFilter() {}

    /**
     * Supplies the {@link ClusterService}/{@link Client} this filter needs, once available -- {@link
     * org.opensearch.plugins.ActionPlugin#getActionFilters()} is called before {@code
     * createComponents} runs, so this filter is constructed with neither yet and gets them wired in
     * afterward, the same "instantiate early, wire in late" shape a plugin needs whenever a
     * component core asks for earlier than the dependency it needs is available.
     *
     * @param clusterService used to read suspended shards.
     * @param client dispatches {@link ReactivateShardsAction} to trigger reactivation.
     */
    public void setClusterServiceAndClient(ClusterService clusterService, Client client) {
        this.clusterService = clusterService;
        this.client = client;
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
        if (currentClusterService != null && currentClient != null && request instanceof IndicesRequest) {
            // Some IndicesRequest implementations (e.g. internal PutMappingRequests issued by
            // MappingUpdatedAction, caught by this filter's own integration test) return null from
            // indices() rather than an empty array -- guard rather than assume every implementation
            // populates it.
            String[] indexNames = ((IndicesRequest) request).indices();
            ClusterState state = currentClusterService.state();
            if (indexNames != null) {
                for (String indexName : indexNames) {
                    IndexMetadata indexMetadata = state.metadata().index(indexName);
                    if (indexMetadata != null && SuspendedShardsMetadata.suspendedShardIds(indexMetadata).isEmpty() == false) {
                        String realIndexName = indexMetadata.getIndex().getName();
                        currentClient.execute(
                            ReactivateShardsAction.INSTANCE,
                            new ReactivateShardsRequest(realIndexName),
                            ActionListener.wrap(
                                response -> {},
                                e -> logger.warn("failed to trigger reactivation of index [" + realIndexName + "]", e)
                            )
                        );
                    }
                }
            }
        }
        chain.proceed(task, action, request, listener);
    }
}
