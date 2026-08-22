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
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
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
 * until the reader copy that would serve the search is actually {@code STARTED} before calling {@code
 * chain.proceed}. The wait is gated on real routing-table state, not just the suspended metadata
 * marker: the marker clears the instant the reactivation cluster-state update commits, but the reader
 * copy reaches {@code STARTED} only later, so a search arriving in that window (marker already clear,
 * copy still {@code INITIALIZING}) must still wait or it would fail with {@code
 * NoShardAvailableActionException} --
 * on timeout it proceeds anyway (fail open, matching how a persistent, unrecoverable problem
 * eventually surfaces as an ordinary error rather than hanging a request forever, the same
 * philosophy core's own bounded retries already use).
 *
 * <p><b>Gated indices reactivate on a second, differently-shaped path -- and until recently on no path at
 * all.</b> Everything above resolves an index through {@code state.metadata().index(name)} and mutates
 * cluster state. A gated index has no metadata entry, by definition, so it matched neither half: the loop
 * skipped it and the transport action returned the state unchanged, while {@code
 * ShardSuspensionCoordinator} went on recording gated suspensions that removed the shard from every
 * routing resolution on the recording node. A gated shard that fell asleep never woke. See {@link
 * #wakeGatedIndex} for the path that now handles it, and why it clears the record locally and
 * synchronously rather than waiting for a cluster state that will never come.
 */
public final class ShardReactivationActionFilter implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(ShardReactivationActionFilter.class);

    private volatile ClusterService clusterService;
    private volatile Client client;
    private volatile ThreadPool threadPool;
    private volatile TimeValue searchReactivationWait = TimeValue.timeValueSeconds(30);

    /**
     * Turns the names on a request into the concrete indices it will actually touch.
     *
     * <p>Built here rather than injected because this filter is constructed before {@code
     * createComponents} runs (see {@link #setDependencies}), and core's own resolver needs nothing but a
     * thread context. Core builds it exactly this way for the same reason.
     */
    private volatile IndexNameExpressionResolver indexNameExpressionResolver;

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
        // Tolerates a null thread pool. This method's own contract is "wired in late", and a caller that
        // is only exercising the parts of the plugin that do not need a thread pool passes null here --
        // which every test of plugin construction does. Dereferencing it eagerly turned that into an NPE
        // during createComponents. The one place the resolver is read already falls back to the requested
        // names when it is absent, so a null here costs expression resolution, not correctness.
        this.indexNameExpressionResolver = threadPool == null ? null : new IndexNameExpressionResolver(threadPool.getThreadContext());
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
        GatedShardSuspensionRegistry gatedSuspensions = GatedShardSuspensionRegistry.installed();
        // Whether this node holds any gated suspension at all. A plain size read, and it is the gate that
        // keeps the gated branch below off the hot path entirely: with nothing asleep it never runs, and
        // in the common case nothing is.
        boolean anyGatedSuspension = gatedSuspensions != null && gatedSuspensions.trackedIndexCount() > 0;
        List<PendingReactivation> pending = new ArrayList<>();
        for (String indexName : resolvedNames(state, (IndicesRequest) request, indexNames)) {
            IndexMetadata indexMetadata = state.metadata().index(indexName);
            if (indexMetadata == null) {
                if (anyGatedSuspension) {
                    wakeGatedIndex(state, gatedSuspensions, currentClient, indexName);
                }
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
            if (isSearch) {
                // Deciding whether to wait off the suspended marker alone leaves a real race:
                // TransportReactivateShardsAction clears the marker in the cluster-state update, but
                // the reader copy itself still takes time (potentially seconds under object-store
                // latency) to actually reach STARTED. A search landing in that window -- marker
                // already clear, reader copy still INITIALIZING -- would otherwise skip the wait,
                // proceed immediately, and fail with NoShardAvailableActionException under the
                // default cluster.routing.search_replica.strict=true, exactly the client-visible
                // failure scale-to-zero must never surface. So gate the wait on ACTUAL routing-table
                // state, not just the metadata marker: wait whenever the reader copy that would serve
                // this search is not yet STARTED, whether the marker is still set or already cleared.
                boolean readerWait = readerSuspended || readerCopyNotYetStarted(state, indexMetadata);
                if (writerSuspended || readerWait) {
                    pending.add(new PendingReactivation(realIndexName, writerSuspended, readerWait));
                }
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
     * The names on the request, plus the concrete indices they resolve to.
     *
     * <p><b>Resolving them was missing, and the gap covered the most common way a search names an
     * index.</b> This filter used to look each requested name up with {@code state.metadata().index(name)},
     * which matches concrete index names and nothing else. A search for {@code logs-*}, or through an alias,
     * or against a data stream, therefore found null and was skipped: no reactivation was triggered, no wait
     * was taken, and under the default {@code cluster.routing.search_replica.strict=true} the client got
     * "all shards failed" -- the exact client-visible failure this filter's own javadoc says scale-to-zero
     * must never surface.
     *
     * <p><b>Only when a name needs it, which is the whole cost argument.</b> Expression resolution is what
     * the transport action below this filter will do anyway, but doing it twice on every request is a real
     * cost at this position in the chain. A name that is already a concrete index in metadata resolves to
     * itself, so those skip it. So does a name that is neither a pattern, nor an alias, nor a data stream:
     * a gated index is exactly that shape, and running it through the resolver would throw {@code
     * IndexNotFoundException} once per request for an index that is perfectly reachable -- an exception
     * built and discarded on the request path, for nothing.
     *
     * <p>The requested names are kept alongside the resolved ones rather than replaced by them, because the
     * gated branch in {@link #apply} needs the name as written: a gated index is absent from metadata by
     * definition, so resolution has nothing to say about it.
     */
    private List<String> resolvedNames(ClusterState state, IndicesRequest request, String[] indexNames) {
        boolean anyNeedsResolving = false;
        for (String indexName : indexNames) {
            if (indexName == null) {
                // Same defensiveness as the null-indices() guard in apply(): some requests populate this
                // array loosely, and a filter is the wrong place to discover it.
                continue;
            }
            if (state.metadata().hasIndex(indexName) == false
                && (org.opensearch.common.regex.Regex.isSimpleMatchPattern(indexName)
                    || state.metadata().hasAlias(indexName)
                    || state.metadata().dataStreams().containsKey(indexName))) {
                anyNeedsResolving = true;
                break;
            }
        }
        if (anyNeedsResolving == false) {
            return java.util.Arrays.asList(indexNames);
        }
        java.util.LinkedHashSet<String> names = new java.util.LinkedHashSet<>(java.util.Arrays.asList(indexNames));
        IndexNameExpressionResolver resolver = this.indexNameExpressionResolver;
        if (resolver != null) {
            try {
                names.addAll(java.util.Arrays.asList(resolver.concreteIndexNames(state, request)));
            } catch (Exception e) {
                // Resolution failing is not this filter's problem to report: the action itself is about to
                // resolve the same names and will surface whatever is wrong with them properly. Falling
                // back to the raw names leaves behaviour exactly as it was before resolution existed here.
                logger.debug("could not resolve index expressions for reactivation, using the requested names as-is", e);
            }
        }
        return new ArrayList<>(names);
    }

    /**
     * Wakes a gated index whose shards this node has recorded as asleep.
     *
     * <p><b>The path that did not exist, which made a gated suspension permanent.</b> Both reactivation
     * routes resolved indices through {@code state.metadata().index(name)}: this filter skipped a null, and
     * {@code TransportReactivateShardsAction} returned the state unchanged for one. A gated index has no
     * metadata entry by definition, so neither could ever wake one -- while {@code
     * ShardSuspensionCoordinator} kept recording suspensions that {@code AbsentIndexRoutingSuppliers}
     * removed from every routing resolution on this node. The shard came back only on cache eviction or a
     * restart.
     *
     * <p>Cleared locally and synchronously, before the request proceeds, rather than dispatched and waited
     * on. A gated suspension is not cluster state: it is this node's own placement input, so clearing it
     * here means the very request that woke the index resolves a complete routing table on the next line.
     * That is also why the search path needs no wait for a gated index -- there is no cluster-state update
     * to observe, and nothing to observe it with.
     *
     * <p>The forwarded {@link ReactivateShardsAction} is for the other node's copy of the record. In
     * practice only the elected cluster manager records gated suspensions, so a node that finds one locally
     * and is not the manager forwards, and the manager itself does not need to talk to anyone.
     */
    private static void wakeGatedIndex(ClusterState state, GatedShardSuspensionRegistry gatedSuspensions, Client client, String indexName) {
        IndexMetadata gated = org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.metadataOrDescriptor(
            state.metadata(),
            indexName
        );
        if (gated == null) {
            return;
        }
        String indexUuid = gated.getIndexUUID();
        boolean woke = gatedSuspensions.reactivateAll(indexUuid, false);
        woke |= gatedSuspensions.reactivateAll(indexUuid, true);
        if (woke == false) {
            return;
        }
        logger.info("woke gated index [{}] on this node for an incoming request", indexName);
        if (state.nodes().isLocalNodeElectedClusterManager() == false) {
            triggerReactivation(client, indexName, false);
            triggerReactivation(client, indexName, true);
        }
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

    /**
     * True iff this index has search-only replicas configured but, right now, at least one of its
     * shards has no {@code STARTED} search-only copy -- i.e. a reader copy is mid-reactivation
     * (INITIALIZING/relocating) and cannot yet serve a strict search. This is the routing-table
     * counterpart to the suspended marker: the marker clears the instant {@link
     * org.opensearch.serverless.storage.scaletozero.action.TransportReactivateShardsAction} commits
     * its cluster-state update, but the copy itself only reaches {@code STARTED} later, so a search
     * arriving in that gap must still wait.
     *
     * <p>Kept cheap for the overwhelmingly common case (nothing reactivating): indices with no
     * search-only replicas configured short-circuit before touching the routing table, and for
     * reader-enabled indices the per-shard scan stops at the first {@code STARTED} copy.
     */
    private static boolean readerCopyNotYetStarted(ClusterState state, IndexMetadata indexMetadata) {
        if (indexMetadata.getNumberOfSearchOnlyReplicas() == 0) {
            return false;
        }
        String indexName = indexMetadata.getIndex().getName();
        if (state.routingTable().hasIndex(indexName) == false) {
            // The caller already established the index is in metadata, so no routing entry means
            // cold, not gone. A cold index has no reader copy anywhere, which is the strongest form
            // of "not yet started" -- answering false here would let the search proceed against an
            // index serving nothing. Unreachable today (nothing removes the entry), and the reason
            // this can land before the mechanism that does.
            return true;
        }
        for (org.opensearch.cluster.routing.IndexShardRoutingTable shardRoutingTable : state.routingTable().index(indexName)) {
            List<org.opensearch.cluster.routing.ShardRouting> searchReplicas = shardRoutingTable.searchOnlyReplicas();
            if (searchReplicas.isEmpty() == false
                && searchReplicas.stream().noneMatch(r -> r.state() == org.opensearch.cluster.routing.ShardRoutingState.STARTED)) {
                return true;
            }
        }
        return false;
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
        // Test the current state before waiting for a newer one. The state this filter read in apply() and
        // the state this observer starts from are two separate reads, and waitForNextChange only ever
        // evaluates states *newer* than the observed one. So a reader copy that reached STARTED in the gap
        // between them satisfies the wait against a state the observer will never test, and if the cluster
        // then goes quiet -- which is the steady state of a cluster that scales to zero, not an unusual
        // condition -- the search hung for the full 30-second wait before failing open. Nothing about that
        // looks like a bug from outside: the search succeeds, slowly, every time.
        if (allFullyReactivated(observer.setAndGetObservedState(), pending)) {
            chain.proceed(task, action, request, listener);
            return;
        }
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
                // Absent routing means two different things, and they need opposite answers. The
                // index-not-in-metadata case above is "gone" -- skip it, nothing will ever start.
                // Reaching here the index IS in metadata, so it is cold: no shard copy of either
                // role exists, so reactivation demonstrably has not finished. Continuing would
                // report the wait satisfied for an index with nothing to serve the query.
                return false;
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
