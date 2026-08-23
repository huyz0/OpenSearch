/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.search.MultiSearchAction;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.serverless.storage.placement.CoordinatorAffinityRouting;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Plan item B4/B5/B6 (plan-100m-index-implementation.md, Area B): for a request naming several
 * indices that a load balancer cannot hash on its own (wildcards, aliases, {@code _bulk},
 * {@code _msearch}) -- the receiving coordinator resolves what the request actually touches and, if
 * every touched index is gated (Area H) and they all agree on one {@link CoordinatorAffinityRouting}
 * target, forwards the whole request there once. A single-index request that a load balancer
 * already hashed correctly never reaches this filter's forwarding branch at all: it's already local.
 *
 * <h2>Why the whole decision runs on {@link ThreadPool.Names#GENERIC}, not the calling thread</h2>
 *
 * Determining "is this index gated" needs {@link AbsentIndexDescriptorSuppliers}, whose own javadoc
 * is explicit that its real (non-cached) supplier does a remote read and must not run on a thread
 * that can't afford to block. This filter's {@code apply} is reached from an ordinary transport
 * worker thread (the same one that received the client's request) -- not one of the three specific
 * cluster-state-update threads that class refuses to block -- so nothing stops it from reaching the
 * real, blocking supplier if called inline. Even {@link IndexNameExpressionResolver#concreteIndexNames}
 * is not safe to call inline for this reason: it resolves gated names through the same seam
 * (traced directly, not assumed -- see that class's own {@code AbsentIndexDescriptorSuppliers.exists}
 * call sites). So the entire resolve-and-decide step, not just the descriptor check, is dispatched
 * onto {@code GENERIC} -- the same choice {@code MetadataCreateIndexService#createGatedIndex} already
 * made for the same reason, rather than trying to build a "cheap fast path" that turns out to share
 * the same hazard on closer reading.
 *
 * <p><b>The honest cost of that choice.</b> Every request this filter is asked to look at pays one
 * thread-pool hop, even when it never ends up forwarding -- there is no zero-cost path once this
 * filter is enabled. That is a real, measurable latency cost on the hottest code in the product, not
 * a theoretical one, and it is exactly why this defaults off (see
 * {@code ServerlessStoragePlugin#SERVERLESS_STORAGE_AFFINITY_FORWARDING_ENABLED_SETTING}) until it
 * has been measured against a real workload, the same discipline WAL batching's own Phase 3 default
 * is still waiting on.
 *
 * <h2>Loop protection</h2>
 *
 * Reuses {@link WritePartitionRoutingActionFilter}'s own pattern (per B5): an identity-based set of
 * requests this filter has itself forwarded, stashed in {@link ThreadContext} so it survives a
 * request being re-entrant-dispatched back through the same filter chain on the same logical
 * request's context. In addition to that marker, the decision is naturally self-terminating even
 * without it: {@link CoordinatorAffinityRouting#getAffinityNode} is a deterministic function of
 * cluster state, so the node this request lands on after one forward computes the same answer and
 * finds itself already the target -- the marker exists for the residual case B5 calls out (a
 * transient cluster-state disagreement between two nodes' reads), not as the only line of defence.
 */
public final class AffinityForwardingActionFilter implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(AffinityForwardingActionFilter.class);

    private static final String FORWARDED_MARKER_KEY = "serverless_storage_affinity_forwarded";

    /**
     * Counts real forward decisions -- incremented at the one place {@code apply} actually commits
     * to sending a request elsewhere, not merely considering it. Exists so a test can assert
     * forwarding genuinely happened rather than only that a request completed correctly, which it
     * would do either way: this filter is a latency optimisation, not a correctness requirement, so
     * "the results were right" alone never distinguishes "forwarded" from "never tried."
     */
    private static final java.util.concurrent.atomic.AtomicLong FORWARD_COUNT = new java.util.concurrent.atomic.AtomicLong();

    /** @return how many times this JVM's filter has committed to forwarding a request. */
    public static long forwardCountForTesting() {
        return FORWARD_COUNT.get();
    }

    /** Resets the counter between tests sharing one JVM. */
    public static void resetForwardCountForTesting() {
        FORWARD_COUNT.set(0);
    }

    private volatile ClusterService clusterService;
    private volatile ThreadPool threadPool;
    private volatile IndexNameExpressionResolver indexNameExpressionResolver;
    private volatile Supplier<TransportService> transportServiceSupplier;
    private volatile boolean enabled;
    private volatile boolean strict;

    /** Creates the filter with no dependencies yet -- see {@link #setDependencies}. */
    public AffinityForwardingActionFilter() {}

    /**
     * Supplies the dependencies this filter needs, once available. {@code transportServiceSupplier}
     * is a supplier rather than a direct reference for the same reason
     * {@link org.opensearch.serverless.storage.ServerlessStoragePlugin#setTransportService} exists at
     * all: {@code createComponents} has no {@code TransportService} parameter, so this plugin only
     * captures one later, from a registered transport action's own Guice-injected constructor. It is
     * guaranteed to be set before any real request can reach this filter (every registered transport
     * action is bound as an eager Guice singleton during node startup), but this filter reads it
     * lazily rather than assume an ordering it doesn't itself control.
     *
     * <p>{@code enabledSetting}/{@code strictSetting} are passed in, rather than declared as
     * constants on this class, because {@code ServerlessStoragePluginTests} enforces that every
     * setting registered in {@code ServerlessStoragePlugin#getSettings()} is also declared as a
     * field directly on that class -- the canonical {@code Setting<Boolean>} objects live there
     * ({@code SERVERLESS_STORAGE_AFFINITY_FORWARDING_ENABLED_SETTING}/{@code
     * SERVERLESS_STORAGE_AFFINITY_FORWARDING_STRICT_SETTING}); this class only needs them long
     * enough to register its own update consumers.
     */
    public void setDependencies(
        ClusterService clusterService,
        ThreadPool threadPool,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<TransportService> transportServiceSupplier,
        ClusterSettings clusterSettings,
        Setting<Boolean> enabledSetting,
        Setting<Boolean> strictSetting,
        boolean enabledDefault,
        boolean strictDefault
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.transportServiceSupplier = transportServiceSupplier;
        this.enabled = enabledDefault;
        this.strict = strictDefault;
        clusterSettings.addSettingsUpdateConsumer(enabledSetting, v -> this.enabled = v);
        clusterSettings.addSettingsUpdateConsumer(strictSetting, v -> this.strict = v);
    }

    @Override
    public int order() {
        // After ShardReactivationActionFilter (Integer.MIN_VALUE) and WritePartitionRoutingActionFilter
        // (Integer.MIN_VALUE + 1): by the time this runs, any write-routing alias has already been
        // rewritten to its real target index, so this filter sees the real name(s), not an alias that
        // has no gated status of its own.
        return Integer.MIN_VALUE + 2;
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
        if (enabled == false) {
            chain.proceed(task, action, request, listener);
            return;
        }
        ClusterService currentClusterService = this.clusterService;
        ThreadPool currentThreadPool = this.threadPool;
        IndexNameExpressionResolver currentResolver = this.indexNameExpressionResolver;
        Supplier<TransportService> currentTransportServiceSupplier = this.transportServiceSupplier;
        TransportService currentTransportService = currentTransportServiceSupplier == null ? null : currentTransportServiceSupplier.get();
        @SuppressWarnings("unchecked")
        TransportResponseReader<Response> reader = (TransportResponseReader<Response>) readerForAction(action);

        if (currentClusterService == null
            || currentThreadPool == null
            || currentResolver == null
            || currentTransportService == null
            || reader == null) {
            chain.proceed(task, action, request, listener);
            return;
        }

        Set<Object> forwardedMarker = forwardedMarkerSet(currentThreadPool.getThreadContext());
        if (forwardedMarker.contains(request)) {
            chain.proceed(task, action, request, listener);
            return;
        }

        Set<String> requestedNames = rawRequestedNames(action, request);
        if (requestedNames == null || requestedNames.isEmpty()) {
            // Empty means either "not a request shape this filter handles" or "no index named at
            // all" (e.g. a search across every index) -- neither is a case where a single affine
            // target could exist, so there is nothing worth a GENERIC dispatch to go find out.
            chain.proceed(task, action, request, listener);
            return;
        }

        currentThreadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            ClusterState state = currentClusterService.state();
            DiscoveryNode affinityNode;
            try {
                affinityNode = resolveSoleAffinityTarget(action, request, requestedNames, state, currentResolver);
            } catch (Exception e) {
                logger.debug("affinity resolution failed for [{}], proceeding locally: {}", action, e.toString());
                chain.proceed(task, action, request, listener);
                return;
            }
            if (affinityNode == null || affinityNode.getId().equals(state.nodes().getLocalNodeId())) {
                chain.proceed(task, action, request, listener);
                return;
            }

            forwardedMarker.add(request);
            FORWARD_COUNT.incrementAndGet();
            boolean currentStrict = strict;
            currentTransportService.sendRequest(affinityNode, action, request, new TransportResponseHandler<TransportResponse>() {
                @Override
                public TransportResponse read(StreamInput in) throws IOException {
                    return reader.read(in);
                }

                @Override
                @SuppressWarnings("unchecked")
                public void handleResponse(TransportResponse response) {
                    listener.onResponse((Response) response);
                }

                @Override
                public void handleException(TransportException exp) {
                    if (currentStrict) {
                        listener.onFailure(exp);
                    } else {
                        logger.debug(
                            "forward to affinity node [{}] failed for [{}], falling back to local execution: {}",
                            affinityNode.getId(),
                            action,
                            exp.toString()
                        );
                        chain.proceed(task, action, request, listener);
                    }
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        });
    }

    /** A {@link org.opensearch.core.common.io.stream.Writeable.Reader}, named so the cast above reads clearly. */
    @FunctionalInterface
    private interface TransportResponseReader<T extends TransportResponse>
        extends
            org.opensearch.core.common.io.stream.Writeable.Reader<T> {}

    private static TransportResponseReader<? extends TransportResponse> readerForAction(String action) {
        if (SearchAction.NAME.equals(action)) {
            return SearchResponse::new;
        }
        if (BulkAction.NAME.equals(action)) {
            return BulkResponse::new;
        }
        if (MultiSearchAction.NAME.equals(action)) {
            return MultiSearchResponse::new;
        }
        return null;
    }

    /**
     * The raw, unresolved names a request mentions -- no wildcard expansion, no descriptor lookups,
     * safe to call on any thread. Only used to decide whether a GENERIC dispatch is worth doing at
     * all (an empty result never is); the real resolution happens in {@link #resolveSoleAffinityTarget}.
     */
    // Package-private rather than private so AffinityForwardingActionFilterTests can drive it directly.
    static Set<String> rawRequestedNames(String action, ActionRequest request) {
        if (SearchAction.NAME.equals(action) && request instanceof SearchRequest searchRequest) {
            String[] indices = searchRequest.indices();
            return indices == null ? Set.of() : Set.of(indices);
        }
        if (BulkAction.NAME.equals(action) && request instanceof BulkRequest bulkRequest) {
            Set<String> names = new HashSet<>();
            for (DocWriteRequest<?> item : bulkRequest.requests()) {
                if (item.index() != null) {
                    names.add(item.index());
                }
            }
            return names;
        }
        if (MultiSearchAction.NAME.equals(action) && request instanceof MultiSearchRequest multiSearchRequest) {
            Set<String> names = new HashSet<>();
            for (SearchRequest sub : multiSearchRequest.requests()) {
                String[] indices = sub.indices();
                if (indices == null || indices.length == 0) {
                    // One sub-request with no index named means "search everything" -- too broad for
                    // a single affine target, and forwarding the rest of the batch without it would
                    // change what the request means. Bail out of the whole batch, not just this item.
                    return Set.of();
                }
                Collections.addAll(names, indices);
            }
            return names;
        }
        return Set.of();
    }

    /**
     * The real resolution: expands wildcards/aliases (for {@code SearchRequest}/{@code MultiSearchRequest};
     * bulk items are taken literally, matching {@link WritePartitionRoutingActionFilter}'s own
     * choice not to resolve aliases on the write path) and checks each concrete name against both
     * {@link AbsentIndexDescriptorSuppliers} (is it gated at all) and {@link CoordinatorAffinityRouting}
     * (which node owns it). Returns the single node every touched index agrees on, or {@code null} if
     * there isn't one -- including because at least one touched index isn't gated at all, since an
     * ordinary index has no descriptor-cache-locality problem for this filter to solve.
     *
     * <p>Must only be called off a thread {@link AbsentIndexDescriptorSuppliers} refuses to block --
     * see this class's own javadoc for why {@code apply} always dispatches before calling this.
     */
    private static DiscoveryNode resolveSoleAffinityTarget(
        String action,
        ActionRequest request,
        Set<String> requestedNames,
        ClusterState state,
        IndexNameExpressionResolver resolver
    ) {
        Set<String> concreteNames = new HashSet<>();
        if (SearchAction.NAME.equals(action) && request instanceof SearchRequest searchRequest) {
            Collections.addAll(concreteNames, resolver.concreteIndexNames(state, searchRequest));
        } else if (BulkAction.NAME.equals(action)) {
            concreteNames.addAll(requestedNames);
        } else if (MultiSearchAction.NAME.equals(action) && request instanceof MultiSearchRequest multiSearchRequest) {
            for (SearchRequest sub : multiSearchRequest.requests()) {
                Collections.addAll(concreteNames, resolver.concreteIndexNames(state, sub));
            }
        } else {
            return null;
        }
        if (concreteNames.isEmpty()) {
            return null;
        }

        DiscoveryNode affinityNode = null;
        for (String name : concreteNames) {
            IndexMetadata published = state.metadata().index(name);
            boolean gated = published == null && AbsentIndexDescriptorSuppliers.metadataOrDescriptor(state.metadata(), name) != null;
            if (gated == false) {
                return null;
            }
            DiscoveryNode candidate = CoordinatorAffinityRouting.getAffinityNode(name, state.nodes());
            if (candidate == null) {
                return null;
            }
            if (affinityNode == null) {
                affinityNode = candidate;
            } else if (affinityNode.getId().equals(candidate.getId()) == false) {
                return null;
            }
        }
        return affinityNode;
    }

    @SuppressWarnings("unchecked")
    private static Set<Object> forwardedMarkerSet(ThreadContext threadContext) {
        Object existing = threadContext.getTransient(FORWARDED_MARKER_KEY);
        if (existing instanceof Set) {
            return (Set<Object>) existing;
        }
        Set<Object> created = Collections.newSetFromMap(new IdentityHashMap<>());
        threadContext.putTransient(FORWARDED_MARKER_KEY, created);
        return created;
    }
}
