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
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexAction;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;

/**
 * The write-side counterpart to {@link RoutingPartitionFilter}: rewrites an {@link
 * IndexAction}-family request naming a write-routing-enabled alias (see {@link
 * org.opensearch.serverless.storage.resharding.action.EnableWritePartitionRoutingAction}) to the one
 * real target partition its document id actually belongs to, before core ever sees the alias name --
 * matching {@link org.opensearch.serverless.storage.scaletozero.ShardReactivationActionFilter}'s own
 * "intercept and rewrite before {@code chain.proceed}" shape.
 *
 * <p><b>Why rewriting here, not adding a new core routing concept</b>: core already refuses a write
 * against an alias spanning more than one index unless exactly one is marked {@code is_write_index}
 * (see {@code IndexNameExpressionResolver}), so a write-routing alias as this plugin builds it
 * (deliberately no {@code is_write_index}, since every target is a legitimate write destination for
 * its own partition) would otherwise be rejected outright before reaching any shard. Rewriting the
 * request's target to the concrete partition index -- entirely locally, from cluster state already
 * held by the coordinating node -- means core never needs to know a write-routing alias is anything
 * but an ordinary single-index write once this filter has run.
 *
 * <p><b>Deliberately scoped to explicit-id requests only, for this first increment</b>: computing the
 * owning partition requires the document id itself ({@link
 * org.opensearch.serverless.storage.resharding.RoutingPartitionFilter}'s hash is a pure function of
 * {@code _id}), but core generates an auto-id for a create-without-id {@code IndexRequest} deeper in
 * the indexing path, after this filter has already run. Handling that case correctly would mean
 * either generating the id here (duplicating/racing core's own generation) or deferring the routing
 * decision past this filter's natural place -- real additional design work, deliberately left as
 * follow-up rather than rushed into this pass (see the progress-tracking doc this effort is recorded
 * under). A request with no explicit id against a write-routing alias is passed through unmodified,
 * which core will then reject with its own "more than one index" error -- a safe, if unhelpful,
 * failure mode: it never silently misroutes a document.
 *
 * <p><b>Fences a target index's direct (alias-bypassing) writes.</b> The distinction this needs --
 * "did the client name the alias (legitimate, gets rewritten below) or the target index directly
 * (illegitimate once that target is write-routing-assigned)" -- is available for free: {@link
 * DocWriteRequest#index()} still holds the client's own original value the first time this filter
 * reads it in one {@link #apply} invocation, before any rewriting happens later in that same call.
 * A request naming an assigned target index directly is rejected outright rather than silently
 * allowed to write into just one partition's worth of documents behind the alias's back. For a
 * {@link BulkRequest}, any single offending item fails the whole request rather than only that item
 * -- blunter than per-item rejection, but simple and safe: a bulk request half fenced and half not
 * would be a worse failure mode than an oversized, uniform rejection.
 *
 * <p><b>Also fences a split source's direct writes, once marked (see {@link
 * org.opensearch.serverless.storage.resharding.SourceSplitFenceMetadata}).</b> Same shape as target
 * fencing above -- a write naming a fenced source index directly is rejected, closing the
 * previously-open, permanently-silent "keep writing to the source forever after cutover" gap {@code
 * TransportOrchestrateShardSplitAction}'s own javadoc used to warn about as entirely unenforced.
 *
 * <p><b>A real re-entrancy bug this fencing check's own integration test caught</b>: a single-item
 * {@code client().prepareIndex(alias)} call does not run through this filter chain once -- core's
 * {@code TransportSingleItemBulkWriteAction} wraps it into a {@code BulkRequest} and dispatches
 * that through the very same filter chain a second time, on the same thread, synchronously. By the
 * second pass, this filter's own first-pass rewrite has already replaced the request's alias name
 * with the real target index name -- indistinguishable, by {@link DocWriteRequest#index()} alone,
 * from a client writing to that target directly. Fixed with a {@link ThreadContext} transient
 * marker: an identity-based {@link Set} of requests this filter has itself rewritten, stashed once
 * per thread-context and consulted (not re-populated) on re-entry, the same "transient, not
 * wire-serialized, survives nested synchronous calls on one thread" property {@code ThreadContext}
 * transients are already used for elsewhere in core.
 */
public final class WritePartitionRoutingActionFilter implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(WritePartitionRoutingActionFilter.class);

    private static final String REWRITTEN_MARKER_KEY = "serverless_storage_write_partition_routing_rewritten";

    private volatile ClusterService clusterService;
    private volatile ThreadPool threadPool;

    /** Creates the filter with no dependencies yet -- see {@link #setDependencies}. */
    public WritePartitionRoutingActionFilter() {}

    /**
     * Supplies the dependencies this filter needs, once available -- see {@link
     * org.opensearch.serverless.storage.scaletozero.ShardReactivationActionFilter#setDependencies}
     * for why this indirection exists.
     *
     * @param clusterService used to read each candidate target's write-routing assignment.
     * @param threadPool used to stash the already-rewritten marker across this filter's own re-entrant
     *                   invocations within one request (see this class's own javadoc).
     */
    public void setDependencies(ClusterService clusterService, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE + 1; // run early, just after ShardReactivationActionFilter's own Integer.MIN_VALUE.
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
        ThreadPool currentThreadPool = this.threadPool;
        if (currentClusterService != null && currentThreadPool != null) {
            ClusterState state = currentClusterService.state();
            Set<DocWriteRequest<?>> alreadyRewritten = rewrittenMarkerSet(currentThreadPool.getThreadContext());
            if (request instanceof DocWriteRequest<?> docWriteRequest) {
                String rejection = rejectIfDirectTargetWrite(state, docWriteRequest, alreadyRewritten);
                if (rejection != null) {
                    listener.onFailure(new IllegalArgumentException(rejection));
                    return;
                }
                rewriteIfAssigned(state, docWriteRequest, alreadyRewritten);
            } else if (request instanceof BulkRequest bulkRequest) {
                for (DocWriteRequest<?> item : bulkRequest.requests()) {
                    String rejection = rejectIfDirectTargetWrite(state, item, alreadyRewritten);
                    if (rejection != null) {
                        listener.onFailure(new IllegalArgumentException(rejection));
                        return;
                    }
                }
                for (DocWriteRequest<?> item : bulkRequest.requests()) {
                    rewriteIfAssigned(state, item, alreadyRewritten);
                }
            }
        }
        chain.proceed(task, action, request, listener);
    }

    /**
     * The identity-based set of requests this filter has itself rewritten during the current
     * request's lifetime, stashed once per {@link ThreadContext} and reused (never re-created) on
     * this filter's own re-entrant invocations -- see this class's own javadoc.
     */
    @SuppressWarnings("unchecked")
    private static Set<DocWriteRequest<?>> rewrittenMarkerSet(ThreadContext threadContext) {
        Object existing = threadContext.getTransient(REWRITTEN_MARKER_KEY);
        if (existing instanceof Set) {
            return (Set<DocWriteRequest<?>>) existing;
        }
        Set<DocWriteRequest<?>> created = Collections.newSetFromMap(new IdentityHashMap<>());
        threadContext.putTransient(REWRITTEN_MARKER_KEY, created);
        return created;
    }

    /**
     * @return a rejection message if {@code request} names a write-routing-assigned target index
     *         directly (bypassing its alias) or a fenced split source, or {@code null} if the
     *         request is fine to proceed -- including because it's this filter's own
     *         already-rewritten request re-entering on a nested dispatch (see this class's own
     *         javadoc).
     */
    private static String rejectIfDirectTargetWrite(
        ClusterState state,
        DocWriteRequest<?> request,
        Set<DocWriteRequest<?>> alreadyRewritten
    ) {
        if (alreadyRewritten.contains(request)) {
            return null;
        }
        String indexName = request.index();
        if (indexName == null) {
            return null;
        }
        IndexMetadata directMetadata = state.metadata().index(indexName);
        if (directMetadata == null) {
            return null;
        }
        if (SourceSplitFenceMetadata.isFencedSource(directMetadata)) {
            return "index ["
                + indexName
                + "] was split and is now fenced -- writes must go through alias ["
                + SourceSplitFenceMetadata.supersedingAlias(directMetadata)
                + "], not this index directly";
        }
        String assignedAlias = WritePartitionRoutingMetadata.writeRoutingAlias(directMetadata);
        if (assignedAlias == null) {
            return null;
        }
        return "index ["
            + indexName
            + "] is a write-routing partition target of alias ["
            + assignedAlias
            + "] -- writes must go through the alias, not the target index directly";
    }

    private static void rewriteIfAssigned(ClusterState state, DocWriteRequest<?> request, Set<DocWriteRequest<?>> alreadyRewritten) {
        String aliasName = request.index();
        String id = request.id();
        if (aliasName == null || id == null) {
            return;
        }
        Metadata metadata = state.metadata();
        IndexAbstraction abstraction = metadata.getIndicesLookup().get(aliasName);
        if (abstraction instanceof IndexAbstraction.Alias == false) {
            return;
        }
        List<String> targetIndexNames = abstraction.getIndices().stream().map(index -> index.getIndex().getName()).toList();
        if (targetIndexNames.isEmpty()) {
            return;
        }
        String resolvedTargetIndexName = resolvePartitionTarget(metadata, aliasName, targetIndexNames, id);
        if (resolvedTargetIndexName != null) {
            request.index(resolvedTargetIndexName);
            alreadyRewritten.add(request);
        }
    }

    private static String resolvePartitionTarget(Metadata metadata, String aliasName, List<String> targetIndexNames, String id) {
        Integer numPartitions = null;
        String[] byPartitionIndex = null;
        for (String targetIndexName : targetIndexNames) {
            IndexMetadata targetMetadata = metadata.index(targetIndexName);
            if (targetMetadata == null || aliasName.equals(WritePartitionRoutingMetadata.writeRoutingAlias(targetMetadata)) == false) {
                continue;
            }
            OptionalInt partitionIndex = WritePartitionRoutingMetadata.partitionIndex(targetMetadata);
            OptionalInt targetNumPartitions = WritePartitionRoutingMetadata.numPartitions(targetMetadata);
            if (partitionIndex.isEmpty() || targetNumPartitions.isEmpty()) {
                continue;
            }
            if (numPartitions == null) {
                numPartitions = targetNumPartitions.getAsInt();
                byPartitionIndex = new String[numPartitions];
            }
            if (partitionIndex.getAsInt() < byPartitionIndex.length) {
                byPartitionIndex[partitionIndex.getAsInt()] = targetIndexName;
            }
        }
        if (numPartitions == null) {
            return null;
        }
        int hash = Murmur3HashFunction.hash(id);
        int partition = Math.floorMod(hash, numPartitions);
        String resolved = byPartitionIndex[partition];
        if (resolved == null) {
            logger.warn("write-routing alias [" + aliasName + "] has no target assigned for partition [" + partition + "]");
        }
        return resolved;
    }
}
