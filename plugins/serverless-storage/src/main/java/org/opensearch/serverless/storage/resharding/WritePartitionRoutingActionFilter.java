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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;

import java.util.List;
import java.util.OptionalInt;

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
 * <p><b>Deliberately does not fence a target index's direct (alias-bypassing) writes yet</b>: doing
 * so safely requires distinguishing this filter's own already-rewritten requests (which legitimately
 * target one specific target index directly, by design) from a client writing to that same target
 * index name directly without going through the alias -- both look identical to this filter once
 * rewriting has already happened earlier in the same filter chain invocation. That distinction is
 * real follow-up work, not implemented here; see the progress-tracking doc.
 */
public final class WritePartitionRoutingActionFilter implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(WritePartitionRoutingActionFilter.class);

    private volatile ClusterService clusterService;

    /** Creates the filter with no dependencies yet -- see {@link #setDependencies}. */
    public WritePartitionRoutingActionFilter() {}

    /**
     * Supplies the dependency this filter needs, once available -- see {@link
     * org.opensearch.serverless.storage.scaletozero.ShardReactivationActionFilter#setDependencies}
     * for why this indirection exists.
     *
     * @param clusterService used to read each candidate target's write-routing assignment.
     */
    public void setDependencies(ClusterService clusterService) {
        this.clusterService = clusterService;
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
        if (currentClusterService != null) {
            if (request instanceof DocWriteRequest<?> docWriteRequest) {
                rewriteIfAssigned(currentClusterService.state(), docWriteRequest);
            } else if (request instanceof BulkRequest bulkRequest) {
                ClusterState state = currentClusterService.state();
                List<DocWriteRequest<?>> requests = bulkRequest.requests();
                for (DocWriteRequest<?> item : requests) {
                    rewriteIfAssigned(state, item);
                }
            }
        }
        chain.proceed(task, action, request, listener);
    }

    private static void rewriteIfAssigned(ClusterState state, DocWriteRequest<?> request) {
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
