/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaleup.action;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The result of a {@link ScaleUpCandidatesAction} evaluation: every reader shard observed anywhere
 * in the cluster, merged across nodes by {@code (indexUuid, shardId)} and joined with this
 * plugin's query-rate/replica-cap thresholds into a single {@link ScaleUpCandidateEntry} list --
 * mirrors {@code org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidatesResponse}'s
 * "merge happens in the response type" shape.
 *
 * <p>Unlike that scale-to-zero counterpart, this merge also needs {@code index.number_of_search_replicas}
 * and each index's current name -- both index <em>metadata</em>, not a per-node signal, so they
 * come from a {@link Metadata} snapshot the coordinating node passes in (see
 * {@link TransportScaleUpCandidatesAction}, which reads it off {@code ClusterService#state()}),
 * rather than from any {@link NodeScaleUpCandidatesResponse}.
 */
public class ScaleUpCandidatesResponse extends BaseNodesResponse<NodeScaleUpCandidatesResponse> implements ToXContentObject {

    private final List<ScaleUpCandidateEntry> candidates;

    /**
     * Deserializes a response. The merged {@link #candidates()} list travels over the wire exactly
     * as the coordinating node computed it, same as {@code ScaleToZeroCandidatesResponse}.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ScaleUpCandidatesResponse}.
     */
    public ScaleUpCandidatesResponse(StreamInput in) throws IOException {
        super(in);
        this.candidates = in.readList(ScaleUpCandidateEntry::new);
    }

    /**
     * Creates a response, computing {@link #candidates()} by merging every node's raw
     * queries-per-minute signals with {@code metadata} and applying the given thresholds.
     *
     * @param clusterName this cluster's name, required by {@link BaseNodesResponse}.
     * @param nodes every node's raw {@link NodeScaleUpCandidatesResponse}.
     * @param failures any per-node failures encountered while fanning out.
     * @param qpmThreshold the queries-per-minute a reader copy must exceed to count as a candidate.
     * @param maxSearchReplicas the maximum {@code index.number_of_search_replicas} an index may
     *                          still be expanded past.
     * @param metadata the cluster state's index metadata, used to resolve each shard's index name
     *                 and current search-replica count.
     */
    public ScaleUpCandidatesResponse(
        ClusterName clusterName,
        List<NodeScaleUpCandidatesResponse> nodes,
        List<FailedNodeException> failures,
        long qpmThreshold,
        int maxSearchReplicas,
        Metadata metadata
    ) {
        super(clusterName, nodes, failures);
        this.candidates = merge(nodes, qpmThreshold, maxSearchReplicas, metadata);
    }

    private static List<ScaleUpCandidateEntry> merge(
        List<NodeScaleUpCandidatesResponse> nodes,
        long qpmThreshold,
        int maxSearchReplicas,
        Metadata metadata
    ) {
        Map<String, Long> highestQpmByShard = new LinkedHashMap<>();
        // Idleness folds with min for the same reason it does in the scale-to-zero merge: a shard is
        // being queried if ANY copy of it is being queried, so the most-recently-queried report is
        // the authoritative one.
        Map<String, Long> lowestIdleByShard = new LinkedHashMap<>();
        for (NodeScaleUpCandidatesResponse node : nodes) {
            for (ShardQueryRateEntry entry : node.queryRates()) {
                String key = key(entry.indexUuid(), entry.shardId());
                highestQpmByShard.merge(key, entry.queriesPerMinute(), Math::max);
                if (entry.millisSinceLastQuery() != ShardQueryRateEntry.UNKNOWN_IDLE) {
                    lowestIdleByShard.merge(key, entry.millisSinceLastQuery(), Math::min);
                }
            }
        }

        Map<String, IndexMetadata> byUuid = new LinkedHashMap<>();
        for (IndexMetadata indexMetadata : metadata.indices().values()) {
            byUuid.put(indexMetadata.getIndexUUID(), indexMetadata);
        }

        List<ScaleUpCandidateEntry> result = new ArrayList<>(highestQpmByShard.size());
        for (Map.Entry<String, Long> entry : highestQpmByShard.entrySet()) {
            int separator = entry.getKey().lastIndexOf('/');
            String indexUuid = entry.getKey().substring(0, separator);
            int shardId = Integer.parseInt(entry.getKey().substring(separator + 1));
            long queriesPerMinute = entry.getValue();

            IndexMetadata indexMetadata = byUuid.get(indexUuid);
            if (indexMetadata == null) {
                // Index has since been deleted, or this node's snapshot raced ahead of this
                // coordinator's own cluster state -- nothing sensible to evaluate, skip it, same
                // "stale entry, safely dropped" tolerance any other cluster-wide merge here has.
                continue;
            }
            String indexName = indexMetadata.getIndex().getName();
            int currentSearchReplicaCount = indexMetadata.getNumberOfSearchOnlyReplicas();
            // Findings S-1 and S-3. The predicate used to be `rate > threshold && replicas < max`
            // and nothing else, which meant a shard whose traffic had stopped an hour ago was still
            // a candidate on every tick -- because queriesPerMinute() freezes at the last busy
            // window's value when there is no next query to roll the window over. The result was a
            // ratchet to max_search_replicas *after* the load ended, with no path back down, and a
            // shard that scale-to-zero was concurrently suspending for idleness.
            //
            // Requiring the shard to have been queried within the rate window makes the two loops
            // agree: past the window, the rate is stale by construction and is not evidence of
            // anything. UNKNOWN_IDLE (no node reported idleness) keeps the old rate-only behaviour,
            // so a node that cannot supply the signal degrades to the previous semantics rather than
            // silently disabling scale-up.
            Long idleMillis = lowestIdleByShard.get(entry.getKey());
            boolean recentlyQueried = idleMillis == null || idleMillis < QUERY_RATE_WINDOW_MILLIS;
            boolean candidate = queriesPerMinute > qpmThreshold && recentlyQueried && currentSearchReplicaCount < maxSearchReplicas;
            result.add(new ScaleUpCandidateEntry(indexUuid, shardId, indexName, queriesPerMinute, currentSearchReplicaCount, candidate));
        }
        return result;
    }

    /**
     * The width of {@code ObjectStoreReaderEngine}'s query-rate window. Duplicated here as a constant
     * rather than read from the engine because this merge runs on the cluster-manager, which has no
     * engine to ask; it must stay in step with {@code ObjectStoreReaderEngine.QUERY_RATE_WINDOW_MILLIS}.
     */
    static final long QUERY_RATE_WINDOW_MILLIS = 60_000L;

    private static String key(String indexUuid, int shardId) {
        return indexUuid + "/" + shardId;
    }

    /**
     * Whether any node failed to answer this fan-out.
     *
     * <p><b>Finding S-5.</b> {@code merge} never read the {@code failures} list, so if the node
     * hosting the busiest reader copy failed to answer, the merged view simply reported whatever the
     * survivors said -- and the REST endpoint showed that partial picture with no indication it was
     * partial. Safe in the expansion direction (a missing node can only lower the observed rate), but
     * an operator, and the expansion coordinator, both need to be able to tell "quiet" from "we did
     * not hear from the node that was busy".
     */
    public boolean hasNodeFailures() {
        return failures().isEmpty() == false;
    }

    /** Every reader shard observed anywhere in the cluster, merged and evaluated against this plugin's thresholds. */
    public List<ScaleUpCandidateEntry> candidates() {
        return candidates;
    }

    @Override
    protected List<NodeScaleUpCandidatesResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeScaleUpCandidatesResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeScaleUpCandidatesResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    /** @param out stream to write this response's fields to, including the merged {@link #candidates()}. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(candidates);
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("partial", hasNodeFailures());
        builder.startArray("failed_nodes");
        for (FailedNodeException failure : failures()) {
            builder.startObject().field("node_id", failure.nodeId()).field("reason", failure.getMessage()).endObject();
        }
        builder.endArray();
        builder.startArray("shards");
        for (ScaleUpCandidateEntry entry : candidates) {
            entry.toXContent(builder, params);
        }
        return builder.endArray().endObject();
    }
}
