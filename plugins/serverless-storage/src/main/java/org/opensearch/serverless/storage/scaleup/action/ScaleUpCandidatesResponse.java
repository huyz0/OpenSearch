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
        for (NodeScaleUpCandidatesResponse node : nodes) {
            for (ShardQueryRateEntry entry : node.queryRates()) {
                String key = key(entry.indexUuid(), entry.shardId());
                highestQpmByShard.merge(key, entry.queriesPerMinute(), Math::max);
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
            boolean candidate = queriesPerMinute > qpmThreshold && currentSearchReplicaCount < maxSearchReplicas;
            result.add(new ScaleUpCandidateEntry(indexUuid, shardId, indexName, queriesPerMinute, currentSearchReplicaCount, candidate));
        }
        return result;
    }

    private static String key(String indexUuid, int shardId) {
        return indexUuid + "/" + shardId;
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
        builder.startObject().startArray("shards");
        for (ScaleUpCandidateEntry entry : candidates) {
            entry.toXContent(builder, params);
        }
        return builder.endArray().endObject();
    }
}
