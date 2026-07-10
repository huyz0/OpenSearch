/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.serverless.storage.readerengine.action.ShardLagEntry;
import org.opensearch.serverless.storage.writerengine.action.IdleShardEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The result of a {@link ScaleToZeroCandidatesAction} evaluation: every shard observed anywhere in
 * the cluster, merged across nodes and joined with this plugin's idle/lag thresholds into a single
 * {@link ScaleToZeroCandidateEntry} list -- the actual "policy" computation lives in this
 * response's constructor rather than in {@link TransportScaleToZeroCandidatesAction} itself, the
 * same "merge happens in the response type" shape {@code BaseNodesResponse} subclasses in core
 * (e.g. cluster health) already use.
 *
 * <p>The merge join key is {@code (indexUuid, shardId)}: a shard that appears as a writer copy on
 * one node's {@link NodeScaleToZeroCandidatesResponse#idleShards()} and a reader copy on another
 * node's {@link NodeScaleToZeroCandidatesResponse#laggingShards()} (the normal case -- writer and
 * reader shard copies are deliberately never co-located, rfc-serverless-opensearch.md &sect;7)
 * still folds into one {@link ScaleToZeroCandidateEntry}.
 */
public class ScaleToZeroCandidatesResponse extends BaseNodesResponse<NodeScaleToZeroCandidatesResponse> implements ToXContentObject {

    private final List<ScaleToZeroCandidateEntry> candidates;

    /**
     * Deserializes a response. The merged {@link #candidates()} list is not itself re-derived on
     * the receiving end -- it travels over the wire exactly as the coordinating node computed it,
     * same as any other computed response field.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ScaleToZeroCandidatesResponse}.
     */
    public ScaleToZeroCandidatesResponse(StreamInput in) throws IOException {
        super(in);
        this.candidates = in.readList(ScaleToZeroCandidateEntry::new);
    }

    /**
     * Creates a response, computing {@link #candidates()} by merging every node's raw signals and
     * applying the given thresholds.
     *
     * @param clusterName this cluster's name, required by {@link BaseNodesResponse}.
     * @param nodes every node's raw {@link NodeScaleToZeroCandidatesResponse}.
     * @param failures any per-node failures encountered while fanning out.
     * @param idleThresholdMillis how idle (in millis) a writer copy must be to count toward a candidate.
     * @param lagThreshold the largest manifest-generation lag a reader copy may have and still count as "caught up."
     */
    public ScaleToZeroCandidatesResponse(
        ClusterName clusterName,
        List<NodeScaleToZeroCandidatesResponse> nodes,
        List<FailedNodeException> failures,
        long idleThresholdMillis,
        long lagThreshold
    ) {
        super(clusterName, nodes, failures);
        this.candidates = merge(nodes, idleThresholdMillis, lagThreshold);
    }

    private static List<ScaleToZeroCandidateEntry> merge(
        List<NodeScaleToZeroCandidatesResponse> nodes,
        long idleThresholdMillis,
        long lagThreshold
    ) {
        Map<String, Long> worstIdleByShard = new LinkedHashMap<>();
        Map<String, Long> worstLagByShard = new LinkedHashMap<>();
        for (NodeScaleToZeroCandidatesResponse node : nodes) {
            for (IdleShardEntry entry : node.idleShards()) {
                String key = key(entry.indexUuid(), entry.shardId());
                worstIdleByShard.merge(key, entry.millisSinceLastActivity(), Math::max);
            }
            for (ShardLagEntry entry : node.laggingShards()) {
                String key = key(entry.indexUuid(), entry.shardId());
                worstLagByShard.merge(key, entry.manifestGenerationLag(), Math::max);
            }
        }

        Map<String, ScaleToZeroCandidateEntry> byShard = new LinkedHashMap<>();
        for (Map.Entry<String, Long> idle : worstIdleByShard.entrySet()) {
            byShard.put(idle.getKey(), buildEntry(idle.getKey(), idle.getValue(), worstLagByShard.get(idle.getKey())));
        }
        for (Map.Entry<String, Long> lag : worstLagByShard.entrySet()) {
            byShard.putIfAbsent(lag.getKey(), buildEntry(lag.getKey(), null, lag.getValue()));
        }

        List<ScaleToZeroCandidateEntry> result = new ArrayList<>(byShard.size());
        for (Map.Entry<String, ScaleToZeroCandidateEntry> entry : byShard.entrySet()) {
            ScaleToZeroCandidateEntry raw = entry.getValue();
            boolean idleEnough = raw.millisSinceLastActivity() != ScaleToZeroCandidateEntry.UNKNOWN
                && raw.millisSinceLastActivity() >= idleThresholdMillis;
            boolean readersCaughtUp = raw.manifestGenerationLag() == ScaleToZeroCandidateEntry.UNKNOWN
                || raw.manifestGenerationLag() <= lagThreshold;
            boolean candidate = idleEnough && readersCaughtUp;
            result.add(
                new ScaleToZeroCandidateEntry(
                    raw.indexUuid(),
                    raw.shardId(),
                    raw.millisSinceLastActivity(),
                    raw.manifestGenerationLag(),
                    candidate
                )
            );
        }
        return result;
    }

    private static ScaleToZeroCandidateEntry buildEntry(String key, Long millisSinceLastActivity, Long manifestGenerationLag) {
        int separator = key.lastIndexOf('/');
        String indexUuid = key.substring(0, separator);
        int shardId = Integer.parseInt(key.substring(separator + 1));
        return new ScaleToZeroCandidateEntry(
            indexUuid,
            shardId,
            millisSinceLastActivity == null ? ScaleToZeroCandidateEntry.UNKNOWN : millisSinceLastActivity,
            manifestGenerationLag == null ? ScaleToZeroCandidateEntry.UNKNOWN : manifestGenerationLag,
            false
        );
    }

    private static String key(String indexUuid, int shardId) {
        return indexUuid + "/" + shardId;
    }

    /** Every shard observed anywhere in the cluster, merged and evaluated against this plugin's thresholds. */
    public List<ScaleToZeroCandidateEntry> candidates() {
        return candidates;
    }

    @Override
    protected List<NodeScaleToZeroCandidatesResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeScaleToZeroCandidatesResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeScaleToZeroCandidatesResponse> nodes) throws IOException {
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
        for (ScaleToZeroCandidateEntry entry : candidates) {
            entry.toXContent(builder, params);
        }
        return builder.endArray().endObject();
    }
}
