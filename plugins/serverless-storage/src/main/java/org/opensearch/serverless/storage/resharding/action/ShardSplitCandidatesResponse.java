/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.ShardRange;
import org.opensearch.cluster.metadata.SplitShardsMetadata;
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
 * The result of a {@link ShardSplitCandidatesAction} evaluation: every writer shard observed
 * anywhere in the cluster, merged across nodes by {@code (indexUuid, shardId)} and joined with
 * this plugin's write-rate threshold into a single {@link ShardSplitCandidateEntry} list -- mirrors
 * {@code org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidatesResponse}'s "merge
 * happens in the response type" shape.
 *
 * <p>Resolves each shard's index name from a {@link Metadata} snapshot the coordinating node
 * passes in (see {@link TransportShardSplitCandidatesAction}, which reads it off {@code
 * ClusterService#state()}), the same reason {@code ScaleUpCandidatesResponse} needs it: the index
 * name is metadata, not a per-node signal.
 */
public class ShardSplitCandidatesResponse extends BaseNodesResponse<NodeShardSplitCandidatesResponse> implements ToXContentObject {

    private final List<ShardSplitCandidateEntry> candidates;

    /**
     * Deserializes a response. The merged {@link #candidates()} list travels over the wire exactly
     * as the coordinating node computed it, same as {@code ScaleUpCandidatesResponse}.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardSplitCandidatesResponse}.
     */
    public ShardSplitCandidatesResponse(StreamInput in) throws IOException {
        super(in);
        this.candidates = in.readList(ShardSplitCandidateEntry::new);
    }

    /**
     * Creates a response, computing {@link #candidates()} by merging every node's raw
     * split-candidate signals with {@code metadata} and applying the given thresholds.
     *
     * @param clusterName this cluster's name, required by {@link BaseNodesResponse}.
     * @param nodes every node's raw {@link NodeShardSplitCandidatesResponse}.
     * @param failures any per-node failures encountered while fanning out.
     * @param writesPerMinuteThreshold the writes-per-minute a writer copy must exceed to count as
     *                                 a split-for-heat candidate.
     * @param sizeThresholdBytes the size in bytes a writer copy must exceed to count as a
     *                           split-for-size candidate.
     * @param metadata the cluster state's index metadata, used to resolve each shard's index name.
     */
    public ShardSplitCandidatesResponse(
        ClusterName clusterName,
        List<NodeShardSplitCandidatesResponse> nodes,
        List<FailedNodeException> failures,
        long writesPerMinuteThreshold,
        long sizeThresholdBytes,
        Metadata metadata
    ) {
        super(clusterName, nodes, failures);
        this.candidates = merge(nodes, writesPerMinuteThreshold, sizeThresholdBytes, metadata);
    }

    private static List<ShardSplitCandidateEntry> merge(
        List<NodeShardSplitCandidatesResponse> nodes,
        long writesPerMinuteThreshold,
        long sizeThresholdBytes,
        Metadata metadata
    ) {
        Map<String, Long> highestWpmByShard = new LinkedHashMap<>();
        Map<String, Long> highestSizeByShard = new LinkedHashMap<>();
        for (NodeShardSplitCandidatesResponse node : nodes) {
            for (ShardWriteRateEntry entry : node.writeRates()) {
                String key = key(entry.indexUuid(), entry.shardId());
                highestWpmByShard.merge(key, entry.writesPerMinute(), Math::max);
                highestSizeByShard.merge(key, entry.shardSizeInBytes(), Math::max);
            }
        }

        Map<String, IndexMetadata> byUuid = new LinkedHashMap<>();
        for (IndexMetadata indexMetadata : metadata.indices().values()) {
            byUuid.put(indexMetadata.getIndexUUID(), indexMetadata);
        }

        List<ShardSplitCandidateEntry> result = new ArrayList<>(highestWpmByShard.size());
        for (Map.Entry<String, Long> entry : highestWpmByShard.entrySet()) {
            int separator = entry.getKey().lastIndexOf('/');
            String indexUuid = entry.getKey().substring(0, separator);
            int shardId = Integer.parseInt(entry.getKey().substring(separator + 1));
            long writesPerMinute = entry.getValue();
            long shardSizeInBytes = highestSizeByShard.getOrDefault(entry.getKey(), ShardSplitCandidateEntry.UNKNOWN);

            IndexMetadata indexMetadata = byUuid.get(indexUuid);
            if (indexMetadata == null) {
                // Index has since been deleted, or this node's snapshot raced ahead of this
                // coordinator's own cluster state -- nothing sensible to evaluate, skip it, same
                // "stale entry, safely dropped" tolerance any other cluster-wide merge here has.
                continue;
            }
            String indexName = indexMetadata.getIndex().getName();
            long ownedSizeInBytes = ownedSize(shardSizeInBytes, indexMetadata, shardId);
            boolean writeRateCandidate = writesPerMinute > writesPerMinuteThreshold;
            // Deliberately the *owned* size, not the reported one: see
            // ShardSplitCandidateEntry#ownedSizeInBytes (finding R-3) for why comparing the reported
            // size against the threshold made automatic split-for-size a non-terminating loop.
            boolean sizeCandidate = ownedSizeInBytes > sizeThresholdBytes;
            result.add(
                new ShardSplitCandidateEntry(
                    indexUuid,
                    shardId,
                    indexName,
                    writesPerMinute,
                    shardSizeInBytes,
                    ownedSizeInBytes,
                    writeRateCandidate,
                    sizeCandidate
                )
            );
        }
        return result;
    }

    /**
     * The share of {@code reportedSizeInBytes} that shard {@code shardId} actually owns, given its
     * {@link ShardRange}'s fraction of the 2^32 hash space. Returns the reported size unchanged for
     * a shard with no split lineage (whose range is the whole space), for {@link
     * ShardSplitCandidateEntry#UNKNOWN}, and for any shard whose range cannot be resolved -- an
     * unresolvable range must not silently shrink a real size signal to zero.
     */
    private static long ownedSize(long reportedSizeInBytes, IndexMetadata indexMetadata, int shardId) {
        if (reportedSizeInBytes == ShardSplitCandidateEntry.UNKNOWN || reportedSizeInBytes <= 0) {
            return reportedSizeInBytes;
        }
        SplitShardsMetadata splitShardsMetadata = indexMetadata.getSplitShardsMetadata();
        if (splitShardsMetadata == null) {
            return reportedSizeInBytes;
        }
        ShardRange range;
        try {
            range = splitShardsMetadata.getRangeOfShard(shardId);
        } catch (RuntimeException e) {
            // A shard id this index's split metadata does not know about (a retired parent, a
            // node's snapshot racing ahead of this coordinator's cluster state). Fail open: report
            // the raw size rather than fabricating a fraction.
            return reportedSizeInBytes;
        }
        if (range == null) {
            return reportedSizeInBytes;
        }
        // Widened to long deliberately: end - start overflows int for a full-space range, which is
        // exactly the common case. +1 because both bounds are inclusive.
        long span = (long) range.end() - (long) range.start() + 1L;
        if (span <= 0 || span >= FULL_HASH_SPACE) {
            return reportedSizeInBytes;
        }
        return (long) (reportedSizeInBytes * ((double) span / (double) FULL_HASH_SPACE));
    }

    /** The number of distinct values a 32-bit routing hash can take -- the span of a never-split shard's range. */
    private static final long FULL_HASH_SPACE = 1L << 32;

    private static String key(String indexUuid, int shardId) {
        return indexUuid + "/" + shardId;
    }

    /** Every writer shard observed anywhere in the cluster, merged and evaluated against this plugin's threshold. */
    public List<ShardSplitCandidateEntry> candidates() {
        return candidates;
    }

    @Override
    protected List<NodeShardSplitCandidatesResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeShardSplitCandidatesResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeShardSplitCandidatesResponse> nodes) throws IOException {
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
        for (ShardSplitCandidateEntry entry : candidates) {
            entry.toXContent(builder, params);
        }
        return builder.endArray().endObject();
    }
}
