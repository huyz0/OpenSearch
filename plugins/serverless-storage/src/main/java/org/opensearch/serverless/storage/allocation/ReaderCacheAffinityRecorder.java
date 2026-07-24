/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The "do the work" half of cache-locality hysteresis (rfc-serverless-opensearch.md &sect;10):
 * persists, via an ordinary {@link ClusterStateUpdateTask}, the node id a reader (search-only)
 * shard just successfully started on -- the same {@link IndexMetadata} custom-data mutation shape
 * {@code ShardSuspensionCoordinator} already uses for suspend/reactivate bookkeeping. {@link
 * ServerlessStorageExistingShardsAllocator#applyStartedShards} is this class's only caller.
 *
 * <p>Deliberately fire-and-forget and best-effort: a failed or superseded update just means the
 * next real shard start (or the existing record's staleness) corrects it later. Losing one record
 * never breaks correctness -- {@link ReaderCacheAffinityMetadata} is consulted only as a placement
 * <em>preference</em>, never a requirement, so there's nothing here worth blocking, retrying with
 * backoff, or surfacing to a caller.
 *
 * <p><b>{@link #recordStarted(List)} batches one whole {@code applyStartedShards} call into a
 * single {@link ClusterStateUpdateTask}</b>, not one task per shard: {@code
 * applyStartedShards} itself is already called once per reroute with every shard that just started
 * in that cycle (core's own batching), commonly many reader shards at once after a node restart,
 * rolling upgrade, or scale-up -- {@link Metadata.Builder#build()} is a full copy of every index in
 * the cluster's metadata, so submitting one task per shard would pay that cost once per shard
 * instead of once per batch. This groups updates by index first and only builds one new {@link
 * Metadata} (and one new {@link ClusterState}) for the whole batch, at all, if at least one index
 * actually changed -- the win this class exists for. Each touched {@link IndexMetadata} can still
 * be rebuilt more than once within that single task, once per one of its own shards in the batch
 * (see {@link ReaderCacheAffinityMetadata#withShardCacheAffinity}, called once per shard record);
 * only the cluster-wide {@link Metadata} copy, the genuinely expensive part, is collapsed to once
 * per batch rather than once per shard.
 */
public final class ReaderCacheAffinityRecorder {

    private static final Logger logger = LogManager.getLogger(ReaderCacheAffinityRecorder.class);

    private final ClusterService clusterService;

    /**
     * Creates a recorder.
     *
     * @param clusterService used to submit the cluster-state update that persists each recorded affinity.
     */
    public ReaderCacheAffinityRecorder(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * One reader shard's just-recorded start, as an element of a {@link #recordStarted(List)} batch.
     *
     * @param indexUuid the index the shard belongs to.
     * @param shardId the shard number that started.
     * @param nodeId the node id it started on.
     */
    public record ShardStartRecord(String indexUuid, int shardId, String nodeId) {}

    /**
     * Records that {@code indexUuid}'s reader shard {@code shardId} just started on {@code
     * nodeId}, so a future reallocation of this shard prefers that node while its cache is
     * presumed still warm. Equivalent to calling {@link #recordStarted(List)} with a single-element
     * list -- prefer that overload when recording more than one shard at once.
     *
     * @param indexUuid the index the shard belongs to.
     * @param shardId the shard number that started.
     * @param nodeId the node id it started on.
     */
    public void recordStarted(String indexUuid, int shardId, String nodeId) {
        recordStarted(List.of(new ShardStartRecord(indexUuid, shardId, nodeId)));
    }

    /**
     * Records that every shard in {@code records} just started on its recorded node, as one batch:
     * exactly one {@link ClusterStateUpdateTask}, and at most one rebuild of the whole cluster's
     * {@link Metadata} -- see this class's own javadoc for why that, not the per-index {@link
     * IndexMetadata} rebuild, is what actually matters here. A no-op if {@code records} is empty.
     *
     * @param records every reader shard that just started, and the node it started on.
     */
    public void recordStarted(List<ShardStartRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        long nowMillis = System.currentTimeMillis();
        clusterService.submitStateUpdateTask("serverless-storage-record-reader-cache-affinity", new ClusterStateUpdateTask(Priority.LOW) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                // LinkedHashMap purely for deterministic iteration order in tests -- correctness
                // never depends on the order these are grouped or applied in.
                Map<String, List<ShardStartRecord>> byIndexUuid = new LinkedHashMap<>();
                for (ShardStartRecord record : records) {
                    byIndexUuid.computeIfAbsent(record.indexUuid(), key -> new java.util.ArrayList<>()).add(record);
                }

                Metadata.Builder metadataBuilder = null;
                for (Map.Entry<String, List<ShardStartRecord>> entry : byIndexUuid.entrySet()) {
                    IndexMetadata indexMetadata = findByUuid(currentState.metadata(), entry.getKey());
                    if (indexMetadata == null) {
                        continue;
                    }
                    IndexMetadata updated = indexMetadata;
                    for (ShardStartRecord record : entry.getValue()) {
                        updated = ReaderCacheAffinityMetadata.withShardCacheAffinity(updated, record.shardId(), record.nodeId(), nowMillis);
                    }
                    if (updated != indexMetadata) {
                        if (metadataBuilder == null) {
                            metadataBuilder = Metadata.builder(currentState.metadata());
                        }
                        metadataBuilder.put(updated, true);
                    }
                }

                if (metadataBuilder == null) {
                    return currentState;
                }
                return ClusterState.builder(currentState).metadata(metadataBuilder).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.debug("failed to record serverless-storage reader cache affinity for " + records.size() + " shard(s)", e);
            }
        });
    }

    private static IndexMetadata findByUuid(Metadata metadata, String indexUuid) {
        for (IndexMetadata indexMetadata : metadata.indices().values()) {
            if (indexUuid.equals(indexMetadata.getIndexUUID())) {
                return indexMetadata;
            }
        }
        return null;
    }
}
