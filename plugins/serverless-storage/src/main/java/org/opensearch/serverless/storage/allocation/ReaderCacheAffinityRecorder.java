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

/**
 * The "do the work" half of cache-locality hysteresis (rfc-serverless-opensearch.md &sect;10):
 * persists, via an ordinary {@link ClusterStateUpdateTask}, the node id a reader (search-only)
 * shard just successfully started on -- the same {@link IndexMetadata} custom-data mutation shape
 * {@code ShardSuspensionCoordinator} already uses for suspend/reactivate bookkeeping. {@link
 * ServerlessStorageExistingShardsAllocator#applyStartedShards} is this class's only caller, firing
 * once per reader shard that finishes starting.
 *
 * <p>Deliberately fire-and-forget and best-effort: a failed or superseded update just means the
 * next real shard start (or the existing record's staleness) corrects it later. Losing one record
 * never breaks correctness -- {@link ReaderCacheAffinityMetadata} is consulted only as a placement
 * <em>preference</em>, never a requirement, so there's nothing here worth blocking, retrying with
 * backoff, or surfacing to a caller.
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
     * Records that {@code indexUuid}'s reader shard {@code shardId} just started on {@code
     * nodeId}, so a future reallocation of this shard prefers that node while its cache is
     * presumed still warm. Idempotent in effect: {@link
     * ReaderCacheAffinityMetadata#withShardCacheAffinity} is itself a no-op when the record is
     * already current, so calling this on every shard start -- most of which will find nothing to
     * change after the very first recording -- is cheap.
     *
     * @param indexUuid the index the shard belongs to.
     * @param shardId the shard number that started.
     * @param nodeId the node id it started on.
     */
    public void recordStarted(String indexUuid, int shardId, String nodeId) {
        long nowMillis = System.currentTimeMillis();
        clusterService.submitStateUpdateTask("serverless-storage-record-reader-cache-affinity", new ClusterStateUpdateTask(Priority.LOW) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                IndexMetadata indexMetadata = findByUuid(currentState.metadata(), indexUuid);
                if (indexMetadata == null) {
                    return currentState;
                }
                IndexMetadata updated = ReaderCacheAffinityMetadata.withShardCacheAffinity(indexMetadata, shardId, nodeId, nowMillis);
                if (updated == indexMetadata) {
                    return currentState;
                }
                return ClusterState.builder(currentState).metadata(Metadata.builder(currentState.metadata()).put(updated, true)).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.debug("failed to record serverless-storage reader cache affinity for shard [" + indexUuid + "][" + shardId + "]", e);
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
