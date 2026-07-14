/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The write-load-driven shard-count recommendation {@link
 * DataStreamShardCountAdvisorSchedulerTask} periodically refreshes and {@link
 * org.opensearch.serverless.storage.ServerlessStorageIndexSettingProvider} synchronously reads at
 * index-creation time -- the small in-memory seam this plugin's Elasticsearch-Serverless-style
 * write-load autosharding design needed (rfc-serverless-opensearch.md &sect;16 Phase 4, tracked in
 * <code>write-routing-and-term-authority-progress.md</code>'s Effort A).
 *
 * <p><b>Why a cache, not a direct call, from the settings-provider hook</b>: {@code
 * IndexSettingProvider#getAdditionalIndexSettings} has no {@link
 * org.opensearch.cluster.ClusterState} parameter and runs synchronously during cluster-state
 * processing, so it cannot itself fan out to every data node to recompute {@code
 * writesPerMinute()} the way {@code ShardSplitCandidatesAction} does -- that would mean blocking
 * index creation on a network round-trip, which core's own extension point contract does not
 * allow. This cache is the seam: {@link DataStreamShardCountAdvisorSchedulerTask} does the
 * expensive cluster-wide aggregation off the hot path, on its own schedule, and the settings
 * provider only ever does a plain, non-blocking in-memory read.
 *
 * <p>Deliberately keyed by data stream name, not by index UUID or backing-index name: the
 * settings-provider hook only knows the *new* backing index's own name (which does not exist yet
 * as a real index, so has no UUID), and that name's data-stream name is recoverable from it via
 * {@link DataStreamBackingIndexNames#parseDataStreamName}.
 */
public final class DataStreamShardCountAdvisorCache {

    private final Map<String, Integer> recommendedShardCounts = new ConcurrentHashMap<>();

    /** Creates an empty cache; nothing is recommended until {@link #record} is called at least once per stream. */
    public DataStreamShardCountAdvisorCache() {}

    /**
     * Records the current recommendation for one data stream's next backing index, overwriting
     * any previous recommendation for the same stream.
     *
     * @param dataStreamName the data stream this recommendation applies to.
     * @param recommendedShardCount how many shards the next backing index should have; must be &gt;= 1.
     */
    public void record(String dataStreamName, int recommendedShardCount) {
        if (recommendedShardCount < 1) {
            throw new IllegalArgumentException("recommendedShardCount must be >= 1, got " + recommendedShardCount);
        }
        recommendedShardCounts.put(dataStreamName, recommendedShardCount);
    }

    /**
     * The current recommendation for {@code dataStreamName}'s next backing index, if this cache
     * has one -- empty if no evaluation has completed for this stream yet.
     *
     * @param dataStreamName the data stream to look up.
     */
    public OptionalInt recommendedShardCount(String dataStreamName) {
        Integer value = recommendedShardCounts.get(dataStreamName);
        return value == null ? OptionalInt.empty() : OptionalInt.of(value);
    }
}
