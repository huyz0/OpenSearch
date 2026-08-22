/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidateEntry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataStreamShardCountAdvisorSchedulerTaskTests extends OpenSearchTestCase {

    public void testRecommendsGrowthWhenACandidateShardIsSustainedHigh() {
        String dataStreamName = "logs-app";
        Index writeIndex = new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 1), UUID.randomUUID().toString());
        ClusterState state = clusterStateWithDataStream(dataStreamName, writeIndex, 2);

        List<ShardSplitCandidateEntry> candidates = List.of(
            new ShardSplitCandidateEntry(
                writeIndex.getUUID(),
                0,
                writeIndex.getName(),
                999_999L,
                ShardSplitCandidateEntry.UNKNOWN,
                true,
                false
            )
        );

        DataStreamShardCountAdvisorCache cache = new DataStreamShardCountAdvisorCache();
        DataStreamShardCountAdvisorSchedulerTask task = newTask(cache);
        invokeEvaluateDataStream(task, state.metadata().dataStreams().get(dataStreamName), state, candidates);

        assertEquals(4, cache.recommendedShardCount(dataStreamName).getAsInt());
    }

    public void testRecommendsUnchangedWhenNoCandidateIsFlagged() {
        String dataStreamName = "logs-app-2";
        Index writeIndex = new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 1), UUID.randomUUID().toString());
        ClusterState state = clusterStateWithDataStream(dataStreamName, writeIndex, 3);

        List<ShardSplitCandidateEntry> candidates = List.of(
            new ShardSplitCandidateEntry(writeIndex.getUUID(), 0, writeIndex.getName(), 10L, ShardSplitCandidateEntry.UNKNOWN, false, false)
        );

        DataStreamShardCountAdvisorCache cache = new DataStreamShardCountAdvisorCache();
        DataStreamShardCountAdvisorSchedulerTask task = newTask(cache);
        invokeEvaluateDataStream(task, state.metadata().dataStreams().get(dataStreamName), state, candidates);

        assertEquals(3, cache.recommendedShardCount(dataStreamName).getAsInt());
    }

    public void testCapsRecommendationAtMaxRecommendedShards() {
        String dataStreamName = "logs-app-3";
        Index writeIndex = new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 1), UUID.randomUUID().toString());
        ClusterState state = clusterStateWithDataStream(
            dataStreamName,
            writeIndex,
            DataStreamShardCountAdvisorSchedulerTask.MAX_RECOMMENDED_SHARDS
        );

        List<ShardSplitCandidateEntry> candidates = List.of(
            new ShardSplitCandidateEntry(
                writeIndex.getUUID(),
                0,
                writeIndex.getName(),
                999_999L,
                ShardSplitCandidateEntry.UNKNOWN,
                true,
                false
            )
        );

        DataStreamShardCountAdvisorCache cache = new DataStreamShardCountAdvisorCache();
        DataStreamShardCountAdvisorSchedulerTask task = newTask(cache);
        invokeEvaluateDataStream(task, state.metadata().dataStreams().get(dataStreamName), state, candidates);

        assertEquals(
            DataStreamShardCountAdvisorSchedulerTask.MAX_RECOMMENDED_SHARDS,
            cache.recommendedShardCount(dataStreamName).getAsInt()
        );
    }

    public void testIgnoresANonServerlessStorageDataStream() {
        String dataStreamName = "logs-app-4";
        Index writeIndex = new Index(DataStream.getDefaultBackingIndexName(dataStreamName, 1), UUID.randomUUID().toString());
        ClusterState state = clusterStateWithDataStream(dataStreamName, writeIndex, 2, false);

        List<ShardSplitCandidateEntry> candidates = List.of(
            new ShardSplitCandidateEntry(
                writeIndex.getUUID(),
                0,
                writeIndex.getName(),
                999_999L,
                ShardSplitCandidateEntry.UNKNOWN,
                true,
                false
            )
        );

        DataStreamShardCountAdvisorCache cache = new DataStreamShardCountAdvisorCache();
        DataStreamShardCountAdvisorSchedulerTask task = newTask(cache);
        invokeEvaluateDataStream(task, state.metadata().dataStreams().get(dataStreamName), state, candidates);

        assertTrue(cache.recommendedShardCount(dataStreamName).isEmpty());
    }

    private static DataStreamShardCountAdvisorSchedulerTask newTask(DataStreamShardCountAdvisorCache cache) {
        // The scheduler's own constructor immediately schedules a real recurring task -- irrelevant
        // to these tests, which call evaluateDataStream directly, so a null threadPool/client/clusterService
        // is fine as long as the constructor itself tolerates it; use a minimal mocked ThreadPool instead
        // to keep the constructor's own scheduling call harmless.
        org.opensearch.threadpool.ThreadPool threadPool = mock(org.opensearch.threadpool.ThreadPool.class);
        org.opensearch.threadpool.Scheduler.Cancellable cancellable = mock(org.opensearch.threadpool.Scheduler.Cancellable.class);
        when(threadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(cancellable);
        Client client = mock(Client.class);
        org.opensearch.cluster.service.ClusterService clusterService = mock(org.opensearch.cluster.service.ClusterService.class);
        DataStreamShardCountAdvisorSchedulerTask task = new DataStreamShardCountAdvisorSchedulerTask(
            threadPool,
            org.opensearch.common.unit.TimeValue.timeValueMinutes(5),
            client,
            clusterService,
            cache
        );
        task.close();
        return task;
    }

    private static void invokeEvaluateDataStream(
        DataStreamShardCountAdvisorSchedulerTask task,
        DataStream dataStream,
        ClusterState state,
        List<ShardSplitCandidateEntry> candidates
    ) {
        task.evaluateDataStream(dataStream, state, candidates);
    }

    private static ClusterState clusterStateWithDataStream(String dataStreamName, Index writeIndex, int numberOfShards) {
        return clusterStateWithDataStream(dataStreamName, writeIndex, numberOfShards, true);
    }

    private static ClusterState clusterStateWithDataStream(
        String dataStreamName,
        Index writeIndex,
        int numberOfShards,
        boolean serverlessStorageEnabled
    ) {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, writeIndex.getUUID())
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT);
        if (serverlessStorageEnabled) {
            settings.put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true);
        }
        IndexMetadata writeIndexMetadata = IndexMetadata.builder(writeIndex.getName()).settings(settings).build();
        DataStream dataStream = new DataStream(
            dataStreamName,
            new DataStream.TimestampField("@timestamp"),
            Collections.singletonList(writeIndex),
            1
        );
        Metadata metadata = Metadata.builder().put(writeIndexMetadata, true).put(dataStream).build();
        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
    }
}
