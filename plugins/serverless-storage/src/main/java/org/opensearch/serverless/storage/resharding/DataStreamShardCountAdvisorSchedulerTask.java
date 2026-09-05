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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidateEntry;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesAction;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesRequest;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesResponse;
import org.opensearch.serverless.storage.scheduling.JitteredScheduling;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.io.Closeable;
import java.util.List;

/**
 * The Elasticsearch-Serverless-style "autosharding" half this plugin was missing
 * (rfc-serverless-opensearch.md &sect;16 Phase 4, tracked in
 * <code>write-routing-and-term-authority-progress.md</code>'s Effort A): periodically recomputes,
 * for every serverless-storage-enabled data stream, a recommended shard count for its *next*
 * backing index (never the current, already-live one) from the same {@code writesPerMinute()}
 * signal {@link ShardSplitCandidatesAction} already aggregates, and publishes the result into a
 * {@link DataStreamShardCountAdvisorCache} that {@link
 * org.opensearch.serverless.storage.ServerlessStorageIndexSettingProvider} reads synchronously,
 * without any network I/O, at the one moment it's actually needed: while a new backing index is
 * being created.
 *
 * <p><b>Deliberately mirrors the real Elastic Cloud Serverless design, not live shard-splitting</b>:
 * researched directly against Elastic's own public autosharding writeup before building this --
 * their own "autosharding" never live-splits an existing, already-written-to shard. It only ever
 * changes the shard count for a data stream's *next* rollover generation, since the old backing
 * index is never touched again once rollover happens. That sidesteps the entire "writes landing
 * mid-split get lost" correctness gap this plugin's own live {@code ShardSplitter} still has
 * (documented in the progress-tracking doc). This class follows that same safe shape: it only ever
 * influences a not-yet-created index, never one already serving traffic.
 *
 * <p>Runs only on the elected cluster-manager node, the same "avoid every node doing identical
 * redundant work" reasoning {@code ScaleUpCandidatesSchedulerTask}/{@code
 * ScaleToZeroCandidatesSchedulerTask} already use, since {@link ShardSplitCandidatesAction} itself
 * already fans out cluster-wide.
 */
public final class DataStreamShardCountAdvisorSchedulerTask implements Closeable {

    private static final Logger logger = LogManager.getLogger(DataStreamShardCountAdvisorSchedulerTask.class);

    /** How much a sustained-high-write-rate data stream's next generation's shard count grows by, matching this plugin's own default 2-way split factor elsewhere. */
    public static final int GROWTH_FACTOR = 2;

    /** The maximum shard count this advisor will ever recommend, however sustained the write rate -- growth must stay bounded, not unbounded. */
    public static final int MAX_RECOMMENDED_SHARDS = 32;

    private final Client client;
    private final ClusterService clusterService;
    private final DataStreamShardCountAdvisorCache cache;
    private final Scheduler.Cancellable task;

    /**
     * Starts the scheduled evaluation.
     *
     * @param threadPool schedules {@link #evaluate()} on a fixed delay.
     * @param interval how often to evaluate.
     * @param client dispatches the cluster-wide {@link ShardSplitCandidatesAction} request.
     * @param clusterService used on every tick to check whether this is currently the elected
     *                       cluster-manager node, and to enumerate data streams and their current
     *                       write indices.
     * @param cache the shared cache this task publishes recommendations into.
     */
    public DataStreamShardCountAdvisorSchedulerTask(
        ThreadPool threadPool,
        TimeValue interval,
        Client client,
        ClusterService clusterService,
        DataStreamShardCountAdvisorCache cache
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.cache = cache;
        // Jittered rather than started on the exact configured interval (finding L-10). Every node
        // constructs this task at roughly the same moment after a cluster restart or a rolling
        // upgrade, and scheduleWithFixedDelay never recomputes the delay, so an un-jittered start
        // leaves every node's copy of this loop ticking in lockstep for the lifetime of the process
        // -- a synchronised burst of cluster-manager work and object-store requests every interval,
        // forever, which is exactly the recovery-stampede shape RFC section 13 asks the reconcilers
        // to avoid. JitteredScheduling only ever extends the first interval, never shortens it.
        this.task = threadPool.scheduleWithFixedDelay(this::evaluateSafely, JitteredScheduling.jitter(interval), ThreadPool.Names.GENERIC);
    }

    private void evaluateSafely() {
        try {
            evaluate();
        } catch (Throwable t) {
            // Deliberately Throwable, not Exception -- see ScaleToZeroCandidatesSchedulerTask's own
            // javadoc for why: ClusterService#state() throws an AssertionError, not an Exception, if
            // called before the node's initial cluster state is applied, a real window this task's
            // very first tick or two can land in.
            logger.warn("data-stream shard-count advisor evaluation failed, will retry next tick", t);
        }
    }

    void evaluate() {
        ClusterState state = clusterService.state();
        if (state.nodes().isLocalNodeElectedClusterManager() == false) {
            return; // not our turn -- see class javadoc for why only the cluster-manager runs this
        }
        List<DataStream> dataStreams = state.metadata().dataStreams().values().stream().toList();
        if (dataStreams.isEmpty()) {
            return;
        }
        client.execute(ShardSplitCandidatesAction.INSTANCE, new ShardSplitCandidatesRequest(), new ActionListener<>() {
            @Override
            public void onResponse(ShardSplitCandidatesResponse response) {
                for (DataStream dataStream : dataStreams) {
                    evaluateDataStream(dataStream, state, response.candidates());
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("data-stream shard-count advisor evaluation failed, will retry next tick", e);
            }
        });
    }

    // Package-private, not private -- test-only direct invocation, avoiding the full
    // ShardSplitCandidatesAction network round-trip evaluate() itself requires.
    void evaluateDataStream(DataStream dataStream, ClusterState state, List<ShardSplitCandidateEntry> candidates) {
        List<Index> indices = dataStream.getIndices();
        if (indices.isEmpty()) {
            return;
        }
        Index writeIndex = indices.get(indices.size() - 1);
        IndexMetadata writeIndexMetadata = state.metadata().index(writeIndex);
        if (writeIndexMetadata == null
            || ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(writeIndexMetadata.getSettings()) == false) {
            return;
        }
        String writeIndexUuid = writeIndexMetadata.getIndexUUID();
        boolean sustainedHighWriteRate = candidates.stream()
            .anyMatch(entry -> writeIndexUuid.equals(entry.indexUuid()) && entry.candidate());
        int currentShardCount = writeIndexMetadata.getNumberOfShards();
        int recommendedShardCount = sustainedHighWriteRate
            ? Math.min(currentShardCount * GROWTH_FACTOR, MAX_RECOMMENDED_SHARDS)
            : currentShardCount;
        cache.record(dataStream.getName(), recommendedShardCount);
        if (sustainedHighWriteRate) {
            logger.info(
                "data-stream [{}]'s write index [{}] shows sustained high write rate; recommending {} shards for its next generation (was {})",
                dataStream.getName(),
                writeIndexMetadata.getIndex().getName(),
                recommendedShardCount,
                currentShardCount
            );
        }
    }

    /** Invokes {@link #evaluate()} synchronously, rather than waiting out the scheduled interval -- test-only visibility. */
    void evaluateForTesting() {
        evaluate();
    }

    /** Cancels the scheduled evaluation; does not touch the cache, which simply stops updating. */
    @Override
    public void close() {
        task.cancel();
    }
}
