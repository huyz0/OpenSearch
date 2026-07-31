/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.allocation.SuspendedShardsMetadata;
import org.opensearch.serverless.storage.scaletozero.ShardSuspensionCoordinator;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidateEntry;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * T21. How long a tenant waits when its first request arrives at a shard that went to sleep.
 *
 * <p>T20 measured that an empty awake shard costs 118 KB and about three file descriptors, so an index per
 * tenant only works if most tenants are asleep most of the time. Scale to zero is what makes that possible,
 * and it moves the cost rather than removing it: a sleeping tenant is free until it is not, and then
 * somebody waits.
 *
 * <p>That wait is a per-tenant service level rather than a cluster metric. A tenant who queries once an hour
 * pays it on every visit, so it is the number that decides whether scale to zero is usable for the long
 * tail rather than only for genuinely dormant accounts.
 *
 * <p>{@code ServerlessStorageShardSuspensionIT} already proves the mechanism works end to end, that a
 * suspended writer shard is genuinely evicted and that a write reactivates it. It does not time it, because
 * proving a mechanism and sizing it are different jobs, and this project has twice found a number quoted for
 * years that nobody had measured under the conditions it was quoted for.
 *
 * <p><b>Method.</b> Suspend a started shard, wait until it is genuinely UNASSIGNED rather than merely
 * marked, then time a single write from submission to acknowledgement. That interval is what the tenant
 * experiences: reactivation, reroute, recovery and the write itself. Repeated, because a first measurement
 * carries JVM and cluster warmup that a tenant on a warm cluster would not pay.
 *
 * <p><b>What this measures and does not.</b> One shard on a two node cluster with one document, recovering
 * from a local path. A real tenant's shard recovers more data, and on a busy cluster it queues behind other
 * reroutes. So this is a floor, and a floor is the useful direction: if waking an almost empty shard is
 * already slow, the long tail cannot be served this way.
 */
@org.opensearch.test.OpenSearchIntegTestCase.ClusterScope(scope = org.opensearch.test.OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageWakeLatencyIT extends ServerlessStorageIntegTestCase {

    private static final String IDX = "wake-latency-it-idx";

    private static final int ROUNDS = 5;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testHowLongATenantWaitsForASleepingShard() throws Exception {
        Path basePath = createTempDir("serverless-storage-wake-latency-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            IDX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(IDX);
        assertEquals(org.opensearch.core.rest.RestStatus.CREATED, client().prepareIndex(IDX).setSource("field", "seed").get().status());

        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(IDX).getIndexUUID();
        ClusterService clusterManagerClusterService = internalCluster().getInstance(ClusterService.class, clusterManagerNode);
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(clusterManagerClusterService, client());

        double[] millis = new double[ROUNDS];
        for (int round = 0; round < ROUNDS; round++) {
            coordinator.suspendCandidates(List.of(new ScaleToZeroCandidateEntry(indexUuid, 0, 0L, 0L, true)));
            assertBusy(() -> {
                IndexMetadata metadata = clusterManagerClusterService.state().metadata().index(IDX);
                assertTrue("the shard must be marked suspended before timing a wake", SuspendedShardsMetadata.isSuspended(metadata, 0));
                assertEquals(
                    "and genuinely evicted, or this times a write to a shard that never slept",
                    ShardRoutingState.UNASSIGNED,
                    clusterManagerClusterService.state().routingTable().index(IDX).shard(0).primaryShard().state()
                );
            });

            long startedAt = System.nanoTime();
            IndexResponse response = client().prepareIndex(IDX).setSource("field", "wake-" + round).get();
            millis[round] = (System.nanoTime() - startedAt) / 1_000_000.0;
            assertEquals(org.opensearch.core.rest.RestStatus.CREATED, response.status());

            assertBusy(
                () -> assertEquals(
                    ShardRoutingState.STARTED,
                    clusterManagerClusterService.state().routingTable().index(IDX).shard(0).primaryShard().state()
                )
            );
        }

        StringBuilder table = new StringBuilder("\nT21 what a tenant waits when its shard is asleep\n");
        for (int round = 0; round < ROUNDS; round++) {
            table.append(
                String.format(Locale.ROOT, "  round %d  %8.1f ms%s%n", round, millis[round], round == 0 ? "   (carries warmup)" : "")
            );
        }
        double[] warm = java.util.Arrays.copyOfRange(millis, 1, ROUNDS);
        java.util.Arrays.sort(warm);
        table.append(String.format(Locale.ROOT, "%n  median excluding the first: %.1f ms%n", warm[warm.length / 2]));
        table.append("  One nearly empty shard, local recovery, idle cluster. A floor.\n");
        logger.warn(table.toString());

        assertTrue("the measurement must be non-zero, or this measured nothing", warm[warm.length / 2] > 0);
    }
}
