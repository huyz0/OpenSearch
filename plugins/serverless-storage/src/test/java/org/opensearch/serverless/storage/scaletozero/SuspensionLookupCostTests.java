/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.util.Locale;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * H9b. What resolving a shard's index by uuid costs as the cluster grows.
 *
 * <p>H1d recorded that {@code findByUuid} scans every index and is called per candidate shard per tick,
 * so a tick suspending a hundred shards scanned the whole cluster a hundred times. It recorded no number,
 * which made the severity a matter of opinion. This attaches one.
 *
 * <p>The memo makes the scan happen once per {@code Metadata} instance rather than once per call, which is
 * sound because {@code Metadata} is immutable and shared so identity is a valid cache key. What it does
 * not do is make the cost independent of index count: the first resolution against a new cluster state
 * still walks everything. Removing that entirely means not consulting metadata here at all, which is H9c,
 * because suspension state currently lives inside {@code IndexMetadata}.
 */
public class SuspensionLookupCostTests extends OpenSearchTestCase {

    private static final int[] POPULATIONS = { 1_000, 10_000, 50_000 };

    /** One tick's worth of shards, which is the multiplier the memo removes. */
    private static final int SHARDS_PER_TICK = 100;

    public void testLookupCostAgainstIndexCount() {
        StringBuilder table = new StringBuilder("\nH9b uuid resolution cost for one tick of 100 shards\n");
        table.append(String.format(Locale.ROOT, "  %9s %18s %18s%n", "indices", "first call (ms)", "100 shards (ms)"));

        for (int population : POPULATIONS) {
            ClusterState state = stateWith(population);
            String targetUuid = "idx-" + (population - 1) + "-uuid";

            ShardSuspensionCoordinator coordinator = coordinator(state);

            long startedAt = System.nanoTime();
            coordinator.suspendWriterShard(targetUuid, 0);
            double firstMillis = (System.nanoTime() - startedAt) / 1_000_000.0;

            startedAt = System.nanoTime();
            for (int shard = 0; shard < SHARDS_PER_TICK; shard++) {
                coordinator.suspendWriterShard(targetUuid, shard);
            }
            double tickMillis = (System.nanoTime() - startedAt) / 1_000_000.0;

            table.append(String.format(Locale.ROOT, "  %,9d %18.3f %18.3f%n", population, firstMillis, tickMillis));
        }
        logger.warn(table.toString());
    }

    /**
     * The property the memo buys, asserted rather than eyeballed: a tick resolving a hundred shards must
     * not cost a hundred scans. Without the memo the tick is roughly a hundred times the first call;
     * with it, the first call absorbs the scan and the rest are map lookups.
     */
    public void testATickDoesNotPayAScanPerShard() {
        ClusterState state = stateWith(50_000);
        String targetUuid = "idx-49999-uuid";
        ShardSuspensionCoordinator coordinator = coordinator(state);

        long startedAt = System.nanoTime();
        coordinator.suspendWriterShard(targetUuid, 0);
        double firstMillis = (System.nanoTime() - startedAt) / 1_000_000.0;

        startedAt = System.nanoTime();
        for (int shard = 1; shard < SHARDS_PER_TICK; shard++) {
            coordinator.suspendWriterShard(targetUuid, shard);
        }
        double remainingMillis = (System.nanoTime() - startedAt) / 1_000_000.0;

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nH9b at 50,000 indices: first resolution %.3f ms, next 99 together %.3f ms%n",
                firstMillis,
                remainingMillis
            )
        );

        assertTrue("both measurements must be non-zero, or this measured nothing", firstMillis > 0 && remainingMillis > 0);
        assertTrue(
            String.format(
                Locale.ROOT,
                "ninety-nine further resolutions against the same cluster state must not cost ninety-nine "
                    + "more scans: first %.3f ms against %.3f ms for the rest",
                firstMillis,
                remainingMillis
            ),
            remainingMillis < firstMillis * 20
        );
    }

    // ---------------------------------------------------------------- helpers

    private static ShardSuspensionCoordinator coordinator(ClusterState state) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        return new ShardSuspensionCoordinator(clusterService, mock(Client.class), 0L);
    }

    private static ClusterState stateWith(int population) {
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 0; i < population; i++) {
            String name = "idx-" + i;
            metadata.put(
                IndexMetadata.builder(name)
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                            .build()
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .build(),
                false
            );
        }
        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata.build()).build();
    }
}
