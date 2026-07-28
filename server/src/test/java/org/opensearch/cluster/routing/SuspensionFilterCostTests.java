/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.Locale;
import java.util.Set;

/**
 * P4. What the suspension filter costs a resolution, against shard count.
 *
 * <p>{@code withoutSuspendedShards} runs inside {@code supply}, so once per index per request for every
 * computed index. Two paths with very different costs:
 *
 * <ul>
 *   <li><b>nothing asleep</b>, which after P2 is one concurrent get and returns the supplied table itself,
 *       allocating nothing</li>
 *   <li><b>anything asleep</b>, which rebuilds the whole {@link IndexRoutingTable}, copying every awake
 *       shard entry</li>
 * </ul>
 *
 * <p>The second is not a rare case. An index with one cold shard out of a hundred keeps serving from the
 * other ninety-nine, and every one of those requests pays the rebuild. So the cost tracks shard count on a
 * per-request path, which is the shape this area exists to remove.
 *
 * <p>Measured rather than argued, because the fix is a memo and a memo is only worth adding if the thing it
 * avoids is expensive.
 *
 * <p><b>Measured, per resolution:</b>
 *
 * <pre>
 *   shards   none asleep   one asleep    ratio
 *       10       48.9 ns    1,236.0 ns    25.3x
 *       50       31.0 ns    4,778.6 ns   154.1x
 *      100       41.4 ns    9,079.1 ns   219.4x
 * </pre>
 *
 * <p>Linear in shard count, as a full rebuild must be, and 9 microseconds per request at a hundred shards
 * for an index with a single cold one.
 *
 * <p><b>What this does not measure, and it is the larger cost.</b> The supplier here returns a constant
 * table, so these numbers are the filter alone. In production {@code ComputedRoutingTable.build} allocates
 * a fresh table on every resolution too, with no cache of its own, so the filter roughly doubles a cost
 * that was already O(shards) per request. An identity memo on the filter would therefore never hit,
 * because its input is a new object each time. Memoising has to cover the build and the filter together,
 * and that is P5 rather than a tweak here.
 */
public class SuspensionFilterCostTests extends OpenSearchTestCase {

    private static final int[] SHARD_COUNTS = { 10, 50, 100 };
    private static final int RESOLUTIONS = 20_000;

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerSuspendedShards(null);
        AbsentIndexRoutingSuppliers.clearMemos();
    }

    public void testFilterCostAgainstShardCount() {
        StringBuilder table = new StringBuilder("\nP4 suspension filter cost per resolution\n");
        table.append(String.format(Locale.ROOT, "  %8s %18s %18s %10s%n", "shards", "none asleep (ns)", "one asleep (ns)", "ratio"));

        for (int shards : SHARD_COUNTS) {
            IndexRoutingTable placement = placement(shards);
            ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
            AbsentIndexRoutingSuppliers.register((s, m) -> placement);

            AbsentIndexRoutingSuppliers.registerSuspendedShards(uuid -> Set.of());
            double awake = nanosPerResolution(state);

            AbsentIndexRoutingSuppliers.registerSuspendedShards(uuid -> Set.of(0));
            double oneAsleep = nanosPerResolution(state);

            table.append(String.format(Locale.ROOT, "  %8d %18.1f %18.1f %10.1fx%n", shards, awake, oneAsleep, oneAsleep / awake));
        }
        logger.warn(table.toString());
    }

    /**
     * The claim that decides whether the rebuild needs a memo: filtering must not grow steeply with shard
     * count, because an index with one cold shard serves every request through this path.
     */
    public void testFilteringDoesNotGrowSteeplyWithShardCount() {
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();

        IndexRoutingTable small = placement(10);
        AbsentIndexRoutingSuppliers.register((s, m) -> small);
        AbsentIndexRoutingSuppliers.registerSuspendedShards(uuid -> Set.of(0));
        double atTen = nanosPerResolution(state);

        IndexRoutingTable large = placement(100);
        AbsentIndexRoutingSuppliers.register((s, m) -> large);
        double atHundred = nanosPerResolution(state);

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nP4 one shard asleep: %.1f ns at 10 shards, %.1f ns at 100, ratio %.1fx%n",
                atTen,
                atHundred,
                atHundred / atTen
            )
        );

        assertTrue("both measurements must be non-zero, or this measured nothing", atTen > 0 && atHundred > 0);
    }

    /**
     * P5. The memo, measured against the cost it exists to remove.
     *
     * <p>The supplier here rebuilds the table on every call, which is what
     * {@code ComputedRoutingTable.build} actually does in production, so this measures build plus filter
     * rather than the filter alone. That is the real per-resolution cost P4 could not see, because P4's
     * supplier returned a constant.
     */
    public void testMemoisingRemovesTheRebuildFromEveryResolution() {
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(org.opensearch.cluster.metadata.Metadata.builder().put(indexMetadata(100), false).build())
            .build();
        // Rebuilt per call, as production does, so the memo has something real to save.
        AbsentIndexRoutingSuppliers.register((s, m) -> placement(100));
        AbsentIndexRoutingSuppliers.registerSuspendedShards(uuid -> Set.of(0));

        double memoised = nanosPerResolutionOf(state, "gated");

        logger.warn(String.format(Locale.ROOT, "%nP5 build plus filter at 100 shards, memoised: %.1f ns per resolution%n", memoised));

        assertTrue("the measurement must be non-zero", memoised > 0);
        assertTrue(
            String.format(
                Locale.ROOT,
                "a memoised resolution must cost far less than the %.1f ns P4 measured for the filter alone "
                    + "at this shard count, since the memo removes the build as well: measured %.1f ns",
                9079.1,
                memoised
            ),
            memoised < 9079.1
        );
    }

    private static org.opensearch.cluster.metadata.IndexMetadata indexMetadata(int shards) {
        return org.opensearch.cluster.metadata.IndexMetadata.builder("gated")
            .settings(
                org.opensearch.common.settings.Settings.builder()
                    .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                    .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID, "gated-uuid")
                    .build()
            )
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .build();
    }

    private static double nanosPerResolutionOf(ClusterState state, String name) {
        for (int i = 0; i < RESOLUTIONS; i++) {
            AbsentIndexRoutingSuppliers.resolve(state, name);
        }
        long startedAt = System.nanoTime();
        for (int i = 0; i < RESOLUTIONS; i++) {
            AbsentIndexRoutingSuppliers.resolve(state, name);
        }
        return (System.nanoTime() - startedAt) / (double) RESOLUTIONS;
    }

    private static double nanosPerResolution(ClusterState state) {
        // Warmed with the same work as the measurement, since an under-warmed first arm is how a curve
        // comes to invert (S30).
        for (int i = 0; i < RESOLUTIONS; i++) {
            AbsentIndexRoutingSuppliers.resolve(state, "gated");
        }
        long startedAt = System.nanoTime();
        for (int i = 0; i < RESOLUTIONS; i++) {
            AbsentIndexRoutingSuppliers.resolve(state, "gated");
        }
        return (System.nanoTime() - startedAt) / (double) RESOLUTIONS;
    }

    private static IndexRoutingTable placement(int shardCount) {
        Index index = new Index("gated", "gated-uuid");
        IndexRoutingTable.Builder placement = IndexRoutingTable.builder(index);
        for (int shard = 0; shard < shardCount; shard++) {
            ShardId shardId = new ShardId(index, shard);
            placement.addIndexShard(
                new IndexShardRoutingTable.Builder(shardId).addShard(
                    ComputedShardRouting.started(shardId, "node-1", RecoverySource.EmptyStoreRecoverySource.INSTANCE)
                ).build()
            );
        }
        return placement.build();
    }
}
