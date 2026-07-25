/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.clusterstate;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.IndexDescriptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Every earlier estimate in this package fixed the shard count at 1 (two at 3), which quietly made
 * "per index" and "per shard" the same number. The stated target is 100M indices at 3-30 shards each,
 * so they are not the same number at all -- somewhere between 300M and 3B shards -- and the structures
 * that scale per shard were never measured.
 *
 * <p>This sweeps shard count across the three representations that matter, so the divergence between
 * them is visible rather than assumed:
 *
 * <ul>
 *   <li>{@code full} -- {@link IndexMetadata} plus its {@link RoutingTable} entry, what a
 *       cluster-manager holds today.
 *   <li>{@code norouting} -- {@link IndexMetadata} alone, with no routing entry. This is what Phase A
 *       makes legal, and the gap between it and {@code full} is exactly what routing absence buys.
 *   <li>{@code descriptor} -- {@link IndexDescriptor} alone, the C5/C6 deferred stub. It carries
 *       {@code totalNumberOfShards} as an {@code int}, so the interesting question is whether it is
 *       genuinely flat in shard count or whether something reachable from it is not.
 * </ul>
 *
 * <p>Retained size, not allocation rate: the question is whether the resident set fits, so this builds
 * N objects, holds them, forces a collection and reads the heap delta -- the same method
 * {@link TenantIndexRetainedHeapEstimate} uses, and for the same reason.
 *
 * <p>Run standalone with a generous heap and assertions off; Gradle turns on {@code -ea -esa} and the
 * difference is roughly 2x:
 *
 * <pre>{@code
 * java -da -dsa -Xms8g -Xmx8g -cp <runtime classpath> \
 *   org.opensearch.benchmark.clusterstate.ShardScalingRetainedHeapEstimate full 100000 30
 * }</pre>
 *
 * <p>With no shard-count argument it sweeps {1, 3, 10, 30} for the given mode, which is the form worth
 * reading -- a single point does not show whether the curve is flat or linear.
 */
public final class ShardScalingRetainedHeapEstimate {

    private static final int[] SHARD_COUNTS = { 1, 3, 10, 30 };

    private ShardScalingRetainedHeapEstimate() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: ShardScalingRetainedHeapEstimate <full|norouting|descriptor> <count> [shards]");
            System.exit(1);
        }
        String mode = args[0];
        int count = Integer.parseInt(args[1]);

        int[] sweep = args.length >= 3 ? new int[] { Integer.parseInt(args[2]) } : SHARD_COUNTS;

        // Warm up class loading and JIT on a throwaway batch, then drop it, so the first measured
        // point does not carry the cost of every class this path touches for the first time.
        List<Object> warmup = buildBatch(mode, 2000, 3);
        warmup.clear();
        warmup = null;
        forceGc();

        System.out.println("mode=" + mode + " count=" + count);
        System.out.printf("%8s %18s %18s %14s%n", "shards", "bytes/index", "bytes/shard", "100M idx (GiB)");

        for (int shards : sweep) {
            long before = usedHeapBytes();
            List<Object> held = buildBatch(mode, count, shards);
            forceGc();
            long after = usedHeapBytes();

            double perIndex = (double) (after - before) / count;
            double perShard = perIndex / shards;
            double gib = perIndex * 100_000_000L / (1024.0 * 1024 * 1024);
            System.out.printf("%8d %18.1f %18.1f %14.1f%n", shards, perIndex, perShard, gib);

            // Keep the batch reachable across the measurement so escape analysis cannot collect it
            // before forceGc() runs, then release it before the next point.
            if (held.isEmpty()) {
                throw new IllegalStateException("unreachable, keeps `held` live");
            }
            held.clear();
            forceGc();
        }
    }

    private static List<Object> buildBatch(String mode, int count, int shards) {
        List<Object> list = new ArrayList<>(count);
        Random random = new Random(42);
        for (int i = 0; i < count; i++) {
            String uuid = Long.toHexString(random.nextLong()) + Long.toHexString(random.nextLong());
            String name = "tenant-" + uuid;
            String aliasName = "tenant-alias-" + uuid;

            IndexMetadata indexMetadata = IndexMetadata.builder(name)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                        .build()
                )
                .numberOfShards(shards)
                .numberOfReplicas(0)
                .putAlias(AliasMetadata.builder(aliasName).build())
                .build();

            switch (mode) {
                case "full":
                    list.add(new Object[] { indexMetadata, RoutingTable.builder().addAsNew(indexMetadata).build() });
                    break;
                case "norouting":
                    list.add(indexMetadata);
                    break;
                case "descriptor":
                    // Built from the IndexMetadata and then the IndexMetadata is dropped, which is the
                    // point: if the descriptor retains a path back to per-shard state, this batch will
                    // not shrink and the sweep will show it.
                    list.add(IndexDescriptor.of(indexMetadata));
                    break;
                default:
                    throw new IllegalArgumentException("unknown mode: " + mode);
            }
        }
        return list;
    }

    private static long usedHeapBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static void forceGc() throws InterruptedException {
        for (int i = 0; i < 4; i++) {
            System.gc();
            Thread.sleep(120);
        }
    }
}
