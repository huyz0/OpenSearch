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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Standalone diagnostic, not a JMH benchmark: {@link TenantIndexRetentionBenchmark}'s
 * {@code gc.alloc.rate.norm} measures total bytes allocated per operation, which includes
 * transient garbage (builder objects, intermediate maps inside {@link IndexMetadata.Builder#build()})
 * that never gets retained. That's the right number for GC-churn/throughput questions (e.g. mass
 * tenant provisioning), but the question this spike actually exists to answer -- will N tenants'
 * worth of {@link IndexMetadata}/{@link RoutingTable} fit in a cluster-manager's resident heap --
 * needs RETAINED size instead. This measures that directly: build N objects, hold strong
 * references so they survive GC, force a full collection, and read the heap delta.
 *
 * <p>Run standalone (not via the JMH {@code run} task) with a generous heap, e.g.:
 *
 * <pre>{@code
 * java -Xms4g -Xmx4g -cp <runtime classpath> \
 *   org.opensearch.benchmark.clusterstate.TenantIndexRetainedHeapEstimate full 200000
 * java -Xms4g -Xmx4g -cp <runtime classpath> \
 *   org.opensearch.benchmark.clusterstate.TenantIndexRetainedHeapEstimate compact 200000
 * }</pre>
 */
public final class TenantIndexRetainedHeapEstimate {

    private TenantIndexRetainedHeapEstimate() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: TenantIndexRetainedHeapEstimate <full|compact> <count>");
            System.exit(1);
        }
        String mode = args[0];
        int count = Integer.parseInt(args[1]);

        // Warm up class loading / JIT on a throwaway batch so it doesn't pollute the measured
        // allocation, then let it go and force a collection before the real measurement starts.
        List<Object> warmup = buildBatch(mode, 2000);
        warmup.clear();
        warmup = null;
        forceGc();

        long before = usedHeapBytes();
        List<Object> held = buildBatch(mode, count);
        forceGc();
        long after = usedHeapBytes();

        long delta = after - before;
        double perObject = (double) delta / count;
        System.out.println("mode=" + mode);
        System.out.println("count=" + count);
        System.out.println("heapBefore=" + before + " bytes");
        System.out.println("heapAfter=" + after + " bytes");
        System.out.println("retainedDelta=" + delta + " bytes");
        System.out.println("retainedBytesPerIndex=" + perObject);
        System.out.println(
            "extrapolatedTo100M=" + (perObject * 100_000_000L) + " bytes (" + (perObject * 100_000_000L / (1024.0 * 1024 * 1024)) + " GiB)"
        );

        // keep the reference alive until after measurement so the JIT/escape analysis can't
        // decide the whole list is dead and collect it before forceGc() above runs.
        if (held.isEmpty()) {
            throw new IllegalStateException("unreachable, keeps `held` live");
        }
    }

    private static List<Object> buildBatch(String mode, int count) {
        List<Object> list = new ArrayList<>(count);
        Random random = new Random(42);
        for (int i = 0; i < count; i++) {
            String uuid = Long.toHexString(random.nextLong()) + Long.toHexString(random.nextLong());
            String name = "tenant-" + uuid;
            String aliasName = "tenant-alias-" + uuid;
            if (mode.equals("full")) {
                Settings settings = Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                    .build();
                IndexMetadata indexMetadata = IndexMetadata.builder(name)
                    .settings(settings)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder(aliasName).build())
                    .build();
                RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();
                list.add(new Object[] { indexMetadata, routingTable });
            } else if (mode.equals("compact")) {
                list.add(new TenantIndexRetentionBenchmark.CompactTenantStub(name, uuid, 1, aliasName));
            } else {
                throw new IllegalArgumentException("mode must be 'full' or 'compact', got: " + mode);
            }
        }
        return list;
    }

    private static void forceGc() throws InterruptedException {
        for (int i = 0; i < 5; i++) {
            System.gc();
            Thread.sleep(200);
        }
    }

    private static long usedHeapBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
