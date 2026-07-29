/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * T20. What one awake shard costs, which is the budget index-per-tenant actually runs out of.
 *
 * <p>Every residency measurement in this project has been about metadata: 698 bytes per index in cluster
 * state, 70 GB per node at a hundred million, reduced to a 32 MB bounded cache by gating. None of it
 * measured a shard.
 *
 * <p>That matters because the target architecture is an index per tenant, or a small group of tenants per
 * index. A hundred million indices at one shard each is a hundred million shards, and a shard is a Lucene
 * index with segment memory, file handles, a merge scheduler and a refresh cycle. Removing the metadata
 * does not remove any of that. Scale to zero is what makes it survivable, so the sizing question is not how
 * many tenants exist but how many are awake at once:
 *
 * <pre>
 *   (peak concurrently awake tenants) x (shards per index)  &lt;=  (shard budget per node) x (nodes)
 * </pre>
 *
 * <p>The grouping factor follows from that inequality and from nothing about creation throughput, which at
 * 850 per second is a one-time migration cost rather than a constraint.
 *
 * <p><b>Method.</b> Create single-shard indices in steps, and after each step force a collection and sample
 * heap and open file descriptors. Report the marginal cost of a shard rather than the total, because the
 * total includes a fixed cluster overhead that does not scale with tenants and would flatter the number.
 *
 * <p><b>What this cannot measure.</b> An empty shard. These indices hold no documents, so segment memory,
 * field data and the merge scheduler are all at rest. A real tenant's shard costs more, so this is a floor,
 * and the floor is the interesting part: if an empty shard is already expensive, a populated one settles the
 * grouping question on its own.
 *
 * <p>Ordinary indices rather than gated ones, deliberately. Gating removes the cluster state entry, not the
 * shard, so the shard cost is the same and an ordinary index measures it without the serverless engine's
 * own overhead confounding the number.
 */
public class ShardResidencyCostIT extends OpenSearchIntegTestCase {

    private static final int[] STEPS = { 0, 50, 150, 300, 500 };

    @Override
    protected int numberOfShards() {
        return 1;
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    public void testWhatAnAwakeShardCosts() throws Exception {
        StringBuilder table = new StringBuilder("\nT20 marginal cost of one awake shard (empty)\n");
        table.append(String.format(Locale.ROOT, "  %8s %14s %12s %16s %12s%n", "shards", "heap MB", "fds", "heap KB/shard", "fds/shard"));

        int created = 0;
        long baseHeap = 0;
        long baseFds = 0;
        long prevHeap = 0;
        long prevFds = 0;

        for (int target : STEPS) {
            while (created < target) {
                client().admin()
                    .indices()
                    .create(
                        new CreateIndexRequest(String.format(Locale.ROOT, "tenant-%05d", created)).settings(
                            Settings.builder()
                                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                                .build()
                        )
                    )
                    .actionGet();
                created++;
            }
            ensureGreen();

            long heap = usedHeapAfterCollection();
            long fds = openFileDescriptors();
            if (target == 0) {
                baseHeap = heap;
                baseFds = fds;
                prevHeap = heap;
                prevFds = fds;
                table.append(
                    String.format(Locale.ROOT, "  %8d %14.1f %12d %16s %12s%n", 0, heap / 1048576.0, fds, "(baseline)", "(baseline)")
                );
                continue;
            }

            int stepShards = target - (STEPS[indexOf(target) - 1]);
            double heapPerShardKb = (heap - prevHeap) / 1024.0 / stepShards;
            double fdsPerShard = (fds - prevFds) / (double) stepShards;
            table.append(
                String.format(Locale.ROOT, "  %8d %14.1f %12d %16.1f %12.2f%n", target, heap / 1048576.0, fds, heapPerShardKb, fdsPerShard)
            );
            prevHeap = heap;
            prevFds = fds;
        }

        int total = STEPS[STEPS.length - 1];
        long finalHeap = usedHeapAfterCollection();
        long finalFds = openFileDescriptors();
        table.append(
            String.format(
                Locale.ROOT,
                "%n  overall across %d shards: %.1f KB and %.2f file descriptors per shard%n",
                total,
                (finalHeap - baseHeap) / 1024.0 / total,
                (finalFds - baseFds) / (double) total
            )
        );
        table.append("  Empty shards, so this is a floor. A tenant with data costs more.\n");
        table.append("  All nodes share this JVM, so heap is cluster-wide rather than per node.\n");
        logger.warn(table.toString());

        assertTrue("the measurement must be non-zero, or this measured nothing", finalHeap > 0 && finalFds > 0);
    }

    private static int indexOf(int target) {
        for (int i = 0; i < STEPS.length; i++) {
            if (STEPS[i] == target) {
                return i;
            }
        }
        throw new AssertionError("step not found: " + target);
    }

    /**
     * Used heap after asking for a collection.
     *
     * <p>Two collections with a pause between, because one leaves recently unreachable objects uncollected
     * and would report a per-shard cost that is mostly garbage.
     */
    private static long usedHeapAfterCollection() throws InterruptedException {
        for (int i = 0; i < 2; i++) {
            System.gc();
            Thread.sleep(300);
        }
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    /** Open descriptors for this JVM, which hosts every node in the test cluster. */
    private static long openFileDescriptors() throws IOException {
        Path fd = Path.of("/proc/self/fd");
        if (Files.isDirectory(fd) == false) {
            return -1;
        }
        try (Stream<Path> entries = Files.list(fd)) {
            return entries.count();
        }
    }
}
