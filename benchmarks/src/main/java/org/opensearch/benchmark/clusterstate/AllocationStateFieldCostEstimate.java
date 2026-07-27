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
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.function.Supplier;

/**
 * F1. What {@code inSyncAllocationIds} actually costs in a {@link Metadata}, measured rather than
 * estimated.
 *
 * <p>Area F proposes emptying the per-shard allocation state that computed placement makes redundant,
 * and its own risk note calls it the change most likely to produce a subtle correctness bug for the
 * least benefit. That trade cannot be judged without knowing the benefit, and
 * {@code DeferredMetadataHeapEstimate} cannot answer it: it builds indices with the default empty
 * in-sync sets, so its per-index figure already excludes what F would remove.
 *
 * <p>The realistic case is what an allocated index carries. Each shard holds one allocation id per
 * active copy, and an allocation id is a 20-character base64 UUID, so a shard with one replica holds
 * two of them in a set inside a map keyed by shard number.
 *
 * <p>Primary terms are measured alongside but are not a candidate for removal: the F1 audit found
 * {@code TransportReplicationAction} reads {@code IndexMetadata#primaryTerm} on every write and
 * {@code IndicesClusterStateService} reads it to open a shard. They are here to size the field, not to
 * propose deleting it.
 *
 * <pre>{@code
 * java -da -dsa -cp <benchmarks runtime classpath> \
 *   org.opensearch.benchmark.clusterstate.AllocationStateFieldCostEstimate [shards]
 * }</pre>
 */
public final class AllocationStateFieldCostEstimate {

    private AllocationStateFieldCostEstimate() {}

    private static final int INDICES = 100_000;

    private static int shards = 3;

    /** One replica, so two active copies per shard, which is what an ordinary index carries. */
    private static final int COPIES_PER_SHARD = 2;

    public static void main(String[] args) throws Exception {
        if (args.length >= 1) {
            shards = Integer.parseInt(args[0]);
        }
        System.out.println("Allocation state cost for " + INDICES + " indices at " + shards + " shards\n");

        long populated = measureRetained(() -> metadataWith(true));
        long empty = measureRetained(() -> metadataWith(false));
        long saving = populated - empty;

        System.out.printf(
            Locale.ROOT,
            "with in-sync ids : %8.1f MB  (%,.0f B/index, %,.0f B/shard)%n",
            populated / 1024.0 / 1024,
            (double) populated / INDICES,
            (double) populated / INDICES / shards
        );
        System.out.printf(
            Locale.ROOT,
            "empty in-sync ids: %8.1f MB  (%,.0f B/index, %,.0f B/shard)%n",
            empty / 1024.0 / 1024,
            (double) empty / INDICES,
            (double) empty / INDICES / shards
        );
        System.out.printf(
            Locale.ROOT,
            "saving           : %8.1f MB  (%,.0f B/index, %.1f%% of the populated total)%n",
            saving / 1024.0 / 1024,
            (double) saving / INDICES,
            100.0 * saving / populated
        );

        // The guard against measuring nothing. If the populated build is not actually carrying the ids,
        // both sides are the same object shape and the saving is noise rather than a result.
        if (saving <= 0) {
            throw new IllegalStateException(
                "populated metadata did not retain more than empty metadata, so this measured nothing: " + populated + " vs " + empty
            );
        }
    }

    private static Metadata metadataWith(boolean withAllocationIds) {
        Metadata.Builder builder = Metadata.builder();
        for (int i = 0; i < INDICES; i++) {
            builder.put(index(name(i), withAllocationIds), false);
        }
        return builder.build();
    }

    private static String name(int i) {
        return "tenant-" + Integer.toHexString(i);
    }

    private static IndexMetadata index(String name, boolean withAllocationIds) {
        IndexMetadata.Builder builder = IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid-000000000000")
            )
            .numberOfShards(shards)
            .numberOfReplicas(1)
            .putAlias(AliasMetadata.builder(name + "-alias").build());

        if (withAllocationIds) {
            for (int shard = 0; shard < shards; shard++) {
                Set<String> ids = new HashSet<>();
                for (int copy = 0; copy < COPIES_PER_SHARD; copy++) {
                    ids.add(allocationId(name, shard, copy));
                }
                builder.putInSyncAllocationIds(shard, ids);
                builder.primaryTerm(shard, 1L);
            }
        }
        return builder.build();
    }

    /**
     * Twenty-two characters, matching the shape of a real allocation id, which is a base64 UUID. The
     * length matters: the measurement is of strings held per shard, so a short synthetic id would
     * understate the cost.
     */
    private static String allocationId(String indexName, int shard, int copy) {
        char[] padding = new char[22];
        Arrays.fill(padding, 'a');
        String seed = indexName + shard + copy;
        String prefix = seed.length() >= 22 ? seed.substring(0, 22) : seed + new String(padding, 0, 22 - seed.length());
        return prefix;
    }

    private static long measureRetained(Supplier<Object> builder) throws Exception {
        Object warm = builder.get();
        if (warm.hashCode() == Integer.MIN_VALUE) {
            throw new IllegalStateException("unreachable");
        }
        warm = null;
        gc();
        long before = used();
        Object held = builder.get();
        gc();
        long after = used();
        if (held.hashCode() == Integer.MIN_VALUE) {
            throw new IllegalStateException("unreachable");
        }
        return after - before;
    }

    private static long used() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static void gc() throws Exception {
        for (int i = 0; i < 4; i++) {
            System.gc();
            Thread.sleep(120);
        }
    }
}
