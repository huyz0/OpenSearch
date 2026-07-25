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
import org.opensearch.cluster.metadata.LazyIndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;

import java.util.Locale;
import java.util.function.Supplier;

/**
 * What a whole {@link Metadata} retains per index, materialized versus deferred.
 *
 * <p>{@code StubResolutionSpike} (S10) measured a bare map of minimal stubs and got 2,074 -> 146 B.
 * That is an upper bound on the saving, not the shipped one: it left out the descriptor fields
 * {@link Metadata} actually needs to build its derived arrays and {@code indicesLookup} (aliases, the
 * hidden and system flags, the shard count, the routing-pool inputs), and it left out
 * {@code Metadata} itself, which keeps one {@code IndexAbstraction} plus several name-array entries per
 * index whether or not the index is deferred. This measures the real thing: build a {@link Metadata},
 * hold it, and see what it costs.
 *
 * <pre>{@code
 * java -da -dsa -cp <benchmarks runtime classpath> \
 *   org.opensearch.benchmark.clusterstate.DeferredMetadataHeapEstimate
 * }</pre>
 */
public final class DeferredMetadataHeapEstimate {

    private DeferredMetadataHeapEstimate() {}

    private static final int INDICES = 100_000;

    public static void main(String[] args) throws Exception {
        System.out.println("Metadata retained heap for " + INDICES + " indices\n");

        long materialized = measureRetained(() -> {
            Metadata.Builder builder = Metadata.builder();
            for (int i = 0; i < INDICES; i++) {
                builder.put(index(name(i)), false);
            }
            return builder.build();
        });

        long deferred = measureRetained(() -> {
            Metadata.Builder builder = Metadata.builder();
            for (int i = 0; i < INDICES; i++) {
                // The loader captures nothing that would keep the metadata alive -- the point is that
                // an unread index costs its descriptor and nothing more.
                final String indexName = name(i);
                builder.putStub(LazyIndexMetadata.of(index(indexName), loaderFor(indexName)));
            }
            return builder.build();
        });

        System.out.printf(
            Locale.ROOT,
            "materialized  : %8.1f MB  (%,.0f B/index)%n",
            materialized / 1024.0 / 1024,
            (double) materialized / INDICES
        );
        System.out.printf(
            Locale.ROOT,
            "deferred      : %8.1f MB  (%,.0f B/index)%n",
            deferred / 1024.0 / 1024,
            (double) deferred / INDICES
        );
        System.out.printf(Locale.ROOT, "reduction     : %8.1fx%n", (double) materialized / deferred);
    }

    /** Deliberately a separate method so the lambda captures only the name, not an IndexMetadata. */
    private static Supplier<IndexMetadata> loaderFor(String name) {
        return () -> index(name);
    }

    private static String name(int i) {
        return "tenant-" + Integer.toHexString(i);
    }

    private static IndexMetadata index(String name) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid-000000000000")
            )
            .numberOfShards(3)
            .numberOfReplicas(1)
            .putAlias(AliasMetadata.builder(name + "-alias").build())
            .build();
    }

    private static long measureRetained(java.util.function.Supplier<Object> builder) throws Exception {
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
            throw new IllegalStateException("unreachable, keeps held live");
        }
        return after - before;
    }

    private static void gc() throws InterruptedException {
        for (int i = 0; i < 5; i++) {
            System.gc();
            Thread.sleep(150);
        }
    }

    private static long used() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }
}
