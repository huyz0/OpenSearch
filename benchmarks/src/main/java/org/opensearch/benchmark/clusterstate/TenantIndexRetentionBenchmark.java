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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Spike benchmark for the "pluggable metadata / lazy per-node residency" line of investigation
 * (100M-tenant, one-index-per-tenant scaling). Measures the real per-tenant-index allocation
 * cost of the object graph {@link org.opensearch.cluster.ClusterState} holds one of per index
 * today ({@link IndexMetadata} + a matching {@link RoutingTable} entry, one shard, one alias --
 * the minimum a real tenant index has), against the theoretical floor of a compact,
 * identity-only DTO holding just the fields a routing/existence check would actually need.
 *
 * <p>Run with the GC profiler to read {@code gc.alloc.rate.norm} (bytes allocated per
 * invocation) for each benchmark method -- that number, multiplied by a tenant count, is the
 * real heap-cost estimate this spike exists to produce:
 *
 * <pre>{@code
 * gradlew -p benchmarks run --args ' TenantIndexRetentionBenchmark -prof gc'
 * }</pre>
 *
 * <p><b>Why the "compact" side is a plain DTO, not a real alternative {@link IndexMetadata}:</b>
 * {@link IndexMetadata}'s only full-data constructor is {@code private} (see
 * {@code IndexMetadata.java}), reachable exclusively through {@link IndexMetadata.Builder#build()},
 * which does substantial unconditional per-index work at construction time (settings validation,
 * filling {@code inSyncAllocationIds} for every shard, routing/virtual-shard validation, building
 * {@code DiscoveryNodeFilters}, etc.) -- there is no lazy or partial construction path, and no
 * subclass hook. {@code Metadata.Builder#buildIndicesLookup()} then unconditionally wraps every
 * entry of the hard-typed {@code Map<String, IndexMetadata>} in a real {@code IndexAbstraction.Index}.
 * A "compact stub" that {@code Metadata}/{@code IndexNameExpressionResolver} could actually use is
 * therefore not buildable as an external plugin type; it would require changing
 * {@code IndexMetadata.java} and {@code Metadata.java} in OpenSearch core itself. This benchmark
 * measures the real object's cost and a plain DTO's theoretical-floor cost precisely so that
 * question -- is the gap even worth that core surgery -- can be answered with real numbers before
 * anyone attempts it.
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class TenantIndexRetentionBenchmark {

    // Power of two so `next()` can use a cheap mask instead of modulo. Large enough that the
    // JIT can't special-case a tiny fixed set of inputs, small enough to build once in @Setup
    // without dominating the benchmark's own footprint.
    private static final int POOL_SIZE = 4096;

    private String[] names;
    private String[] uuids;
    private String[] aliasNames;
    private int cursor;

    @Setup(Level.Trial)
    public void setUp() {
        names = new String[POOL_SIZE];
        uuids = new String[POOL_SIZE];
        aliasNames = new String[POOL_SIZE];
        Random random = new Random(42);
        for (int i = 0; i < POOL_SIZE; i++) {
            String uuid = Long.toHexString(random.nextLong()) + Long.toHexString(random.nextLong());
            uuids[i] = uuid;
            names[i] = "tenant-" + uuid;
            aliasNames[i] = "tenant-alias-" + uuid;
        }
        cursor = 0;
    }

    private int next() {
        cursor = (cursor + 1) & (POOL_SIZE - 1);
        return cursor;
    }

    /** The real object graph ClusterState holds one of per tenant index today. */
    @Benchmark
    public void fullIndexMetadataAndRoutingTable(Blackhole bh) {
        int i = next();
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, uuids[i])
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder(names[i])
            .settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder(aliasNames[i]).build())
            .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetadata).build();
        bh.consume(indexMetadata);
        bh.consume(routingTable);
    }

    /**
     * Theoretical floor: only the fields that are semantically irreducible identity data for a
     * tenant index, held in a plain object with no OpenSearch object graph behind it. Not a real
     * Metadata-lookup-compatible object -- see class javadoc.
     */
    @Benchmark
    public void compactStub(Blackhole bh) {
        int i = next();
        bh.consume(new CompactTenantStub(names[i], uuids[i], 1, aliasNames[i]));
    }

    static final class CompactTenantStub {
        final String name;
        final String uuid;
        final int numberOfShards;
        final String aliasName;

        CompactTenantStub(String name, String uuid, int numberOfShards, String aliasName) {
            this.name = name;
            this.uuid = uuid;
            this.numberOfShards = numberOfShards;
            this.aliasName = aliasName;
        }
    }
}
