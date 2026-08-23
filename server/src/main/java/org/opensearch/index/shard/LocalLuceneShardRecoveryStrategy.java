/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.index.store.Store;

/**
 * Core's own {@link ShardRecoveryStrategy}, registered under {@link
 * ShardRecoveryStrategy#LOCAL_LUCENE} and the default for every index: the shard's local Lucene
 * files on this node's disk <em>are</em> its authoritative copy, so there is nowhere else to
 * materialize them from and nothing that can make a readable local commit stale.
 *
 * <p>Registered into the same map plugins contribute to rather than special-cased ahead of it --
 * the same dogfooding shape {@code RepositoriesModule} uses for {@code fs}, {@code
 * IndexModule#createBuiltInDirectoryFactories} for {@code niofs}/{@code mmapfs}/{@code hybridfs},
 * and {@code ClusterModule} for the balanced allocator. Every answer here is the "no" core gave
 * before this seam existed, so a stock node behaves identically whether it resolves this strategy
 * or, as it did previously, simply had no seam to consult:
 *
 * <ul>
 *   <li>{@link #recoverLocalStore} {@code false} for {@code EXISTING_STORE} leaves {@code
 *       StoreRecovery} on its original path of failing the shard with {@code "shard allocated for
 *       local recovery (post api), should exist, but doesn't"}.
 *   <li>{@link #recoverLocalStore} {@code false} for the two in-place-resharding types leaves such
 *       a shard treated as a plain new empty index, which is what core does for them on its own.
 *   <li>The inherited {@code localStoreIsStale}/{@code ownsRemoteSegmentDurability}/{@code
 *       engineNativeSnapshots} defaults are the same "no" as well, and are inherited rather than
 *       restated so the default and core's own answer cannot drift apart.
 * </ul>
 *
 * @opensearch.internal
 */
public final class LocalLuceneShardRecoveryStrategy implements ShardRecoveryStrategy {

    /** The single, stateless instance registered under {@link ShardRecoveryStrategy#LOCAL_LUCENE}. */
    public static final LocalLuceneShardRecoveryStrategy INSTANCE = new LocalLuceneShardRecoveryStrategy();

    private LocalLuceneShardRecoveryStrategy() {}

    @Override
    public boolean recoverLocalStore(IndexShard indexShard, Store store, RecoverySource.Type recoverySourceType) {
        return false;
    }
}
