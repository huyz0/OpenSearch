/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Node-local, in-memory index of every reader shard's {@link LocalDiskCachingBundleStore} on this
 * node, by (indexUuid, shardId) -- makes rfc-serverless-opensearch.md &sect;9's "cache hit-rate and
 * cold-read latency are first-class metrics" goal reachable from outside the engine itself (a
 * transport action), the same shape {@link org.opensearch.serverless.storage.readerengine.ReaderShardActivityRegistry}
 * already established for manifest-generation lag.
 *
 * <p>Same "{@link WeakReference}, not a strong one" reasoning as that registry: this plugin has no
 * hook into a reader shard's disk cache being discarded on shard close, so a strong reference would
 * leak every reader shard this node has ever hosted, across every relocation, for the node's entire
 * lifetime.
 */
public final class CacheStatsRegistry {

    private final Map<String, WeakReference<LocalDiskCachingBundleStore>> diskCaches = new ConcurrentHashMap<>();

    /** Creates an empty registry; entries are added only via {@link #register}. */
    public CacheStatsRegistry() {}

    private static String key(String indexUuid, int shardId) {
        return indexUuid + "/" + shardId;
    }

    /**
     * Registers {@code diskCache} as the currently-live local disk cache for (indexUuid, shardId)
     * on this node, replacing whatever was previously registered under the same key.
     *
     * @param indexUuid the UUID of the index the shard belongs to
     * @param shardId the shard number within {@code indexUuid}
     * @param diskCache the disk cache to register
     */
    public void register(String indexUuid, int shardId, LocalDiskCachingBundleStore diskCache) {
        diskCaches.put(key(indexUuid, shardId), new WeakReference<>(diskCache));
    }

    /**
     * Forgets the entry for (indexUuid, shardId) -- called when that shard's local data, cache
     * directory included, is actually gone from this node. The {@link WeakReference} above means a
     * stale entry is not a leak, but it is a lie until the store is collected: {@link #snapshotAll()}
     * would keep reporting hit/miss counters for a shard this node no longer has. Idempotent; a key
     * that was never registered simply isn't there.
     *
     * @param indexUuid the UUID of the index the shard belongs to
     * @param shardId the shard number within {@code indexUuid}
     */
    public void deregister(String indexUuid, int shardId) {
        diskCaches.remove(key(indexUuid, shardId));
    }

    /**
     * Forgets every shard entry for {@code indexUuid} at once -- the index-deletion counterpart to
     * {@link #deregister}, which does not require knowing how many shards this node happened to
     * host. Idempotent, and safe to call for an index that never had a reader shard here.
     *
     * @param indexUuid the UUID of the deleted index
     */
    public void deregisterIndex(String indexUuid) {
        String prefix = indexUuid + "/";
        diskCaches.keySet().removeIf(key -> key.startsWith(prefix));
    }

    /** One reader shard's local disk cache stats, as reported by {@link #snapshotAll()}. */
    public record ShardCacheStats(String indexUuid, int shardId, long hitCount, long missCount, long averageColdReadLatencyMillis) {

        /**
         * Creates an entry.
         *
         * @param indexUuid UUID of the index the shard belongs to.
         * @param shardId the shard number within {@code indexUuid}.
         * @param hitCount reads served from this shard's local disk cache so far.
         * @param missCount reads that missed this shard's local disk cache so far.
         * @param averageColdReadLatencyMillis average wall-clock time spent fetching from the
         *                                     delegate on a cache miss, 0 if there have been no
         *                                     misses yet.
         */
        public ShardCacheStats {
        }

        /** UUID of the index the shard belongs to. */
        @Override
        public String indexUuid() {
            return indexUuid;
        }

        /** The shard number within {@link #indexUuid()}. */
        @Override
        public int shardId() {
            return shardId;
        }

        /** Reads served from this shard's local disk cache so far. */
        @Override
        public long hitCount() {
            return hitCount;
        }

        /** Reads that missed this shard's local disk cache so far. */
        @Override
        public long missCount() {
            return missCount;
        }

        /** Average wall-clock time, in milliseconds, spent fetching from the delegate on a cache miss, 0 if there have been no misses yet. */
        @Override
        public long averageColdReadLatencyMillis() {
            return averageColdReadLatencyMillis;
        }
    }

    /**
     * A point-in-time snapshot of every reader shard's local disk cache stats currently tracked on
     * this node -- silently skips any entry whose {@link WeakReference} has already been collected.
     *
     * @return every (indexUuid, shardId) key's currently-live disk cache stats
     */
    public List<ShardCacheStats> snapshotAll() {
        List<ShardCacheStats> snapshot = new ArrayList<>();
        for (Map.Entry<String, WeakReference<LocalDiskCachingBundleStore>> entry : diskCaches.entrySet()) {
            LocalDiskCachingBundleStore diskCache = entry.getValue().get();
            if (diskCache == null) {
                continue;
            }
            String key = entry.getKey();
            int separator = key.lastIndexOf('/');
            String indexUuid = key.substring(0, separator);
            int shardId = Integer.parseInt(key.substring(separator + 1));
            snapshot.add(
                new ShardCacheStats(
                    indexUuid,
                    shardId,
                    diskCache.hitCount(),
                    diskCache.missCount(),
                    diskCache.averageColdReadLatencyMillis()
                )
            );
        }
        return snapshot;
    }
}
