/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.directory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;

/**
 * A single directory node's view (rfc-serverless-metadata-plane.md &sect;11: "tens of directory
 * nodes, DynamoDB-style"): an in-memory, TTL-expiring map, no persistence, no replication, no
 * partitioning across peers. This class is deliberately just that one node's correct local
 * behavior -- sharding lookups/reports across many directory node instances (so no single
 * instance holds all 2M active-shard entries) and replicating each entry for availability are
 * separate, not-yet-built concerns this class is the correct foundation for, exactly as
 * {@code LocalDiskCachingBundleStore} is the foundation an eviction policy would sit on top of.
 *
 * <p>Because a directory entry is only ever a hint (see {@link ShardDirectory}'s javadoc), simple
 * TTL expiry is sufficient correctness: a stale entry is indistinguishable from a wrong one to a
 * caller, and both are handled the same way (a wasted routing hop, then fallback to a shard-head
 * read) regardless of which one is actually true. No active eviction thread is needed either --
 * expired entries are simply skipped and lazily removed on next lookup, keeping this class free
 * of any background lifecycle to manage.
 */
public final class InMemoryShardDirectory implements ShardDirectory {

    private final ConcurrentMap<Key, ShardDirectoryEntry> entriesByKey = new ConcurrentHashMap<>();
    private final LongSupplier nowMillisSupplier;

    public InMemoryShardDirectory() {
        this(System::currentTimeMillis);
    }

    /** Package-visible for tests that need deterministic control over "now" to test TTL expiry. */
    InMemoryShardDirectory(LongSupplier nowMillisSupplier) {
        this.nowMillisSupplier = nowMillisSupplier;
    }

    @Override
    public Optional<ShardDirectoryEntry> lookup(String indexUuid, int shardId) {
        Key key = new Key(indexUuid, shardId);
        ShardDirectoryEntry entry = entriesByKey.get(key);
        if (entry == null) {
            return Optional.empty();
        }
        if (entry.isExpired(nowMillisSupplier.getAsLong())) {
            // Best-effort cleanup, not required for correctness: only remove if it's still the
            // exact entry we just observed as expired, so a concurrent report() isn't clobbered.
            entriesByKey.remove(key, entry);
            return Optional.empty();
        }
        return Optional.of(entry);
    }

    @Override
    public void report(String indexUuid, int shardId, ShardDirectoryEntry entry) {
        entriesByKey.put(new Key(indexUuid, shardId), entry);
    }

    @Override
    public void drop(String indexUuid, int shardId) {
        entriesByKey.remove(new Key(indexUuid, shardId));
    }

    /** Exposed only for tests/metrics -- the number of entries currently held, expired or not. */
    int size() {
        return entriesByKey.size();
    }

    private static final class Key {
        private final String indexUuid;
        private final int shardId;

        Key(String indexUuid, int shardId) {
            this.indexUuid = indexUuid;
            this.shardId = shardId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key)) return false;
            Key key = (Key) o;
            return shardId == key.shardId && indexUuid.equals(key.indexUuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexUuid, shardId);
        }
    }
}
