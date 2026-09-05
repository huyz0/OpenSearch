/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Node-local, in-memory index of every {@link ObjectStoreReaderEngine} currently open on this
 * node, by (indexUuid, shardId) -- the reader-tier counterpart to {@code
 * org.opensearch.serverless.storage.writerengine.ShardActivityRegistry}, making {@link
 * ObjectStoreReaderEngine#manifestGenerationLag()} reachable from outside the engine itself (a
 * transport action, eventually a future autoscaling controller), rfc-serverless-opensearch.md
 * &sect;10's still-open "search tier: manifest-generation lag" autoscaling hook.
 *
 * <p>Same "one instance shared node-wide, {@link WeakReference} not a strong one" shape as that
 * writer-tier registry, for the identical reason: this plugin has no hook into a reader engine's
 * own {@code close()} to explicitly deregister, so a strong reference would leak every reader
 * engine this node has ever hosted, across every shard relocation, for the node's entire lifetime.
 */
public final class ReaderShardActivityRegistry {

    private final Map<String, WeakReference<ObjectStoreReaderEngine>> engines = new ConcurrentHashMap<>();

    /** Creates an empty registry; entries are added only via {@link #register}. */
    public ReaderShardActivityRegistry() {}

    private static String key(String indexUuid, int shardId) {
        return indexUuid + "/" + shardId;
    }

    /**
     * Registers {@code engine} as the currently-live reader engine for (indexUuid, shardId) on
     * this node, replacing whatever was previously registered under the same key.
     *
     * @param indexUuid the UUID of the index the shard belongs to
     * @param shardId the shard number within {@code indexUuid}
     * @param engine the engine to register
     */
    public void register(String indexUuid, int shardId, ObjectStoreReaderEngine engine) {
        engines.put(key(indexUuid, shardId), new WeakReference<>(engine));
        // Hand the engine a way back here so it can remove itself the moment it stops being live,
        // rather than lingering as a stale-but-strongly-reachable entry until the GC happens to
        // clear the WeakReference. The window matters: between an engine failing and its
        // WeakReference clearing, every lag/idle/query-rate lookup and every pollNow for this shard
        // is answered by a dead engine, which is worse than answering "not on this node".
        engine.attachActivityRegistry(this, indexUuid, shardId);
    }

    /**
     * Removes {@code engine}'s entry for (indexUuid, shardId), but only if that is still the
     * engine registered there.
     *
     * <p>Conditional, not an unconditional remove, for the same reason {@code
     * ShardDirectory#dropIfMatches} is: a shard that relocated away and back, or was closed and
     * reopened, has already registered a <em>newer</em> engine under this key by the time the old
     * one gets around to cleaning up, and dropping that would blind every lookup for a shard that
     * is perfectly healthy.
     *
     * @param indexUuid the UUID of the index the shard belongs to
     * @param shardId the shard number within {@code indexUuid}
     * @param engine the engine that is no longer live
     */
    public void unregister(String indexUuid, int shardId, ObjectStoreReaderEngine engine) {
        String mapKey = key(indexUuid, shardId);
        WeakReference<ObjectStoreReaderEngine> current = engines.get(mapKey);
        if (current != null && current.get() == engine) {
            engines.remove(mapKey, current);
        }
    }

    /**
     * Looks up how far behind the currently-live reader engine for (indexUuid, shardId) on this
     * node is from the latest manifest generation it has observed published.
     *
     * @param indexUuid the UUID of the index the shard belongs to
     * @param shardId the shard number within {@code indexUuid}
     * @return the reader engine's own {@link ObjectStoreReaderEngine#manifestGenerationLag()}, or
     *         empty if no reader engine for this shard is currently tracked on this node (never
     *         registered, or already collected after closing)
     */
    public Optional<Long> manifestGenerationLag(String indexUuid, int shardId) {
        WeakReference<ObjectStoreReaderEngine> ref = engines.get(key(indexUuid, shardId));
        ObjectStoreReaderEngine engine = ref == null ? null : ref.get();
        return engine == null ? Optional.empty() : Optional.of(engine.manifestGenerationLag());
    }

    /**
     * Looks up how long since the currently-live reader engine for (indexUuid, shardId) on this
     * node last saw a real client-facing search/get (rfc-serverless-opensearch.md &sect;7.3's
     * reader-shard scale-to-zero query-activity signal).
     *
     * @param indexUuid the UUID of the index the shard belongs to
     * @param shardId the shard number within {@code indexUuid}
     * @return the reader engine's own {@link ObjectStoreReaderEngine#millisSinceLastQuery()}, or
     *         empty if no reader engine for this shard is currently tracked on this node
     */
    public Optional<Long> millisSinceLastQuery(String indexUuid, int shardId) {
        WeakReference<ObjectStoreReaderEngine> ref = engines.get(key(indexUuid, shardId));
        ObjectStoreReaderEngine engine = ref == null ? null : ref.get();
        return engine == null ? Optional.empty() : Optional.of(engine.millisSinceLastQuery());
    }

    /**
     * Looks up the currently-live reader engine for (indexUuid, shardId) on this node and, if
     * found, asynchronously waits on its own {@link ObjectStoreReaderEngine#waitForGeneration(long,
     * org.opensearch.common.unit.TimeValue, org.opensearch.core.action.ActionListener)} -- the
     * lookup half of &sect;8's read-after-write mechanism, letting a transport action reach a
     * specific node's specific reader engine instance by (indexUuid, shardId) alone, without
     * blocking the calling thread for the wait's duration (see that method's own javadoc for why
     * that matters).
     *
     * @param indexUuid the UUID of the index the shard belongs to
     * @param shardId the shard number within {@code indexUuid}
     * @param minGeneration the manifest generation the reader engine must reach
     * @param timeout how long to wait before giving up
     * @param listener resolved with empty if no reader engine for this shard is currently tracked
     *                  on this node; otherwise with {@code true}/{@code false} per {@link
     *                  ObjectStoreReaderEngine#waitForGeneration(long, org.opensearch.common.unit.TimeValue,
     *                  org.opensearch.core.action.ActionListener)}.
     */
    public void waitForGeneration(
        String indexUuid,
        int shardId,
        long minGeneration,
        org.opensearch.common.unit.TimeValue timeout,
        org.opensearch.core.action.ActionListener<Optional<Boolean>> listener
    ) {
        WeakReference<ObjectStoreReaderEngine> ref = engines.get(key(indexUuid, shardId));
        ObjectStoreReaderEngine engine = ref == null ? null : ref.get();
        if (engine == null) {
            listener.onResponse(Optional.empty());
            return;
        }
        engine.waitForGeneration(
            minGeneration,
            timeout,
            org.opensearch.core.action.ActionListener.wrap(reached -> listener.onResponse(Optional.of(reached)), listener::onFailure)
        );
    }

    /**
     * Looks up the currently-live reader engine for (indexUuid, shardId) on this node and, if
     * found, forces it to {@link ObjectStoreReaderEngine#pollNow()} -- the lookup half of
     * &sect;8's publication notification mechanism.
     *
     * @param indexUuid the UUID of the index the shard belongs to
     * @param shardId the shard number within {@code indexUuid}
     * @return {@code true} if a reader engine was found and polled; {@code false} if no reader
     *         engine for this shard is currently tracked on this node
     */
    public boolean pollNow(String indexUuid, int shardId) {
        WeakReference<ObjectStoreReaderEngine> ref = engines.get(key(indexUuid, shardId));
        ObjectStoreReaderEngine engine = ref == null ? null : ref.get();
        if (engine == null) {
            return false;
        }
        engine.pollNow();
        return true;
    }

    /**
     * A point-in-time snapshot of every reader engine currently tracked on this node, keyed the
     * same {@code "indexUuid/shardId"} way as {@code
     * org.opensearch.serverless.storage.writerengine.ShardActivityRegistry#snapshotAll} -- silently
     * skips any entry whose {@link WeakReference} has already been collected.
     *
     * @return every (indexUuid, shardId) key together with that shard's current manifest-generation lag
     */
    public Map<String, Long> snapshotAll() {
        Map<String, Long> snapshot = new java.util.HashMap<>();
        for (Map.Entry<String, WeakReference<ObjectStoreReaderEngine>> entry : engines.entrySet()) {
            ObjectStoreReaderEngine engine = entry.getValue().get();
            if (engine != null) {
                snapshot.put(entry.getKey(), engine.manifestGenerationLag());
            }
        }
        return snapshot;
    }

    /**
     * A point-in-time snapshot of every reader engine currently tracked on this node's own {@link
     * ObjectStoreReaderEngine#millisSinceLastQuery()} -- the query-activity counterpart to {@link
     * #snapshotAll()}'s manifest-lag snapshot, silently skipping any entry whose {@link
     * WeakReference} has already been collected.
     *
     * @return every (indexUuid, shardId) key together with that shard's current query idle time
     */
    public Map<String, Long> snapshotQueryIdleMillis() {
        Map<String, Long> snapshot = new java.util.HashMap<>();
        for (Map.Entry<String, WeakReference<ObjectStoreReaderEngine>> entry : engines.entrySet()) {
            ObjectStoreReaderEngine engine = entry.getValue().get();
            if (engine != null) {
                snapshot.put(entry.getKey(), engine.millisSinceLastQuery());
            }
        }
        return snapshot;
    }

    /**
     * Looks up the currently-live reader engine for (indexUuid, shardId) on this node's own {@link
     * ObjectStoreReaderEngine#queriesPerMinute()} -- the scale-up counterpart to {@link
     * #millisSinceLastQuery}'s scale-to-zero signal.
     *
     * @param indexUuid the UUID of the index the shard belongs to
     * @param shardId the shard number within {@code indexUuid}
     * @return the reader engine's own {@link ObjectStoreReaderEngine#queriesPerMinute()}, or empty
     *         if no reader engine for this shard is currently tracked on this node
     */
    public Optional<Long> queriesPerMinute(String indexUuid, int shardId) {
        WeakReference<ObjectStoreReaderEngine> ref = engines.get(key(indexUuid, shardId));
        ObjectStoreReaderEngine engine = ref == null ? null : ref.get();
        return engine == null ? Optional.empty() : Optional.of(engine.queriesPerMinute());
    }

    /**
     * A point-in-time snapshot of every reader engine currently tracked on this node's own {@link
     * ObjectStoreReaderEngine#queriesPerMinute()} -- the scale-up counterpart to {@link
     * #snapshotQueryIdleMillis()}, silently skipping any entry whose {@link WeakReference} has
     * already been collected.
     *
     * @return every (indexUuid, shardId) key together with that shard's current queries-per-minute estimate
     */
    public Map<String, Long> snapshotQueriesPerMinute() {
        Map<String, Long> snapshot = new java.util.HashMap<>();
        for (Map.Entry<String, WeakReference<ObjectStoreReaderEngine>> entry : engines.entrySet()) {
            ObjectStoreReaderEngine engine = entry.getValue().get();
            if (engine != null) {
                snapshot.put(entry.getKey(), engine.queriesPerMinute());
            }
        }
        return snapshot;
    }
}
