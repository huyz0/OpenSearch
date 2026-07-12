/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Node-local, in-memory index of every {@link ObjectStoreWriterEngine} currently open on this
 * node, by (indexUuid, shardId) -- the seam that makes {@link ObjectStoreWriterEngine#millisSinceLastActivity()}
 * reachable from outside the engine itself (a transport action, eventually a future scale-to-zero
 * controller), rfc-serverless-opensearch.md &sect;16 Phase 4's next open increment after the
 * signal itself landed.
 *
 * <p>One instance is shared node-wide (constructed once in {@code ServerlessStoragePlugin}, the
 * same shape as {@code sharedBundleCache}/{@code sharedWalChunkService}), not one per shard --
 * every writer shard on this node registers itself into the same map.
 *
 * <p>Holds a {@link WeakReference}, not a strong one, to each registered engine: this plugin has
 * no hook into an engine's own {@code close()} to explicitly deregister from here, so a strong
 * reference would keep every writer engine this node has ever hosted alive forever, a real memory
 * leak across shard relocations. A {@link WeakReference} instead lets a closed engine actually be
 * collected once core drops its own last strong reference to it -- {@link #millisSinceLastActivity}
 * simply reports "not currently tracked" for an entry whose referent has already been collected,
 * which is the correct answer for a shard that no longer has a live writer engine on this node
 * anyway. The transiently stale key left behind until the next GC cycle is harmless: it is
 * self-correcting garbage, not something that needs cleaning up.
 */
public final class ShardActivityRegistry {

    private final Map<String, WeakReference<ObjectStoreWriterEngine>> engines = new ConcurrentHashMap<>();

    /** Creates an empty registry; entries are added only via {@link #register}. */
    public ShardActivityRegistry() {}

    private static String key(String indexUuid, int shardId) {
        return indexUuid + "/" + shardId;
    }

    /**
     * Registers {@code engine} as the currently-live writer engine for (indexUuid, shardId) on
     * this node, replacing whatever was previously registered under the same key (a prior engine
     * for the same shard, if this node has hosted it before and it was reallocated back).
     *
     * @param indexUuid the UUID of the index the shard belongs to
     * @param shardId the shard number within {@code indexUuid}
     * @param engine the engine to register
     */
    public void register(String indexUuid, int shardId, ObjectStoreWriterEngine engine) {
        engines.put(key(indexUuid, shardId), new WeakReference<>(engine));
    }

    /**
     * Looks up how long the currently-live writer engine for (indexUuid, shardId) on this node has
     * been idle.
     *
     * @param indexUuid the UUID of the index the shard belongs to
     * @param shardId the shard number within {@code indexUuid}
     * @return how long it's been since the currently-live writer engine for (indexUuid, shardId)
     *         on this node last saw a real client write, or empty if no writer engine for this
     *         shard is currently tracked on this node (never registered, or already collected
     *         after closing)
     */
    public Optional<Long> millisSinceLastActivity(String indexUuid, int shardId) {
        WeakReference<ObjectStoreWriterEngine> ref = engines.get(key(indexUuid, shardId));
        ObjectStoreWriterEngine engine = ref == null ? null : ref.get();
        return engine == null ? Optional.empty() : Optional.of(engine.millisSinceLastActivity());
    }

    /**
     * Looks up the currently-live writer engine for (indexUuid, shardId) on this node and, if
     * found, performs a real-time {@code get} by document id directly against its own live
     * version map (rfc-serverless-opensearch.md &sect;8) -- the lookup half of the "route to the
     * writer for true realtime gets" mechanism, letting a transport action reach a specific node's
     * specific writer engine instance by (indexUuid, shardId) alone, the same shape as {@link
     * org.opensearch.serverless.storage.readerengine.ReaderShardActivityRegistry#waitForGeneration}
     * on the reader side.
     *
     * @param indexUuid the UUID of the index the shard belongs to
     * @param shardId the shard number within {@code indexUuid}
     * @param id the document id to look up
     * @return empty if no writer engine for this shard is currently tracked on this node;
     *         otherwise the real-time existence/version/seqNo result
     */
    public Optional<RealtimeGetResult> realtimeGet(String indexUuid, int shardId, String id) {
        WeakReference<ObjectStoreWriterEngine> ref = engines.get(key(indexUuid, shardId));
        ObjectStoreWriterEngine engine = ref == null ? null : ref.get();
        return engine == null ? Optional.empty() : Optional.of(engine.realtimeGet(id));
    }

    /**
     * A point-in-time snapshot of every writer engine currently tracked on this node -- the
     * cluster-wide counterpart to {@link #millisSinceLastActivity}, letting a caller (an
     * autoscaling/suspension controller, {@code rfc-serverless-opensearch.md} &sect;7.3/&sect;10's
     * still-open "signal collection" bullet) discover which shards on this node are idle, and by
     * how much, without already knowing every (indexUuid, shardId) it might want to ask about.
     *
     * <p>Silently skips any entry whose {@link WeakReference} has already been collected (a closed
     * engine's stale key -- see this class's own javadoc for why that key is harmless, self-correcting
     * garbage) rather than reporting it with a stale or default value.
     *
     * @return every (indexUuid, shardId) key (see {@link #key}'s {@code "indexUuid/shardId"} format)
     *         together with how long that shard's writer engine has been idle on this node, for
     *         every entry whose engine is still live
     */
    public Map<String, Long> snapshotAll() {
        Map<String, Long> snapshot = new java.util.HashMap<>();
        for (Map.Entry<String, WeakReference<ObjectStoreWriterEngine>> entry : engines.entrySet()) {
            ObjectStoreWriterEngine engine = entry.getValue().get();
            if (engine != null) {
                snapshot.put(entry.getKey(), engine.millisSinceLastActivity());
            }
        }
        return snapshot;
    }

    /**
     * Looks up the currently-live writer engine's own {@link ObjectStoreWriterEngine#writesPerMinute()}
     * for (indexUuid, shardId) -- the write-rate counterpart to {@link #millisSinceLastActivity},
     * reachable from outside the engine itself for whenever a future consumer needs it (see {@link
     * ObjectStoreWriterEngine#writesPerMinute()}'s own javadoc for why nothing consumes it yet).
     *
     * @param indexUuid the UUID of the index the shard belongs to
     * @param shardId the shard number within {@code indexUuid}
     * @return the currently-live writer engine's own {@link ObjectStoreWriterEngine#writesPerMinute()},
     *         or empty if no writer engine for this shard is currently tracked on this node
     */
    public Optional<Long> writesPerMinute(String indexUuid, int shardId) {
        WeakReference<ObjectStoreWriterEngine> ref = engines.get(key(indexUuid, shardId));
        ObjectStoreWriterEngine engine = ref == null ? null : ref.get();
        return engine == null ? Optional.empty() : Optional.of(engine.writesPerMinute());
    }

    /**
     * A point-in-time snapshot of every tracked writer engine's own {@link
     * ObjectStoreWriterEngine#writesPerMinute()} -- the write-rate counterpart to {@link
     * #snapshotAll()}, silently skipping any entry whose {@link WeakReference} has already been
     * collected, same as {@link #snapshotAll()} does.
     *
     * @return every (indexUuid, shardId) key (see {@link #key}'s {@code "indexUuid/shardId"} format)
     *         together with that shard's writer engine's own {@link
     *         ObjectStoreWriterEngine#writesPerMinute()}, for every entry whose engine is still live
     */
    public Map<String, Long> snapshotWritesPerMinute() {
        Map<String, Long> snapshot = new java.util.HashMap<>();
        for (Map.Entry<String, WeakReference<ObjectStoreWriterEngine>> entry : engines.entrySet()) {
            ObjectStoreWriterEngine engine = entry.getValue().get();
            if (engine != null) {
                snapshot.put(entry.getKey(), engine.writesPerMinute());
            }
        }
        return snapshot;
    }
}
