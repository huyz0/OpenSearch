/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.engine.OverlayReadOnlyEngine;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The search node's half of the tail overlay: takes the acknowledged writes a writer pushes, and the ones it fetches
 * when it opens a shard, and holds them for the shard's {@link OverlayReadOnlyEngine} until a commit holds them.
 *
 * <p>Opening a shard is what subscribes this node to its writes: the fetch names this node, and the writer pushes the
 * shard's next writes to the nodes that fetched. A push for a shard with no overlay here is answered "not wanted", and
 * the writer stops. So a writer pushes only to the search nodes that serve the shard right now -- one to a few --
 * however many search nodes there are, and a shard nobody searches costs no network at all.
 *
 * <p>What a shard holds here runs without a gap past its commit: a push that is lost, a record the writer had no room
 * to keep, or one there is no room for here, each stops the pushes until the shard is next opened on a commit, which
 * fetches what follows it again. Until then the reader serves its commit and what it held before the gap.
 *
 * <p>Records are kept only for shards with an overlay open here, or that had one a moment ago: a reader moving onto a
 * new commit closes one engine and opens the next, and what was pushed in between is what the next one needs. Each
 * record takes an ordinal as it arrives, so an engine reads each once, in arrival order; the engine orders them by
 * sequence number itself. When an engine opens on a commit it drops every record its commit already holds.
 */
public final class TailOverlays implements OverlayReadOnlyEngine.Tail {

    private static final Logger LOG = LogManager.getLogger(TailOverlays.class);

    /**
     * The node's default for whether writes are searchable before they are published: writers push what they
     * acknowledge to the search nodes serving the shard, and those search it on top of the commit. On by default.
     */
    public static final String ENABLED = "serverless.search.tail_overlay.enabled";

    /** The index setting, which wins over the cluster's: {@code true} or {@code false}, dynamic. */
    public static final String INDEX_SETTING = "index.serverless.tail_overlay";

    /** The cluster setting, in the {@code cluster/config} register, which wins over the node's. */
    public static final String CLUSTER_SETTING = "serverless.tail_overlay";

    /**
     * How long a shard whose writes its search nodes already see may wait to be published, when its index sets no
     * refresh interval: the node's publish age is raised to this. Publishing then decides only what a new reader opens
     * and how much a successor replays, not how soon a write is searchable, and each publish is PUTs.
     */
    public static final String PUBLISH_MAX_AGE = "serverless.search.tail_overlay.publish_max_age_millis";

    /** The default of {@link #PUBLISH_MAX_AGE}. */
    public static final long DEFAULT_PUBLISH_MAX_AGE_MILLIS = 10_000L;

    /** The index setting's definition. */
    public static final org.opensearch.common.settings.Setting<String> INDEX = new org.opensearch.common.settings.Setting<>(
        INDEX_SETTING,
        "",
        TailOverlays::parse,
        org.opensearch.common.settings.Setting.Property.IndexScope,
        org.opensearch.common.settings.Setting.Property.Dynamic
    );

    /** How long a shard's records are kept after its last overlay closed, for the one about to open. */
    static final long KEEP_AFTER_CLOSE_NANOS = java.util.concurrent.TimeUnit.SECONDS.toNanos(30);

    /** How many records are held for a shard; past it, more wait for the commit. */
    static final int MAX_RECORDS_PER_SHARD = OverlayReadOnlyEngine.MAX_DOCUMENTS * 2;

    /**
     * The node's default.
     *
     * @param settings the node's settings
     * @return true if on
     */
    public static boolean enabled(Settings settings) {
        return settings.getAsBoolean(ENABLED, true);
    }

    static String parse(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        final String lower = value.toLowerCase(java.util.Locale.ROOT);
        if ("true".equals(lower) == false && "false".equals(lower) == false) {
            throw new IllegalArgumentException("tail overlay must be [true] or [false], got [" + value + "]");
        }
        return lower;
    }

    /**
     * Checks a value given for the index or the cluster setting.
     *
     * @param value the value
     * @throws IllegalArgumentException if it is neither true nor false
     */
    public static void check(String value) {
        parse(value);
    }

    /**
     * Whether a shard of an index has its writes pushed and searched before they are published: the index's setting,
     * then the cluster's, then the node's.
     *
     * @param indexSettings the index's settings, or null
     * @param cluster the cluster's value, or null
     * @param nodeDefault the node's default
     * @return true if on
     */
    public static boolean on(Settings indexSettings, String cluster, boolean nodeDefault) {
        final String index = indexSettings == null ? null : indexSettings.get(INDEX_SETTING);
        for (String value : new String[] { index, cluster }) {
            if (value == null || value.isEmpty()) {
                continue;
            }
            try {
                return "true".equals(parse(value));
            } catch (IllegalArgumentException e) {
                // A value the register holds that is neither: the next, less specific, setting decides.
            }
        }
        return nodeDefault;
    }

    private volatile String cluster;

    /**
     * Takes the cluster's settings as last read or written.
     *
     * @param settings the cluster's settings
     */
    public void applyCluster(Settings settings) {
        this.cluster = settings.get(CLUSTER_SETTING);
    }

    /**
     * Whether a shard with these index settings has its writes pushed and searched before they are published.
     *
     * @param indexSettings the index's settings
     * @return true if on
     */
    public boolean onFor(Settings indexSettings) {
        return on(indexSettings, cluster, enabled);
    }

    /**
     * Whether an open shard has its writes pushed and searched before they are published.
     *
     * @param shardId the shard
     * @return true if on; false if it is not open here
     */
    public boolean onFor(ShardId shardId) {
        final var shard = router.node.reconciler() == null ? null : router.node.reconciler().shard(shardId);
        return shard != null && onFor(shard.indexSettings().getSettings());
    }

    /** @return the publish age for shards whose writes are searched before they are published */
    public long publishMaxAgeMillis() {
        return publishMaxAgeMillis;
    }

    /** What is held for one shard. */
    private static final class Held {
        final ConcurrentSkipListMap<Long, WalRecord> byOrdinal = new ConcurrentSkipListMap<>();
        final AtomicLong nextOrdinal = new AtomicLong();
        int engines;
        long localCheckpoint = -1L;
        long releasedAtNanos;
        /** Set when a record did not fit: later ones would leave a gap, so none is taken until an engine opens anew. */
        boolean refusing;
    }

    private final ShardRouter router;
    private final boolean enabled;
    private final Map<ShardId, Held> held = new ConcurrentHashMap<>();
    private final long publishMaxAgeMillis;
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong fetched = new AtomicLong();

    TailOverlays(ShardRouter router) {
        this.router = router;
        this.enabled = enabled(router.node.settings());
        this.publishMaxAgeMillis = router.node.settings().getAsLong(PUBLISH_MAX_AGE, DEFAULT_PUBLISH_MAX_AGE_MILLIS);
    }

    /** Registers the handler that takes pushes. Called once, at startup, after the router's own. */
    public void registerHandlers() {
        router.transportService.registerRequestHandler(
            TailPushRequest.ACTION,
            ThreadPool.Names.GENERIC,
            TailPushRequest::new,
            this::handle
        );
    }

    /** @return records taken from pushes, and from fetches */
    public long[] counts() {
        return new long[] { received.get(), fetched.get() };
    }

    void handle(TailPushRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        router.requireMac(TailPushRequest.ACTION, request);
        final java.util.Set<ShardId> unwanted = new java.util.LinkedHashSet<>();
        for (Map.Entry<ShardId, List<WalRecord>> entry : request.records().entrySet()) {
            final int taken = take(entry.getKey(), entry.getValue());
            if (taken < 0) {
                unwanted.add(entry.getKey());
            } else {
                received.addAndGet(taken);
            }
        }
        channel.sendResponse(new TailPushResponse(unwanted));
    }

    /** Holds records for a shard if an overlay wants them; returns how many were taken, or -1 if none is wanted. */
    int take(ShardId shardId, List<WalRecord> records) {
        final Held shard = held.get(shardId);
        if (shard == null) {
            return -1;
        }
        int taken = 0;
        synchronized (shard) {
            if (shard.engines == 0 && System.nanoTime() - shard.releasedAtNanos > KEEP_AFTER_CLOSE_NANOS) {
                held.remove(shardId, shard);
                return -1;
            }
            if (shard.refusing) {
                return -1;
            }
            for (WalRecord record : records) {
                if (record.seqNo() <= shard.localCheckpoint) {
                    continue;
                }
                if (shard.byOrdinal.size() >= MAX_RECORDS_PER_SHARD) {
                    // Full: the writer stops pushing, and the reader serves what it holds until it opens a newer commit.
                    shard.refusing = true;
                    return -1;
                }
                shard.byOrdinal.put(shard.nextOrdinal.getAndIncrement(), record);
                taken++;
            }
        }
        return taken;
    }

    @Override
    public void track(ShardId shardId, long localCheckpoint) {
        final Held shard = held.computeIfAbsent(shardId, ignored -> new Held());
        synchronized (shard) {
            shard.engines++;
            shard.localCheckpoint = Math.max(shard.localCheckpoint, localCheckpoint);
            // A new engine fetches what follows its commit, so what was refused is asked for again.
            shard.refusing = false;
            shard.byOrdinal.values().removeIf(record -> record.seqNo() <= localCheckpoint);
        }
    }

    @Override
    public void untrack(ShardId shardId) {
        final Held shard = held.get(shardId);
        if (shard == null) {
            return;
        }
        synchronized (shard) {
            shard.engines = Math.max(0, shard.engines - 1);
            if (shard.engines == 0) {
                shard.releasedAtNanos = System.nanoTime();
            }
        }
        // Shards nobody reopened are let go of here, as well as when a push for them arrives.
        final long now = System.nanoTime();
        held.entrySet().removeIf(entry -> {
            synchronized (entry.getValue()) {
                return entry.getValue().engines == 0 && now - entry.getValue().releasedAtNanos > KEEP_AFTER_CLOSE_NANOS;
            }
        });
    }

    @Override
    public long nextOrdinal(ShardId shardId) {
        final Held shard = held.get(shardId);
        return shard == null ? 0L : shard.nextOrdinal.get();
    }

    @Override
    public Map<Long, WalRecord> from(ShardId shardId, long fromOrdinal) {
        final Held shard = held.get(shardId);
        return shard == null ? Map.of() : shard.byOrdinal.tailMap(fromOrdinal, true);
    }

    /**
     * Asks a shard's writer, in the background, for what it has acknowledged past the commit just opened here, which
     * also has it push the shard's next writes here. Does nothing if overlays are off for the shard or the commit names
     * no writer.
     *
     * @param shardId the shard just opened
     * @param writer the node that published the commit
     * @param localCheckpoint the commit's local checkpoint
     */
    public void opened(ShardId shardId, String writer, long localCheckpoint) {
        if (writer == null || writer.equals(router.node.localNode().getId()) || onFor(shardId) == false) {
            return;
        }
        router.node.threadPool().generic().execute(() -> {
            try {
                final Optional<DiscoveryNode> peer = router.peer(writer, TailPush.SEND_TIMEOUT);
                if (peer.isEmpty()) {
                    return;
                }
                final TailFetchResponse answer = router.send(
                    peer.get(),
                    TailFetchRequest.ACTION,
                    new TailFetchRequest(shardId, localCheckpoint, router.node.localNode().getId()),
                    TailFetchResponse::new,
                    TailPush.SEND_TIMEOUT
                );
                fetched.addAndGet(Math.max(0, take(shardId, answer.records())));
            } catch (Exception e) {
                // The pushes still come; until they do, the commit is what the reader serves.
                LOG.debug("could not fetch the acknowledged tail of {} from [{}]", shardId, writer, e);
            }
        });
    }
}
