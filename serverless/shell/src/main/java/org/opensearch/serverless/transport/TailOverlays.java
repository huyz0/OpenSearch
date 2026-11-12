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
 *
 * <p>In {@code everywhere} mode a shard answers here only while this node holds a grant from its writer; see
 * {@link #ensureFresh}.
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
        /** Set while the writer is being asked for what it acknowledged past the commit an engine just opened on. */
        boolean fetching;
        /** Set when an engine left out a record for a reason other than holding a newer write; cleared by a new engine. */
        boolean skipped;
        /** The writer this node is pushed the shard by, under {@link #subscription}; null if none. */
        String grantWriter;
        /** The subscription, or {@link TailFetchResponse#NONE}. */
        long subscription = TailFetchResponse.NONE;
        /** Whether what the subscription's fetch answered, with the commit, held every write its writer acknowledged. */
        boolean complete;
        /** When this node's grant runs out, on the nano clock. */
        long grantUntilNanos;
        /** Held while the writer is asked for a grant, so searches that find none ask once between them. */
        final Object asking = new Object();

        /** Whether the shard may answer under a grant from this writer now. Under the lock. */
        boolean granted(String writer, long now) {
            return engines > 0
                && refusing == false
                && skipped == false
                && complete
                && subscription >= 0
                && writer.equals(grantWriter)
                && grantUntilNanos - now > 0;
        }
    }

    private final ShardRouter router;
    private final boolean enabled;
    private final Map<ShardId, Held> held = new ConcurrentHashMap<>();
    private final long publishMaxAgeMillis;
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong fetched = new AtomicLong();
    private final AtomicLong grantChecks = new AtomicLong();
    private final AtomicLong refusals = new AtomicLong();
    private final AtomicLong unreachable = new AtomicLong();

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

    /**
     * @return round trips made to a writer for a grant before a search, searches refused for want of one, and searches
     *     answered from the commit because the writer could not be reached
     */
    public long[] grantCounts() {
        return new long[] { grantChecks.get(), refusals.get(), unreachable.get() };
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
    public void awaitFetch(ShardId shardId) {
        final Held shard = held.get(shardId);
        if (shard == null) {
            return;
        }
        synchronized (shard) {
            final long deadline = System.nanoTime() + TailPush.SEND_TIMEOUT.nanos();
            while (shard.fetching) {
                final long left = deadline - System.nanoTime();
                if (left <= 0) {
                    return;
                }
                try {
                    java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(shard, left);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    @Override
    public void track(ShardId shardId, long localCheckpoint) {
        final Held shard = held.computeIfAbsent(shardId, ignored -> new Held());
        synchronized (shard) {
            shard.engines++;
            shard.localCheckpoint = Math.max(shard.localCheckpoint, localCheckpoint);
            // A new engine fetches what follows its commit, so what was refused is asked for again.
            shard.refusing = false;
            // It applies every record held from the first, so what an earlier engine left out is tried again.
            shard.skipped = false;
            shard.byOrdinal.values().removeIf(record -> record.seqNo() <= localCheckpoint);
        }
    }

    @Override
    public void skipped(ShardId shardId) {
        final Held shard = held.get(shardId);
        if (shard != null) {
            synchronized (shard) {
                shard.skipped = true;
            }
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
     * Asks a shard's writer for what it has acknowledged past the commit just opened here, which also has it push the
     * shard's next writes here. In the background, unless refreshing writes wait for search nodes: then before the reader
     * answers its first search. Does nothing if overlays are off for the shard or the commit names
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
        final Runnable fetch = () -> {
            try {
                final long sentNanos = System.nanoTime();
                final TailFetchResponse answer = ask(shardId, writer, localCheckpoint, TailFetchRequest.NEW);
                if (answer != null) {
                    answered(shardId, writer, TailFetchRequest.NEW, sentNanos, answer);
                }
            } catch (Exception e) {
                // The pushes still come; until they do, the commit is what the reader serves.
                LOG.debug("could not fetch the acknowledged tail of {} from [{}]", shardId, writer, e);
            }
        };
        if (router.refreshVisibility().waitsForSearchNodes()) {
            // Before the reader answers anything: a write acknowledged before this node subscribed is in the answer and
            // nowhere else, and its writer does not wait for a node it did not know served the shard. A search that finds
            // the engine open meanwhile waits for the answer too; see awaitFetch.
            final Held shard = held.get(shardId);
            if (shard != null) {
                synchronized (shard) {
                    shard.fetching = true;
                }
            }
            try {
                fetch.run();
            } finally {
                if (shard != null) {
                    synchronized (shard) {
                        shard.fetching = false;
                        shard.notifyAll();
                    }
                }
            }
        } else {
            router.node.threadPool().generic().execute(fetch);
        }
    }

    /** Asks a shard's writer; null if it holds no live lease. */
    private TailFetchResponse ask(ShardId shardId, String writer, long afterSeqNo, long subscription) throws Exception {
        final Optional<DiscoveryNode> peer = router.peer(writer, TailPush.SEND_TIMEOUT);
        if (peer.isEmpty()) {
            return null;
        }
        return router.send(
            peer.get(),
            TailFetchRequest.ACTION,
            new TailFetchRequest(shardId, afterSeqNo, router.node.localNode().getId(), subscription),
            TailFetchResponse::new,
            TailPush.SEND_TIMEOUT
        );
    }

    /** Takes a writer's answer to a fetch or a renewal sent at a moment on the nano clock. */
    private void answered(ShardId shardId, String writer, long asked, long sentNanos, TailFetchResponse answer) {
        final int taken = take(shardId, answer.records());
        fetched.addAndGet(Math.max(0, taken));
        final Held shard = held.get(shardId);
        if (shard == null) {
            return;
        }
        synchronized (shard) {
            if (answer.subscription() == TailFetchResponse.NONE) {
                if (writer.equals(shard.grantWriter)) {
                    shard.grantWriter = null;
                    shard.subscription = TailFetchResponse.NONE;
                }
                return;
            }
            final boolean renewed = asked >= 0 && answer.subscription() == asked && writer.equals(shard.grantWriter);
            if (renewed == false) {
                // A subscription anew: the records it answered, with the commit, are all there is to go on.
                shard.grantWriter = writer;
                shard.subscription = answer.subscription();
                shard.complete = answer.complete() && taken >= 0;
                shard.grantUntilNanos = sentNanos;
            }
            final long until = sentNanos + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(RefreshVisibility.GRANT_MILLIS);
            if (until - shard.grantUntilNanos > 0) {
                shard.grantUntilNanos = until;
            }
        }
    }

    /**
     * Whether a shard open here as a reader may answer a search that started before this call, in {@code everywhere}
     * mode: whether it holds every write its writer may have answered as refreshed. Always true in the other modes.
     *
     * <p>Under a grant from the writer, it does: see {@link TailPush}. Without one it asks the writer, once for every
     * search that finds none meanwhile: for a renewal of the subscription it holds, or a subscription anew with what
     * is kept past its commit. One that still holds no grant -- no overlay, a gap, a record its engine left out, a
     * writer that does not answer -- may answer only if its commit holds every write the writer says it may have
     * answered as refreshed.
     *
     * @param shardId the shard
     * @param writer the shard's live writer, which is not this node
     * @param commitCheckpoint the local checkpoint of the commit the reader is open on
     * @param applyPending has the reader's engine take what is held for it, so a record it cannot take is known
     * @return true if it may answer
     */
    public boolean ensureFresh(ShardId shardId, String writer, long commitCheckpoint, Runnable applyPending) {
        if (router.refreshVisibility().grants() == false) {
            return true;
        }
        if (grantedNow(shardId, writer)) {
            applyPending.run();
            if (grantedNow(shardId, writer)) {
                return true;
            }
        }
        long claimedThrough = Long.MAX_VALUE;
        boolean asked = false;
        final Held before = held.get(shardId);
        synchronized (before == null ? new Object() : before.asking) {
            if (grantedNow(shardId, writer) == false) {
                final Held shard = held.get(shardId);
                long subscription = TailFetchRequest.CHECK;
                if (shard != null) {
                    synchronized (shard) {
                        if (shard.engines > 0 && shard.refusing == false && shard.skipped == false) {
                            subscription = writer.equals(shard.grantWriter) && shard.subscription >= 0
                                ? shard.subscription
                                : TailFetchRequest.NEW;
                        }
                    }
                }
                grantChecks.incrementAndGet();
                final long sentNanos = System.nanoTime();
                try {
                    final TailFetchResponse answer = ask(shardId, writer, commitCheckpoint, subscription);
                    if (answer != null) {
                        asked = true;
                        claimedThrough = answer.claimedThrough();
                        if (subscription != TailFetchRequest.CHECK) {
                            answered(shardId, writer, subscription, sentNanos, answer);
                        }
                    }
                } catch (Exception e) {
                    LOG.debug("could not ask the writer [{}] of {} for a grant", writer, shardId, e);
                }
            }
        }
        applyPending.run();
        if (grantedNow(shardId, writer)) {
            return true;
        }
        if (asked == false) {
            try {
                final TailFetchResponse answer = ask(shardId, writer, commitCheckpoint, TailFetchRequest.CHECK);
                if (answer == null) {
                    unreachable.incrementAndGet();
                    return true;
                }
                claimedThrough = answer.claimedThrough();
            } catch (Exception e) {
                // A writer that cannot be reached is treated as one that died: the reader serves its commit, as it does
                // for any acknowledged write until the shard is taken over, rather than fail every search meanwhile.
                LOG.debug("could not reach the writer [{}] of {}; serving its commit", writer, shardId, e);
                unreachable.incrementAndGet();
                return true;
            }
        }
        if (commitCheckpoint >= claimedThrough) {
            return true;
        }
        refusals.incrementAndGet();
        return false;
    }

    private boolean grantedNow(ShardId shardId, String writer) {
        final Held shard = held.get(shardId);
        if (shard == null) {
            return false;
        }
        synchronized (shard) {
            return shard.granted(writer, System.nanoTime());
        }
    }
}
