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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The writer's half of the tail overlay: hands each write it acknowledges to the shard's search nodes, and keeps what it
 * has acknowledged and not yet published so a search node opening the shard can ask for it.
 *
 * <p><b>Who is pushed to.</b> Only the search nodes serving the shard now: a search node subscribes when it opens the
 * shard, by fetching what the writer holds past its commit, and is dropped when it answers a push saying it no longer
 * has the shard open, or its lease is gone. However many search nodes there are, a shard's writes go to the one to
 * few that serve it, and a shard nobody searches sends nothing.
 *
 * <p>Writes acknowledged within {@value #LINGER_MILLIS} ms of each other go out together, one message per search
 * node for every shard it serves: a log window acknowledges its writes at once, so a window is about one message per
 * search node. Records are handed over only after the write is acknowledged -- after its log append has landed -- so a
 * search node never sees a write its writer could still fail. What is kept for a shard is dropped as its publishes
 * cover it, and is bounded per shard; a reader whose writer has dropped some of what it asks for gets what remains and
 * serves the commit for the rest, as before overlays. No object-store request is made. On or off per index or
 * cluster; see {@link TailOverlays#onFor(org.opensearch.common.settings.Settings)}.
 */
public final class TailPush {

    private static final Logger LOG = LogManager.getLogger(TailPush.class);

    /** How long acknowledged records wait for others to share a message. */
    static final long LINGER_MILLIS = 2L;

    /** How long a push or a fetch waits for its answer. */
    static final TimeValue SEND_TIMEOUT = TimeValue.timeValueMillis(500);

    /** How many unpublished records are kept for one shard before the oldest are dropped. */
    static final int MAX_KEPT_PER_SHARD = 20_000;

    /** How many bytes of unpublished records are kept for one shard before the oldest are dropped. */
    static final long MAX_KEPT_BYTES_PER_SHARD = 16L * 1024 * 1024;

    /** How many bytes of unpublished records this node keeps across its shards; past it, records are pushed but not kept. */
    static final long MAX_KEPT_BYTES_PER_NODE = 256L * 1024 * 1024;

    /** A shard's acknowledged records not yet in a published commit. */
    private static final class Kept {
        final ArrayDeque<WalRecord> records = new ArrayDeque<>();
        long bytes;
        long publishedThrough = -1L;
        /** The highest sequence number acknowledged here and not kept, for want of room: a fetch below it has a gap. */
        long droppedThrough = -1L;
    }

    private record Acked(ShardId shardId, List<WalRecord> records) {
    }

    private final ShardRouter router;
    private final Map<ShardId, Kept> kept = new ConcurrentHashMap<>();
    /** The search nodes serving each shard: those that fetched its tail when they opened it, until they say otherwise. */
    private final Map<ShardId, java.util.Set<String>> subscribers = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Acked> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean flushScheduled = new AtomicBoolean();
    private final AtomicLong keptBytes = new AtomicLong();
    private final AtomicLong recordsPushed = new AtomicLong();
    private final AtomicLong messagesSent = new AtomicLong();
    private final AtomicLong sendFailures = new AtomicLong();
    private final AtomicLong fetchesServed = new AtomicLong();

    TailPush(ShardRouter router) {
        this.router = router;
    }

    /** Registers the handler that answers search nodes' fetches. Called once, at startup, after the router's own. */
    public void registerHandlers() {
        router.transportService.registerRequestHandler(
            TailFetchRequest.ACTION,
            ThreadPool.Names.GENERIC,
            TailFetchRequest::new,
            this::handleFetch
        );
    }

    /** @return records pushed, messages sent, failed sends, and fetches served */
    public long[] counts() {
        return new long[] { recordsPushed.get(), messagesSent.get(), sendFailures.get(), fetchesServed.get() };
    }

    /**
     * Takes records this node has just acknowledged: keeps them until published, and pushes them to the shard's search
     * nodes shortly. Does nothing if overlays are off for the shard.
     *
     * @param shardId the shard
     * @param records the records, each with its sequence number
     */
    public void acked(ShardId shardId, List<WalRecord> records) {
        if (records.isEmpty() || router.tailOverlays().onFor(shardId) == false) {
            return;
        }
        final Kept shard = kept.computeIfAbsent(shardId, ignored -> new Kept());
        synchronized (shard) {
            for (WalRecord record : records) {
                if (record.seqNo() <= shard.publishedThrough) {
                    continue;
                }
                if (keptBytes.get() > MAX_KEPT_BYTES_PER_NODE) {
                    shard.droppedThrough = Math.max(shard.droppedThrough, record.seqNo());
                    continue;
                }
                shard.records.addLast(record);
                final long bytes = WalRecord.contentBytes(record.id(), record.source());
                shard.bytes += bytes;
                keptBytes.addAndGet(bytes);
            }
            while (shard.records.size() > MAX_KEPT_PER_SHARD || shard.bytes > MAX_KEPT_BYTES_PER_SHARD) {
                final WalRecord dropped = shard.records.pollFirst();
                shard.droppedThrough = Math.max(shard.droppedThrough, dropped.seqNo());
                final long bytes = WalRecord.contentBytes(dropped.id(), dropped.source());
                shard.bytes -= bytes;
                keptBytes.addAndGet(-bytes);
            }
        }
        queue.add(new Acked(shardId, records));
        if (flushScheduled.compareAndSet(false, true)) {
            router.node.threadPool().schedule(this::flush, TimeValue.timeValueMillis(LINGER_MILLIS), ThreadPool.Names.GENERIC);
        }
    }

    /**
     * Drops what a shard's published commit now holds: every record at or below its local checkpoint.
     *
     * @param shardId the shard
     * @param localCheckpoint the published commit's local checkpoint
     */
    public void published(ShardId shardId, long localCheckpoint) {
        final Kept shard = kept.get(shardId);
        if (shard == null) {
            return;
        }
        synchronized (shard) {
            shard.publishedThrough = Math.max(shard.publishedThrough, localCheckpoint);
            shard.records.removeIf(record -> {
                if (record.seqNo() <= localCheckpoint) {
                    final long bytes = WalRecord.contentBytes(record.id(), record.source());
                    shard.bytes -= bytes;
                    keptBytes.addAndGet(-bytes);
                    return true;
                }
                return false;
            });
        }
    }

    /**
     * Forgets a shard this node no longer writes.
     *
     * @param shardId the shard
     */
    public void released(ShardId shardId) {
        subscribers.remove(shardId);
        final Kept shard = kept.remove(shardId);
        if (shard != null) {
            synchronized (shard) {
                keptBytes.addAndGet(-shard.bytes);
            }
        }
    }

    /**
     * The records kept for a shard above a sequence number, oldest first.
     *
     * @param shardId the shard
     * @param afterSeqNo the sequence number
     * @return the records
     */
    public List<WalRecord> keptAfter(ShardId shardId, long afterSeqNo) {
        final Kept shard = kept.get(shardId);
        if (shard == null) {
            return List.of();
        }
        synchronized (shard) {
            final List<WalRecord> out = new ArrayList<>();
            for (WalRecord record : shard.records) {
                if (record.seqNo() > afterSeqNo) {
                    out.add(record);
                }
            }
            return out;
        }
    }

    /** How often what is kept for shards no longer written here is let go of. */
    static final long SWEEP_NANOS = java.util.concurrent.TimeUnit.SECONDS.toNanos(30);

    private volatile long sweptAtNanos = System.nanoTime();

    /** Lets go of what is kept for shards this node no longer writes, however they were let go of. */
    private void sweepReleased() {
        final long now = System.nanoTime();
        if (now - sweptAtNanos < SWEEP_NANOS) {
            return;
        }
        sweptAtNanos = now;
        final var reconciler = router.node.reconciler();
        if (reconciler == null) {
            return;
        }
        for (ShardId shardId : List.copyOf(kept.keySet())) {
            if (reconciler.shard(shardId) == null || reconciler.isReader(shardId)) {
                released(shardId);
            }
        }
        for (ShardId shardId : List.copyOf(subscribers.keySet())) {
            if (reconciler.shard(shardId) == null || reconciler.isReader(shardId)) {
                subscribers.remove(shardId);
            }
        }
    }

    /**
     * Whether anything is kept for a shard, so its publishes have something to drop.
     *
     * @param shardId the shard
     * @return true if records are kept
     */
    public boolean keeps(ShardId shardId) {
        return kept.containsKey(shardId);
    }

    void flush() {
        flushScheduled.set(false);
        sweepReleased();
        final Map<ShardId, List<WalRecord>> byShard = new LinkedHashMap<>();
        for (Acked acked; (acked = queue.poll()) != null;) {
            byShard.computeIfAbsent(acked.shardId(), ignored -> new ArrayList<>()).addAll(acked.records());
        }
        if (byShard.isEmpty()) {
            return;
        }
        final Map<String, Map<ShardId, List<WalRecord>>> byTarget = new HashMap<>();
        for (Map.Entry<ShardId, List<WalRecord>> entry : byShard.entrySet()) {
            final java.util.Set<String> serving = subscribers.get(entry.getKey());
            if (serving == null) {
                continue;
            }
            for (String target : serving) {
                byTarget.computeIfAbsent(target, ignored -> new LinkedHashMap<>()).put(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<String, Map<ShardId, List<WalRecord>>> entry : byTarget.entrySet()) {
            final TailPushRequest request = new TailPushRequest(entry.getValue());
            final int count = entry.getValue().values().stream().mapToInt(List::size).sum();
            router.node.threadPool().generic().execute(() -> send(entry.getKey(), request, count));
        }
        if (queue.isEmpty() == false && flushScheduled.compareAndSet(false, true)) {
            router.node.threadPool().generic().execute(this::flush);
        }
    }

    private void send(String target, TailPushRequest request, int count) {
        try {
            final Optional<DiscoveryNode> peer = router.peer(target, SEND_TIMEOUT);
            if (peer.isEmpty()) {
                // No live lease: it serves nothing any more.
                for (ShardId shardId : request.records().keySet()) {
                    unsubscribe(shardId, target);
                }
                return;
            }
            final TailPushResponse answer = router.send(peer.get(), TailPushRequest.ACTION, request, TailPushResponse::new, SEND_TIMEOUT);
            messagesSent.incrementAndGet();
            recordsPushed.addAndGet(count);
            for (ShardId shardId : answer.unwanted()) {
                unsubscribe(shardId, target);
            }
        } catch (Exception e) {
            // Lost. Later pushes would leave a gap in what the search node holds, so it is pushed nothing more: it keeps
            // what it has, which runs without a gap, and subscribes again when it next opens the shard on a commit.
            sendFailures.incrementAndGet();
            for (ShardId shardId : request.records().keySet()) {
                unsubscribe(shardId, target);
            }
            LOG.debug("could not push acknowledged records to [{}]", target, e);
        }
    }

    private void unsubscribe(ShardId shardId, String target) {
        subscribers.computeIfPresent(shardId, (id, set) -> {
            set.remove(target);
            return set.isEmpty() ? null : set;
        });
    }

    /**
     * The search nodes this writer pushes a shard's writes to.
     *
     * @param shardId the shard
     * @return the node ids
     */
    public java.util.Set<String> subscribersOf(ShardId shardId) {
        return java.util.Set.copyOf(subscribers.getOrDefault(shardId, java.util.Set.of()));
    }

    void handleFetch(TailFetchRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        router.requireMac(TailFetchRequest.ACTION, request);
        fetchesServed.incrementAndGet();
        if (router.tailOverlays().onFor(request.shardId()) == false || gapAfter(request.shardId(), request.afterSeqNo())) {
            // Off, or this node let go of writes past that commit for want of room: the search node serves its commit
            // until it opens a newer one, rather than a tail with a gap in it.
            channel.sendResponse(new TailFetchResponse(List.of()));
            return;
        }
        // Subscribed before the answer is built, so a write acknowledged in between is pushed if it is not in it.
        subscribers.computeIfAbsent(request.shardId(), ignored -> ConcurrentHashMap.newKeySet()).add(request.subscriber());
        channel.sendResponse(new TailFetchResponse(keptAfter(request.shardId(), request.afterSeqNo())));
    }

    /** Whether a write past this sequence number was acknowledged here and not kept. */
    boolean gapAfter(ShardId shardId, long afterSeqNo) {
        final Kept shard = kept.get(shardId);
        if (shard == null) {
            return false;
        }
        synchronized (shard) {
            return shard.droppedThrough > afterSeqNo;
        }
    }
}
