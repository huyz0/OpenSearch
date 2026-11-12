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
 *
 * <p><b>Waiting for search nodes.</b> A write asked to be refreshed can wait until every search node serving its shard
 * has taken it: see {@link #awaitSearchNodes}. Each shard's acknowledged records take positions in the order they are
 * queued, and each subscribed node is confirmed through the position of the last push it has answered, counting only
 * pushes answered in the order they were sent. A node that subscribes later is confirmed through the position at which it
 * subscribed, because its fetch's answer holds everything kept until then.
 */
public final class TailPush {

    private static final Logger LOG = LogManager.getLogger(TailPush.class);

    /** How long acknowledged records wait for others to share a message, by default. */
    static final long LINGER_MILLIS = 2L;

    /** The node setting for how long acknowledged records wait for others to share a message. */
    public static final String LINGER_SETTING = "serverless.search.tail_overlay.push_linger_millis";

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
        /** How many records have been queued for pushing: the position of the last one. */
        long position;
    }

    /** Records queued for pushing, and the shard's position once they are. */
    private record Acked(ShardId shardId, List<WalRecord> records, long endPosition) {
    }

    /** One search node pushed a shard's writes, and how far it is confirmed to have them. */
    private static final class Subscription {
        /** The shard's position through which this node has every record. */
        long confirmed;
        /** The number the next push of this shard to this node takes. */
        long nextSend;
        /** The lowest push number not yet answered. */
        long nextToConfirm;
        /** Pushes sent and not yet counted, by number: the shard's position each one reaches. */
        final java.util.TreeMap<Long, Long> sent = new java.util.TreeMap<>();
        /** Pushes answered out of order, waiting for an earlier one. */
        final java.util.Set<Long> answered = new java.util.HashSet<>();

        Subscription(long confirmed) {
            this.confirmed = confirmed;
        }

        long send(long endPosition) {
            final long number = nextSend++;
            sent.put(number, endPosition);
            return number;
        }

        void answeredPush(long number) {
            answered.add(number);
            while (answered.remove(nextToConfirm)) {
                final Long reached = sent.remove(nextToConfirm);
                if (reached != null) {
                    confirmed = Math.max(confirmed, reached);
                }
                nextToConfirm++;
            }
        }
    }

    /** A refreshing write waiting for the search nodes serving its shard. */
    private static final class Waiter {
        final long target;
        final java.util.Set<String> pending;
        final java.util.concurrent.CompletableFuture<Boolean> done = new java.util.concurrent.CompletableFuture<>();
        boolean lost;

        Waiter(long target, java.util.Set<String> pending) {
            this.target = target;
            this.pending = pending;
        }
    }

    /** A shard's subscribed search nodes, and the writes waiting for them. Guarded by itself. */
    private static final class Serving {
        final Map<String, Subscription> byNode = new LinkedHashMap<>();
        final List<Waiter> waiters = new ArrayList<>();

        /** Settles each waiter that no longer waits for anybody. */
        void settle() {
            waiters.removeIf(waiter -> {
                waiter.pending.removeIf(node -> {
                    final Subscription subscription = byNode.get(node);
                    return subscription != null && subscription.confirmed >= waiter.target;
                });
                if (waiter.pending.isEmpty()) {
                    waiter.done.complete(waiter.lost == false);
                    return true;
                }
                return false;
            });
        }

        /** Drops a node; a waiter still waiting for it is settled as not reached if the node may still be serving. */
        void drop(String node, boolean mayStillServe) {
            byNode.remove(node);
            for (Waiter waiter : waiters) {
                if (waiter.pending.remove(node) && mayStillServe) {
                    waiter.lost = true;
                }
            }
            settle();
        }

        void dropAll() {
            byNode.clear();
            for (Waiter waiter : waiters) {
                waiter.done.complete(false);
            }
            waiters.clear();
        }
    }

    private final ShardRouter router;
    private final Map<ShardId, Kept> kept = new ConcurrentHashMap<>();
    /** The search nodes serving each shard: those that fetched its tail when they opened it, until they say otherwise. */
    private final Map<ShardId, Serving> subscribers = new ConcurrentHashMap<>();
    /** Held while a flush numbers its pushes, so each node's pushes of a shard are numbered in queue order. */
    private final Object flushLock = new Object();
    private final ConcurrentLinkedQueue<Acked> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean flushScheduled = new AtomicBoolean();
    private final AtomicLong keptBytes = new AtomicLong();
    private final AtomicLong recordsPushed = new AtomicLong();
    private final AtomicLong messagesSent = new AtomicLong();
    private final AtomicLong sendFailures = new AtomicLong();
    private final AtomicLong fetchesServed = new AtomicLong();
    private final AtomicLong waitsStarted = new AtomicLong();
    private final AtomicLong waitsNotReached = new AtomicLong();

    private final long lingerMillis;

    TailPush(ShardRouter router) {
        this.router = router;
        this.lingerMillis = Math.max(0L, router.node.settings().getAsLong(LINGER_SETTING, LINGER_MILLIS));
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
            // Queued under the shard's lock, so positions follow the queue's order.
            shard.position += records.size();
            queue.add(new Acked(shardId, records, shard.position));
        }
        if (flushScheduled.compareAndSet(false, true)) {
            router.node.threadPool().schedule(this::flush, TimeValue.timeValueMillis(lingerMillis), ThreadPool.Names.GENERIC);
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
        final Serving serving = subscribers.remove(shardId);
        if (serving != null) {
            synchronized (serving) {
                serving.dropAll();
            }
        }
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
                final Serving serving = subscribers.remove(shardId);
                if (serving != null) {
                    synchronized (serving) {
                        serving.dropAll();
                    }
                }
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
        synchronized (flushLock) {
            flushScheduled.set(false);
            sweepReleased();
            final Map<ShardId, List<WalRecord>> byShard = new LinkedHashMap<>();
            final Map<ShardId, Long> reached = new HashMap<>();
            for (Acked acked; (acked = queue.poll()) != null;) {
                byShard.computeIfAbsent(acked.shardId(), ignored -> new ArrayList<>()).addAll(acked.records());
                reached.merge(acked.shardId(), acked.endPosition(), Math::max);
            }
            if (byShard.isEmpty()) {
                return;
            }
            final Map<String, Map<ShardId, List<WalRecord>>> byTarget = new HashMap<>();
            final Map<String, Map<ShardId, Numbered>> numbers = new HashMap<>();
            for (Map.Entry<ShardId, List<WalRecord>> entry : byShard.entrySet()) {
                final Serving serving = subscribers.get(entry.getKey());
                if (serving == null) {
                    continue;
                }
                synchronized (serving) {
                    for (Map.Entry<String, Subscription> node : serving.byNode.entrySet()) {
                        byTarget.computeIfAbsent(node.getKey(), ignored -> new LinkedHashMap<>()).put(entry.getKey(), entry.getValue());
                        numbers.computeIfAbsent(node.getKey(), ignored -> new HashMap<>())
                            .put(entry.getKey(), new Numbered(node.getValue(), node.getValue().send(reached.get(entry.getKey()))));
                    }
                }
            }
            for (Map.Entry<String, Map<ShardId, List<WalRecord>>> entry : byTarget.entrySet()) {
                final TailPushRequest request = new TailPushRequest(entry.getValue());
                final int count = entry.getValue().values().stream().mapToInt(List::size).sum();
                final Map<ShardId, Numbered> sent = numbers.get(entry.getKey());
                router.node.threadPool().generic().execute(() -> send(entry.getKey(), request, count, sent));
            }
        }
        if (queue.isEmpty() == false && flushScheduled.compareAndSet(false, true)) {
            router.node.threadPool().generic().execute(this::flush);
        }
    }

    private void send(String target, TailPushRequest request, int count, Map<ShardId, Numbered> numbers) {
        try {
            final Optional<DiscoveryNode> peer = router.peer(target, SEND_TIMEOUT);
            if (peer.isEmpty()) {
                // No live lease: it serves nothing any more.
                for (ShardId shardId : request.records().keySet()) {
                    unsubscribe(shardId, target, false, sentUnder(numbers, shardId));
                }
                return;
            }
            final TailPushResponse answer = router.send(peer.get(), TailPushRequest.ACTION, request, TailPushResponse::new, SEND_TIMEOUT);
            messagesSent.incrementAndGet();
            recordsPushed.addAndGet(count);
            for (ShardId shardId : request.records().keySet()) {
                if (answer.unwanted().contains(shardId)) {
                    // It has no overlay for the shard, so it does not serve it from what it was pushed.
                    unsubscribe(shardId, target, false, sentUnder(numbers, shardId));
                } else {
                    confirm(shardId, target, numbers.get(shardId));
                }
            }
        } catch (Exception e) {
            // Lost. Later pushes would leave a gap in what the search node holds, so it is pushed nothing more: it keeps
            // what it has, which runs without a gap, and subscribes again when it next opens the shard on a commit.
            sendFailures.incrementAndGet();
            for (ShardId shardId : request.records().keySet()) {
                unsubscribe(shardId, target, true, sentUnder(numbers, shardId));
            }
            LOG.debug("could not push acknowledged records to [{}]", target, e);
        }
    }

    private static Subscription sentUnder(Map<ShardId, Numbered> numbers, ShardId shardId) {
        final Numbered numbered = numbers.get(shardId);
        return numbered == null ? null : numbered.subscription();
    }

    private void confirm(ShardId shardId, String target, Numbered numbered) {
        final Serving serving = subscribers.get(shardId);
        if (serving == null || numbered == null) {
            return;
        }
        synchronized (serving) {
            // Only the subscription the push was numbered in: a node that fetched again since has a new one, numbered
            // from zero, which an answer to an older push says nothing about.
            if (serving.byNode.get(target) == numbered.subscription()) {
                numbered.subscription().answeredPush(numbered.number());
                serving.settle();
            }
        }
    }

    /** A push's number within the subscription it was sent under. */
    private record Numbered(Subscription subscription, long number) {
    }

    /**
     * Stops pushing a shard to a node.
     *
     * @param mayStillServe whether the node may still serve the shard from what it holds: a lost push, not an answer
     *     saying it has no overlay or a lease that is gone
     * @param sentUnder the subscription the push was sent under, or null for whichever the node has
     */
    private void unsubscribe(ShardId shardId, String target, boolean mayStillServe, Subscription sentUnder) {
        final Serving serving = subscribers.get(shardId);
        if (serving == null) {
            return;
        }
        synchronized (serving) {
            if (sentUnder != null && serving.byNode.get(target) != sentUnder) {
                // The node fetched again since this push was sent: what became of the push says nothing of the new one.
                return;
            }
            // Left in the map even when empty: a fetch may be subscribing to it now. What this node no longer writes is
            // dropped by the sweep, and when the shard is released.
            serving.drop(target, mayStillServe);
        }
    }

    /**
     * Waits until every search node serving a shard has taken every record this node has acknowledged for it so far, or
     * until a timeout. Pushes what is queued at once rather than after the linger. A node that subscribes meanwhile
     * needs no wait: its fetch's answer holds what was kept when it subscribed.
     *
     * @param shardId the shard
     * @param timeoutMillis how long to wait at most
     * @return true if every node serving the shard confirmed; false if one was not reached, the wait timed out, or this
     *     node let the shard go meanwhile
     */
    public boolean awaitSearchNodes(ShardId shardId, long timeoutMillis) {
        final Kept shard = kept.get(shardId);
        if (shard == null) {
            return true;
        }
        final long target;
        synchronized (shard) {
            target = shard.position;
        }
        final Serving serving = subscribers.get(shardId);
        if (serving == null) {
            return true;
        }
        final Waiter waiter;
        synchronized (serving) {
            final java.util.Set<String> pending = new java.util.HashSet<>();
            for (Map.Entry<String, Subscription> node : serving.byNode.entrySet()) {
                if (node.getValue().confirmed < target) {
                    pending.add(node.getKey());
                }
            }
            if (pending.isEmpty()) {
                return true;
            }
            waiter = new Waiter(target, pending);
            serving.waiters.add(waiter);
        }
        waitsStarted.incrementAndGet();
        router.node.threadPool().generic().execute(this::flush);
        try {
            final boolean reached = waiter.done.get(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
            if (reached == false) {
                waitsNotReached.incrementAndGet();
            }
            return reached;
        } catch (java.util.concurrent.TimeoutException e) {
            waitsNotReached.incrementAndGet();
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (java.util.concurrent.ExecutionException e) {
            return false;
        } finally {
            synchronized (serving) {
                serving.waiters.remove(waiter);
            }
        }
    }

    /** @return refreshing writes that waited for search nodes, and those of them not confirmed by every node */
    public long[] waitCounts() {
        return new long[] { waitsStarted.get(), waitsNotReached.get() };
    }

    /**
     * The search nodes this writer pushes a shard's writes to.
     *
     * @param shardId the shard
     * @return the node ids
     */
    public java.util.Set<String> subscribersOf(ShardId shardId) {
        final Serving serving = subscribers.get(shardId);
        if (serving == null) {
            return java.util.Set.of();
        }
        synchronized (serving) {
            return java.util.Set.copyOf(serving.byNode.keySet());
        }
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
        // Subscribed before the answer is built, so a write acknowledged in between is pushed if it is not in it. Both
        // under the shard's lock, so the position the node is confirmed through is the one the answer covers.
        final Kept shard = kept.computeIfAbsent(request.shardId(), ignored -> new Kept());
        final List<WalRecord> answer;
        synchronized (shard) {
            final Serving serving = subscribers.computeIfAbsent(request.shardId(), ignored -> new Serving());
            synchronized (serving) {
                serving.byNode.put(request.subscriber(), new Subscription(shard.position));
                serving.settle();
            }
            answer = keptAfter(request.shardId(), request.afterSeqNo());
        }
        channel.sendResponse(new TailFetchResponse(answer));
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
