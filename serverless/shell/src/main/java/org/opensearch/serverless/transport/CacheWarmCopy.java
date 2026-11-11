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
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.CacheHotSet;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.BlockCache;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Copies a joining search node's share of the cluster's warm cache from the peers that hold it, so that its first
 * searches read memory rather than the object store.
 *
 * <p>A search node that joins cold serves its first searches from S3: every block a query touches is a GET, and the
 * shards it is handed were warm on another node a moment before. Those blocks are worth the same on any node, since a
 * cache key names a shard's storage and a file, not the node. So a joiner asks each search peer which of the peer's
 * readers it will serve once ready ({@link CacheHotSet}), then pulls those blocks, hottest first, over the transport.
 *
 * <p>Bounded on both sides. The joiner paces itself to {@value #MAX_BYTES_PER_SECOND} and asks for
 * {@value #BATCH_BLOCKS} blocks at a time, so a peer serving searches gives up a little bandwidth and no more; the peer
 * answers the listing on its management pool, never the threads that serve searches. A block the peer has evicted by
 * the time it is asked for is skipped, and so is everything from a peer that cannot be reached: the copy is an
 * optimisation, and a block it did not bring is fetched on first read exactly as it would have been without it.
 */
public final class CacheWarmCopy {

    private static final Logger LOG = LogManager.getLogger(CacheWarmCopy.class);

    /** The most bytes a second a joiner copies, across all its peers. */
    public static final String MAX_BYTES_PER_SECOND = "serverless.search.warm_copy.max_bytes_per_second";

    /** How many blocks one transfer asks a peer for. */
    public static final String BATCH_BLOCKS = "serverless.search.warm_copy.batch_blocks";

    /** The default pace: about a gigabit, a share of a node's network a peer serving searches can spare. */
    static final long DEFAULT_MAX_BYTES_PER_SECOND = 128L * 1024 * 1024;

    /** The default batch: 4 MB of 64 KB blocks. */
    static final int DEFAULT_BATCH_BLOCKS = 64;

    /** How many peers a joiner copies from at once. */
    public static final String PARALLEL_PEERS = "serverless.search.warm_copy.parallel_peers";

    /** The default: enough to keep a joiner's network busy without one peer's slowness setting the pace of all. */
    static final int DEFAULT_PARALLEL_PEERS = 8;

    /** The most blocks a peer hands over in one answer, whatever it is asked for. */
    static final int MAX_BATCH_BLOCKS = 1024;

    /** Waits, so a test can count the pauses rather than take them. */
    @FunctionalInterface
    public interface Pause {
        /**
         * Waits.
         *
         * @param nanos how long
         * @throws InterruptedException if interrupted
         */
        void sleep(long nanos) throws InterruptedException;
    }

    /**
     * A shard the joiner will serve.
     *
     * @param indexName the index
     * @param indexUuid the index's uuid
     * @param shard the shard number
     */
    public record Shard(String indexName, String indexUuid, int shard) {
    }

    /**
     * What a copy did.
     *
     * @param shards the shards the joiner will serve, as its peers ranked them
     * @param peersAsked search peers asked
     * @param peersFailed peers that could not be reached or answered with a failure
     * @param blocksCopied blocks stored in this node's cache
     * @param bytesCopied bytes stored
     * @param blocksMissed blocks named that were no longer held, already here, or not reached
     * @param pausedNanos time spent waiting to keep to the pace
     * @param unheld per shard, the blocks peers named as warm that this node still does not hold, hottest first
     */
    public record Outcome(Set<Shard> shards, int peersAsked, int peersFailed, long blocksCopied, long bytesCopied, long blocksMissed,
        long pausedNanos, Map<Shard, List<String>> unheld) {
    }

    private final ShardRouter router;
    private final long maxBytesPerSecond;
    private final int batchBlocks;
    private final int parallelPeers;
    private volatile LongSupplier nanoClock = System::nanoTime;
    private volatile Pause pause = nanos -> Thread.sleep(nanos / 1_000_000L, (int) (nanos % 1_000_000L));

    CacheWarmCopy(ShardRouter router) {
        this.router = router;
        this.maxBytesPerSecond = Math.max(1L, router.node.settings().getAsLong(MAX_BYTES_PER_SECOND, DEFAULT_MAX_BYTES_PER_SECOND));
        this.batchBlocks = Math.max(1, Math.min(MAX_BATCH_BLOCKS, router.node.settings().getAsInt(BATCH_BLOCKS, DEFAULT_BATCH_BLOCKS)));
        this.parallelPeers = Math.max(1, router.node.settings().getAsInt(PARALLEL_PEERS, DEFAULT_PARALLEL_PEERS));
    }

    /**
     * Returns a copier for a node. The node must have started, since its router is built then.
     *
     * <p>Made afresh rather than kept per node: it holds nothing but its settings, and a static map from a node to
     * something that holds the node would keep every node alive.
     *
     * @param node the node
     * @return a copier
     */
    public static CacheWarmCopy of(ServerlessNode node) {
        return new CacheWarmCopy(node.router());
    }

    /**
     * Replaces the clock and the wait the pacing uses.
     *
     * @param clock a nanosecond clock
     * @param wait how to wait
     * @return this
     */
    public CacheWarmCopy pacedBy(LongSupplier clock, Pause wait) {
        this.nanoClock = clock;
        this.pause = wait;
        return this;
    }

    /** Registers the handlers that answer a joiner. Called once, at startup, after the router's own. */
    public void registerHandlers() {
        router.transportService.registerRequestHandler(
            HotSetRequest.ACTION,
            ThreadPool.Names.MANAGEMENT,
            HotSetRequest::new,
            this::handleHotSet
        );
        router.transportService.registerRequestHandler(
            CacheBlocksRequest.ACTION,
            ThreadPool.Names.GENERIC,
            CacheBlocksRequest::new,
            this::handleBlocks
        );
        router.transportService.registerRequestHandler(
            HandOffRequest.ACTION,
            ThreadPool.Names.GENERIC,
            HandOffRequest::new,
            this::handleHandOff
        );
    }

    void handleHandOff(HandOffRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        router.requireMac(HandOffRequest.ACTION, request);
        final MetadataPlane plane = router.plane.get();
        if (plane == null) {
            channel.sendResponse(new HandOffResponse(0, 0));
            return;
        }
        final org.opensearch.serverless.shell.SearchWarmup.Outcome outcome = org.opensearch.serverless.shell.SearchWarmup.takeOverFrom(
            router.node,
            plane,
            request.drainingId()
        );
        channel.sendResponse(new HandOffResponse(outcome.opened(), outcome.copy().blocksCopied()));
    }

    /**
     * Asks a search peer to take over this draining node's share of its readers, and waits until it has: copied the
     * warm blocks, opened the shards.
     *
     * @param peerId the peer
     * @param timeout how long to wait
     * @return its answer, or null if it could not be reached
     * @throws IOException if the request cannot be sent
     */
    public HandOffResponse handOffTo(String peerId, org.opensearch.common.unit.TimeValue timeout) throws IOException {
        final Optional<DiscoveryNode> peer = router.peer(peerId);
        if (peer.isEmpty()) {
            return null;
        }
        return router.send(
            peer.get(),
            HandOffRequest.ACTION,
            new HandOffRequest(router.node.localNode().getId()),
            HandOffResponse::new,
            timeout
        );
    }

    void handleHotSet(HotSetRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        router.requireMac(HotSetRequest.ACTION, request);
        final MetadataPlane plane = router.plane.get();
        if (plane == null) {
            channel.sendResponse(new HotSetResponse(List.of()));
            return;
        }
        Set<NodeLease> members = plane.membership().current();
        if (members.stream().noneMatch(l -> l.nodeId().equals(request.joinerId()))) {
            // The joiner renewed a moment ago and this node's snapshot has not caught up: it asked because it is a member.
            members = plane.membership().refresh();
        }
        final ServerlessNode node = router.node;
        channel.sendResponse(
            new HotSetResponse(
                CacheHotSet.forJoiner(
                    node,
                    members,
                    request.joinerId(),
                    org.opensearch.serverless.cluster.HotShardSpread.of(node, node.settings())
                )
            )
        );
    }

    void handleBlocks(CacheBlocksRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        router.requireMac(CacheBlocksRequest.ACTION, request);
        final List<String> keys = request.keys();
        channel.sendResponse(
            new CacheBlocksResponse(router.node.blockCache().copyOut(keys.subList(0, Math.min(keys.size(), MAX_BATCH_BLOCKS))))
        );
    }

    /**
     * Copies this node's share of its search peers' warm blocks into its cache.
     *
     * @return what was copied, and the shards this node will serve
     * @throws IOException if the membership cannot be read
     */
    public Outcome copyFromPeers() throws IOException {
        return copyFromPeers(1.0, shards -> {});
    }

    /**
     * Copies this node's share of its search peers' warm blocks, saying when a share of them is held; see
     * {@link #copyFrom(List, double, java.util.function.Consumer)}.
     *
     * @param fraction the share of named blocks at which to call {@code atFraction}
     * @param atFraction called once with the shards this node will serve
     * @return what was copied
     * @throws IOException if the membership cannot be read
     */
    public Outcome copyFromPeers(double fraction, java.util.function.Consumer<Set<Shard>> atFraction) throws IOException {
        final MetadataPlane plane = router.plane.get();
        if (plane == null) {
            atFraction.accept(Set.of());
            return new Outcome(Set.of(), 0, 0, 0, 0, 0, 0, Map.of());
        }
        final String self = router.node.localNode().getId();
        final List<String> peers = new ArrayList<>();
        final long now = plane.clock().getAsLong();
        for (NodeLease lease : plane.membership().refresh()) {
            if (lease.nodeId().equals(self) == false
                && lease.roles().contains(ServerlessNode.ROLE_SEARCH)
                && lease.isExpiredAt(now) == false) {
                peers.add(lease.nodeId());
            }
        }
        peers.sort(null);
        return copyFrom(peers, fraction, atFraction);
    }

    /**
     * Copies this node's share of some peers' warm blocks into its cache, whatever roles those peers advertise: a
     * draining peer has already dropped the search role, and is asked precisely because it is leaving.
     *
     * @param peers the peers to copy from
     * @return what was copied, and the shards this node will serve
     */
    public Outcome copyFrom(List<String> peers) {
        return copyFrom(peers, 1.0, shards -> {});
    }

    /**
     * Copies from some peers, all at once, and says when a share of the named blocks is held.
     *
     * <p>Every peer is asked for its hot set together, and a block named by several (a hot shard is spread over
     * several readers) is fetched from only one of them. The blocks are then taken from every peer at once, up to
     * {@value #PARALLEL_PEERS}, within one shared pace: a joiner's time is the slowest peer's, not their sum. Each
     * peer's blocks go hottest first across its shards, so the share that arrives first is the share searches want.
     *
     * @param peers the peers to copy from
     * @param fraction the share of named blocks, from 0 to 1, at which to call {@code atFraction}
     * @param atFraction called once, on a copying thread, with the shards this node will serve, when that share is held,
     *                   or at the end if it never is
     * @return what was copied, and the shards this node will serve
     */
    public Outcome copyFrom(List<String> peers, double fraction, java.util.function.Consumer<Set<Shard>> atFraction) {
        final MetadataPlane plane = router.plane.get();
        if (plane == null) {
            atFraction.accept(Set.of());
            return new Outcome(Set.of(), 0, 0, 0, 0, 0, 0, Map.of());
        }
        final String self = router.node.localNode().getId();
        final BlockCache cache = router.node.blockCache();
        final Pacer pacer = new Pacer();
        final java.util.concurrent.atomic.AtomicInteger failed = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicLong copied = new java.util.concurrent.atomic.AtomicLong();
        final java.util.concurrent.atomic.AtomicLong bytes = new java.util.concurrent.atomic.AtomicLong();
        final java.util.concurrent.atomic.AtomicLong missed = new java.util.concurrent.atomic.AtomicLong();
        try {
            // Every peer's hot set at once.
            final List<java.util.concurrent.Callable<Optional<Map.Entry<DiscoveryNode, HotSetResponse>>>> asks = new ArrayList<>();
            for (String peerId : peers) {
                asks.add(() -> {
                    final Optional<DiscoveryNode> peer = router.peer(peerId);
                    if (peer.isEmpty()) {
                        return Optional.empty();
                    }
                    final HotSetResponse hot = router.send(peer.get(), HotSetRequest.ACTION, new HotSetRequest(self), HotSetResponse::new);
                    return Optional.of(Map.entry(peer.get(), hot));
                });
            }
            final List<java.util.concurrent.Future<Optional<Map.Entry<DiscoveryNode, HotSetResponse>>>> answers = runBounded(asks);
            final Map<String, java.util.concurrent.Future<Optional<Map.Entry<DiscoveryNode, HotSetResponse>>>> asked =
                new LinkedHashMap<>();
            for (int i = 0; i < peers.size(); i++) {
                asked.put(peers.get(i), answers.get(i));
            }
            final Map<Shard, java.util.LinkedHashSet<String>> shards = new LinkedHashMap<>();
            final Map<DiscoveryNode, List<String>> wantedFrom = new LinkedHashMap<>();
            final Set<String> claimed = new java.util.HashSet<>();
            long held = 0;
            for (Map.Entry<String, java.util.concurrent.Future<Optional<Map.Entry<DiscoveryNode, HotSetResponse>>>> answer : asked
                .entrySet()) {
                final Optional<Map.Entry<DiscoveryNode, HotSetResponse>> got;
                try {
                    got = answer.getValue().get();
                } catch (java.util.concurrent.ExecutionException e) {
                    failed.incrementAndGet();
                    LOG.warn("search peer [" + answer.getKey() + "] did not say what it holds for this node", e.getCause());
                    continue;
                }
                if (got.isEmpty()) {
                    failed.incrementAndGet();
                    continue;
                }
                final List<List<String>> perShard = new ArrayList<>();
                for (CacheHotSet.HotShard shard : got.get().getValue().shards()) {
                    shards.computeIfAbsent(
                        new Shard(shard.indexName(), shard.indexUuid(), shard.shard()),
                        k -> new java.util.LinkedHashSet<>()
                    ).addAll(shard.keys());
                    final List<String> mine = new ArrayList<>();
                    for (String key : shard.keys()) {
                        if (cache.holds(key)) {
                            held++;
                            missed.incrementAndGet();
                        } else if (claimed.add(key)) {
                            mine.add(key);
                        }
                    }
                    perShard.add(mine);
                }
                wantedFrom.put(got.get().getKey(), hottestFirst(perShard));
            }
            final Set<Shard> share = Set.copyOf(shards.keySet());
            final Threshold threshold = new Threshold(
                fraction,
                shards.values().stream().mapToLong(Set::size).sum(),
                held,
                () -> atFraction.accept(share)
            );
            // Then every peer's blocks at once.
            final List<java.util.concurrent.Callable<Object>> work = new ArrayList<>();
            for (Map.Entry<DiscoveryNode, List<String>> from : wantedFrom.entrySet()) {
                work.add(() -> {
                    final List<String> wanted = from.getValue();
                    try {
                        for (int i = 0; i < wanted.size(); i += batchBlocks) {
                            final List<String> batch = wanted.subList(i, Math.min(wanted.size(), i + batchBlocks));
                            final CacheBlocksResponse answer = router.send(
                                from.getKey(),
                                CacheBlocksRequest.ACTION,
                                new CacheBlocksRequest(batch),
                                CacheBlocksResponse::new
                            );
                            long batchBytes = 0;
                            int stored = 0;
                            for (Map.Entry<String, byte[]> block : answer.blocks().entrySet()) {
                                if (cache.putCopied(block.getKey(), block.getValue())) {
                                    stored++;
                                    batchBytes += block.getValue().length;
                                } else {
                                    missed.incrementAndGet();
                                }
                            }
                            copied.addAndGet(stored);
                            missed.addAndGet(batch.size() - answer.blocks().size());
                            bytes.addAndGet(batchBytes);
                            threshold.held(stored);
                            pacer.took(batchBytes);
                        }
                    } catch (IOException | RuntimeException e) {
                        // A peer that went away mid-copy costs this node only the blocks it did not hand over.
                        failed.incrementAndGet();
                        LOG.warn("could not copy warm blocks from search peer [" + from.getKey().getId() + "]", e);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                });
            }
            for (java.util.concurrent.Future<?> transfer : runBounded(work)) {
                try {
                    transfer.get();
                } catch (java.util.concurrent.ExecutionException e) {
                    failed.incrementAndGet();
                }
            }
            threshold.end();
            final Map<Shard, List<String>> unheld = new LinkedHashMap<>();
            for (Map.Entry<Shard, java.util.LinkedHashSet<String>> shard : shards.entrySet()) {
                final List<String> keys = shard.getValue().stream().filter(key -> cache.holds(key) == false).toList();
                if (keys.isEmpty() == false) {
                    unheld.put(shard.getKey(), keys);
                }
            }
            return new Outcome(
                Set.copyOf(shards.keySet()),
                peers.size(),
                failed.get(),
                copied.get(),
                bytes.get(),
                missed.get(),
                pacer.paused(),
                unheld
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            atFraction.accept(Set.of());
            return new Outcome(Set.of(), peers.size(), peers.size(), copied.get(), bytes.get(), missed.get(), pacer.paused(), Map.of());
        }
    }

    /**
     * Runs tasks on the node's generic pool, at most {@link #parallelPeers} at a time.
     *
     * @return each task's result, in order
     */
    private <T> List<java.util.concurrent.Future<T>> runBounded(List<java.util.concurrent.Callable<T>> tasks) {
        final List<java.util.concurrent.CompletableFuture<T>> results = new ArrayList<>(tasks.size());
        for (int i = 0; i < tasks.size(); i++) {
            results.add(new java.util.concurrent.CompletableFuture<>());
        }
        final java.util.concurrent.atomic.AtomicInteger next = new java.util.concurrent.atomic.AtomicInteger();
        final Runnable worker = () -> {
            for (int i = next.getAndIncrement(); i < tasks.size(); i = next.getAndIncrement()) {
                try {
                    results.get(i).complete(tasks.get(i).call());
                } catch (Exception e) {
                    results.get(i).completeExceptionally(e);
                }
            }
        };
        final int workers = Math.min(parallelPeers, tasks.size());
        for (int w = 0; w < workers; w++) {
            try {
                router.node.threadPool().generic().execute(worker);
            } catch (RuntimeException rejected) {
                // A pool that refuses leaves the work to the workers already running, or to this thread.
                worker.run();
            }
        }
        return new ArrayList<>(results);
    }

    /** Interleaves shards' hottest-first key lists, so the first blocks taken are the hottest of every shard. */
    static List<String> hottestFirst(List<List<String>> perShard) {
        final List<String> order = new ArrayList<>();
        for (int i = 0, left = perShard.size(); left > 0; i++) {
            left = 0;
            for (List<String> keys : perShard) {
                if (i < keys.size()) {
                    order.add(keys.get(i));
                }
                if (i + 1 < keys.size()) {
                    left++;
                }
            }
        }
        return order;
    }

    /** Calls back once when a share of the named blocks is held, or at the end. */
    private static final class Threshold {
        private final long needed;
        private final Runnable callback;
        private final java.util.concurrent.atomic.AtomicLong held;
        private final java.util.concurrent.atomic.AtomicBoolean called = new java.util.concurrent.atomic.AtomicBoolean();

        Threshold(double fraction, long named, long alreadyHeld, Runnable callback) {
            this.needed = (long) Math.ceil(Math.max(0.0, Math.min(1.0, fraction)) * named);
            this.callback = callback;
            this.held = new java.util.concurrent.atomic.AtomicLong(alreadyHeld);
            if (alreadyHeld >= needed) {
                fire();
            }
        }

        void held(long blocks) {
            if (held.addAndGet(blocks) >= needed) {
                fire();
            }
        }

        void end() {
            fire();
        }

        private void fire() {
            if (called.compareAndSet(false, true)) {
                callback.run();
            }
        }
    }

    /** Keeps the copy to {@link #maxBytesPerSecond} over its whole run. */
    private final class Pacer {
        private final long started = nanoClock.getAsLong();
        private long total;
        private long paused;

        /** Counts a transfer against the shared pace and waits, outside the lock, for as long as it ran ahead. */
        void took(long bytes) throws InterruptedException {
            final long ahead;
            synchronized (this) {
                total += bytes;
                final long due = started + (long) (total * 1e9 / maxBytesPerSecond);
                ahead = due - nanoClock.getAsLong();
                if (ahead > 0) {
                    paused += ahead;
                }
            }
            if (ahead > 0) {
                pause.sleep(ahead);
            }
        }

        synchronized long paused() {
            return paused;
        }
    }
}
