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
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.serverless.cluster.HotShardSpread;
import org.opensearch.serverless.cluster.ReaderPlacement;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shard.ShardReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tells a shard's search nodes about each commit its writer publishes, so a reader moves onto it at once instead of at
 * its next look at the register.
 *
 * <p><b>Why.</b> A search node that has a shard open learns of a newer commit only when it reads the shard's register,
 * which it does every other lease renewal: twenty seconds at the default lease TTL. A write that was published within
 * a second or two of its acknowledgement was then searchable on the readers that answer for it only after another ten
 * seconds on average, and up to twenty.
 *
 * <p><b>How.</b> After each publish the writer sends the commit it published to the search nodes that placement ranks
 * first for the shard -- the same candidates a search is sent to. A node that has the shard open on an older commit
 * lets its reader go and opens the pushed commit in the background, so the next search finds it open. The commit rides
 * in the message, so the reopen reads no register; the segment blocks it reads are the ones the next search would have
 * read. A push adds no object-store request at all, and is sent after the publish, off the write path, so it adds
 * nothing to a write's acknowledgement.
 *
 * <p><b>What it does not change.</b> The register look stays, as the backstop for a lost push, a reader that placement
 * did not name, and a writer that died. A pushed commit only ever moves a reader forward: one no newer than the commit
 * open is ignored, and a commit is trusted as far as a writer's answer to {@link NewestCommits} is, since it comes from
 * the same authenticated peer. On unless {@value #ENABLED} is set to false.
 *
 * <p><b>New files ride along.</b> The reader's reopen used to fetch every block of the commit's new segment from the
 * object store, though the writer had just written those bytes and still holds them on disk: at one refresh every ten
 * seconds that was the largest share of a lightly used index's reads. So a push carries the blocks of the files that
 * are new since the previous push of the shard, read from the writer's own copies, smallest first and whole files only
 * up to {@value #INLINE_BYTES} (1 MB by default, 0 turns it off). The reader puts them in its block cache before it
 * reopens, under the keys its own reads use, so the reopen and the next search read memory. A larger segment, a merge's
 * say, is left to the reader's reads from the store as before. A block already held is not replaced.
 */
public final class CommitPush {

    private static final Logger LOG = LogManager.getLogger(CommitPush.class);

    /** Whether this node, as a writer, pushes the commits it publishes to the shard's search nodes. */
    public static final String ENABLED = "serverless.search.commit_push.enabled";

    /** The most bytes of a commit's new files one push carries to each search node; 0 sends the commit alone. */
    public static final String INLINE_BYTES = "serverless.search.commit_push.inline_bytes";

    /** The default: well above a refresh's segment, well below a merge's. */
    public static final long DEFAULT_INLINE_BYTES = 1L << 20;

    /** How long a push waits for a search node to take it. */
    static final TimeValue SEND_TIMEOUT = TimeValue.timeValueMillis(500);

    /** How long a pushed commit is kept for the open it is meant for; older, the register decides again. */
    static final long PUSHED_TTL_MILLIS = 10_000L;

    /** How long a reader that a query holds is waited for before the register look is left to move it. */
    static final long BUSY_RETRY_MILLIS = 25L;
    static final int BUSY_RETRIES = 40;

    /** How many pushed commits are kept before the ones past their age are dropped. */
    static final int MAX_PUSHED = 4096;

    /** A pushed commit and when it came. */
    private record Pushed(CommitManifest commit, long atNanos, long mappingVersion, long settingsVersion) {
    }

    /** Whether a reader moved onto a pushed commit reuses the descriptor it holds when the writer's is no newer. */
    public static final String REUSE_DESCRIPTOR = "serverless.search.commit_push.reuse_descriptor";

    private final ShardRouter router;
    private final boolean enabled;
    private final Map<String, Pushed> pushed = new ConcurrentHashMap<>();
    private final Set<String> moving = ConcurrentHashMap.newKeySet();
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong sendFailures = new AtomicLong();
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong reopened = new AtomicLong();
    private final AtomicLong blocksSent = new AtomicLong();
    private final AtomicLong blocksTaken = new AtomicLong();
    private final long inlineBytes;
    private final boolean reuseDescriptor;
    /** Per shard this node writes, the files of the commit it last pushed, so the next push knows which are new. */
    private final Map<String, Set<String>> lastPushedFiles = new ConcurrentHashMap<>();

    private final ReaderRegisterLooks registerLooks;

    CommitPush(ShardRouter router) {
        this.router = router;
        this.enabled = router.node.settings().getAsBoolean(ENABLED, true);
        this.reuseDescriptor = router.node.settings().getAsBoolean(REUSE_DESCRIPTOR, true);
        this.inlineBytes = Math.max(
            0L,
            router.node.settings()
                .getAsBytesSize(INLINE_BYTES, new org.opensearch.core.common.unit.ByteSizeValue(DEFAULT_INLINE_BYTES))
                .getBytes()
        );
        this.registerLooks = new ReaderRegisterLooks(router.node, router.plane, router.node.settings());
    }

    /** @return which readers' periodic register looks this node's received pushes make unnecessary */
    public ReaderRegisterLooks registerLooks() {
        return registerLooks;
    }

    /** Registers the handler that takes pushes. Called once, at startup, after the router's own. */
    public void registerHandlers() {
        router.transportService.registerRequestHandler(
            CommitPushRequest.ACTION,
            ThreadPool.Names.MANAGEMENT,
            CommitPushRequest::new,
            this::handle
        );
    }

    /** @return whether this node pushes the commits it publishes */
    public boolean enabled() {
        return enabled;
    }

    /** @return pushes sent, failed, received, readers reopened on one, blocks sent along, and blocks taken into the cache */
    public long[] counts() {
        return new long[] { sent.get(), sendFailures.get(), received.get(), reopened.get(), blocksSent.get(), blocksTaken.get() };
    }

    /**
     * Pushes a commit just published to the shard's search nodes, in the background. Does nothing if pushing is off.
     *
     * @param shardId the shard
     * @param commit the commit published
     */
    public void published(ShardId shardId, CommitManifest commit) {
        final MetadataPlane plane = router.plane.get();
        if (enabled == false || commit == null || plane == null) {
            return;
        }
        final List<String> targets = readerTargets(router, plane, shardId);
        final String key = key(shardId);
        final Set<String> previous = inlineBytes > 0 ? lastPushedFiles.put(key, Set.copyOf(commit.files().keySet())) : null;
        if (lastPushedFiles.size() > MAX_PUSHED) {
            lastPushedFiles.keySet().removeIf(k -> k.equals(key) == false);
        }
        if (targets.isEmpty()) {
            return;
        }
        final long[] versions = router.node.hostedDescriptorVersions(shardId.getIndexName());
        router.node.threadPool().generic().execute(() -> {
            final CommitPushRequest request = new CommitPushRequest(
                shardId.getIndexName(),
                shardId.getIndex().getUUID(),
                shardId.id(),
                commit,
                previous == null ? Map.of() : newBlocks(shardId, commit, previous),
                versions == null ? -1L : versions[0],
                versions == null ? -1L : versions[1]
            );
            for (String target : targets) {
                router.node.threadPool().generic().execute(() -> {
                    try {
                        final Optional<DiscoveryNode> peer = router.peer(target, SEND_TIMEOUT);
                        if (peer.isPresent()) {
                            router.send(
                                peer.get(),
                                CommitPushRequest.ACTION,
                                request,
                                in -> TransportResponse.Empty.INSTANCE,
                                SEND_TIMEOUT
                            );
                            sent.incrementAndGet();
                            blocksSent.addAndGet(request.blocks().size());
                        }
                    } catch (Exception e) {
                        // Lost: the reader's register look moves it, as before pushes existed.
                        sendFailures.incrementAndGet();
                        LOG.debug("could not push the commit of {} to [{}]", shardId, target, e);
                    }
                });
            }
        });
    }

    /**
     * The blocks of a commit's files that were not in the previous push, read from this node's own copies; empty if the
     * shard is not open here or they cannot be read. Without a previous push there is nothing to tell new from old, and
     * the reader's first open reads the store as before.
     */
    private Map<String, byte[]> newBlocks(ShardId shardId, CommitManifest commit, Set<String> previous) {
        final List<String> added = commit.files().keySet().stream().filter(name -> previous.contains(name) == false).toList();
        if (added.isEmpty()) {
            return Map.of();
        }
        try {
            final org.opensearch.index.shard.IndexShard shard = router.node.reconciler() == null
                ? null
                : router.node.reconciler().shard(shardId);
            final org.opensearch.serverless.store.BlockCacheDirectory directory = shard == null
                ? null
                : org.opensearch.serverless.store.BlockCacheDirectory.unwrap(shard.store().directory());
            return directory == null ? Map.of() : directory.localBlocks(commit, added, inlineBytes);
        } catch (Exception e) {
            // A file merged away since the publish, or a shard closing: the reader reads the store, as before.
            LOG.debug("could not read the new files of {}'s pushed commit", shardId, e);
            return Map.of();
        }
    }

    /**
     * The search nodes a shard's searches are sent to, best first, other than this node: a hot shard's spread, and two
     * at the least. Empty if they cannot be ranked.
     */
    static List<String> readerTargets(ShardRouter router, MetadataPlane plane, ShardId shardId) {
        try {
            final HotShardSpread spread = HotShardSpread.of(router.node, router.node.settings());
            final List<String> ranked = ReaderPlacement.candidatesFor(
                shardId.getIndexName(),
                shardId.id(),
                plane.membership().current(),
                ServerlessNode.ROLE_SEARCH,
                Math.max(2, spread.maxReaders())
            );
            final String self = router.node.localNode().getId();
            return ranked.stream().filter(id -> id.equals(self) == false).toList();
        } catch (RuntimeException e) {
            LOG.debug("could not rank the readers of {}", shardId, e);
            return List.of();
        }
    }

    void handle(CommitPushRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        router.requireMac(CommitPushRequest.ACTION, request);
        channel.sendResponse(TransportResponse.Empty.INSTANCE);
        received.incrementAndGet();
        registerLooks.pushedFrom(request.commit().writer());
        take(
            new ShardId(new Index(request.indexName(), request.indexUuid()), request.shard()),
            request.commit(),
            request.blocks(),
            request.mappingVersion(),
            request.settingsVersion()
        );
    }

    /**
     * Takes a pushed commit: if this node has the shard open as a reader on an older commit, moves the reader onto it.
     *
     * @param shardId the shard
     * @param commit the commit its writer pushed
     */
    void take(ShardId shardId, CommitManifest commit) {
        take(shardId, commit, Map.of(), -1L, -1L);
    }

    /**
     * Takes a pushed commit with blocks of its new files: if this node has the shard open as a reader on an older commit,
     * puts the blocks into its cache, then moves the reader onto the commit.
     *
     * @param shardId the shard
     * @param commit the commit its writer pushed
     * @param blocks blocks of the commit's new files, by cache key
     * @param mappingVersion the mapping version the writer holds, or -1
     * @param settingsVersion the settings version the writer holds, or -1
     */
    void take(ShardId shardId, CommitManifest commit, Map<String, byte[]> blocks, long mappingVersion, long settingsVersion) {
        final ShardReconciler reconciler = router.node.reconciler();
        if (reconciler == null || reconciler.frozenShards().contains(shardId)) {
            // A frozen view serves the commit it was made on, by definition.
            return;
        }
        final Optional<CommitManifest> open = reconciler.readerCommit(shardId);
        if (open.isEmpty() || newer(commit, open.get()) == false) {
            // Not served here, or already on it: the next open reads the register as it would have.
            return;
        }
        if (blocks.isEmpty() == false) {
            final org.opensearch.serverless.store.BlockCache cache = router.node.blockCache();
            long stored = 0;
            for (Map.Entry<String, byte[]> block : blocks.entrySet()) {
                if (cache.putCopied(block.getKey(), block.getValue())) {
                    stored++;
                }
            }
            blocksTaken.addAndGet(stored);
        }
        final String key = key(shardId);
        if (pushed.size() > MAX_PUSHED) {
            // A push whose reader was let go and never reopened is not taken; past its age it is only weight.
            final long now = System.nanoTime();
            pushed.values().removeIf(p -> now - p.atNanos() > TimeValue.timeValueMillis(PUSHED_TTL_MILLIS).nanos());
        }
        pushed.merge(
            key,
            new Pushed(commit, System.nanoTime(), mappingVersion, settingsVersion),
            (had, now) -> newer(now.commit(), had.commit()) ? now : had
        );
        if (moving.add(key)) {
            router.node.threadPool().generic().execute(() -> move(shardId, key, 0));
        }
    }

    private void move(ShardId shardId, String key, int attempt) {
        boolean again = false;
        try {
            final Pushed latest = pushed.get(key);
            final ShardReconciler reconciler = router.node.reconciler();
            final Optional<CommitManifest> open = reconciler.readerCommit(shardId);
            if (latest == null || open.isEmpty() || newer(latest.commit(), open.get()) == false) {
                // Let go by someone else, or moved already: the next open takes the pushed commit if it is still fresh.
                return;
            }
            if (reconciler.inFlight(shardId) > 0) {
                // A query holds it; closing it under the query would fail the query. Look again shortly, for a while.
                again = attempt < BUSY_RETRIES;
                return;
            }
            if (reconciler.releaseReader(
                shardId,
                open.get(),
                "its writer pushed a newer commit"
            ) != ShardReconciler.ReaderRelease.RELEASED) {
                return;
            }
            final org.opensearch.serverless.cluster.IndexDescriptor had = router.node.readers().servedDescriptor(shardId.getIndexName());
            router.node.readers().forgetReader(shardId);
            final MetadataPlane plane = router.plane.get();
            if (plane != null) {
                // Opened now, so the next search finds it open; the open takes the pushed commit rather than the register.
                router.node.readers()
                    .serveAsReaderOnPush(
                        plane,
                        shardId.getIndexName(),
                        shardId.id(),
                        shardId.getIndex().getUUID(),
                        had,
                        reuseDescriptor ? latest.mappingVersion() : -1L,
                        reuseDescriptor ? latest.settingsVersion() : -1L
                    );
                reopened.incrementAndGet();
            }
        } catch (Exception e) {
            LOG.debug("could not move the reader of {} onto its pushed commit", shardId, e);
        } finally {
            if (again) {
                router.node.threadPool()
                    .schedule(
                        () -> move(shardId, key, attempt + 1),
                        TimeValue.timeValueMillis(BUSY_RETRY_MILLIS),
                        ThreadPool.Names.GENERIC
                    );
            } else {
                moving.remove(key);
                // A push that came while this ran, for a reader that is still behind it, is moved now rather than at the next push.
                final Pushed latest = pushed.get(key);
                final Optional<CommitManifest> open = router.node.reconciler().readerCommit(shardId);
                if (latest != null && open.isPresent() && newer(latest.commit(), open.get()) && attempt < BUSY_RETRIES && moving.add(key)) {
                    router.node.threadPool().generic().execute(() -> move(shardId, key, attempt + 1));
                }
            }
        }
    }

    /**
     * Returns the commit pushed for a shard, for the reader open about to read its register, and forgets it. Empty if
     * none was pushed, or the push is old enough that the register should decide.
     *
     * @param indexName the index
     * @param indexUuid the index's uuid
     * @param shard the shard number
     * @return the pushed commit
     */
    public Optional<CommitManifest> takePushed(String indexName, String indexUuid, int shard) {
        if (pushed.isEmpty()) {
            return Optional.empty();
        }
        final Pushed taken = pushed.remove(key(new ShardId(new Index(indexName, indexUuid), shard)));
        if (taken == null || System.nanoTime() - taken.atNanos() > TimeValue.timeValueMillis(PUSHED_TTL_MILLIS).nanos()) {
            return Optional.empty();
        }
        return Optional.of(taken.commit());
    }

    private static String key(ShardId shardId) {
        return shardId.getIndex().getUUID() + "#" + shardId.id();
    }

    /**
     * Whether one commit of a shard is later than another: a later term, or in the same term a later Lucene commit
     * generation, or a later stage of the same writer.
     *
     * @param candidate the commit that may be later
     * @param than the commit it is compared with
     * @return true if the candidate is later
     */
    public static boolean newer(CommitManifest candidate, CommitManifest than) {
        if (candidate.term() != than.term()) {
            return candidate.term() > than.term();
        }
        if (candidate.stagedAfter(than)) {
            return true;
        }
        final long a = generation(candidate);
        final long b = generation(than);
        return a >= 0 && b >= 0 && a > b;
    }

    /** The generation of the Lucene commit a manifest names, or -1 if it names none. */
    static long generation(CommitManifest commit) {
        long generation = -1;
        for (String file : commit.files().keySet()) {
            if (file.startsWith(org.apache.lucene.index.IndexFileNames.SEGMENTS + "_")) {
                generation = Math.max(generation, SegmentInfos.generationFromSegmentsFileName(file));
            }
        }
        return generation;
    }
}
