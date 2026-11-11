/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.NodeLogBlob;
import org.opensearch.serverless.store.NodeLogBlob.ShardKey;
import org.opensearch.serverless.store.NodeLogEpoch;
import org.opensearch.serverless.store.NodeLogStore;
import org.opensearch.serverless.store.NodeLogTakeover;
import org.opensearch.serverless.testkit.CrashPointBlobStore.Fault;
import org.opensearch.serverless.testkit.CrashPointBlobStore.Kind;
import org.opensearch.serverless.testkit.CrashPointBlobStore.Step;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * The per-node write log on real nodes: a writer that crashes or fails at any call of its log, a successor that crashes
 * at any call of its takeover, a writer whose lease ran out without its knowing, a restart under the same id and
 * several successors taking one dead node's shards at once.
 *
 * <p>The one property checked throughout: a write a node acknowledged is found by whoever holds its shard next. Every
 * node has its own view of the store, as separate processes do, so a fault injected into one node's view leaves the
 * others honest. A node whose view has crashed can make no further call, which is what a dead process looks like to
 * everyone else; a node whose clock is held back keeps writing after everyone else considers its lease lapsed, which is
 * what a paused or partitioned process looks like.
 *
 * <p>Subclasses give the store. Each test gets a fresh one.
 */
public abstract class NodeLogNodeFaultTestCase extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String INDEX = "alpha";
    private static final String INDEX_UUID = "uuid-alpha-00000000";
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    /**
     * Returns a fresh, empty store for one test.
     *
     * @return the store
     * @throws IOException if it cannot be created
     */
    protected abstract BlobStore newStore() throws IOException;

    /** Every acknowledged write, by shard. */
    private static final class Ledger {
        private final Map<Integer, Set<String>> acknowledged = new ConcurrentHashMap<>();

        void acknowledged(int shard, String id) {
            acknowledged.computeIfAbsent(shard, ignored -> ConcurrentHashMap.newKeySet()).add(id);
        }

        int size() {
            return acknowledged.values().stream().mapToInt(Set::size).sum();
        }

        void assertFoundOn(ServerlessNode node, String what) throws IOException {
            for (Map.Entry<Integer, Set<String>> entry : acknowledged.entrySet()) {
                final ShardId shardId = shardOf(node, entry.getKey());
                assertNotNull(what + ": shard " + entry.getKey() + " is open on " + node.localNode().getName(), shardId);
                for (String id : entry.getValue()) {
                    assertTrue(what + ": the acknowledged write " + id + " is found on " + shardId, node.get(shardId, id).found());
                }
            }
        }
    }

    private Settings nodeSettings(String name, Path home, long idSeed) {
        return Settings.builder()
            .put("node.name", name)
            .put("node.id.seed", idSeed)
            .put("cluster.name", "serverless-node-log-faults")
            .put("path.home", home)
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.window_millis", 10)
            .build();
    }

    private static MetadataPlane planeOn(BlobStore store, AtomicLong clock) {
        return new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
    }

    private static void createIndex(BlobStore store, AtomicLong clock, int shards) throws IOException {
        planeOn(store, clock).createIndex(new IndexDescriptor(INDEX, INDEX_UUID, shards, MAPPING, null));
    }

    private ServerlessNode startNode(String name, Path home, MetadataPlane plane) throws Exception {
        return startNode(name, home, randomLong(), plane);
    }

    /** Starts a node whose id comes from a seed, so a restart with the same seed has the same id. */
    private ServerlessNode startNode(String name, Path home, long idSeed, MetadataPlane plane) throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings(name, home, idSeed));
        try {
            node.start();
            node.setMetadataPlane(plane);
        } catch (Exception | AssertionError e) {
            closeQuietly(node);
            throw e;
        }
        return node;
    }

    private static ShardId shardOf(ServerlessNode node, int shard) {
        for (ShardId shardId : node.reconciler().openShards()) {
            if (shardId.id() == shard) {
                return shardId;
            }
        }
        return null;
    }

    /** Renews the node's lease and takes the given shards in one reconciler pass. */
    private static void take(ServerlessNode node, MetadataPlane plane, AtomicLong clock, List<Integer> shards) throws Exception {
        node.renewLease(plane);
        plane.membership().refresh();
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (int shard : shards) {
            loop.want(INDEX, shard);
        }
        loop.tick(clock.get());
        for (int shard : shards) {
            assertNotNull("shard " + shard + " opened on " + node.localNode().getName(), shardOf(node, shard));
        }
    }

    private static List<Integer> range(int from, int to) {
        final List<Integer> out = new ArrayList<>();
        for (int i = from; i < to; i++) {
            out.add(i);
        }
        return out;
    }

    /** Indexes one document; records it if acknowledged. Returns whether it was. */
    private static boolean write(ServerlessNode node, int shard, String id, Ledger ledger) {
        final ShardId shardId = shardOf(node, shard);
        if (shardId == null) {
            return false;
        }
        try {
            node.index(shardId, id, "{\"msg\":\"m\",\"n\":" + shard + "}");
        } catch (Exception e) {
            if (CrashPointBlobStore.isInjected(e) == false && e instanceof IOException == false) {
                throw new AssertionError("a write failed for a reason no fault explains", e);
            }
            return false;
        }
        ledger.acknowledged(shard, id);
        return true;
    }

    private static void publish(ServerlessNode node, int shard) {
        final ShardId shardId = shardOf(node, shard);
        if (shardId == null) {
            return;
        }
        try {
            node.reconciler().publish(shardId, node.reconciler().shard(shardId).getOperationPrimaryTerm());
        } catch (Exception e) {
            // A publish under a fault may fail; what it would have published is still in the log.
        }
    }

    private static void closeQuietly(ServerlessNode node) {
        if (node == null) {
            return;
        }
        try {
            node.close();
        } catch (Exception e) {
            // A node whose store crashed, or that lost its shards, may report it on close.
        }
    }

    /** Kills a node's view of the store, so nothing it does from here reaches the store, closing it included. */
    private static void kill(CrashPointBlobStore view) {
        view.on(step -> true, Fault.CRASH_BEFORE, Integer.MAX_VALUE);
    }

    /** The calls a node makes for its write log: its blobs, the lease swaps that record epochs, and the head checks. */
    private static boolean nodeLogCall(Step step, String nodeId) {
        if (step.path().startsWith("nodelog/")) {
            return true;
        }
        if (step.kind() == Kind.SWAP_REGISTER && step.path().startsWith("cluster/members") && step.blobName().equals("lease-" + nodeId)) {
            return true;
        }
        return step.kind() == Kind.READ_REGISTER && step.path().startsWith("shards");
    }

    /** Every node-log blob of one epoch, by ordinal. */
    private static Map<Long, NodeLogBlob.Entry> entriesOf(NodeLogStore log, BlobStore store, long epoch) throws IOException {
        final Map<Long, NodeLogBlob.Entry> out = new TreeMap<>();
        for (String name : store.blobContainer(log.epochPath(epoch)).listBlobs().keySet()) {
            if (name.chars().allMatch(Character::isDigit) == false) {
                // Not a slot: the test filesystem adds stray files of its own.
                continue;
            }
            final long ordinal = Long.parseLong(name);
            log.readEntry(epoch, ordinal).ifPresent(entry -> out.put(ordinal, entry));
        }
        return out;
    }

    private static long fences(Map<Long, NodeLogBlob.Entry> entries) {
        return entries.values().stream().filter(entry -> entry instanceof NodeLogBlob.Fence).count();
    }

    private static List<NodeLogEpoch> leaseEpochs(MetadataPlane plane, String nodeId) throws IOException {
        plane.membership().refresh();
        return plane.membership().read(nodeId).orElseThrow().nodeLog();
    }

    /**
     * The writing a crash-point run does: two rounds of one write per shard and a publish of one shard, then an append
     * whose answer is lost, which ends the epoch and closes its shard to writes, a reopen of that shard from the writer's
     * own log, and two more rounds in the epoch that opens next. So a fault can land on the first epoch's recording, its
     * first and later blobs, the reopen, or the next epoch's recording and blobs.
     */
    private static void writeRounds(
        ServerlessNode node,
        MetadataPlane plane,
        AtomicLong clock,
        CrashPointBlobStore view,
        int shards,
        Ledger ledger
    ) {
        for (int round = 0; round < 4; round++) {
            for (int shard = 0; shard < shards; shard++) {
                write(node, shard, "w-" + round + "-" + shard, ledger);
            }
            if (round == 1) {
                publish(node, 0);
                view.on(step -> step.kind() == Kind.CREATE && step.path().startsWith("nodelog/"), Fault.AMBIGUOUS, 1);
                write(node, 0, "rolls-the-epoch", new Ledger());
                reopen(node, plane, clock, shards);
            }
        }
    }

    /**
     * One reconciler pass over every shard, which reopens a shard a failed append closed. A fault may stop it, directly
     * or through a shard it released and cannot yet take back; the run checks after its rounds that the writer recovers.
     */
    private static void reopen(ServerlessNode node, MetadataPlane plane, AtomicLong clock, int shards) {
        try {
            take(node, plane, clock, range(0, shards));
        } catch (Exception | AssertionError e) {
            // Checked by the reopen after the rounds.
        }
    }

    /** One run with a fault at the n-th node-log call after the writer took its shards; returns the calls it saw. */
    private int runWithFaultAt(int nth, Fault fault, int shards) throws Exception {
        final BlobStore store = newStore();
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        createIndex(store, clock, shards);
        final CrashPointBlobStore view = new CrashPointBlobStore(store);
        final MetadataPlane writerPlane = planeOn(view, clock);
        final Ledger ledger = new Ledger();
        final String what = fault + " at node-log call " + nth;
        ServerlessNode writer = null;
        ServerlessNode successor = null;
        try {
            writer = startNode("writer", createTempDir(), writerPlane);
            take(writer, writerPlane, clock, range(0, shards));
            final String writerId = writer.localNode().getId();
            // Counts every node-log call from here; the n-th gets the fault, and with n of zero none does.
            final AtomicInteger seen = new AtomicInteger();
            final Predicate<Step> counted = step -> nodeLogCall(step, writerId) && seen.incrementAndGet() == nth;
            view.on(counted, fault == null ? Fault.FAIL_BEFORE : fault, 1);
            try {
                writeRounds(writer, writerPlane, clock, view, shards, ledger);
            } catch (AssertionError e) {
                throw new AssertionError(what + " (writer crashed at " + view.crashedAt() + ")", e);
            }
            final int calls = seen.get();
            if (view.crashedAt().isEmpty()) {
                // The writer survived the fault. A shard a failed append closed is reopened from the writer's own log,
                // and every write it acknowledged is there; then it carries on.
                take(writer, writerPlane, clock, range(0, shards));
                ledger.assertFoundOn(writer, what + ", reopened by the writer");
                for (int shard = 0; shard < shards; shard++) {
                    assertTrue(what + ": the writer writes again after the fault", write(writer, shard, "after-" + shard, ledger));
                }
            }
            kill(view);

            clock.addAndGet(2 * TTL);
            final MetadataPlane successorPlane = planeOn(store, clock);
            successor = startNode("successor", createTempDir(), successorPlane);
            take(successor, successorPlane, clock, range(0, shards));
            ledger.assertFoundOn(successor, what + ", taken over");
            return calls;
        } finally {
            closeQuietly(successor);
            closeQuietly(writer);
            store.close();
        }
    }

    /**
     * A writer crashes before or after any call its write log makes, or one of them fails or loses its answer: every
     * write it acknowledged is found by the node that takes its shards, and a writer that survives the fault finds them
     * itself when it reopens a shard the failure closed.
     */
    public void testEveryAcknowledgedWriteSurvivesAFaultAtAnyNodeLogCall() throws Exception {
        final int shards = 2;
        final int calls = runWithFaultAt(0, null, shards);
        assertTrue("the writer's rounds made node-log calls, got " + calls, calls >= 6);
        for (int nth = 1; nth <= calls; nth++) {
            for (Fault fault : Fault.values()) {
                runWithFaultAt(nth, fault, shards);
            }
        }
    }

    /**
     * One run in which a successor taking a dead writer's shards crashes before its n-th call; returns how many calls
     * the successor made, which with n of zero is the whole takeover.
     */
    private int runWithSuccessorCrashAt(int nth, int shards) throws Exception {
        final BlobStore store = newStore();
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        createIndex(store, clock, shards);
        final CrashPointBlobStore writerView = new CrashPointBlobStore(store);
        final MetadataPlane writerPlane = planeOn(writerView, clock);
        final Ledger ledger = new Ledger();
        final String what = "a successor crashed before its call " + nth;
        ServerlessNode writer = null;
        ServerlessNode successor = null;
        ServerlessNode next = null;
        try {
            writer = startNode("writer", createTempDir(), writerPlane);
            take(writer, writerPlane, clock, range(0, shards));
            for (int round = 0; round < 2; round++) {
                for (int shard = 0; shard < shards; shard++) {
                    assertTrue(write(writer, shard, "w-" + round + "-" + shard, ledger));
                }
                if (round == 0) {
                    publish(writer, 0);
                }
            }
            kill(writerView);

            clock.addAndGet(2 * TTL);
            final CrashPointBlobStore successorView = new CrashPointBlobStore(store);
            final MetadataPlane successorPlane = planeOn(successorView, clock);
            successor = startNode("successor", createTempDir(), successorPlane);
            final int before = successorView.steps().size();
            if (nth > 0) {
                successorView.at(before + nth - 1, Fault.CRASH_BEFORE);
            }
            try {
                take(successor, successorPlane, clock, range(0, shards));
            } catch (Exception | AssertionError e) {
                if (successorView.crashedAt().isEmpty()) {
                    throw new AssertionError(what + ": the takeover failed without a crash", e);
                }
            }
            final int calls = successorView.steps().size() - before;
            if (successorView.crashedAt().isEmpty()) {
                ledger.assertFoundOn(successor, what + ", by the successor");
            }
            kill(successorView);

            // The successor's lease runs out too; the next node finds the first writer's writes.
            clock.addAndGet(2 * TTL);
            final MetadataPlane nextPlane = planeOn(store, clock);
            next = startNode("next", createTempDir(), nextPlane);
            take(next, nextPlane, clock, range(0, shards));
            ledger.assertFoundOn(next, what + ", by the node after it");
            return calls;
        } finally {
            closeQuietly(next);
            closeQuietly(successor);
            closeQuietly(writer);
            store.close();
        }
    }

    /**
     * A successor taking a dead writer's shards crashes at any call of its takeover, after fencing the writer's log or
     * before, after moving a head or before, part way through replay or publish: the node after it finds every write the
     * first writer acknowledged. A crash after a call that landed leaves the store as a crash before the next call does,
     * so crashing before each call, and after the last, covers every point.
     */
    public void testASuccessorCrashingAtAnyCallOfItsTakeoverLeavesEveryWriteForTheNext() throws Exception {
        final int shards = 1;
        final int calls = runWithSuccessorCrashAt(0, shards);
        assertTrue("the takeover made calls, got " + calls, calls > 10);
        for (int nth = 1; nth <= calls; nth++) {
            runWithSuccessorCrashAt(nth, shards);
        }
    }

    /**
     * A writer whose lease has run out without its knowing keeps writing while a successor takes its shards: every write
     * it acknowledged is found by the successor, and every write it starts once the successor has taken over is refused.
     */
    public void testALapsedWriterRacingItsSuccessorLosesNoAcknowledgedWrite() throws Exception {
        final int shards = 3;
        final BlobStore store = newStore();
        final long start = System.currentTimeMillis();
        // The writer's clock never moves, so its lease looks good to it throughout.
        final AtomicLong writerClock = new AtomicLong(start);
        final AtomicLong clock = new AtomicLong(start);
        createIndex(store, clock, shards);
        final MetadataPlane writerPlane = planeOn(store, writerClock);
        final Ledger ledger = new Ledger();
        final AtomicBoolean takenOver = new AtomicBoolean();
        final AtomicBoolean stop = new AtomicBoolean();
        final List<String> acknowledgedAfterTakeover = new CopyOnWriteArrayList<>();
        final AtomicInteger refusedAfterTakeover = new AtomicInteger();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        ServerlessNode writer = null;
        ServerlessNode successor = null;
        final List<Thread> threads = new ArrayList<>();
        try {
            writer = startNode("lapsed", createTempDir(), writerPlane);
            take(writer, writerPlane, writerClock, range(0, shards));
            for (int shard = 0; shard < shards; shard++) {
                assertTrue(write(writer, shard, "before-" + shard, ledger));
            }
            final ServerlessNode lapsed = writer;
            final CountDownLatch writing = new CountDownLatch(shards);
            for (int s = 0; s < shards; s++) {
                final int shard = s;
                final Thread thread = new Thread(() -> {
                    try {
                        for (int i = 0; stop.get() == false; i++) {
                            final boolean after = takenOver.get();
                            final boolean acknowledged = write(lapsed, shard, "racing-" + shard + "-" + i, ledger);
                            if (i == 0) {
                                writing.countDown();
                            }
                            if (after && acknowledged) {
                                acknowledgedAfterTakeover.add("racing-" + shard + "-" + i);
                            } else if (after) {
                                refusedAfterTakeover.incrementAndGet();
                            }
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                }, "lapsed-writer-" + shard);
                threads.add(thread);
                thread.start();
            }
            assertTrue(writing.await(30, TimeUnit.SECONDS));

            clock.addAndGet(2 * TTL);
            final MetadataPlane successorPlane = planeOn(store, clock);
            successor = startNode("successor", createTempDir(), successorPlane);
            take(successor, successorPlane, clock, range(0, shards));
            takenOver.set(true);
            assertBusy(
                () -> assertTrue(
                    "the writer is refused once taken over: " + acknowledgedAfterTakeover,
                    refusedAfterTakeover.get() >= 3 * shards
                ),
                30,
                TimeUnit.SECONDS
            );
            stop.set(true);
            for (Thread thread : threads) {
                thread.join(TimeUnit.SECONDS.toMillis(30));
                assertFalse(thread.getName() + " stopped", thread.isAlive());
            }
            assertNull("the racing writers failed for no fault's reason", failure.get());
            assertEquals("no write started after the takeover is acknowledged", List.of(), acknowledgedAfterTakeover);
            assertTrue("the writer raced the takeover, got " + ledger.size() + " acknowledged", ledger.size() > shards);
            ledger.assertFoundOn(successor, "after racing a lapsed writer");
        } finally {
            stop.set(true);
            for (Thread thread : threads) {
                thread.join(TimeUnit.SECONDS.toMillis(30));
            }
            closeQuietly(successor);
            closeQuietly(writer);
            store.close();
        }
    }

    /**
     * A lapsed writer whose epoch has ended opens a new one in the window between the successor moving a shard's head and
     * reading the writer's lease. The successor never learns of that epoch, so nothing of the shard may land in it: the
     * ownership check after the epoch is recorded refuses the write.
     *
     * <p>The successor revokes the writer's lease before moving the head, and the writer's first lease write after that
     * fails, so the window is reached by two writes: one to a shard the writer keeps, which trips over the revocation,
     * and one to the taken shard, whose lease write then goes over the revocation and records the epoch.
     */
    public void testAnEpochOpenedAfterTheHeadMovedTakesNothingOfTheShard() throws Exception {
        final BlobStore store = newStore();
        final long start = System.currentTimeMillis();
        final AtomicLong writerClock = new AtomicLong(start);
        final AtomicLong clock = new AtomicLong(start);
        createIndex(store, clock, 2);
        final CrashPointBlobStore writerView = new CrashPointBlobStore(store);
        final MetadataPlane writerPlane = planeOn(writerView, writerClock);
        final Ledger ledger = new Ledger();
        ServerlessNode writer = null;
        ServerlessNode successor = null;
        try {
            writer = startNode("lapsed", createTempDir(), writerPlane);
            take(writer, writerPlane, writerClock, List.of(0, 1));
            assertTrue(write(writer, 0, "before", ledger));
            // An append whose answer is lost ends the epoch, so the writer's next write opens a new one.
            writerView.on(step -> step.kind() == Kind.CREATE && step.path().startsWith("nodelog/"), Fault.AMBIGUOUS, 1);
            assertFalse(write(writer, 0, "lost-answer", new Ledger()));
            // The failed append closed the shard to writes; the writer reopens it from its own log.
            take(writer, writerPlane, writerClock, List.of(0, 1));
            final String writerId = writer.localNode().getId();
            final long highestBefore = leaseEpochs(planeOn(store, clock), writerId).stream()
                .mapToLong(NodeLogEpoch::epoch)
                .max()
                .orElseThrow();

            final ServerlessNode lapsed = writer;
            final AtomicBoolean headMoved = new AtomicBoolean();
            final AtomicReference<Boolean> lateAcknowledged = new AtomicReference<>();
            final CrashPointBlobStore successorView = new CrashPointBlobStore(store);
            // Runs the writer's late writes after the successor's head swap has landed and before its next call.
            successorView.on(step -> {
                if (headMoved.get() && lateAcknowledged.get() == null) {
                    write(lapsed, 1, "trips-the-revocation", new Ledger());
                    lateAcknowledged.set(write(lapsed, 0, "late", ledger));
                }
                if (step.kind() == Kind.SWAP_REGISTER && step.path().startsWith("shards")) {
                    headMoved.set(true);
                }
                return false;
            }, Fault.FAIL_BEFORE, 1);

            clock.addAndGet(2 * TTL);
            final MetadataPlane successorPlane = planeOn(successorView, clock);
            successor = startNode("successor", createTempDir(), successorPlane);
            take(successor, successorPlane, clock, List.of(0));
            assertEquals("the late write ran in the window and was refused", Boolean.FALSE, lateAcknowledged.get());
            ledger.assertFoundOn(successor, "after a late epoch");
            assertFalse("the refused write is not found", successor.get(shardOf(successor, 0), "late").found());

            final List<NodeLogEpoch> later = leaseEpochs(planeOn(store, clock), writerId).stream()
                .filter(epoch -> epoch.epoch() > highestBefore)
                .toList();
            assertFalse("the writer recorded an epoch the successor never read", later.isEmpty());
            final NodeLogTakeover reader = new NodeLogTakeover(
                new NodeLogStore(store, BlobPath.cleanPath(), writerId),
                later,
                "checker",
                0L
            );
            assertEquals("and nothing of the taken shard is in it", List.of(), reader.records(new ShardKey(INDEX_UUID, 0)));
        } finally {
            closeQuietly(successor);
            closeQuietly(writer);
            store.close();
        }
    }

    /**
     * A node that crashes and restarts under the same id fences what its earlier process wrote, finds every write that
     * process acknowledged, keeps naming the earlier epochs in its lease and writes above them; a successor that takes the
     * shards from the restarted node finds the writes of both.
     */
    public void testARestartUnderTheSameIdFencesAndKeepsItsEarlierEpochs() throws Exception {
        final int shards = 2;
        final BlobStore store = newStore();
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        createIndex(store, clock, shards);
        final Path home = createTempDir();
        final long idSeed = randomLong();
        final Ledger ledger = new Ledger();
        final CrashPointBlobStore firstView = new CrashPointBlobStore(store);
        final MetadataPlane firstPlane = planeOn(firstView, clock);
        final CrashPointBlobStore restartedView = new CrashPointBlobStore(store);
        ServerlessNode first = null;
        ServerlessNode restarted = null;
        ServerlessNode successor = null;
        try {
            first = startNode("restarting", home, idSeed, firstPlane);
            take(first, firstPlane, clock, range(0, shards));
            for (int shard = 0; shard < shards; shard++) {
                assertTrue(write(first, shard, "first-" + shard, ledger));
            }
            final String nodeId = first.localNode().getId();
            kill(firstView);
            closeQuietly(first);
            first = null;
            // A process under the same id is refused while the dead one's lease still looks live.
            clock.addAndGet(2 * TTL);

            final MetadataPlane plane = planeOn(restartedView, clock);
            final List<NodeLogEpoch> earlier = leaseEpochs(plane, nodeId);
            assertFalse("the first process left epochs in its lease", earlier.isEmpty());
            restarted = startNode("restarting", home, idSeed, plane);
            assertEquals("the same id", nodeId, restarted.localNode().getId());
            take(restarted, plane, clock, range(0, shards));
            ledger.assertFoundOn(restarted, "after a restart under the same id");

            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), nodeId);
            for (NodeLogEpoch epoch : earlier) {
                assertEquals(
                    "epoch " + epoch.epoch() + " of the earlier process is fenced once",
                    1L,
                    fences(entriesOf(log, store, epoch.epoch()))
                );
            }
            for (int shard = 0; shard < shards; shard++) {
                assertTrue(write(restarted, shard, "restarted-" + shard, ledger));
            }
            final List<NodeLogEpoch> now = leaseEpochs(plane, nodeId);
            assertEquals("the earlier epochs are still named", earlier, now.subList(0, earlier.size()));
            final long highestEarlier = earlier.stream().mapToLong(NodeLogEpoch::epoch).max().orElseThrow();
            assertTrue(
                "the restarted process writes above them",
                restarted.nodeLogWriter().committer().positions().stream().allMatch(epoch -> epoch.epoch() > highestEarlier)
            );

            // The restarted process crashes too; a successor finds what both acknowledged.
            kill(restartedView);
            closeQuietly(restarted);
            restarted = null;
            clock.addAndGet(2 * TTL);
            final MetadataPlane successorPlane = planeOn(store, clock);
            successor = startNode("successor", createTempDir(), successorPlane);
            take(successor, successorPlane, clock, range(0, shards));
            ledger.assertFoundOn(successor, "after both processes");
        } finally {
            closeQuietly(successor);
            closeQuietly(restarted);
            closeQuietly(first);
            store.close();
        }
    }

    /**
     * Several successors take different shards of one dead node at the same time: each finds every write the dead node
     * acknowledged to its shards, and each of the dead node's epochs gets one fence, shared by them all.
     */
    public void testSuccessorsTakingDifferentShardsOfOneDeadNodeShareItsFences() throws Exception {
        final int successors = 4;
        final int perSuccessor = 2;
        final int shards = successors * perSuccessor;
        final BlobStore store = newStore();
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        createIndex(store, clock, shards);
        final CrashPointBlobStore deadView = new CrashPointBlobStore(store);
        final MetadataPlane deadPlane = planeOn(deadView, clock);
        final Ledger ledger = new Ledger();
        ServerlessNode dead = null;
        final List<ServerlessNode> taking = new CopyOnWriteArrayList<>();
        try {
            dead = startNode("dead", createTempDir(), deadPlane);
            take(dead, deadPlane, clock, range(0, shards));
            for (int round = 0; round < 2; round++) {
                for (int shard = 0; shard < shards; shard++) {
                    assertTrue(write(dead, shard, "dead-" + round + "-" + shard, ledger));
                }
            }
            final String deadId = dead.localNode().getId();
            kill(deadView);

            clock.addAndGet(2 * TTL);
            final CountDownLatch go = new CountDownLatch(1);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < successors; i++) {
                final MetadataPlane plane = planeOn(store, clock);
                final ServerlessNode node = startNode("successor-" + i, createTempDir(), plane);
                taking.add(node);
                final List<Integer> mine = range(i * perSuccessor, (i + 1) * perSuccessor);
                final Thread thread = new Thread(() -> {
                    try {
                        go.await();
                        take(node, plane, clock, mine);
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                }, "successor-" + i);
                threads.add(thread);
                thread.start();
            }
            go.countDown();
            for (Thread thread : threads) {
                thread.join(TimeUnit.SECONDS.toMillis(120));
                assertFalse(thread.getName() + " finished", thread.isAlive());
            }
            if (failure.get() != null) {
                throw new AssertionError("a successor failed to take its shards", failure.get());
            }
            for (int i = 0; i < successors; i++) {
                final ServerlessNode node = taking.get(i);
                for (int shard : range(i * perSuccessor, (i + 1) * perSuccessor)) {
                    final ShardId shardId = shardOf(node, shard);
                    for (String id : List.of("dead-0-" + shard, "dead-1-" + shard)) {
                        assertTrue(id + " found on " + shardId, node.get(shardId, id).found());
                    }
                }
            }
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), deadId);
            final List<NodeLogEpoch> epochs = leaseEpochs(planeOn(store, clock), deadId);
            assertFalse(epochs.isEmpty());
            for (NodeLogEpoch epoch : epochs) {
                assertEquals("epoch " + epoch.epoch() + " has one fence", 1L, fences(entriesOf(log, store, epoch.epoch())));
            }
        } finally {
            for (ServerlessNode node : taking) {
                closeQuietly(node);
            }
            closeQuietly(dead);
            store.close();
        }
    }
}
