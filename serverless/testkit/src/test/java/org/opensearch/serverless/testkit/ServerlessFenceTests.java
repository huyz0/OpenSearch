/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.serverless.store.WalStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * The rule these tests make explicit: <b>a node whose own lease lapsed has lost every shard it held</b>,
 * and a write is acknowledged only while the node can prove it still holds the shard.
 *
 * <p>Every test here names an interleaving that acknowledged a write and then never replayed it. None
 * of them needed a broken store, a corrupt log or a clock off by more than a second; they needed only
 * the ordinary sequence of a renewal, an idle release or a takeover to land between two steps of a
 * write. The fixes are ordering and fencing, which is why each test is built around holding one step
 * open: a store that parks a specific operation on demand is what turns "this could interleave" into
 * "this did".
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessFenceTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-fence")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private static MetadataPlane planeOver(BlobStore store, AtomicLong clock) throws Exception {
        return new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
    }

    private static void createAlpha(MetadataPlane plane) throws Exception {
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
    }

    /** Opens a node on the shard it is told to want, and returns the shard. */
    private static ShardId hold(ServerlessNode node, MetadataPlane plane, BackgroundReconciler loop, AtomicLong clock) throws Exception {
        node.start();
        node.setMetadataPlane(plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());
        return node.reconciler().openShards().iterator().next();
    }

    // ---------------------------------------------------------------- the zombie-renew window

    /**
     * A node whose lease lapsed lets go of every shard <em>before</em> it renews, so the window between
     * renewing and re-reading the heads acknowledges nothing.
     *
     * <p><b>The interleaving.</b> Z paused past its TTL; S took the shard and sealed the log; Z resumed
     * and its heartbeat renewed the lease first and read the heads second. For the length of that head
     * scan Z's lease was valid again by its own clock, so a write reaching Z -- forwarded by a
     * coordinator whose hint still named Z -- was appended under Z's old term, behind S's seal, and
     * acknowledged. S never replays it. Nobody does.
     *
     * <p>The store parks Z's first shard-head read once the takeover is done, which holds that window
     * open on demand. The write must be refused inside it. It used to be acknowledged.
     */
    public void testALapsedLeaseReleasesEveryShardBeforeItIsRenewed() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final GatedBlobStore store = new GatedBlobStore(new FsBlobStore(1024, createTempDir(), false));
        // One plane per node over the one store: a plane carries its node's own lease state, and two
        // nodes sharing one would share a deadline, which is not what two processes do.
        final MetadataPlane planeZ = planeOver(store, clock);
        final MetadataPlane planeS = planeOver(store, clock);
        createAlpha(planeZ);

        try (
            ServerlessNode z = new ServerlessNode(nodeSettings("fence-z"));
            ServerlessNode s = new ServerlessNode(nodeSettings("fence-s"))
        ) {
            final BackgroundReconciler loopZ = new BackgroundReconciler(z, planeZ);
            final ShardId onZ = hold(z, planeZ, loopZ, clock);
            assertTrue(z.index(onZ, "before", "{\"msg\":\"before\"}").seqNo() >= 0);

            // Z pauses past its TTL. S takes the shard over and seals the log where Z's history ended.
            s.start();
            clock.set(1_000L + TTL);
            final ShardId onS = s.activateWriter(planeS, "alpha", 0).orElseThrow();
            assertTrue(s.get(onS, "before").found());

            // Z resumes and heartbeats. The store parks the first shard-head read of that heartbeat,
            // which is exactly the window after the renewal and before Z could learn it lost anything.
            final GatedBlobStore.Gate headRead = store.arm(GatedBlobStore.Point.READ_REGISTER, "shards/");
            final AtomicReference<Exception> heartbeatFailure = new AtomicReference<>();
            final Thread heartbeat = new Thread(() -> {
                try {
                    z.heartbeat(planeZ);
                } catch (Exception e) {
                    heartbeatFailure.set(e);
                }
            }, "fence-z-heartbeat");
            heartbeat.start();
            // Either the heartbeat parks at the head read, or -- once the release comes first -- it has
            // nothing to read and finishes. Both are "the renewal has happened".
            while (heartbeat.isAlive() && headRead.reached.await(20, TimeUnit.MILLISECONDS) == false) {
                // spin
            }

            final IOException refused = expectThrows(
                IOException.class,
                "a write inside the renew-to-head-check window must be refused; it used to be acknowledged behind S's seal",
                () -> z.index(onZ, "zombie", "{\"msg\":\"zombie\"}")
            );
            logger.info("fence: the zombie's write was refused with: {}", refused.getMessage());

            headRead.release();
            heartbeat.join(TimeUnit.SECONDS.toMillis(30));
            assertNull("the heartbeat must not have failed: " + heartbeatFailure.get(), heartbeatFailure.get());
            assertNull("Z must have let go of the shard it lost", z.reconciler().shard(onZ));
            assertFalse("nothing Z wrote after losing the shard may be visible on S", s.get(onS, "zombie").found());
        }
    }

    // ---------------------------------------------------------------- a write during an idle release

    /**
     * A write that lands during an idle release is either refused or replayed; it is never acknowledged
     * and lost.
     *
     * <p><b>The interleaving.</b> Idle release is publish, release the head, close, on the reconcile
     * thread, with nothing excluding the request threads. A write that applied to the engine after the
     * publish and appended to the log after the head release -- by which time a successor had acquired
     * and sealed -- was acknowledged, and its record sat behind the seal forever.
     *
     * <p>Three parked operations reproduce it exactly: the head-release swap is held before it runs (so
     * the publish is done and the shard is still open), the write is let in and parked at its log
     * append, the swap is let through and held after it (head released, shard still open), a successor
     * takes over and seals, and only then is the append released. Under the fence the write cannot get
     * in while the release is in progress at all: it waits, finds no shard, and is refused. Without the
     * fence it is acknowledged and the successor has never heard of it.
     */
    public void testAWriteDuringAnIdleReleaseIsRefusedOrReplayedNeverLost() throws Exception {
        final long idleAfter = 10_000L;
        final AtomicLong clock = new AtomicLong(1_000L);
        final GatedBlobStore store = new GatedBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane planeA = planeOver(store, clock);
        final MetadataPlane planeS = planeOver(store, clock);
        createAlpha(planeA);

        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("fence-idle-a"));
            ServerlessNode s = new ServerlessNode(nodeSettings("fence-idle-s"))
        ) {
            final BackgroundReconciler loop = new BackgroundReconciler(a, planeA).setIdleAfterMillis(idleAfter);
            final ShardId onA = hold(a, planeA, loop, clock);
            s.start();
            a.index(onA, "before", "{\"msg\":\"before\"}");
            loop.tick(clock.addAndGet(1_000L));   // published

            // Idle long enough to be let go, inside the lease so the lease is not what fences.
            clock.addAndGet(idleAfter + 1);
            final GatedBlobStore.Gate beforeRelease = store.arm(GatedBlobStore.Point.BEFORE_CAS, "shards/");
            final GatedBlobStore.Gate afterRelease = store.arm(GatedBlobStore.Point.AFTER_CAS, "shards/");
            final GatedBlobStore.Gate append = store.arm(GatedBlobStore.Point.WRITE_BLOB, "wal/t=");

            final AtomicReference<Exception> tickFailure = new AtomicReference<>();
            final Thread tick = new Thread(() -> {
                try {
                    loop.tick(clock.get());
                } catch (Exception e) {
                    tickFailure.set(e);
                }
            }, "fence-idle-tick");
            tick.start();
            assertTrue("the idle release must reach its head-release swap", beforeRelease.reached.await(30, TimeUnit.SECONDS));

            // The write arrives now: after the publish, before the head release.
            final AtomicReference<Object> outcome = new AtomicReference<>();
            final Thread write = new Thread(() -> {
                try {
                    outcome.set(a.index(onA, "during", "{\"msg\":\"during\"}"));
                } catch (Exception e) {
                    outcome.set(e);
                }
            }, "fence-idle-write");
            write.start();
            final boolean applied = append.reached.await(1, TimeUnit.SECONDS);
            logger.info("fence: the write {} the engine while the release was in progress", applied ? "reached" : "was kept out of");

            beforeRelease.release();
            assertTrue("the head-release swap must complete", afterRelease.reached.await(30, TimeUnit.SECONDS));
            // Head released, shard still open on A: a successor acquires and seals here.
            final ShardId onS = s.activateWriter(planeS, "alpha", 0).orElseThrow();
            assertTrue("the published history must have reached the successor", s.get(onS, "before").found());

            append.release();
            afterRelease.release();
            tick.join(TimeUnit.SECONDS.toMillis(30));
            write.join(TimeUnit.SECONDS.toMillis(30));
            assertNull("the tick must not have failed: " + tickFailure.get(), tickFailure.get());
            assertFalse("the write thread must have finished", write.isAlive());

            final boolean acknowledged = outcome.get() instanceof ServerlessNode.WriteOutcome;
            logger.info("fence: the write during the release was {}", acknowledged ? "acknowledged" : "refused: " + outcome.get());
            if (acknowledged) {
                assertTrue("an acknowledged write must be visible on the successor; it was behind the seal", s.get(onS, "during").found());
            } else {
                assertFalse("a refused write must not surface anywhere", s.get(onS, "during").found());
            }
        }
    }

    // ---------------------------------------------------------------- the listing is a hint

    /**
     * A sync keeps a shard whose head still names this node even when the claim blob is missing.
     *
     * <p>Truth's assignment list comes from a listing of this node's claim blobs, and a listing is a hint.
     * A claim whose write failed after the head was won, or a listing that lagged, made the sync omit a
     * shard the head still assigned here, and the old close loop released it on that evidence -- a
     * shard given up because a hint said nothing about it, which is the exact rule §5.2 forbids.
     */
    public void testASyncKeepsAShardWhoseHeadStillNamesThisNodeWhenTheClaimIsMissing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = planeOver(store, clock);
        createAlpha(plane);

        try (ServerlessNode a = new ServerlessNode(nodeSettings("fence-claim"))) {
            final BackgroundReconciler loop = new BackgroundReconciler(a, plane);
            final ShardId onA = hold(a, plane, loop, clock);
            assertEquals(a.localNode().getId(), plane.heads().read("alpha", 0).orElseThrow().ownerNodeId());

            // The claim vanishes; the head does not.
            store.blobContainer(RegisterMap.assignments(plane.basePath(), a.localNode().getId()))
                .deleteBlobsIgnoringIfNotExists(List.of(RegisterMap.assignmentBlob("alpha", 0)));

            a.syncFrom(plane);
            assertNotNull("the head names this node; a missing claim blob is no reason to let the shard go", a.reconciler().shard(onA));
            assertEquals(a.localNode().getId(), plane.heads().read("alpha", 0).orElseThrow().ownerNodeId());
            assertTrue("and it still serves", a.index(onA, "still", "{\"msg\":\"still\"}").seqNo() >= 0);
        }
    }

    // ---------------------------------------------------------------- two clocks

    /**
     * A holder stops acknowledging a skew margin before its stamped expiry, so a taker whose clock runs
     * ahead by less than the margin cannot seal a log the holder is still appending to.
     *
     * <p>The successor's clock is half a second ahead: it reads the holder's lease as expired, acquires
     * and seals, all at a moment the holder's own clock still calls "inside my lease". Without the
     * margin the holder acknowledged the write, and the record sat behind the seal. The loss needed no
     * skew "beyond the TTL"; it needed any skew at all.
     */
    public void testAHolderStopsAcknowledgingBeforeATakerWithAFasterClockSteals() throws Exception {
        final long skew = 500L;
        final AtomicLong clockZ = new AtomicLong(1_000L);
        final AtomicLong clockS = new AtomicLong(1_000L + skew);
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane planeZ = planeOver(store, clockZ);
        final MetadataPlane planeS = planeOver(store, clockS);
        createAlpha(planeZ);
        assertTrue("the test needs a skew inside the holder's margin", skew < planeZ.membership().skewMarginMillis());

        try (ServerlessNode z = new ServerlessNode(nodeSettings("skew-z")); ServerlessNode s = new ServerlessNode(nodeSettings("skew-s"))) {
            final BackgroundReconciler loopZ = new BackgroundReconciler(z, planeZ);
            final ShardId onZ = hold(z, planeZ, loopZ, clockZ);
            z.index(onZ, "before", "{\"msg\":\"before\"}");
            s.start();

            // By S's clock Z's lease has expired; by Z's, it has half a second left.
            clockS.set(1_000L + TTL);
            clockZ.set(1_000L + TTL - skew);
            final ShardId onS = s.activateWriter(planeS, "alpha", 0).orElseThrow();
            assertTrue(s.get(onS, "before").found());

            expectThrows(
                IOException.class,
                "inside the skew margin the holder must refuse; it used to acknowledge a record behind the successor's seal",
                () -> z.index(onZ, "skewed", "{\"msg\":\"skewed\"}")
            );
            assertFalse(s.get(onS, "skewed").found());
        }
    }

    // ---------------------------------------------------------------- the seal's own term

    /**
     * A seal never bounds the sealer's own term.
     *
     * <p>A shard released locally and reopened at the same term seals again at that term, and its
     * container already holds records. Writing that term into the seal was a latent loss: the records
     * the reopened writer went on to acknowledge were bounded by that listing if a later sealer's write
     * succeeded and its delete of the older seal did not, because seals merge by taking the minimum.
     */
    public void testASealNeverBoundsTheSealersOwnTerm() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(new FsBlobStore(1024, createTempDir(), false), clock);
        createAlpha(plane);
        final WalStore log = plane.walStore("alpha", "uuid-alpha-00000000", 0);

        log.append(1L, List.of(new WalRecord("a", "{\"msg\":\"a\"}"), new WalRecord("b", "{\"msg\":\"b\"}")));
        final Map<Long, String> reopened = log.sealAt(1L);
        assertEquals("the reopened writer's own records replay", "00000000000000000001", reopened.get(1L));
        assertFalse("the sealer's own term must not be written into the seal", log.sealedPosition().containsKey(1L));

        // The reopened writer goes on; a successor at the next term seals, and every record replays.
        log.append(1L, List.of(new WalRecord("c", "{\"msg\":\"c\"}")));
        final Map<Long, String> successor = plane.walStore("alpha", "uuid-alpha-00000000", 0).sealAt(2L);
        assertEquals("records the reopened writer acknowledged after its own seal must replay", "00000000000000000002", successor.get(1L));
        assertEquals("00000000000000000002", log.sealedPosition().get(1L));
    }

    // ---------------------------------------------------------------- forwarded writes ask the head

    /**
     * A shard that is open here but whose head names another node is not held as a writer.
     *
     * <p>Being open was the only check a forwarded write got. The heartbeat notes every held head once
     * per renewal, so a young enough sighting costs nothing; one older than that is read, and a head
     * naming somebody else closes the shard on the spot rather than at the next pass.
     */
    public void testAForwardedWriteIsRefusedWhenTheHeadNamesAnotherNode() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(new FsBlobStore(1024, createTempDir(), false), clock);
        createAlpha(plane);

        try (ServerlessNode a = new ServerlessNode(nodeSettings("hold-a")); ServerlessNode b = new ServerlessNode(nodeSettings("hold-b"))) {
            final BackgroundReconciler loop = new BackgroundReconciler(a, plane);
            final ShardId onA = hold(a, plane, loop, clock);
            b.start();
            assertTrue("the head read by the heartbeat a moment ago names this node", a.holdsAsWriter(onA, TTL));

            // The head is given away underneath A -- what a deferred give-back or an operator does --
            // and B takes it while A's lease is still perfectly live.
            assertTrue(plane.heads().release("alpha", 0, a.localNode().getId()));
            final ShardId onB = b.activateWriter(plane, "alpha", 0).orElseThrow();
            assertTrue(b.index(onB, "b", "{\"msg\":\"b\"}").seqNo() >= 0);

            clock.incrementAndGet();
            assertFalse("a head that names B is no evidence A holds the shard", a.holdsAsWriter(onA, 0L));
            assertNull("and A stops serving it at once", a.reconciler().shard(onA));
        }
    }

    // ---------------------------------------------------------------- brownout

    /**
     * A failed log append fences the shard against writes and keeps it readable; the next heartbeat
     * that reaches the store reopens it from its log, without the operation the caller was told failed.
     *
     * <p>The shard used to be closed on the spot, so a store that hiccuped once took a fully cached copy
     * offline for reads too. §13 says reads degrade to staleness and writes to rejection.
     */
    public void testAFailedAppendFencesWritesKeepsReadsAndReopensFromTheLog() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final GatedBlobStore store = new GatedBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = planeOver(store, clock);
        createAlpha(plane);

        try (ServerlessNode a = new ServerlessNode(nodeSettings("brownout"))) {
            final BackgroundReconciler loop = new BackgroundReconciler(a, plane);
            final ShardId onA = hold(a, plane, loop, clock);
            a.index(onA, "earlier", "{\"msg\":\"earlier\"}");

            store.failWritesUnder("wal/t=");
            final IOException refused = expectThrows(IOException.class, () -> a.index(onA, "lost", "{\"msg\":\"lost\"}"));
            assertTrue(refused.getMessage(), refused.getMessage().contains("not acknowledged"));
            assertNotNull("the shard stays open for reads", a.reconciler().shard(onA));
            assertTrue("and still answers from its own copy", a.get(onA, "earlier").found());
            assertTrue(a.isWriteFenced(onA));
            final IOException fenced = expectThrows(IOException.class, () -> a.index(onA, "next", "{\"msg\":\"next\"}"));
            assertTrue(
                "a fenced shard refuses before applying anything: " + fenced.getMessage(),
                fenced.getMessage().contains("refusing to write")
            );

            // The store returns. The heartbeat reopens the shard from its log and the wanted activation
            // takes it back at a bumped term.
            store.failWritesUnder(null);
            loop.tick(clock.addAndGet(1_000L));
            final ShardId reopened = a.reconciler().openShards().iterator().next();
            assertFalse(a.isWriteFenced(reopened));
            assertTrue("history the log holds survives the reopen", a.get(reopened, "earlier").found());
            assertFalse("the operation the caller was told failed must not survive it", a.get(reopened, "lost").found());
            assertTrue("and writes flow again", a.index(reopened, "after", "{\"msg\":\"after\"}").seqNo() >= 0);
        }
    }

    // ---------------------------------------------------------------- a store that parks on demand

    /**
     * A blob store that holds one chosen operation open until the test says otherwise, and can refuse
     * writes under a path.
     *
     * <p>Register reads, blob writes and the two sides of a compare-and-swap can each be gated on a path
     * fragment. A gate fires once, for the first matching operation after it is armed, and parks that
     * thread until released. This is what lets a test stand inside an interleaving instead of racing
     * for it.
     */
    static final class GatedBlobStore implements BlobStore {

        enum Point {
            READ_REGISTER,
            WRITE_BLOB,
            BEFORE_CAS,
            AFTER_CAS
        }

        static final class Gate {
            final Point point;
            final String pathFragment;
            final AtomicBoolean armed = new AtomicBoolean(true);
            final CountDownLatch reached = new CountDownLatch(1);
            private final CountDownLatch released = new CountDownLatch(1);

            Gate(Point point, String pathFragment) {
                this.point = point;
                this.pathFragment = pathFragment;
            }

            /** Lets the parked thread go, and disarms the gate if nothing reached it. */
            void release() {
                armed.set(false);
                released.countDown();
            }
        }

        private final BlobStore delegate;
        private final List<Gate> gates = new CopyOnWriteArrayList<>();
        private volatile String failWritesUnder;

        GatedBlobStore(BlobStore delegate) {
            this.delegate = delegate;
        }

        Gate arm(Point point, String pathFragment) {
            final Gate gate = new Gate(point, pathFragment);
            gates.add(gate);
            return gate;
        }

        void failWritesUnder(String pathFragment) {
            this.failWritesUnder = pathFragment;
        }

        private void pass(Point point, String path) throws IOException {
            for (Gate gate : gates) {
                if (gate.point == point && path.contains(gate.pathFragment) && gate.armed.compareAndSet(true, false)) {
                    gate.reached.countDown();
                    try {
                        if (gate.released.await(2, TimeUnit.MINUTES) == false) {
                            throw new IOException("a gated " + point + " under " + path + " was never released");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted while parked at " + point, e);
                    }
                }
            }
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new Gated(delegate.blobContainer(path));
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        private final class Gated extends DelegatingBlobContainer {

            private final String pathString;

            Gated(BlobContainer inner) {
                super(inner);
                this.pathString = inner.path().buildAsString();
            }

            @Override
            public Optional<BlobRegister> readRegister(String blobName) throws IOException {
                pass(Point.READ_REGISTER, pathString);
                return super.readRegister(blobName);
            }

            @Override
            public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
                final String broken = failWritesUnder;
                if (broken != null && pathString.contains(broken)) {
                    throw new IOException("injected: the store refused a write under " + pathString);
                }
                pass(Point.WRITE_BLOB, pathString);
                super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
            }

            @Override
            public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
                throws IOException {
                pass(Point.BEFORE_CAS, pathString);
                final BlobRegisterCasResult result = super.compareAndSwapRegister(blobName, expectedGeneration, newValue);
                pass(Point.AFTER_CAS, pathString);
                return result;
            }

            @Override
            public Map<String, BlobContainer> children() throws IOException {
                return super.children().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new Gated(e.getValue())));
            }
        }
    }
}
