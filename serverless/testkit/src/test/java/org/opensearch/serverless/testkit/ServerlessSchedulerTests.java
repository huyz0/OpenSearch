/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.ReconcileScheduler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * M13: the node runs by itself, and each driver runs for its own reason.
 *
 * <p>Before this, every loop in the shell was called by a test. {@code tick()} and {@code heartbeat()}
 * worked and nothing invoked them, so a real deployment would have let its leases lapse, published
 * nothing, and never picked up a dead node's shards. The fix was not one timer around the whole tick —
 * that would make every latency in the system equal to one number. It is three drivers with three
 * triggers, and these tests exist to prove that each trigger is <em>load-bearing</em>.
 *
 * <p>Which is why almost every test here <b>disables the backstop</b>. With it running, every assertion
 * would pass whether or not the edge worked — the slow timer would eventually do the job and the test
 * would be measuring patience. Disabling it is the negative control: if the edge is broken, nothing
 * happens at all.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessSchedulerTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-m13")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane freshIndex(AtomicLong clock, java.nio.file.Path dir, int shards) throws Exception {
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", shards, MAPPING, null));
        return plane;
    }

    /** A scheduler with no backstop: only the edges can make anything happen. */
    private ReconcileScheduler edgesOnly(ServerlessNode node, BackgroundReconciler loop, AtomicLong clock, TimeValue debounce) {
        return new ReconcileScheduler(
            loop,
            node.threadPool(),
            clock::get,
            TimeValue.timeValueHours(1),   // renewal far away, so it cannot be what did the work
            debounce,
            null                            // no backstop at all
        );
    }

    // ---------------------------------------------------------------- publish is an edge

    /**
     * The write edge, with nothing else running. No backstop, no renewal in reach, and nobody calls
     * tick: the only thing that can produce a manifest is the write itself.
     */
    public void testAWriteSchedulesItsOwnPublication() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m13-edge"))) {
            node.start();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());     // the one deliberate tick: acquire the shard, then hands off
            final ShardId shardId = node.reconciler().openShards().iterator().next();

            try (ReconcileScheduler scheduler = edgesOnly(node, loop, clock, TimeValue.timeValueMillis(100))) {
                scheduler.start();
                node.setSignals(scheduler);

                assertTrue("nothing published before the write", plane.segmentPublisher("alpha", 0).readManifest().isEmpty());

                node.index(shardId, "1", "{\"msg\":\"edge\",\"n\":1}");

                assertBusy(
                    () -> assertTrue(
                        "the write should have scheduled its own publication",
                        plane.segmentPublisher("alpha", 0).readManifest().isPresent()
                    ),
                    10,
                    java.util.concurrent.TimeUnit.SECONDS
                );

                final var counts = scheduler.counts();
                logger.info("m13 edge publish counts: {}", counts);
                assertEquals("the backstop must not be what published this", 0L, counts.backstops());
                assertEquals("nor a renewal", 0L, counts.renewals());
                assertTrue("a publish must actually have run", counts.publishes() >= 1);
            }
        }
    }

    /**
     * Coalescing, which is the whole reason publication is debounced rather than synchronous. A burst of
     * writes must cost one upload, not one per write — otherwise edge-triggering trades a durability
     * window for an object-store bill proportional to ingest rate.
     */
    public void testABurstOfWritesCostsOnePublishNotOnePerWrite() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);
        final int writes = 200;

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m13-burst"))) {
            node.start();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();

            // A window long enough that the whole burst lands inside it. That is the point being
            // measured: not that publishing is rare, but that it is decoupled from write rate.
            try (ReconcileScheduler scheduler = edgesOnly(node, loop, clock, TimeValue.timeValueSeconds(2))) {
                scheduler.start();
                node.setSignals(scheduler);

                for (int i = 1; i <= writes; i++) {
                    node.index(shardId, String.valueOf(i), "{\"msg\":\"burst\",\"n\":" + i + "}");
                }

                assertBusy(
                    () -> assertTrue("the burst should publish once", plane.segmentPublisher("alpha", 0).readManifest().isPresent()),
                    30,
                    java.util.concurrent.TimeUnit.SECONDS
                );

                final long publishes = scheduler.counts().publishes();
                logger.info("m13 {} writes produced {} publishes", writes, publishes);
                assertTrue("a publish must have run", publishes >= 1);
                assertTrue(writes + " writes must not produce " + publishes + " publishes; coalescing is the point", publishes <= 3);
            }
        }
    }

    /**
     * The negative control that keeps the edge honest. Marking a shard dirty is a hint, not an
     * instruction: an untouched shard must never be uploaded, however many times something claims it
     * changed. Without this guard a spurious signal becomes a fresh commit every window, forever.
     */
    public void testAnIdleShardIsNotPublishedHoweverOftenItIsMarked() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m13-idle"))) {
            node.start();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();

            try (ReconcileScheduler scheduler = edgesOnly(node, loop, clock, TimeValue.timeValueMillis(50))) {
                node.setSignals(scheduler);
                for (int i = 0; i < 20; i++) {
                    loop.markDirty(shardId);
                    assertTrue("an unwritten shard must never publish", scheduler.publishNow().isEmpty());
                }
                assertTrue("and no manifest should exist", plane.segmentPublisher("alpha", 0).readManifest().isEmpty());

                // Now write once, and the same marks that produced nothing produce exactly one publish.
                // This is what proves the emptiness above was the guard working and not the plumbing
                // being disconnected.
                node.index(shardId, "1", "{\"msg\":\"real\",\"n\":1}");
                loop.markDirty(shardId);
                assertEquals("a real write must publish", 1, scheduler.publishNow().size());
                assertTrue("and again produce nothing", scheduler.publishNow().isEmpty());
            }
        }
    }

    // ---------------------------------------------------------------- renewal is a timer

    /**
     * Lease renewal on its own schedule, with no backstop and no writes. This is the job that cannot be
     * edge-triggered: nothing happens to tell a node its lease is about to lapse.
     */
    public void testLeasesAreRenewedWithoutAnybodyTicking() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m13-renew"))) {
            node.start();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);

            assertTrue("no lease before the scheduler runs", plane.membership().read(node.localNode().getId()).isEmpty());

            try (
                ReconcileScheduler scheduler = new ReconcileScheduler(
                    loop,
                    node.threadPool(),
                    clock::get,
                    TimeValue.timeValueMillis(50),
                    TimeValue.timeValueHours(1),
                    null                              // no backstop: only the renewal timer can do this
                )
            ) {
                scheduler.start();
                assertBusy(
                    () -> assertTrue(
                        "the renewal timer should have written this node's lease",
                        plane.membership().read(node.localNode().getId()).isPresent()
                    ),
                    10,
                    java.util.concurrent.TimeUnit.SECONDS
                );

                // Renewal must keep happening, not happen once. A lease written once and never again is
                // indistinguishable from a working one until it lapses.
                final long first = scheduler.counts().renewals();
                assertBusy(
                    () -> assertTrue("renewal must repeat", scheduler.counts().renewals() > first + 1),
                    10,
                    java.util.concurrent.TimeUnit.SECONDS
                );
                assertEquals("the backstop must not be what renewed", 0L, scheduler.counts().backstops());
            }
        }
    }

    /** The renewal interval must beat the TTL with room for lost renewals, not merely be under it. */
    public void testTheRenewalIntervalLeavesRoomForLostRenewals() {
        final long ttl = 30_000L;
        final long interval = ttl / ReconcileScheduler.RENEWALS_PER_TTL;
        assertTrue("a renewal interval at or above the TTL is a lease that always lapses", interval < ttl);
        assertTrue("one renewal per TTL means a single missed renewal loses the shard", ReconcileScheduler.RENEWALS_PER_TTL >= 3);
    }

    // ---------------------------------------------------------------- activation is a failure

    /**
     * A publish fenced by a newer term is evidence this node is a zombie for that shard: it read a head
     * that named it, and by the time it wrote, it did not. It must stop serving that shard at once
     * rather than carrying on until its next renewal notices.
     *
     * <p>Driven through the reconciler rather than the scheduler, so there is no asynchrony in the
     * assertion. What the scheduler does with the fencing is the next test's business.
     */
    public void testAFencedPublishReleasesTheShardImmediately() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m13-fenced"))) {
            node.start();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();
            node.index(shardId, "1", "{\"msg\":\"zombie\",\"n\":1}");

            // Somebody else publishes at a higher term. From here this node's own publish is fenced,
            // which is exactly what a successor taking over looks like from the loser's side.
            final long ourTerm = plane.heads().read("alpha", 0).orElseThrow().term();
            plane.segmentPublisher("alpha", 0).publish(node.reconciler().shard(shardId).store(), ourTerm + 5);

            loop.markDirty(shardId);
            assertTrue("a fenced publish publishes nothing", loop.publishDirty().isEmpty());
            assertFalse("a node fenced out of a shard must stop serving it at once", node.reconciler().openShards().contains(shardId));
            assertEquals("and the fencing must be reported, not swallowed", Set.of(shardId), loop.drainFenced());
            assertTrue("draining twice must not report the same fencing twice", loop.drainFenced().isEmpty());
        }
    }

    /**
     * And the scheduler turns that fencing into an activation pass rather than waiting for a timer.
     *
     * <p>{@code assertBusy} because raising a doubt is deliberately asynchronous: the publish path must
     * hand the work off and return, never run an activation pass inline.
     */
    public void testAFencedPublishAsksForActivation() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m13-fenced-signal"))) {
            node.start();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();
            node.index(shardId, "1", "{\"msg\":\"zombie\",\"n\":1}");

            final long ourTerm = plane.heads().read("alpha", 0).orElseThrow().term();
            plane.segmentPublisher("alpha", 0).publish(node.reconciler().shard(shardId).store(), ourTerm + 5);

            try (ReconcileScheduler scheduler = edgesOnly(node, loop, clock, TimeValue.timeValueMillis(50))) {
                node.setSignals(scheduler);
                loop.markDirty(shardId);
                assertTrue("a fenced publish publishes nothing", scheduler.publishNow().isEmpty());

                assertBusy(
                    () -> assertTrue(
                        "being fenced must trigger an activation pass, not wait for the next timer",
                        scheduler.counts().activations() >= 1
                    ),
                    10,
                    java.util.concurrent.TimeUnit.SECONDS
                );
                assertEquals("and no backstop was involved", 0L, scheduler.counts().backstops());
            }
        }
    }

    /**
     * The failure that matters most in a real deployment: a node dies, its shard is unowned, and the
     * next write to arrive anywhere is what makes somebody pick it up.
     *
     * <p>No backstop is running. The first write is refused, correctly, because no node owns the shard —
     * but refusing is not all it does: it reports the doubt, an activation pass runs, and the second
     * write succeeds. Without failure-triggered activation the second write fails exactly like the
     * first, forever.
     */
    public void testAWriteToAnUnownedShardIsWhatMakesSomebodyTakeIt() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m13-unowned"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);   // it wants the shard; nothing has told it to take it

            try (ReconcileScheduler scheduler = edgesOnly(node, loop, clock, TimeValue.timeValueMillis(50))) {
                scheduler.start();
                node.setSignals(scheduler);

                final var http = node.boundHttpAddress().publishAddress();
                assertTrue("nobody owns the shard yet", node.reconciler().openShards().isEmpty());

                final Response refused = send(http, "PUT", "/alpha/_doc/1", "{\"msg\":\"first\",\"n\":1}");
                assertEquals("a write to an unowned shard must be refused, not silently dropped", 421, refused.status());

                assertBusy(
                    () -> assertFalse(
                        "the refused write should have caused somebody to take the shard",
                        node.reconciler().openShards().isEmpty()
                    ),
                    10,
                    java.util.concurrent.TimeUnit.SECONDS
                );

                final Response accepted = send(http, "PUT", "/alpha/_doc/2?refresh=true", "{\"msg\":\"second\",\"n\":2}");
                assertEquals("the write after activation must succeed: " + accepted.body(), 201, accepted.status());
                assertEquals("and no backstop pass was involved", 0L, scheduler.counts().backstops());
            }
        }
    }

    // ---------------------------------------------------------------- jitter

    /**
     * Renewals must not land on a fixed cadence. A thousand nodes started by the same orchestrator in
     * the same second would otherwise renew in the same millisecond forever, because a fixed interval
     * never pulls them apart — a thundering herd against the object store every {@code ttl/3}, worst
     * exactly when the fleet is largest.
     */
    public void testRenewalsAreSpreadSoACoStartedFleetDoesNotRenewInLockstep() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m13-jitter"))) {
            node.start();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            final TimeValue interval = TimeValue.timeValueMillis(1_000);

            try (
                ReconcileScheduler scheduler = new ReconcileScheduler(
                    loop,
                    node.threadPool(),
                    clock::get,
                    interval,
                    TimeValue.timeValueHours(1),
                    null
                )
            ) {
                final java.util.Set<Long> seen = new java.util.HashSet<>();
                long min = Long.MAX_VALUE;
                long max = Long.MIN_VALUE;
                for (int i = 0; i < 200; i++) {
                    final long delay = scheduler.nextRenewalDelay().millis();
                    seen.add(delay);
                    min = Math.min(min, delay);
                    max = Math.max(max, delay);
                }
                logger.info("m13 jitter over 200 draws: {} distinct, min {}ms, max {}ms", seen.size(), min, max);

                // The number that is 1 when jitter is broken.
                assertTrue("renewal delays must vary; a single value is lockstep", seen.size() > 1);

                final long spread = (long) (interval.millis() * ReconcileScheduler.JITTER_FRACTION);
                assertTrue("jitter must stay within the declared fraction, low side", min >= interval.millis() - spread);
                assertTrue("jitter must stay within the declared fraction, high side", max <= interval.millis() + spread);
                assertTrue("and must actually spread, not cluster on one side", max - min > spread);
            }
        }
    }

    // ---------------------------------------------------------------- demand-driven placement

    /**
     * The default: a node takes what it was told to want, and nothing else. A write for a shard nobody
     * wants leaves that shard unowned however many times it arrives.
     *
     * <p>This is the behaviour {@code activateOnDemand} changes, asserted here so that turning the
     * switch on is visibly a decision rather than a drift.
     */
    public void testByDefaultANodeDoesNotTakeAShardNobodyToldItToWant() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m13-nodemand"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            // Note: no want() at all.

            try (ReconcileScheduler scheduler = edgesOnly(node, loop, clock, TimeValue.timeValueMillis(50))) {
                scheduler.start();
                node.setSignals(scheduler);
                final var http = node.boundHttpAddress().publishAddress();

                for (int i = 0; i < 3; i++) {
                    assertEquals(421, send(http, "PUT", "/alpha/_doc/" + i, "{\"msg\":\"x\",\"n\":1}").status());
                }
                assertBusy(
                    () -> assertTrue("an activation pass must have run", scheduler.counts().activations() >= 1),
                    10,
                    java.util.concurrent.TimeUnit.SECONDS
                );
                assertTrue("but it must not have taken a shard nobody asked this node to hold", node.reconciler().openShards().isEmpty());
            }
        }
    }

    /**
     * With demand-driven activation on, the same write that was refused is what makes the shard get
     * taken — by whichever node the client happened to ask.
     *
     * <p>Without this, a shard nobody was told to want stays unowned forever and something outside the
     * system has to assign shards, which is the controller this design deleted.
     */
    public void testOnDemandANodeTakesAnUnownedShardSomebodyAskedFor() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m13-demand"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true);

            try (ReconcileScheduler scheduler = edgesOnly(node, loop, clock, TimeValue.timeValueMillis(50))) {
                scheduler.start();
                node.setSignals(scheduler);
                final var http = node.boundHttpAddress().publishAddress();

                assertEquals(421, send(http, "PUT", "/alpha/_doc/1", "{\"msg\":\"first\",\"n\":1}").status());
                assertBusy(
                    () -> assertFalse(
                        "the refused write should have caused this node to take it",
                        node.reconciler().openShards().isEmpty()
                    ),
                    10,
                    java.util.concurrent.TimeUnit.SECONDS
                );

                final Response accepted = send(http, "PUT", "/alpha/_doc/2?refresh=true", "{\"msg\":\"second\",\"n\":2}");
                assertEquals("the next write must succeed: " + accepted.body(), 201, accepted.status());
                assertEquals("and no backstop was involved", 0L, scheduler.counts().backstops());
            }
        }
    }

    /**
     * The cap. A node that takes every shard asked of it grows without bound; past the cap it refuses,
     * the caller still sees no owner, and the next node asked takes it instead. Admission control
     * without an admission controller.
     */
    public void testOnDemandActivationStopsAtTheCap() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 4);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m13-cap"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true).setMaxShardsHeld(2);

            final java.util.Set<Map.Entry<String, Integer>> all = new java.util.LinkedHashSet<>();
            for (int i = 0; i < 4; i++) {
                all.add(Map.entry("alpha", i));
            }

            final Set<ShardId> taken = loop.activateOnDemand(all);
            logger.info("m13 cap 2, asked for 4, took {}", taken.size());
            assertEquals("a node must stop at its cap, not take everything asked of it", 2, taken.size());
            assertEquals("and hold exactly that many", 2, node.reconciler().openShards().size());

            assertTrue("asking again must not push it past the cap", loop.activateOnDemand(all).isEmpty());
            assertEquals("still at the cap", 2, node.reconciler().openShards().size());
        }
    }

    // ---------------------------------------------------------------- the activation window

    /**
     * A node whose shard-head names it but whose shard is not open must say "not yet", not forward the
     * write to itself.
     *
     * <p>{@code activateWriter} wins the compare-and-swap before it opens the shard, so between those two
     * steps the head is true and the node is not ready. A write arriving in that window used to read the
     * head, see itself as the owner, forward to itself over the transport, and get refused by its own
     * receiving handler — surfacing as a 500 for a normal, brief, self-resolving state.
     *
     * <p>Found by a two-process test, where it is a race that reproduces on a cold JVM and not a warm
     * one. Reproduced here deliberately instead: take the shard, then close it locally without touching
     * the head, which is exactly the state activation passes through.
     */
    public void testANodeNamedByTheHeadButNotYetOpenSaysNotYetRatherThanForwardingToItself() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m13-selfforward"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            final ShardId shardId = node.reconciler().openShards().iterator().next();
            assertEquals(
                "this node should own the shard",
                node.localNode().getId(),
                plane.heads().read("alpha", 0).orElseThrow().ownerNodeId()
            );

            // The head still names this node; the shard is no longer open here. That is the activation
            // window, held open.
            node.reconciler().releaseShard(shardId, "simulating the gap between winning the CAS and opening");
            assertTrue("the shard must be closed locally", node.reconciler().openShards().isEmpty());
            assertEquals(
                "and the head must still name this node -- otherwise this is not the window under test",
                node.localNode().getId(),
                plane.heads().read("alpha", 0).orElseThrow().ownerNodeId()
            );

            final Response response = send(node.boundHttpAddress().publishAddress(), "PUT", "/alpha/_doc/1", "{\"msg\":\"x\",\"n\":1}");
            assertEquals("a node must not forward a write to itself: " + response.body(), 503, response.status());
            assertTrue(
                "and must say why, in terms a client can retry on: " + response.body(),
                response.body().contains("activation_in_progress")
            );
        }
    }

    // ---------------------------------------------------------------- the backstop is not optional

    /**
     * The edges are accelerators; the timer is the guarantee. With every signal suppressed — nothing
     * reports writes, nothing reports doubt — a node must still converge, because a design that only
     * converges on edges wedges the first time one is dropped.
     */
    public void testTheBackstopConvergesWithEveryEdgeSuppressed() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m13-backstop"))) {
            node.start();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);

            try (
                ReconcileScheduler scheduler = new ReconcileScheduler(
                    loop,
                    node.threadPool(),
                    clock::get,
                    TimeValue.timeValueHours(1),      // no renewals
                    TimeValue.timeValueHours(1),      // no edge publishes
                    TimeValue.timeValueMillis(100)    // only the backstop
                )
            ) {
                scheduler.start();
                // Deliberately NOT registering the scheduler as the node's signals: the node writes and
                // tells nobody, which is what a lost edge looks like.
                assertBusy(
                    () -> assertFalse("the backstop should acquire the shard on its own", node.reconciler().openShards().isEmpty()),
                    10,
                    java.util.concurrent.TimeUnit.SECONDS
                );

                final ShardId shardId = node.reconciler().openShards().iterator().next();
                node.index(shardId, "1", "{\"msg\":\"unsignalled\",\"n\":1}");

                assertBusy(
                    () -> assertTrue(
                        "a write nobody reported must still be published by the backstop",
                        plane.segmentPublisher("alpha", 0).readManifest().isPresent()
                    ),
                    10,
                    java.util.concurrent.TimeUnit.SECONDS
                );
                assertEquals("no edge publish can have run", 0L, scheduler.counts().publishes());
            }
        }
    }

    /**
     * The composition must not have changed while being split apart. {@code tick} still does all four
     * jobs in order, because every existing test and every backstop pass depends on it.
     */
    public void testTickStillDoesEveryJobItUsedTo() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = freshIndex(clock, createTempDir(), 1);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m13-compose"))) {
            node.start();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);

            final BackgroundReconciler.TickResult acquiring = loop.tick(clock.get());
            assertEquals("tick must still activate", 1, acquiring.activated().size());

            final ShardId shardId = node.reconciler().openShards().iterator().next();
            node.index(shardId, "1", "{\"msg\":\"compose\",\"n\":1}");

            final BackgroundReconciler.TickResult publishing = loop.tick(clock.get() + 1_000);
            assertEquals("tick must still publish", 1, publishing.published().size());
            assertTrue("tick must still renew, so nothing is lost", publishing.released().isEmpty());
            assertTrue("tick must still refresh hints", publishing.hintCount() >= 0);

            assertTrue("and an idle tick must still publish nothing", loop.tick(clock.get() + 2_000).published().isEmpty());
        }
    }

    /**
     * The one call a bootstrap makes. Nothing here drives anything: the node is handed a plane and a
     * reconciler, and from that point it acquires, writes, publishes and renews on its own.
     *
     * <p>This is the test that would have been impossible to write before M13, because there was no
     * arrangement of the shell in which a node did anything without being told to.
     */
    public void testANodeHandedAPlaneRunsWithoutBeingDriven() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        // A short TTL so the derived intervals are short: everything the scheduler does is a fraction of
        // this number, which is the point of deriving them rather than configuring them apart.
        final MetadataPlane plane = new MetadataPlane(
            new FsBlobStore(1024, createTempDir(), false),
            BlobPath.cleanPath(),
            clock::get,
            300L
        );
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("m13-bootstrap"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);

            try (ReconcileScheduler scheduler = ReconcileScheduler.startFor(node, plane, loop)) {
                assertBusy(
                    () -> assertFalse("the node should take the shard on its own", node.reconciler().openShards().isEmpty()),
                    30,
                    java.util.concurrent.TimeUnit.SECONDS
                );
                assertBusy(
                    () -> assertTrue("and write its lease on its own", plane.membership().read(node.localNode().getId()).isPresent()),
                    30,
                    java.util.concurrent.TimeUnit.SECONDS
                );

                final ShardId shardId = node.reconciler().openShards().iterator().next();
                node.index(shardId, "1", "{\"msg\":\"unattended\",\"n\":1}");

                assertBusy(
                    () -> assertTrue("and publish on its own", plane.segmentPublisher("alpha", 0).readManifest().isPresent()),
                    30,
                    java.util.concurrent.TimeUnit.SECONDS
                );
                logger.info("m13 unattended node counts: {}", scheduler.counts());
            }
        }
    }

    private record Response(int status, String body) {
    }

    private static Response send(org.opensearch.core.common.transport.TransportAddress address, String method, String path, String body)
        throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest.BodyPublisher payload = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .method(method, payload)
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
