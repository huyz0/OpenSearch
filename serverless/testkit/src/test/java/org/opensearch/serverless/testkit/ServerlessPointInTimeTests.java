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
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.cluster.ReaderPlacement;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.GarbageCollector;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A frozen view of an index, and the promise that its bytes survive.
 *
 * <p>{@code search_after} pages through a result set and the index moves between pages. A point in time
 * makes both pages read the same commits, which is the difference between exporting a result set and
 * exporting whatever happened to be there each time you asked.
 *
 * <p><b>The hard half is not freezing, it is the garbage collector.</b> The sweep deletes a blob when the
 * live commit does not name it and it belongs to a dead term — which is exactly what a frozen view's files
 * become a moment after the writer publishes again. A view whose files were swept would dissolve under a
 * paging caller and look like corruption. So the test that matters here is the one that publishes over a
 * view, sweeps, and then reads it anyway.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessPointInTimeTests extends OpenSearchTestCase {

    private static final long TTL = 300_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"rank\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-pit")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private Settings searchNodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-pit")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "search")
            .build();
    }

    /** A view keeps showing what it froze while the index moves on. */
    public void testAFrozenViewDoesNotSeeLaterWrites() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("frozen", "uuid-frozen-0000000", 2, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("pit-basic"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "frozen", 2);

            for (int i = 1; i <= 4; i++) {
                assertEquals(201, send(node, "PUT", "/frozen/_doc/a" + i + "?refresh=true", body(i)).status());
            }
            loop.tick(clock.get());   // publish, so there is a commit to freeze

            final Response taken = send(node, "POST", "/frozen/_pit?keep_alive=10m", null);
            assertEquals(taken.body(), 200, taken.status());
            final String pit = field(taken.body(), "pit_id");

            // The index moves on, and is published, so the frozen commit is genuinely superseded.
            for (int i = 5; i <= 8; i++) {
                assertEquals(201, send(node, "PUT", "/frozen/_doc/a" + i + "?refresh=true", body(i)).status());
            }
            loop.tick(clock.get() + 1);

            final Response live = send(node, "POST", "/frozen/_search", "{\"size\":20,\"query\":{\"match_all\":{}}}");
            assertTrue("a live search must see everything: " + live.body(), live.body().contains("\"value\":8"));

            final Response frozen = send(node, "POST", "/frozen/_search?pit=" + pit, "{\"size\":20,\"query\":{\"match_all\":{}}}");
            assertEquals(frozen.body(), 200, frozen.status());
            assertTrue("and the view only what it froze: " + frozen.body(), frozen.body().contains("\"value\":4"));
            assertTrue("over every shard it froze: " + frozen.body(), frozen.body().contains("\"complete\":true"));
        }
    }

    /**
     * The collector does not delete what a view is holding.
     *
     * <p>The assertion this whole feature stands on. Without it a frozen view is a promise the sweep never
     * heard, and a caller paging through one would find it dissolving halfway — a failure that would look
     * like corruption rather than like a deletion.
     */
    public void testASweepDoesNotCollectWhatAViewIsHolding() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("swept", "uuid-swept-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("pit-sweep"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "swept", 1);

            for (int i = 1; i <= 4; i++) {
                assertEquals(201, send(node, "PUT", "/swept/_doc/s" + i + "?refresh=true", body(i)).status());
            }
            loop.tick(clock.get());

            final String pit = field(send(node, "POST", "/swept/_pit?keep_alive=10m", null).body(), "pit_id");

            // Move the shard to a new term and publish there, so the frozen commit's files are in a dead
            // term container and unreferenced by the live manifest -- the exact condition the sweep deletes.
            assertTrue(plane.heads().release("swept", 0, node.localNode().getId()));
            node.activateWriter(plane, "swept", 0);
            for (int i = 5; i <= 12; i++) {
                assertEquals(201, send(node, "PUT", "/swept/_doc/s" + i + "?refresh=true", body(i)).status());
            }
            loop.tick(clock.get() + 1);

            final var swept = new GarbageCollector(store, BlobPath.cleanPath()).collectShard(plane, "swept", 0);
            logger.info("pit: the sweep collected {} blobs with a view held", swept.size());

            // And the view still reads.
            final Response frozen = send(node, "POST", "/swept/_search?pit=" + pit, "{\"size\":20,\"query\":{\"match_all\":{}}}");
            assertEquals("the view must still be readable after a sweep: " + frozen.body(), 200, frozen.status());
            assertTrue("and still show what it froze: " + frozen.body(), frozen.body().contains("\"value\":4"));
        }
    }

    /** Paging a frozen view sees a consistent result set, which is what it is for. */
    public void testPagingAFrozenViewIsConsistent() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("paged", "uuid-paged-00000000", 2, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("pit-paging"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "paged", 2);

            for (int rank = 1; rank <= 6; rank++) {
                assertEquals(201, send(node, "PUT", "/paged/_doc/p" + rank + "?refresh=true", body(rank)).status());
            }
            loop.tick(clock.get());
            final String pit = field(send(node, "POST", "/paged/_pit?keep_alive=10m", null).body(), "pit_id");

            final Response first = send(node, "POST", "/paged/_search?pit=" + pit, "{\"size\":3,\"sort\":[{\"rank\":\"asc\"}]}");
            assertEquals(java.util.List.of(1L, 2L, 3L), ranksIn(first.body()));

            // A view has the name of the index it is a view of, and every lookup on the write path finds a
            // shard by name and number. So the two must not be in the same set: the node is serving two
            // shards of "paged" and holding two views of them, and a write asking for shard 0 of "paged"
            // must find the shard it has been writing to rather than a reader that happens to share the
            // name. Asserted rather than left to the writes below, which caught it only when a set
            // iterated the wrong one first.
            assertEquals("the views must not be counted among the shards this node serves", 2, node.reconciler().openShards().size());
            for (var shardId : node.reconciler().openShards()) {
                assertEquals("uuid-paged-00000000", shardId.getIndex().getUUID());
            }
            assertEquals("and both views are held", 2, node.reconciler().frozenShards().size());

            // Documents arrive between the pages, and are published. A live paged search would shift; this
            // must not.
            for (int rank = 7; rank <= 12; rank++) {
                final Response written = send(node, "PUT", "/paged/_doc/p" + rank + "?refresh=true", body(rank));
                assertEquals(written.body(), 201, written.status());
            }
            loop.tick(clock.get() + 1);

            final Response second = send(
                node,
                "POST",
                "/paged/_search?pit=" + pit,
                "{\"size\":3,\"sort\":[{\"rank\":\"asc\"}],\"search_after\":[3]}"
            );
            assertEquals("page two must continue the page one the view showed", java.util.List.of(4L, 5L, 6L), ranksIn(second.body()));

            final Response third = send(
                node,
                "POST",
                "/paged/_search?pit=" + pit,
                "{\"size\":3,\"sort\":[{\"rank\":\"asc\"}],\"search_after\":[6]}"
            );
            assertTrue("and the view ends where it was frozen: " + third.body(), ranksIn(third.body()).isEmpty());
        }
    }

    /** A released or expired view is gone, and says so rather than answering emptily. */
    public void testAViewCanBeReleasedAndExpires() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("brief", "uuid-brief-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("pit-release"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "brief", 1);
            assertEquals(201, send(node, "PUT", "/brief/_doc/1?refresh=true", body(1)).status());
            loop.tick(clock.get());

            final String released = field(send(node, "POST", "/brief/_pit?keep_alive=10m", null).body(), "pit_id");
            assertEquals(200, send(node, "POST", "/brief/_search?pit=" + released, "{\"query\":{\"match_all\":{}}}").status());
            assertEquals(200, send(node, "DELETE", "/_pit/" + released, null).status());
            final Response afterRelease = send(node, "POST", "/brief/_search?pit=" + released, "{\"query\":{\"match_all\":{}}}");
            assertEquals("a released view must not answer: " + afterRelease.body(), 404, afterRelease.status());
            assertEquals("and releasing it twice finds nothing", 404, send(node, "DELETE", "/_pit/" + released, null).status());

            // Expiry is by the clock, and a search does not extend it.
            final String expiring = field(send(node, "POST", "/brief/_pit?keep_alive=1m", null).body(), "pit_id");
            assertEquals(200, send(node, "POST", "/brief/_search?pit=" + expiring, "{\"query\":{\"match_all\":{}}}").status());
            clock.addAndGet(120_000L);
            final Response expired = send(node, "POST", "/brief/_search?pit=" + expiring, "{\"query\":{\"match_all\":{}}}");
            assertEquals("an expired view must not answer either: " + expired.body(), 404, expired.status());

            // And the collector reaps it, so its files stop being held.
            assertEquals("the expired view must be reaped", 1, plane.reapPointsInTime(clock.get()));
            assertEquals("and nothing is held any more", java.util.List.of(), plane.livePointsInTime(clock.get()));
        }
    }

    /** A shard that has published nothing cannot be frozen, because a partial view is a wrong one. */
    public void testAnIndexWithNothingPublishedCannotBeFrozen() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("fresh", "uuid-fresh-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("pit-fresh"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, "fresh", 1);

            final Response tooSoon = send(node, "POST", "/fresh/_pit", null);
            assertEquals("nothing published, nothing to freeze: " + tooSoon.body(), 409, tooSoon.status());
            assertTrue(tooSoon.body().contains("published nothing yet"));
            assertEquals(404, send(node, "POST", "/absent/_pit", null).status());
        }
    }

    /**
     * A reconcile pass reaps expired views and closes what this node was holding for them.
     *
     * <p><b>Nothing did either, and the second half is the one that leaks.</b> Releasing a view over REST
     * closes its shards on the node that opened them; letting one expire closed nothing at all, so a node
     * that had served one search over a view held those shards for the life of the process — counted
     * against its bound, never a candidate for eviction, serving a commit nobody could still ask for.
     * Expiry existed as an answer to a request and not as a thing that happened.
     */
    public void testAPassReapsExpiredViewsAndClosesTheirShards() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("reaped", "uuid-reaped-0000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("pit-reap"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "reaped", 1);
            assertEquals(201, send(node, "PUT", "/reaped/_doc/1?refresh=true", body(1)).status());
            loop.tick(clock.get());

            final String pit = field(send(node, "POST", "/reaped/_pit?keep_alive=1m", null).body(), "pit_id");
            assertEquals(200, send(node, "POST", "/reaped/_search?pit=" + pit, "{\"query\":{\"match_all\":{}}}").status());
            assertEquals("the search must have opened the view here", 1, node.reconciler().frozenShards().size());

            // Nothing has expired yet, so the pass leaves it exactly alone.
            assertEquals(0, loop.reapExpiredViews());
            assertEquals(1, node.reconciler().frozenShards().size());

            clock.addAndGet(120_000L);
            assertEquals("the expired view must be reaped", 1, loop.reapExpiredViews());
            assertTrue("and its record is gone", plane.livePointsInTime(clock.get()).isEmpty());
            assertEquals("and the node stops holding its shards", 0, node.reconciler().frozenShards().size());
            assertTrue(
                "with nothing left open under the view's identity",
                node.reconciler().heldShards().stream().noneMatch(s -> s.getIndex().getUUID().equals(pit))
            );
        }
    }

    /**
     * A stray object in the points-in-time container does not switch off the garbage collector.
     *
     * <p><b>Found by a test filesystem doing exactly this.</b> Lucene's {@code ExtrasFS} drops a file named
     * {@code extra0} into random directories, one landed in the points-in-time container, and the plane
     * reported a live view holding everything — for ever, since nothing would ever delete it. The rule that
     * an unreadable record pins its files is right and stays; what was wrong is that it applied to anything
     * at all that appeared in that container. One console upload or backup tool and a deployment's storage
     * grows without bound with nothing but a log line to say why.
     *
     * <p>A half-written record — the case the conservative rule exists for — still has the name this system
     * gave it, so it is still treated as live. That is the second half of this test, and without it the
     * first half would be an argument for deleting things we cannot read.
     */
    public void testAStrayObjectAmongTheViewsIsNotTreatedAsOne() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final var store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        final var container = store.blobContainer(org.opensearch.serverless.metadata.RegisterMap.pointsInTime(BlobPath.cleanPath()));

        // Not named "extra0", although that is the name that found this: ExtrasFS creates that file
        // itself often enough that writing it here fails with FileAlreadyExists about one run in three.
        // A fixture that collides with the thing it is imitating is a flaky test rather than a stricter
        // one.
        final String stray = "somebody-elses-file.txt";
        final var junk = new org.opensearch.core.common.bytes.BytesArray("not a point in time");
        container.writeBlob(stray, junk.streamInput(), junk.length(), true);
        assertEquals("a name this system would never mint must be ignored", java.util.List.of(), plane.livePointsInTime(clock.get()));
        assertEquals("and must not be deleted either", 0, plane.reapPointsInTime(clock.get()));
        assertTrue("it is somebody else's file and stays where it is", container.blobExists(stray));

        // A record with a name we did mint, whose bytes are unreadable: that is the half-written case, and
        // it must pin until a human looks at it.
        final String plausible = org.opensearch.common.UUIDs.randomBase64UUID();
        container.writeBlob(plausible, junk.streamInput(), junk.length(), true);
        final var live = plane.livePointsInTime(clock.get());
        assertEquals("an unreadable record of ours must still hold: " + live, 1, live.size());
        assertEquals(plausible, live.get(0).id());
    }

    /**
     * A frozen search is forwarded to the placement-preferred node rather than opened by whichever node
     * happens to coordinate it.
     *
     * <p>Every shard of a view used to be opened unconditionally on the coordinating node -- so a caller
     * whose paged requests land on different nodes (any load balancer with no session affinity) paid the
     * view's whole cost twice. This is the property that stops that: the node that does not hold the view
     * yet must ask the placement-preferred peer for it, not open it itself.
     */
    public void testAFrozenSearchIsForwardedToThePlacementPreferredNode() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("routed", "uuid-routed-00000000", 1, MAPPING, null));

        final String pit;
        try (ServerlessNode writer = new ServerlessNode(nodeSettings("pit-route-w"))) {
            writer.start();
            writer.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(writer, plane, clock, "routed", 1);
            assertEquals(201, send(writer, "PUT", "/routed/_doc/1?refresh=true", body(1)).status());
            loop.tick(clock.get());
            pit = field(send(writer, "POST", "/routed/_pit?keep_alive=10m", null).body(), "pit_id");
        }
        // The writer that froze the view is gone. A view has no owner, only a record in the object store,
        // so what follows must work from that alone.

        try (
            ServerlessNode a = new ServerlessNode(searchNodeSettings("pit-route-a"));
            ServerlessNode b = new ServerlessNode(searchNodeSettings("pit-route-b"))
        ) {
            a.start();
            a.setMetadataPlane(plane);
            b.start();
            b.setMetadataPlane(plane);
            a.heartbeat(plane);
            b.heartbeat(plane);

            final var members = plane.membership().refresh();
            final var preferred = ReaderPlacement.candidatesFor("routed", 0, members, ServerlessNode.ROLE_SEARCH, 1);
            assertEquals("both nodes advertise search, so there must be a top candidate", 1, preferred.size());
            final ServerlessNode winner = preferred.get(0).equals(a.localNode().getId()) ? a : b;
            final ServerlessNode loser = winner == a ? b : a;

            final Response found = send(
                loser.boundHttpAddress().publishAddress(),
                "POST",
                "/routed/_search?pit=" + pit,
                "{\"query\":{\"match_all\":{}}}"
            );
            assertEquals(found.body(), 200, found.status());
            assertTrue("the forwarded search must still find the frozen document: " + found.body(), found.body().contains("\"value\":1"));

            assertEquals("the placement-preferred node must have opened the view", 1, winner.reconciler().frozenShards().size());
            assertEquals("the coordinator must not also have opened it locally", 0, loser.reconciler().frozenShards().size());
        }
    }

    /**
     * A forwarded frozen search still reads the frozen commit over the wire, not whatever is live on the
     * node it lands on.
     *
     * <p>The property the whole feature exists not to break: forwarding is new, "the view sees only what
     * it froze" is not, and the two must still hold together. A bug that wired the forwarded handler to
     * open the current commit rather than the one the view named would pass every other test here and
     * only show up as a write from after the freeze leaking into a page that must not contain it.
     */
    public void testAForwardedFrozenSearchStillSeesTheFrozenCommitNotLiveWrites() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("stale", "uuid-stale-000000000", 1, MAPPING, null));

        final String pit;
        try (ServerlessNode writer = new ServerlessNode(nodeSettings("pit-hop-w"))) {
            writer.start();
            writer.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(writer, plane, clock, "stale", 1);
            assertEquals(201, send(writer, "PUT", "/stale/_doc/1?refresh=true", body(1)).status());
            loop.tick(clock.get());
            pit = field(send(writer, "POST", "/stale/_pit?keep_alive=10m", null).body(), "pit_id");

            // Written and published after the freeze -- a forwarded search must not see this either.
            assertEquals(201, send(writer, "PUT", "/stale/_doc/2?refresh=true", body(2)).status());
            loop.tick(clock.get() + 1);
        }

        try (
            ServerlessNode a = new ServerlessNode(searchNodeSettings("pit-hop-a"));
            ServerlessNode b = new ServerlessNode(searchNodeSettings("pit-hop-b"))
        ) {
            a.start();
            a.setMetadataPlane(plane);
            b.start();
            b.setMetadataPlane(plane);
            a.heartbeat(plane);
            b.heartbeat(plane);

            final var members = plane.membership().refresh();
            final var preferred = ReaderPlacement.candidatesFor("stale", 0, members, ServerlessNode.ROLE_SEARCH, 1);
            final ServerlessNode loser = preferred.get(0).equals(a.localNode().getId()) ? b : a;

            final Response found = send(
                loser.boundHttpAddress().publishAddress(),
                "POST",
                "/stale/_search?pit=" + pit,
                "{\"size\":20,\"query\":{\"match_all\":{}}}"
            );
            assertEquals(found.body(), 200, found.status());
            assertTrue(
                "the forwarded search must see only the frozen commit, not the write made after it: " + found.body(),
                found.body().contains("\"value\":1")
            );
        }
    }

    /**
     * A dead placement-preferred candidate costs a fallback, not a failed search.
     *
     * <p>Placement is a hint read from a lease that can go stale between renewals -- a node can advertise
     * search and be gone a moment later, with its record not yet expired. Correctness cannot depend on
     * that record being current, so the coordinator must fall through to opening the view itself rather
     * than reporting the search as unanswerable.
     */
    public void testAFrozenSearchFallsBackToTheCoordinatorWhenThePreferredNodeIsUnreachable() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("orphaned", "uuid-orphaned-0000000", 1, MAPPING, null));

        final String pit;
        try (ServerlessNode writer = new ServerlessNode(nodeSettings("pit-fallback-w"))) {
            writer.start();
            writer.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(writer, plane, clock, "orphaned", 1);
            assertEquals(201, send(writer, "PUT", "/orphaned/_doc/1?refresh=true", body(1)).status());
            loop.tick(clock.get());
            pit = field(send(writer, "POST", "/orphaned/_pit?keep_alive=10m", null).body(), "pit_id");
        }

        try (
            ServerlessNode a = new ServerlessNode(searchNodeSettings("pit-fallback-a"));
            ServerlessNode b = new ServerlessNode(searchNodeSettings("pit-fallback-b"))
        ) {
            a.start();
            a.setMetadataPlane(plane);
            b.start();
            b.setMetadataPlane(plane);
            a.heartbeat(plane);
            b.heartbeat(plane);

            final var members = plane.membership().refresh();
            final var ranked = ReaderPlacement.candidatesFor("orphaned", 0, members, ServerlessNode.ROLE_SEARCH, 2);
            final ServerlessNode preferred = ranked.get(0).equals(a.localNode().getId()) ? a : b;
            final ServerlessNode survivor = preferred == a ? b : a;

            // The preferred candidate dies with its lease still live and not yet expired -- advertised but
            // unreachable, the exact case placement being a hint has to survive. try-with-resources closes
            // it a second time on the way out; ServerlessNode#close is written to tolerate that.
            preferred.close();

            final Response found = send(
                survivor.boundHttpAddress().publishAddress(),
                "POST",
                "/orphaned/_search?pit=" + pit,
                "{\"query\":{\"match_all\":{}}}"
            );
            assertEquals("a dead preferred candidate must not turn into a failed search: " + found.body(), 200, found.status());
            assertTrue("the frozen document must still be found: " + found.body(), found.body().contains("\"value\":1"));
            assertEquals(
                "the survivor must have fallen back to opening the view itself",
                1,
                survivor.reconciler().frozenShards().size()
            );
        }
    }

    private BackgroundReconciler hold(ServerlessNode node, MetadataPlane plane, AtomicLong clock, String index, int shards)
        throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (int shard = 0; shard < shards; shard++) {
            loop.want(index, shard);
        }
        loop.tick(clock.get());
        return loop;
    }

    private static String body(int rank) {
        return "{\"msg\":\"doc\",\"rank\":" + rank + "}";
    }

    private static final Pattern RANK = Pattern.compile("\"rank\":(\\d+)");

    private static java.util.List<Long> ranksIn(String body) {
        final java.util.List<Long> ranks = new java.util.ArrayList<>();
        final Matcher matcher = RANK.matcher(body);
        while (matcher.find()) {
            ranks.add(Long.parseLong(matcher.group(1)));
        }
        return ranks;
    }

    private static String field(String body, String name) {
        final Matcher matcher = Pattern.compile("\"" + name + "\":\"([^\"]+)\"").matcher(body);
        assertTrue("no " + name + " in " + body, matcher.find());
        return java.net.URLEncoder.encode(matcher.group(1), StandardCharsets.UTF_8);
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        return send(node.boundHttpAddress().publishAddress(), method, path, body);
    }

    /** For a request that must land on a specific node in a multi-node test, not whichever coordinates. */
    private static Response send(TransportAddress address, String method, String path, String body) throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .method(
                    method,
                    body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)
                )
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
