/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * Two nodes, two JVMs, one object store.
 *
 * <p>Every test before this ran nodes as objects inside the test's own process. That is enough for
 * logic and not enough for the claim the design actually rests on: that a node which <em>dies</em> is
 * recovered from. {@code close()} unwinds cleanly, releases its lease and is the opposite of a crash.
 *
 * <p><b>D5:</b> a shared local directory stands in for the object store.
 */
public class ServerlessContentionTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    /** Short, so a crashed node's lease lapses inside a test rather than inside a coffee break. */
    /**
     * Short enough that a crashed node's lease lapses inside a test, long enough that a busy machine does
     * not expire a healthy one's. Three seconds did the first and failed the second under a full build.
     */
    private static final String TTL = "8000";

    private Map<String, String> settings() {
        return Map.of(ServerlessBootstrap.LEASE_TTL, TTL);
    }

    /**
     * The harness itself: a forked JVM really does boot, bind, and answer. Everything below depends on
     * this, so it is worth failing on its own rather than as a confusing symptom of something else.
     */
    public void testANodeInItsOwnProcessStartsAndAnswers() throws Exception {
        final var store = createTempDir();
        try (NodeProcess a = NodeProcess.start("proc-a", store, createTempDir(), settings())) {
            assertTrue("the process should be alive", a.alive());
            assertNotNull("it should have reported an HTTP address", a.http());
            assertNotNull("and a node id", a.nodeId());

            final Response health = send(a, "GET", "/_serverless/health", null);
            assertEquals("a forked node must answer over HTTP: " + health.body(), 200, health.status());
            logger.info("proc-a at {} reports {}", a.http(), health.body());
        }
    }

    /**
     * Two processes reaching for the same unowned shard at the same instant. Exactly one may have it.
     *
     * <p>The writes are <b>concurrent and simultaneous</b>, released together off a barrier, and that is
     * the whole point. An earlier version of this test alternated between the nodes one write at a time,
     * which never produced a race at all: whichever node was asked first activated, and the other simply
     * forwarded to a live owner it never tried to take. Deleting the "a live lease is not stolen" rule
     * left that version passing, which is how it was found out.
     *
     * <p>With both nodes attempting acquisition at once, the compare-and-swap is doing real work. If it
     * could be bypassed, both would open the shard, each would take a share of the writes, and the two
     * halves would never appear in one search — so the count is the assertion that fails.
     */
    public void testTwoProcessesRacingForOneShardProduceExactlyOneOwner() throws Exception {
        final var store = createTempDir();
        try (
            NodeProcess a = NodeProcess.start("race-a", store, createTempDir(), settings());
            NodeProcess b = NodeProcess.start("race-b", store, createTempDir(), settings())
        ) {
            assertEquals(200, send(a, "PUT", "/alpha?shards=1", MAPPING).status());

            final int perNode = 15;
            final var barrier = new java.util.concurrent.CountDownLatch(1);
            final var failures = java.util.Collections.synchronizedList(new java.util.ArrayList<String>());
            final var threads = new java.util.ArrayList<Thread>();

            for (NodeProcess target : java.util.List.of(a, b)) {
                final Thread thread = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < perNode; i++) {
                            final String id = target.name() + "-" + i;
                            final Response written = putDoc(target, "alpha", id, "{\"msg\":\"contended\",\"n\":" + i + "}");
                            if (written.status() != 201) {
                                failures.add(id + " via " + target.name() + ": " + written.status() + " " + written.body());
                            }
                        }
                    } catch (Exception e) {
                        failures.add(target.name() + " threw " + e);
                    }
                }, "writer-" + target.name());
                threads.add(thread);
                thread.start();
            }

            barrier.countDown();   // both nodes get their first write in the same instant
            for (Thread thread : threads) {
                thread.join(java.util.concurrent.TimeUnit.MINUTES.toMillis(3));
            }
            assertEquals("every write must be accepted by one node or the other: " + failures, java.util.List.of(), failures);

            final var head = plane(store).heads().read("alpha", 0);
            assertTrue("the shard must have an owner", head.isPresent());
            final String owner = head.get().ownerNodeId();
            logger.info("race: owner is {} (a={}, b={})", owner, a.nodeId(), b.nodeId());
            assertTrue("the owner must be exactly one of the two nodes: " + owner, owner.equals(a.nodeId()) ^ owner.equals(b.nodeId()));

            // The number that drops when two nodes both believe they own the shard: each would hold a
            // share of the writes, and no single search would ever see all of them.
            send(a, "POST", "/alpha/_refresh", null);
            for (NodeProcess reader : java.util.List.of(a, b)) {
                final Response found = search(reader, "alpha", "msg:contended");
                assertEquals(200, found.status());
                assertEquals(
                    "all " + (2 * perNode) + " documents must be visible from " + reader.name() + ": " + found.body(),
                    2 * perNode,
                    hitCount(found.body())
                );
            }
        }
    }

    /**
     * A node dies without unwinding. Another takes its shard and loses none of its writes.
     *
     * <p>Everything before this proved recovery from a {@code close()}, which releases the lease and
     * publishes on the way out — the cooperative case, and the opposite of what a crash does. Here the
     * process is destroyed: no shutdown hook, no release, no final publish. The survivor has nothing to
     * go on but the lease expiring, and the writes it recovers come from the write-ahead log rather than
     * from anything the dead node did on its way down.
     */
    public void testAHardKilledOwnerLosesNothingAndItsShardIsTakenOver() throws Exception {
        final var store = createTempDir();
        try (NodeProcess b = NodeProcess.start("kill-b", store, createTempDir(), settings())) {
            final String deadNodeId;

            final NodeProcess a = NodeProcess.start("kill-a", store, createTempDir(), settings());
            try {
                assertEquals(200, send(a, "PUT", "/alpha?shards=1", MAPPING).status());
                for (int i = 1; i <= 10; i++) {
                    assertEquals(201, putDoc(a, "alpha", String.valueOf(i), "{\"msg\":\"survives\",\"n\":" + i + "}").status());
                }
                deadNodeId = plane(store).heads().read("alpha", 0).orElseThrow().ownerNodeId();
                assertEquals("node a should have won the shard, since it wrote first", a.nodeId(), deadNodeId);
            } finally {
                a.killHard();
            }
            assertFalse("the process must actually be dead", a.alive());

            // The negative control that makes this a crash rather than a shutdown: a killed node cannot
            // have released anything. If this lease were gone, the test below would be measuring the
            // clean-shutdown path it is meant to avoid.
            assertTrue(
                "a hard-killed node must leave its lease behind; otherwise this is not a crash",
                plane(store).membership().read(deadNodeId).isPresent()
            );

            // b has to wait the lease out. Nothing tells it a is gone.
            final Response written = putDoc(b, "alpha", "11", "{\"msg\":\"survives\",\"n\":11}");
            assertEquals("b must eventually accept a write: " + written.body(), 201, written.status());
            assertEquals("and must now own the shard", b.nodeId(), plane(store).heads().read("alpha", 0).orElseThrow().ownerNodeId());

            send(b, "POST", "/alpha/_refresh", null);
            final Response found = search(b, "alpha", "msg:survives");
            assertEquals(200, found.status());
            assertEquals(
                "all ten writes the dead node acknowledged must survive it, plus the new one: " + found.body(),
                11,
                hitCount(found.body())
            );
        }
    }

    /**
     * A frozen owner must not wedge a node that has never talked to it.
     *
     * <p>A frozen process still has an open listening socket, so TCP connects and the <em>transport
     * handshake</em> is what hangs — for the default 30 seconds, before a byte of the write is sent and
     * before any per-request timeout can apply. This is the cold path: no existing connection.
     */
    public void testAFrozenOwnerDoesNotWedgeANodeConnectingToItForTheFirstTime() throws Exception {
        assertFrozenOwnerAnswersPromptly(false);
    }

    /**
     * And must not wedge one that already holds a connection to it.
     *
     * <p>The warm path, bounded by a different setting: the connection is already established, so nothing
     * hangs in the handshake and the wait is for a response that never comes. Both are worth their own
     * test — fixing either one alone leaves the other unbounded, which is exactly what happened here.
     */
    public void testAFrozenOwnerDoesNotWedgeANodeAlreadyConnectedToIt() throws Exception {
        assertFrozenOwnerAnswersPromptly(true);
    }

    /**
     * Asserts on <b>elapsed time and a returned status</b>, not on the status alone: the failure mode is
     * not a wrong answer, it is no answer. A retry loop papers straight over this, which an earlier
     * version of this suite did.
     */
    private void assertFrozenOwnerAnswersPromptly(boolean warmTheConnection) throws Exception {
        final var store = createTempDir();
        try (
            NodeProcess a = NodeProcess.start("frozen-a", store, createTempDir(), settings());
            NodeProcess b = NodeProcess.start("frozen-b", store, createTempDir(), settings())
        ) {
            assertEquals(200, send(a, "PUT", "/alpha?shards=1", MAPPING).status());
            assertEquals(201, putDoc(a, "alpha", "1", "{\"msg\":\"before\",\"n\":1}").status());
            assertEquals(a.nodeId(), plane(store).heads().read("alpha", 0).orElseThrow().ownerNodeId());

            if (warmTheConnection) {
                // Forward once while a is healthy, so b holds an open connection to it.
                assertEquals(201, putDoc(b, "alpha", "warm", "{\"msg\":\"before\",\"n\":0}").status());
            }

            a.pause();
            try {
                // Immediately, while a's lease still reads as live: b has no choice but to forward, and
                // there is nobody on the other end.
                final long startedAt = System.nanoTime();
                final Response response;
                try {
                    response = send(b, "PUT", "/alpha/_doc/2?refresh=true", "{\"msg\":\"during\",\"n\":2}");
                } catch (Exception e) {
                    // Elapsed time even on this path: with the client's own timeout now well above every
                    // server-side bound, reaching this branch at all is itself informative, and it should
                    // not be silent about how long it took to get here.
                    final long elapsedMillis = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
                    final var tail = b.output();
                    fail(
                        "the write never came back after "
                            + elapsedMillis
                            + "ms ("
                            + e
                            + "); b's last output was:\n"
                            + String.join("\n", tail.subList(Math.max(0, tail.size() - 25), tail.size()))
                    );
                    return;
                }
                final long elapsedMillis = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
                logger.info(
                    "write to b while a is frozen ({} connection) returned {} after {}ms",
                    warmTheConnection ? "warm" : "cold",
                    response.status(),
                    elapsedMillis
                );

                assertTrue("a forward to a frozen owner must give up, not hang: took " + elapsedMillis + "ms", elapsedMillis < 20_000L);
                assertNotEquals("and it must not claim success", 201, response.status());
            } finally {
                a.resume();
            }
        }
    }

    /**
     * A process frozen past its lease, resumed after its shard has moved on, and the data still intact.
     *
     * <p><b>Read the limitation before trusting this test.</b> It is an outcome check, not a proof of any
     * mechanism. Three separate defences were deleted one at a time and this test passed every time:
     * term fencing in {@code SegmentPublisher}, the ownership recheck in {@code BackgroundReconciler
     * .publish}, both together, and the shard release in {@code ServerlessNode.heartbeat}. So whatever
     * keeps the resumed node harmless here, it is none of those — the zombie simply never gets a
     * damaging publish out within the window, and I did not isolate why.
     *
     * <p>It is kept because the scenario is real and exercised end to end across two processes — SIGSTOP,
     * lease lapse, takeover, resume — and because a regression that broke it would be worth knowing
     * about. It is <em>not</em> evidence that zombie writes are fenced. The mechanism that is proven is
     * {@code ServerlessSchedulerTests.testAFencedPublishReleasesTheShardImmediately}, in one JVM, where
     * the window can be held open deliberately instead of hoped for.
     *
     * <p>Two things had to be fixed before it measured even this much, and both are worth keeping in
     * mind when writing anything similar. The publish debounce must straddle the freeze: too short and
     * the zombie wakes with nothing to flush, too long and it never publishes at all. And the assertion
     * must read the <em>published commit</em> — the term does not move, because {@code publish} uses the
     * term it reads from the head rather than the one it acquired at, and searching the successor proves
     * nothing because it answers from an index it already holds open.
     */
    public void testAFrozenNodeResumingAfterTakeoverLeavesTheDataIntact() throws Exception {
        final var store = createTempDir();
        try (
            // The debounce has to straddle the freeze, and both ends of that matter. Too short and a
            // publishes before it is frozen, wakes with nothing new, and the idle-shard guard alone keeps
            // it harmless. Too long and it never publishes at all, so nothing is defended against either.
            // Ten seconds means the write is still pending when a is stopped, and the pending publish --
            // overdue by then -- fires the moment it is resumed, at a term that is no longer current.
            NodeProcess a = NodeProcess.start(
                "zombie-a",
                store,
                createTempDir(),
                Map.of(ServerlessBootstrap.LEASE_TTL, TTL, ServerlessBootstrap.PUBLISH_DEBOUNCE, "10000")
            );
            NodeProcess b = NodeProcess.start("zombie-b", store, createTempDir(), settings())
        ) {
            assertEquals(200, send(a, "PUT", "/alpha?shards=1", MAPPING).status());
            assertEquals(201, putDoc(a, "alpha", "1", "{\"msg\":\"before\",\"n\":1}").status());
            final long termA = plane(store).heads().read("alpha", 0).orElseThrow().term();
            assertEquals("a should own the shard", a.nodeId(), plane(store).heads().read("alpha", 0).orElseThrow().ownerNodeId());

            a.pause();
            try {
                assertEquals(201, putDoc(b, "alpha", "2", "{\"msg\":\"after\",\"n\":2}").status());
                final var afterTakeover = plane(store).heads().read("alpha", 0).orElseThrow();
                assertEquals("b must have taken the shard", b.nodeId(), afterTakeover.ownerNodeId());
                assertTrue("taking over must bump the term: " + termA + " -> " + afterTakeover.term(), afterTakeover.term() > termA);
                assertEquals(201, putDoc(b, "alpha", "3", "{\"msg\":\"after\",\"n\":3}").status());
            } finally {
                a.resume();
            }

            final long successorTerm = plane(store).heads().read("alpha", 0).orElseThrow().term();

            // Read the PUBLISHED COMMIT, not the term and not the successor's own search results.
            //
            // Neither of the obvious assertions works here, and both were tried. The term does not move:
            // publish() uses the term it reads from the head, so a zombie that gets past the ownership
            // recheck writes its stale bytes under the successor's own current term. And searching the
            // successor proves nothing either -- it answers from the Lucene index it already has open in
            // memory, which is unaffected by anything overwritten underneath it in the object store.
            //
            // What changes is the content of the published commit: the successor's documents stop being
            // in it. So restore the commit somewhere fresh and count.
            assertBusy(
                () -> assertTrue("the successor must publish its writes before this can mean anything", docsInPublishedCommit(store) >= 3),
                30,
                java.util.concurrent.TimeUnit.SECONDS
            );

            // Then hold that as an invariant. Sampled rather than assertBusy, because a clobber is
            // transient by nature -- the successor republishes and repairs it -- and "wait until true"
            // is satisfied just as well by a value that dipped and recovered as by one that never dipped.
            final long until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(15);
            int lowest = Integer.MAX_VALUE;
            while (System.nanoTime() < until) {
                final int docs = docsInPublishedCommit(store);
                lowest = Math.min(lowest, docs);
                assertTrue("a zombie must not publish a commit that drops its successor's writes: saw " + docs + " documents", docs >= 3);
                Thread.sleep(150);
            }
            logger.info("zombie: fewest documents seen in the published commit over 15s was {}", lowest);

            send(b, "POST", "/alpha/_refresh", null);
            final Response found = search(b, "alpha", "msg:after");
            assertEquals(200, found.status());
            assertEquals("a zombie must not erase its successor's writes: " + found.body(), 2, hitCount(found.body()));
        }
    }

    // ---------------------------------------------------------------- helpers

    /**
     * Restores the published commit somewhere fresh and counts what is in it.
     *
     * <p>Deliberately not asking a running node: a node answers from the index it already has open, so it
     * keeps reporting documents that have been overwritten underneath it. This opens the object store's
     * own bytes and reads what is actually there.
     *
     * @param store the shared object store
     * @return how many documents the published commit contains, or 0 if nothing is published yet
     * @throws Exception if the commit cannot be read
     */
    private int docsInPublishedCommit(java.nio.file.Path store) throws Exception {
        final var metadata = plane(store);
        final var descriptor = metadata.describe("alpha").orElseThrow();
        try (var directory = new org.apache.lucene.store.ByteBuffersDirectory()) {
            final var restored = metadata.segmentPublisher("alpha", 0)
                .restoreInto(
                    directory,
                    new org.opensearch.core.index.shard.ShardId(new org.opensearch.core.index.Index("alpha", descriptor.uuid()), 0)
                );
            if (restored.isEmpty()) {
                return 0;
            }
            try (var reader = org.apache.lucene.index.DirectoryReader.open(directory)) {
                return reader.numDocs();
            }
        }
    }

    private org.opensearch.serverless.metadata.MetadataPlane plane(java.nio.file.Path store) throws Exception {
        // The test reads the same registers the nodes arbitrate on. That is the authoritative answer to
        // "who owns this", and it does not depend on asking either node to tell the truth about itself.
        return new org.opensearch.serverless.metadata.MetadataPlane(
            new org.opensearch.common.blobstore.fs.FsBlobStore(8192, store, false),
            org.opensearch.common.blobstore.BlobPath.cleanPath(),
            System::currentTimeMillis,
            Long.parseLong(TTL)
        );
    }

    /**
     * Writes a document, retrying while the shard has no owner.
     *
     * <p>A 421 is a routing answer, not a failure: it means nobody holds the shard yet, and it is also
     * the signal that makes somebody take it. A real client retries exactly like this. The retries also
     * cover the lease having to expire before a successor may acquire, which is why the budget is
     * generous.
     */
    private Response putDoc(NodeProcess node, String index, String id, String source) throws Exception {
        Response last = null;
        final long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(90);
        while (System.nanoTime() < deadline) {
            try {
                last = send(node, "PUT", "/" + index + "/_doc/" + id + "?refresh=true", source);
                if (last.status() != 421 && last.status() != 503) {
                    return last;
                }
            } catch (java.net.http.HttpTimeoutException e) {
                // Also a retry. A request that hangs means the node it routed to is not answering, which
                // is the same situation as a 503 and resolves the same way once the lease lapses.
                last = new Response(-1, "request timed out");
            }
            Thread.sleep(250);
        }
        return last;
    }

    private Response search(NodeProcess node, String index, String query) throws Exception {
        return send(node, "GET", "/" + index + "/_search?q=" + query + "&size=100", null);
    }

    private static int hitCount(String body) {
        // Counting "_id" occurrences rather than trusting the reported total: the total is a number the
        // node computes, and the hits are what it actually returned. When those disagree the test should
        // fail, not report the friendlier of the two.
        int count = 0;
        int at = body.indexOf("\"_id\"");
        while (at >= 0) {
            count++;
            at = body.indexOf("\"_id\"", at + 1);
        }
        return count;
    }

    private record Response(int status, String body) {
    }

    private static Response send(NodeProcess node, String method, String path, String body) throws Exception {
        return send(node.http(), method, path, body);
    }

    private static Response send(String hostPort, String method, String path, String body) throws Exception {
        // Deliberately far above every server-side bound this suite measures (the forward timeout is the
        // lease TTL, 8s here). ServerlessForwardBoundTests measured that bound in isolation at 8033ms --
        // essentially exact -- so the margin below was never protecting against a slow server; it was
        // protecting against nothing, and under load it lost that race instead: the client's own timeout
        // fired before the server's, and a real measurement turned into an HttpTimeoutException with
        // nothing to diagnose from. Widening it does not weaken this test -- the assertion the test cares
        // about (elapsedMillis < 20_000L, asserted after send() returns) is unchanged; only the outer
        // client margin around that assertion is loosened, so a legitimately slow scheduler produces a
        // number to look at rather than an exception in its place.
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build()) {
            final HttpRequest.BodyPublisher payload = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + hostPort + path))
                .timeout(Duration.ofSeconds(90))
                .header("Content-Type", "application/json")
                .method(method, payload)
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
