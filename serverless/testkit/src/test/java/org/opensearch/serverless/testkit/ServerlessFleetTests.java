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
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Three or more processes: the properties two cannot show.
 *
 * <p>With two nodes every interesting question degenerates. One owns and one forwards, so "does a search
 * gather from several owners" is really "does it gather from one", and "can a shard move again after it
 * has moved" cannot be asked at all — there is nobody left to move it to.
 *
 * <p>Ownership is spread <b>deterministically</b> rather than hoped for: each node is capped at two
 * shards, so a four-shard index cannot land on one node however the writes happen to arrive. Without
 * that, a run where a single node happened to take everything would pass while testing nothing, and it
 * would do so silently.
 *
 * <p><b>D5:</b> a shared local directory stands in for the object store.
 */
public class ServerlessFleetTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";
    /**
     * Long enough that a loaded machine does not expire a healthy node's lease.
     *
     * <p>At three seconds these tests failed intermittently under a full build with
     * {@code searched:3, unreachable:1} — and not because of a bug: a node missed a renewal because the
     * machine was busy, its lease genuinely lapsed, and its peers correctly stopped believing in it. A
     * TTL tuned to make tests finish quickly had become a TTL that manufactures failures.
     */
    private static final String TTL = "10000";
    private static final int SHARDS = 4;

    /** Capped at two shards each, so four shards cannot fit on one node. */
    private Map<String, String> cappedSettings() {
        return Map.of(ServerlessBootstrap.LEASE_TTL, TTL, ServerlessBootstrap.MAX_SHARDS, "2");
    }

    private Map<String, String> settings() {
        return Map.of(ServerlessBootstrap.LEASE_TTL, TTL);
    }

    /**
     * Three nodes, four shards, and every node must answer for the whole index.
     *
     * <p>The cap guarantees at least two nodes own shards, so every search is a real gather across
     * processes rather than a local lookup dressed up as one. A node that only fanned out to what it held
     * itself would return a fraction of the index — and would report {@code complete: true} while doing
     * it, which is the failure worth catching: not a wrong count, a wrong count presented as authoritative.
     */
    public void testAFleetSpreadsShardsAndEveryNodeAnswersForTheWholeIndex() throws Exception {
        final var store = createTempDir();
        try (
            NodeProcess a = NodeProcess.start("fleet-a", store, createTempDir(), cappedSettings());
            NodeProcess b = NodeProcess.start("fleet-b", store, createTempDir(), cappedSettings());
            NodeProcess c = NodeProcess.start("fleet-c", store, createTempDir(), cappedSettings())
        ) {
            final List<NodeProcess> fleet = List.of(a, b, c);
            assertEquals(200, send(a, "PUT", "/alpha?shards=" + SHARDS, MAPPING).status());

            final int perNode = 12;
            writeConcurrently(fleet, perNode, "spread");

            final Set<String> owners = new LinkedHashSet<>();
            for (int shard = 0; shard < SHARDS; shard++) {
                final var head = plane(store).heads().read("alpha", shard);
                assertTrue("shard " + shard + " must have an owner", head.isPresent());
                owners.add(head.get().ownerNodeId());
            }
            logger.info("fleet: {} shards held by {} distinct nodes", SHARDS, owners.size());
            assertTrue(
                "the cap must have forced the shards across more than one node, or this tests nothing: " + owners,
                owners.size() > 1
            );

            final int expected = fleet.size() * perNode;
            for (NodeProcess reader : fleet) {
                send(reader, "POST", "/alpha/_refresh", null);
            }
            for (NodeProcess reader : fleet) {
                // Converges, rather than instantly correct. A shard whose owner's lease lapsed is
                // re-acquired and served again; demanding completeness on the first attempt is demanding
                // zero-latency convergence, which the design does not offer and never claimed to.
                //
                // The deadline is what is relaxed, not the property. A fan-out that only searched local
                // shards never becomes complete, so it times out here rather than passing -- confirmed by
                // planting exactly that defect.
                assertBusy(() -> {
                    final Response found = search(reader, "msg:spread");
                    assertEquals(200, found.status());
                    assertTrue(
                        "a partial answer must not be reported as complete: " + found.body() + diagnose(reader, found),
                        found.body().contains("\"complete\":true")
                    );
                    assertEquals(
                        "every node must answer for the whole index; " + reader.name() + " returned: " + found.body(),
                        expected,
                        hitCount(found.body())
                    );
                }, 60, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * A shard survives being handed over twice: A dies, B takes it, B dies, C takes it.
     *
     * <p>Two processes can show a takeover once. They cannot show that takeover is <em>repeatable</em> —
     * that the successor's own recovered state is itself recoverable, that the term keeps advancing rather
     * than resetting, and that writes from all three owners end up in one index. A recovery path that
     * works once and corrupts on the second pass would look perfectly healthy in a two-node test.
     */
    public void testAShardSurvivesBeingHandedOverTwice() throws Exception {
        final var store = createTempDir();
        try (NodeProcess c = NodeProcess.start("chain-c", store, createTempDir(), settings())) {
            final List<Long> terms = new ArrayList<>();

            final NodeProcess a = NodeProcess.start("chain-a", store, createTempDir(), settings());
            final NodeProcess b = NodeProcess.start("chain-b", store, createTempDir(), settings());
            try {
                assertEquals(200, send(a, "PUT", "/alpha?shards=1", MAPPING).status());
                for (int i = 1; i <= 5; i++) {
                    assertEquals(201, putDoc(a, String.valueOf(i), "{\"msg\":\"chain\",\"n\":" + i + "}").status());
                }
                assertEquals("a should hold it first", a.nodeId(), owner(store));
                terms.add(term(store));

                a.killHard();
                for (int i = 6; i <= 10; i++) {
                    assertEquals(201, putDoc(b, String.valueOf(i), "{\"msg\":\"chain\",\"n\":" + i + "}").status());
                }
                assertEquals("b should have taken it", b.nodeId(), owner(store));
                terms.add(term(store));
            } finally {
                b.killHard();
            }

            for (int i = 11; i <= 15; i++) {
                assertEquals(201, putDoc(c, String.valueOf(i), "{\"msg\":\"chain\",\"n\":" + i + "}").status());
            }
            assertEquals("c should have taken it in turn", c.nodeId(), owner(store));
            terms.add(term(store));

            logger.info("chain of custody: terms {}", terms);
            assertEquals("each handover must bump the term", 3, terms.size());
            assertTrue("terms must strictly increase across handovers: " + terms, terms.get(1) > terms.get(0));
            assertTrue("including the second handover: " + terms, terms.get(2) > terms.get(1));

            send(c, "POST", "/alpha/_refresh", null);
            final Response found = search(c, "msg:chain");
            assertEquals(200, found.status());
            assertEquals("writes from all three owners must survive two handovers: " + found.body(), 15, hitCount(found.body()));
        }
    }

    /**
     * Four nodes reaching for the same unowned shard at the same instant. Still exactly one owner.
     *
     * <p>Two contenders exercise a compare-and-swap once. Four exercise it as a herd, where three of the
     * four lose and must each turn that loss into a forward rather than a retry or an error — which is
     * where an arbitration that is merely usually-right starts producing two owners and a split index.
     */
    public void testAHerdOfNodesReachingForOneShardStillProducesOneOwner() throws Exception {
        final var store = createTempDir();
        try (
            NodeProcess a = NodeProcess.start("herd-a", store, createTempDir(), settings());
            NodeProcess b = NodeProcess.start("herd-b", store, createTempDir(), settings());
            NodeProcess c = NodeProcess.start("herd-c", store, createTempDir(), settings());
            NodeProcess d = NodeProcess.start("herd-d", store, createTempDir(), settings())
        ) {
            final List<NodeProcess> herd = List.of(a, b, c, d);
            assertEquals(200, send(a, "PUT", "/alpha?shards=1", MAPPING).status());

            final int perNode = 8;
            writeConcurrently(herd, perNode, "herd");

            final String owner = owner(store);
            logger.info("herd: owner is {}", owner);
            assertEquals("exactly one of the four may own it", 1, herd.stream().filter(node -> node.nodeId().equals(owner)).count());

            send(a, "POST", "/alpha/_refresh", null);
            for (NodeProcess reader : herd) {
                assertBusy(() -> {
                    final Response found = search(reader, "msg:herd");
                    assertEquals(200, found.status());
                    assertEquals(
                        "no write may be lost to the arbitration; "
                            + reader.name()
                            + " returned: "
                            + found.body()
                            + diagnose(reader, found),
                        herd.size() * perNode,
                        hitCount(found.body())
                    );
                }, 60, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * When a shard cannot be reached, the answer must say so rather than quietly leaving it out.
     *
     * <p>This is the failure that matters more than a wrong count: a wrong count <em>presented as
     * authoritative</em>. A caller reading only the hits has no way to tell an answer computed over the
     * whole index from one computed over three quarters of it, and a search that silently drops a shard
     * reads exactly like a query with fewer matches.
     *
     * <p>Constructing genuine unreachability takes care. Nodes are given a publish debounce they never
     * reach, so nothing is in the object store: a survivor asked for the dead node's shard cannot fall
     * back to opening the published commit, because there is no published commit. Without that the fleet
     * heals itself and there is nothing to be honest about. The search also happens promptly, inside the
     * lease, because once a successor takes over it replays the write-ahead log and the data comes back —
     * which is the system working, and not what is under test here.
     */
    public void testASearchMissingAShardSaysSoInsteadOfUnderReportingSilently() throws Exception {
        final var store = createTempDir();
        final Map<String, String> neverPublish = Map.of(
            ServerlessBootstrap.LEASE_TTL,
            TTL,
            ServerlessBootstrap.MAX_SHARDS,
            "2",
            ServerlessBootstrap.PUBLISH_DEBOUNCE,
            "600000"
        );
        try (
            NodeProcess b = NodeProcess.start("honest-b", store, createTempDir(), neverPublish);
            NodeProcess c = NodeProcess.start("honest-c", store, createTempDir(), neverPublish)
        ) {
            final NodeProcess a = NodeProcess.start("honest-a", store, createTempDir(), neverPublish);
            final int perNode;
            try {
                assertEquals(200, send(a, "PUT", "/alpha?shards=" + SHARDS, MAPPING).status());
                perNode = 12;
                writeConcurrently(List.of(a, b, c), perNode, "honest");

                final Set<String> owners = new LinkedHashSet<>();
                for (int shard = 0; shard < SHARDS; shard++) {
                    owners.add(plane(store).heads().read("alpha", shard).orElseThrow().ownerNodeId());
                }
                assertTrue("a must own something for killing it to remove anything: " + owners, owners.contains(a.nodeId()));
            } finally {
                a.killHard();
            }

            // Promptly, while a's shards are genuinely unreachable: nothing was ever published, and no
            // successor has been given a reason to take them.
            final Response found = search(b, "msg:honest");
            assertEquals(200, found.status());
            final int hits = hitCount(found.body());
            final boolean claimsComplete = found.body().contains("\"complete\":true");
            logger.info("honest search after killing an owner: {} hits, complete={}", hits, claimsComplete);

            final int expected = 3 * perNode;
            if (claimsComplete) {
                // Allowed -- but only if it really did see everything.
                assertEquals("an answer that claims completeness must have everything: " + found.body(), expected, hits);
            } else {
                assertTrue(
                    "an incomplete answer must report which shards it could not reach: " + found.body(),
                    found.body().contains("\"failed\"") && found.body().contains("\"successful\"")
                );
                assertTrue("and it must be missing something, or 'incomplete' is a lie too", hits < expected);
            }
        }
    }

    // ---------------------------------------------------------------- helpers

    /** Releases every node's first write at the same instant, so acquisition is genuinely contended. */
    private void writeConcurrently(List<NodeProcess> fleet, int perNode, String marker) throws Exception {
        final var barrier = new CountDownLatch(1);
        final var failures = java.util.Collections.synchronizedList(new ArrayList<String>());
        final var threads = new ArrayList<Thread>();
        for (NodeProcess target : fleet) {
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < perNode; i++) {
                        final String id = target.name() + "-" + i;
                        final Response written = putDoc(target, id, "{\"msg\":\"" + marker + "\",\"n\":" + i + "}");
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
        barrier.countDown();
        for (Thread thread : threads) {
            thread.join(TimeUnit.MINUTES.toMillis(4));
        }
        assertEquals("every write must be accepted by one node or another: " + failures, List.of(), failures);
    }

    /**
     * Adds the node's own log tail to a failure message when it could not reach a shard.
     *
     * <p>A four-node herd test once failed with {@code searched:0, unreachable:1} and nothing else to go
     * on: the node that knew why had logged it, in a forked JVM whose output the harness captured and
     * only ever printed when startup failed. An incomplete answer is a report about a node that did not
     * respond, so the interesting evidence is always in a process other than the one asserting.
     *
     * @param reader the node that answered
     * @param response its answer
     * @return log lines worth reading, or empty if the answer was complete
     */
    private String diagnose(NodeProcess reader, Response response) {
        if (response.body().contains("\"complete\":false") == false) {
            return "";
        }
        final List<String> output = reader.output();
        final List<String> interesting = output.stream()
            .filter(line -> line.contains("was not served") || line.contains("WARN") || line.contains("Exception"))
            .toList();
        final List<String> tail = interesting.isEmpty() ? output.subList(Math.max(0, output.size() - 15), output.size()) : interesting;
        return "\n--- " + reader.name() + " said ---\n" + String.join("\n", tail);
    }

    private MetadataPlane plane(Path store) throws Exception {
        return new MetadataPlane(new FsBlobStore(8192, store, false), BlobPath.cleanPath(), System::currentTimeMillis, Long.parseLong(TTL));
    }

    private String owner(Path store) throws Exception {
        return plane(store).heads().read("alpha", 0).orElseThrow().ownerNodeId();
    }

    private long term(Path store) throws Exception {
        return plane(store).heads().read("alpha", 0).orElseThrow().term();
    }

    /**
     * Writes a document, retrying while the shard has no owner or its owner is unreachable.
     *
     * <p>A 421 is a routing answer rather than a failure, and it is also the signal that makes somebody
     * take the shard. The budget is generous because a successor cannot acquire until the dead owner's
     * lease has actually lapsed.
     */
    private Response putDoc(NodeProcess node, String id, String source) throws Exception {
        Response last = null;
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (System.nanoTime() < deadline) {
            try {
                last = send(node, "PUT", "/alpha/_doc/" + id + "?refresh=true", source);
                if (last.status() != 421 && last.status() != 503) {
                    return last;
                }
            } catch (java.net.http.HttpTimeoutException e) {
                last = new Response(-1, "request timed out");
            }
            Thread.sleep(250);
        }
        return last;
    }

    private Response search(NodeProcess node, String query) throws Exception {
        return send(node, "GET", "/alpha/_search?q=" + query + "&size=200", null);
    }

    private static int hitCount(String body) {
        // Counting returned hits rather than trusting the reported total: the total is a number the node
        // computes and the hits are what it actually sent. When they disagree the test should fail, not
        // quietly believe the friendlier of the two.
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
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build()) {
            final HttpRequest.BodyPublisher payload = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + node.http() + path))
                .timeout(Duration.ofSeconds(25))
                .header("Content-Type", "application/json")
                .method(method, payload)
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
