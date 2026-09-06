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
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * M51: the cluster-shaped endpoints this design can answer without a cluster manager.
 *
 * <p><b>What changed and why it is not a softening of D2.</b> These endpoints were refused with one reason —
 * "there is no cluster-wide state; no node can answer this, and a node-local answer would be misleading". The
 * first clause is true. The second stopped being true when leases became an address book: every node
 * publishes one and reads the others' in order to forward writes, so a node answering "who is in this fleet"
 * is answering a question it already answers on every heartbeat. Refusing it was the same stale-refusal shape
 * M50 found in {@code _bulk}.
 *
 * <p>What actually separates answerable from refused here is <b>cost class</b>, not locality. A question about
 * nodes is one bounded listing. A question about one index's shards is a descriptor plus a head per shard. A
 * question aggregated over every index is enumeration, and stays refused.
 *
 * <p><b>The colours are the design.</b> Classic green/yellow/red asserts things about replica placement, and
 * there are no replicas here — so yellow is unreachable, and an unowned shard is the healthy resting state
 * rather than a fault. Mapping "unowned" onto yellow would make {@code wait_for_status=green} block forever in
 * a system where shards activate on demand, which is why {@link #testADormantShardIsHealthyNotDegraded} is the
 * test that matters most in this suite.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessClusterEndpointTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private Settings nodeSettings(String name, String roles, Path home) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-cluster-endpoints")
            .put("path.home", home)
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .build();
    }

    private record Answer(int status, String body) {
        boolean has(String fragment) {
            return body.contains(fragment);
        }
    }

    private Answer call(int port, String method, String path) throws Exception {
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                    .header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.noBody())
                    .timeout(Duration.ofSeconds(30))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
            return new Answer(response.statusCode(), response.body());
        }
    }

    private Answer put(int port, String path, String body) throws Exception {
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString(body))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
            return new Answer(response.statusCode(), response.body());
        }
    }

    private static int portOf(ServerlessNode node) {
        return node.boundHttpAddress().publishAddress().getPort();
    }

    private static MetadataPlane plane(AtomicLong clock, Path store) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, store, false), BlobPath.cleanPath(), clock::get, TTL);
    }

    /**
     * A shard nobody currently owns is green, and is reported as dormant rather than unassigned.
     *
     * <p><b>This is the assertion the whole design rests on.</b> "Unassigned" in classic OpenSearch means
     * allocation failed and nobody can serve the shard. Here it would mean nobody is writing to it right now,
     * and the next write activates it — so reporting it as unassigned is the inverse of a lie about health:
     * a fault where there is none. And it is not academic: a client calling
     * {@code wait_for_status=green&timeout=30s} at startup, against a deployment where shards activate on
     * demand, would block until traffic it has not sent yet arrives. A compatibility gesture that deadlocks
     * the caller is worse than the 501 it replaced.
     */
    public void testADormantShardIsHealthyNotDegraded() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = new ServerlessNode(nodeSettings("health-dormant", "ingest", createTempDir()))) {
            node.start();
            node.setMetadataPlane(plane);
            final int port = portOf(node);
            put(port, "/alpha", "{\"settings\":{\"number_of_shards\":3}}");

            // Not one shard is activated: this is a brand new index nobody has written to.
            final Answer cold = call(port, "GET", "/_cluster/health/alpha");
            assertEquals(cold.body(), 200, cold.status());
            assertTrue("an index nobody has written to is healthy: " + cold.body(), cold.has("\"status\":\"green\""));
            assertTrue("every shard is dormant: " + cold.body(), cold.has("\"dormant_shards\":3"));
            assertTrue("and none is unassigned, because none has failed: " + cold.body(), cold.has("\"unassigned_shards\":0"));
            assertTrue("so the percentage a client gates on is 100: " + cold.body(), cold.has("\"active_shards_percent_as_number\":100"));

            // And a client waiting for green gets it immediately rather than blocking on activation.
            final Answer waited = call(port, "GET", "/_cluster/health/alpha?wait_for_status=green&timeout=2s");
            assertEquals(waited.body(), 200, waited.status());
            assertFalse("waiting for green must not time out on a dormant index: " + waited.body(), waited.has("\"timed_out\":true"));

            // Activating one shard moves it out of dormant without changing the colour: both states are green.
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final Answer warm = call(port, "GET", "/_cluster/health/alpha");
            assertTrue("activation changes the dormant count: " + warm.body(), warm.has("\"dormant_shards\":2"));
            assertTrue("not the colour: " + warm.body(), warm.has("\"status\":\"green\""));
        }
    }

    /** Health says, on every answer, that green does not mean copies exist. */
    public void testHealthSaysWhatGreenDoesNotMean() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("health-honest", "ingest", createTempDir()))) {
            node.start();
            node.setMetadataPlane(plane(clock, createTempDir()));
            final int port = portOf(node);

            final Answer health = call(port, "GET", "/_cluster/health");
            assertTrue(
                "the replication model must be stated, not inferred: " + health.body(),
                health.has("\"replication\":\"object-store\"")
            );
            assertTrue("and that yellow is never coming: " + health.body(), health.has("\"yellow_reachable\":false"));
        }
    }

    /**
     * Unscoped health answers the node half and omits the shard half, saying which it did.
     *
     * <p>Reporting {@code active_shards: 0} would be a claim about how many shards exist. The true answer is
     * that none were examined, because examining them means enumerating every index — the inventory operation
     * this design refuses everywhere. {@code complete} is the field search responses already use to say
     * exactly this, so the answer borrows an idiom rather than inventing one.
     */
    public void testUnscopedHealthOmitsWhatItDidNotCount() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = new ServerlessNode(nodeSettings("health-scope", "ingest", createTempDir()))) {
            node.start();
            node.setMetadataPlane(plane);
            final int port = portOf(node);
            put(port, "/alpha", "{\"settings\":{\"number_of_shards\":2}}");

            final Answer wide = call(port, "GET", "/_cluster/health");
            assertTrue("the node half is real: " + wide.body(), wide.has("\"number_of_nodes\":1"));
            assertTrue("the shard half is declared missing: " + wide.body(), wide.has("\"complete\":false"));
            assertFalse("and must not be reported as zero: " + wide.body(), wide.has("\"active_shards\":"));

            final Answer scoped = call(port, "GET", "/_cluster/health/alpha");
            assertTrue("naming an index makes it complete: " + scoped.body(), scoped.has("\"complete\":true"));
            assertTrue("and the counters appear: " + scoped.body(), scoped.has("\"active_shards\":2"));
        }
    }

    /** {@code level=shards} reports each shard, from the same reads the index answer already made. */
    public void testPerShardDetailIsAvailableWithoutASecondRead() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = new ServerlessNode(nodeSettings("health-shards", "ingest", createTempDir()))) {
            node.start();
            node.setMetadataPlane(plane);
            final int port = portOf(node);
            put(port, "/alpha", "{\"settings\":{\"number_of_shards\":2}}");
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            final Answer detail = call(port, "GET", "/_cluster/health/alpha?level=shards");
            assertEquals(detail.body(), 200, detail.status());
            assertTrue("the owned shard is named as owned: " + detail.body(), detail.has("\"state\":\"owned\""));
            assertTrue("and the other as dormant: " + detail.body(), detail.has("\"state\":\"dormant\""));
        }
    }

    /**
     * Two nodes, and both endpoints see both — which is the claim the old refusal said no node could make.
     */
    public void testANodeReportsTheWholeFleetNotJustItself() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (
            ServerlessNode first = new ServerlessNode(nodeSettings("fleet-a", "ingest", createTempDir()));
            ServerlessNode second = new ServerlessNode(nodeSettings("fleet-b", "search", createTempDir()))
        ) {
            first.start();
            first.setMetadataPlane(plane);
            second.start();
            second.setMetadataPlane(plane);
            // Each publishes a lease; the reconcile pass is what does it in a running deployment.
            new BackgroundReconciler(first, plane).tick(clock.get());
            new BackgroundReconciler(second, plane).tick(clock.get());

            final Answer nodes = call(portOf(first), "GET", "/_nodes");
            assertEquals(nodes.body(), 200, nodes.status());
            assertTrue("a node must report its peer, not only itself: " + nodes.body(), nodes.has("fleet-b"));
            assertTrue("and itself: " + nodes.body(), nodes.has("fleet-a"));
            assertTrue("with the peer's role, which only the peer knows: " + nodes.body(), nodes.has("\"search\""));
            assertTrue("and the fan-out accounting a client checks: " + nodes.body(), nodes.has("\"total\":2"));

            final Answer health = call(portOf(second), "GET", "/_cluster/health");
            assertTrue("health counts the same fleet from the other node: " + health.body(), health.has("\"number_of_nodes\":2"));

            // A named node resolves by name as well as by id, which is how a person asks.
            final Answer one = call(portOf(first), "GET", "/_nodes/fleet-b");
            assertTrue("one node, named: " + one.body(), one.has("fleet-b"));
            assertFalse("and only that one: " + one.body(), one.has("fleet-a"));

            // wait_for_nodes is answerable here for the same reason the list is.
            final Answer met = call(portOf(first), "GET", "/_cluster/health?wait_for_nodes=2&timeout=2s");
            assertFalse("a condition that already holds must not time out: " + met.body(), met.has("\"timed_out\":true"));
            final Answer unmet = call(portOf(first), "GET", "/_cluster/health?wait_for_nodes=9&timeout=1s");
            assertTrue("one that cannot must say it timed out rather than lie: " + unmet.body(), unmet.has("\"timed_out\":true"));
        }
    }

    /** A node's name and version travel in its lease, because that is the only place a peer can read them. */
    public void testANodeReportsItsPeersNameAndVersion() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = new ServerlessNode(nodeSettings("named-node", "ingest", createTempDir()))) {
            node.start();
            node.setMetadataPlane(plane);
            new BackgroundReconciler(node, plane).tick(clock.get());

            final Answer nodes = call(portOf(node), "GET", "/_nodes");
            assertTrue("the configured name, not the generated id: " + nodes.body(), nodes.has("\"name\":\"named-node\""));
            assertTrue("and a version, which is never guessed on a peer's behalf: " + nodes.body(), nodes.has("\"version\":"));
        }
    }

    /** {@code _cat} answers as a table for a person and as JSON for a program, and refuses to fake column selection. */
    public void testCatAnswersInBothShapesAndRefusesColumnSelection() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = new ServerlessNode(nodeSettings("cat-node", "ingest", createTempDir()))) {
            node.start();
            node.setMetadataPlane(plane);
            new BackgroundReconciler(node, plane).tick(clock.get());
            final int port = portOf(node);

            final Answer text = call(port, "GET", "/_cat/nodes?v=true");
            assertEquals(text.body(), 200, text.status());
            assertTrue("a header row when asked for one: " + text.body(), text.has("name"));
            assertTrue("and the node in it: " + text.body(), text.has("cat-node"));
            assertFalse("text, not JSON: " + text.body(), text.has("{"));

            final Answer json = call(port, "GET", "/_cat/nodes?format=json");
            assertTrue("JSON when asked for it: " + json.body(), json.has("\"name\":\"cat-node\""));

            final Answer health = call(port, "GET", "/_cat/health?v=true");
            assertTrue("cat health names the deployment: " + health.body(), health.has("serverless-cluster-endpoints"));
            assertTrue("and its colour: " + health.body(), health.has("green"));

            // Column selection is refused rather than accepted and ignored, because a table that is not the
            // one asked for is worse than no table.
            final Answer picked = call(port, "GET", "/_cat/nodes?h=id,name");
            assertEquals(picked.body(), 501, picked.status());
            assertTrue("and says why: " + picked.body(), picked.has("not the one asked for"));
        }
    }

    /**
     * What stays refused, and with a reason that is true of it rather than a shared one.
     *
     * <p>The general reason — "no node can answer this" — was retired because it had become false. The
     * endpoints still refused are refused for three specific things this design does not have: a snapshot
     * across every index, an allocator, and a cluster-manager task queue. {@code _nodes/stats} is the
     * interesting one: a node now knows where every other node is, so the honest obstacle is the missing
     * fan-out, not an impossibility.
     */
    public void testWhatStaysRefusedSaysWhatItActuallyLacks() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("refused-node", "ingest", createTempDir()))) {
            node.start();
            node.setMetadataPlane(plane(clock, createTempDir()));
            final int port = portOf(node);

            for (String path : new String[] { "/_cluster/state", "/_cluster/stats", "/_cluster/reroute" }) {
                final Answer refused = call(port, "GET", path);
                assertEquals(path + " must stay refused: " + refused.body(), 501, refused.status());
            }

            final Answer stats = call(port, "GET", "/_nodes/stats");
            assertEquals(stats.body(), 501, stats.status());
            // The reason has changed twice, and each time because the previous one stopped being true. It was
            // "no node can answer this", which the lease registry made false; then "the fan-out does not
            // exist", which ?nodes=_all made false. What is left is the only thing still true of it: the
            // body means core's schema, and this shell does not produce those numbers.
            assertTrue("the reason must be the real one: " + stats.body(), stats.has("core's node-statistics schema"));
            assertTrue("and point at what does answer: " + stats.body(), stats.has("/_serverless/stats?nodes=_all"));
            assertFalse("not the one that stopped being true: " + stats.body(), stats.has("no node can answer this"));
            assertFalse("nor the one after it: " + stats.body(), stats.has("what does not exist yet is the fan-out"));

            // Allocation-shaped waits are refused rather than silently satisfied.
            final Answer allocation = call(port, "GET", "/_cluster/health?wait_for_active_shards=2");
            assertEquals(allocation.body(), 501, allocation.status());
            assertTrue("naming what does not exist: " + allocation.body(), allocation.has("there is no allocator here"));
        }
    }
}
