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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The administrative endpoints, held to what they claim.
 *
 * <p>Each test here is a number or a status that used to be wrong while looking right: a writer count
 * that went to zero when a view was open, a health check that answered 200 after timing out, a
 * {@code wait_for_nodes=<3} that meant {@code >=3}, and a busy shard reported dormant thirty seconds after
 * its owner took it. None of them threw; every one of them was a confident answer.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessAdminTruthTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"rank\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-admin-truth")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane plane(AtomicLong clock) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
    }

    private static BackgroundReconciler hold(ServerlessNode node, MetadataPlane plane, AtomicLong clock, String index, int shards)
        throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (int shard = 0; shard < shards; shard++) {
            loop.want(index, shard);
        }
        loop.tick(clock.get());
        return loop;
    }

    /**
     * The writer count survives a frozen view being opened.
     *
     * <p>{@code writers} was {@code open - readers}; a view is in the reader set and out of the open set,
     * so one writer and one view reported zero writers, and two views with no writer reported minus two.
     * An operator reading the number to see whether a node still takes writes was told it did not.
     */
    public void testWritersAreCountedWithAViewOpen() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("paged", "uuid-paged-000000000", 2, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("stats-views"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "paged", 2);
            for (int i = 0; i < 4; i++) {
                assertEquals(
                    201,
                    send(node, "PUT", "/paged/_doc/p" + i + "?refresh=true", "{\"msg\":\"doc\",\"rank\":" + i + "}").status()
                );
            }
            loop.tick(clock.get());   // publish, so there is a commit to freeze

            final Response before = send(node, "GET", "/_serverless/stats", null);
            assertTrue("two writers before the view: " + before.body(), before.body().contains("\"writers\":2"));

            final Response taken = send(node, "POST", "/paged/_pit?keep_alive=10m", null);
            assertEquals(taken.body(), 200, taken.status());
            final String pit = field(taken.body(), "pit_id");
            // Paging the view opens its shards here: this node is the only one there is.
            final Response frozen = send(node, "POST", "/paged/_search?pit=" + pit, "{\"query\":{\"match_all\":{}}}");
            assertEquals(frozen.body(), 200, frozen.status());
            assertEquals("the view's shards are open here", 2, node.reconciler().frozenShards().size());

            final Response after = send(node, "GET", "/_serverless/stats", null);
            assertEquals(after.body(), 200, after.status());
            assertTrue("still two writers with the view open: " + after.body(), after.body().contains("\"writers\":2"));
            assertTrue("the view counted apart: " + after.body(), after.body().contains("\"frozen_views\":2"));
            assertTrue("and no live reader invented: " + after.body(), after.body().contains("\"readers\":0"));
            assertTrue("each held shard is listed with its kind: " + after.body(), after.body().contains("\"kind\":\"frozen_view\""));
            assertTrue(after.body(), after.body().contains("\"kind\":\"writer\""));
        }
    }

    /**
     * A health check that timed out says so in the status code, not only in the body.
     *
     * <p>{@code curl -f ...?wait_for_nodes=3&timeout=30s} in a readiness script reads the status and
     * nothing else, and passed on a two-node fleet. Core answers 408.
     */
    public void testATimedOutHealthCheckAnswers408() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("health-408"))) {
            node.start();
            node.setMetadataPlane(plane(clock));

            final Response unmet = send(node, "GET", "/_cluster/health?wait_for_nodes=9&timeout=300ms", null);
            assertEquals("a wait that ran out is a 408: " + unmet.body(), 408, unmet.status());
            assertTrue("and the body agrees: " + unmet.body(), unmet.body().contains("\"timed_out\":true"));

            // A started node has enrolled its own lease, so the fleet it can see is one node.
            final Response met = send(node, "GET", "/_cluster/health?wait_for_nodes=1&timeout=300ms", null);
            assertEquals("a condition that holds is a 200: " + met.body(), 200, met.status());
            assertTrue(met.body(), met.body().contains("\"timed_out\":false"));
        }
    }

    /**
     * {@code wait_for_nodes} understands core's grammar, {@code <} included.
     *
     * <p>Every non-digit used to be stripped, so {@code <3}, {@code le(3)} and {@code lt(3)} all meant
     * {@code >=3}: a caller waiting for a fleet to shrink blocked until the timeout and was then told,
     * with a 200, that it had.
     */
    public void testWaitForNodesUnderstandsLessThan() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("health-lt"))) {
            node.start();
            node.setMetadataPlane(plane(clock));

            // A started node has enrolled its own lease, so the fleet it can see is exactly one node.
            final Response fleet = send(node, "GET", "/_cluster/health", null);
            assertTrue("one node: " + fleet.body(), fleet.body().contains("\"number_of_nodes\":1"));
            for (String form : new String[] { "%3C3", "lt(3)", "le(3)", "%3C=3", "gt(0)", "ge(1)", "1" }) {
                final Response holds = send(node, "GET", "/_cluster/health?wait_for_nodes=" + form + "&timeout=300ms", null);
                assertEquals(form + " holds on a one-node fleet: " + holds.body(), 200, holds.status());
                assertTrue(form + ": " + holds.body(), holds.body().contains("\"timed_out\":false"));
            }
            for (String form : new String[] { "%3E=3", "ge(3)", "gt(1)", "%3C1", "lt(1)", "3" }) {
                final Response more = send(node, "GET", "/_cluster/health?wait_for_nodes=" + form + "&timeout=300ms", null);
                assertEquals(form + " cannot hold on a one-node fleet: " + more.body(), 408, more.status());
            }
            final Response nonsense = send(node, "GET", "/_cluster/health?wait_for_nodes=many&timeout=300ms", null);
            assertEquals("a condition that is not one is refused: " + nonsense.body(), 400, nonsense.status());
        }
    }

    /**
     * A shard whose owner is alive stays {@code owned} past one lease TTL.
     *
     * <p>Under batched liveness the head's stamp is written once and never renewed -- one lease renewal
     * covers every shard a node holds -- so after one TTL the stamp is in the past and the owner's lease
     * is what says whether the shard is held. Health classified on the stamp alone, and reported a shard
     * that was taking writes as dormant thirty seconds after it was activated; an autoscaler reading
     * {@code dormant_shards} would have concluded a busy index had scaled to zero.
     */
    public void testAnOwnedShardStaysOwnedPastOneTtlWhileItsOwnerIsAlive() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("busy", "uuid-busy-0000000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("health-alive"))) {
            node.start();
            node.setMetadataPlane(plane);
            plane.membership().renew(leaseFor(node));
            hold(node, plane, clock, "busy", 1);

            final Response fresh = send(node, "GET", "/_cluster/health/busy?level=shards", null);
            assertTrue("owned inside the TTL: " + fresh.body(), fresh.body().contains("\"state\":\"owned\""));

            // Past the stamp, with the owner still renewing its one lease.
            clock.addAndGet(TTL + 1);
            plane.membership().renew(leaseFor(node));

            final Response later = send(node, "GET", "/_cluster/health/busy?level=shards", null);
            assertEquals(later.body(), 200, later.status());
            assertTrue("still owned, because the owner is alive: " + later.body(), later.body().contains("\"state\":\"owned\""));
            assertTrue("and not counted dormant: " + later.body(), later.body().contains("\"dormant_shards\":0"));

            // And when the owner's lease lapses the shard really is dormant, which is what the field is for.
            clock.addAndGet(TTL + 1);
            final Response lapsed = send(node, "GET", "/_cluster/health/busy?level=shards", null);
            assertTrue("dormant once nobody holds it: " + lapsed.body(), lapsed.body().contains("\"state\":\"dormant\""));
        }
    }

    private static NodeLease leaseFor(ServerlessNode node) {
        return new NodeLease(node.localNode().getId(), node.localNode().getEphemeralId(), "127.0.0.1:9300", Set.of("ingest"), 0L);
    }

    private static String field(String body, String name) {
        final Matcher matcher = Pattern.compile("\"" + name + "\":\"([^\"]+)\"").matcher(body);
        assertTrue("no " + name + " in " + body, matcher.find());
        return java.net.URLEncoder.encode(matcher.group(1), StandardCharsets.UTF_8);
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
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
