/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.serverless.auth.ServerlessAuthPlugin;
import org.opensearch.serverless.cluster.IndexDescriptor;
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
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The login throttle's semantics, stated the way an operator would: a guesser is slowed, and nobody else
 * is.
 *
 * <p>The first throttle counted per account and per address and refused when either had earned a wait,
 * which made two lockouts out of one guesser: everyone behind the guesser's address, and whichever
 * account the guesser named -- including the configured account, whose whole purpose is to be the
 * credential that still works on a bad day. Each test here is one of the properties that replaced that,
 * written so that the old behaviour fails it. {@link ServerlessAuthTests} keeps the doubling-and-recovery
 * arithmetic; this file is about who a wait may and may not touch.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessThrottleTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String AUTH_INDEX = ".serverless_auth";
    private static final String AUTH_MAPPING = "{\"properties\":{\"user\":{\"type\":\"keyword\"},\"hash\":{\"type\":\"keyword\"}}}";

    private static final String ADMIN = "admin";
    private static final String ADMIN_PASSWORD = "correct-horse-battery-staple";

    /** The floor the setting allows; these tests authenticate dozens of times. */
    private static final int TEST_ITERATIONS = 10_000;

    /** Lets a test move the plugin's clock without sleeping; a scheduled delay still runs in real time. */
    private final AtomicLong authClock = new AtomicLong(1_000_000L);

    private Settings.Builder nodeSettings(String name) {
        final org.opensearch.common.settings.MockSecureSettings secure = new org.opensearch.common.settings.MockSecureSettings();
        secure.setString("serverless.auth.bootstrap.password", ADMIN_PASSWORD);
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-throttle")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.auth.bootstrap.username", ADMIN)
            .put("serverless.auth.hash.iterations", TEST_ITERATIONS)
            .put("serverless.auth.cache.ttl", "60s")
            .setSecureSettings(secure);
    }

    private MetadataPlane plane(AtomicLong clock) throws java.io.IOException {
        return new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
    }

    private ServerlessAuthPlugin authPlugin(Settings settings) {
        return ServerlessAuthPlugin.withClock(settings, authClock::get);
    }

    /**
     * One bad account behind a shared address locks neither a good account behind it nor the configured
     * one.
     *
     * <p>Every client in a cloud deployment shares an address with every other -- a NAT, a load balancer,
     * an ingress -- so a throttle keyed on the address alone is a throttle anyone can point at everyone.
     * The wait belongs to the pair of address and account that earned it: the guesser's own attempts at
     * the name they were guessing are refused, and a caller this node already knows, a caller it has not
     * seen yet, and the configured account all pass.
     */
    public void testOneBadAccountBehindASharedAddressLocksNobodyElse() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        final Settings settings = nodeSettings("throttle-shared-address").build();

        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings)))) {
            node.start();
            node.setMetadataPlane(plane);
            ready(node, plane, clock);
            assertEquals(
                200,
                send(node, basic(ADMIN, ADMIN_PASSWORD), "PUT", "/_serverless/security/users/alice", "{\"password\":\"wonder\"}").status()
            );
            assertEquals(
                200,
                send(node, basic(ADMIN, ADMIN_PASSWORD), "PUT", "/_serverless/security/users/bob", "{\"password\":\"builder\"}").status()
            );
            assertEquals("alice logs in, and is now cached here", 200, send(node, basic("alice", "wonder")).status());

            // The guesser, from the same address as everyone else in this test.
            for (int i = 0; i < 5; i++) {
                assertEquals(401, send(node, basic("mallory", "guess-" + i)).status());
            }
            final Response guesser = send(node, basic("mallory", "guess-5"));
            assertEquals("the guesser's own pair is refused: " + guesser.body(), 429, guesser.status());
            assertEquals("1", guesser.header("Retry-After"));
            assertTrue("and told which key earned it: " + guesser.body(), guesser.body().contains("for this account from this address"));

            // The consequence, from the same address. Before the pair key existed, all three were 429.
            assertEquals("a cached caller is not asked", 200, send(node, basic("alice", "wonder")).status());
            assertEquals("a caller this node has never checked is checked", 200, send(node, basic("bob", "builder")).status());
            assertEquals("and the configured account is served", 200, send(node, basic(ADMIN, ADMIN_PASSWORD)).status());
        }
    }

    /**
     * The configured account is never refused at the door: a run of failures delays it, and a correct
     * password still gets checked.
     *
     * <p>This is the recovery-path property. The configured account exists for the day the account store
     * is unreadable, and a throttle that let an unauthenticated caller lock it by sending one wrong
     * password a minute took that away with a request nobody could tell from noise. The wait is real --
     * measured here in wall-clock time, because a scheduled delay runs on the node's scheduler and not on
     * the plugin's injectable clock -- and it ends in a check rather than a 429.
     */
    public void testTheRecoveryAccountIsDelayedAndNeverRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        // A one-second cap keeps the delay this test waits through to a second per attempt.
        final Settings settings = nodeSettings("throttle-recovery").put("serverless.auth.throttle.max_delay", "1s").build();

        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings)))) {
            node.start();
            node.setMetadataPlane(plane(clock));

            for (int i = 0; i < 5; i++) {
                assertEquals(401, send(node, basic(ADMIN, "guess-" + i)).status());
            }

            // A sixth wrong password is checked, after the wait, rather than refused at the door.
            long started = System.nanoTime();
            final Response sixth = send(node, basic(ADMIN, "guess-5"));
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertEquals("a wrong password is still checked, not refused: " + sixth.body(), 401, sixth.status());
            assertTrue("and the check waited for the delay: " + elapsed + "ms", elapsed >= 900L);

            // And the right one, which is the whole point.
            started = System.nanoTime();
            final Response recovered = send(node, basic(ADMIN, ADMIN_PASSWORD));
            elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertEquals("the configured account gets in while under attack: " + recovered.body(), 200, recovered.status());
            assertTrue("slowed rather than locked: " + elapsed + "ms", elapsed >= 900L);

            // A success cleared the run, and the credential is cached: the next one is not delayed.
            assertEquals(200, send(node, basic(ADMIN, ADMIN_PASSWORD)).status());
        }
    }

    /**
     * A stored record that asks for an absurd work factor is refused unread, in the time a parse takes.
     *
     * <p>The iteration count is read from the record so that it can be raised without invalidating every
     * stored password -- and a record is a document, which anything with write access to the index can
     * plant. One asking for two billion iterations would hold a checker thread for hours; four would hold
     * the pool, and every uncached login on the node would be a 503. So the record is checked against a
     * bound before any derivation, and a record past it is an unreadable record, which denies.
     */
    public void testAPlantedRecordWithAHugeWorkFactorIsRefusedQuickly() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        final Settings settings = nodeSettings("throttle-planted").build();

        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings)))) {
            node.start();
            node.setMetadataPlane(plane);
            ready(node, plane, clock);

            // A syntactically perfect record with the largest count the parser accepts, written straight
            // into the index the way a plugin with the client, or a bucket editor, would write it.
            final Base64.Encoder encoder = Base64.getEncoder().withoutPadding();
            final String planted = "pbkdf2_sha512$"
                + Integer.MAX_VALUE
                + "$"
                + encoder.encodeToString(new byte[16])
                + "$"
                + encoder.encodeToString(new byte[64]);
            node.client()
                .index(
                    new IndexRequest(AUTH_INDEX).id("planted")
                        .source("{\"user\":\"planted\",\"hash\":\"" + planted + "\"}", XContentType.JSON)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                )
                .actionGet();

            final long started = System.nanoTime();
            final Response refused = send(node, basic("planted", "anything"));
            final long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertEquals("a record past the bound must deny: " + refused.body(), 401, refused.status());
            assertTrue("and deny without deriving: " + elapsed + "ms", elapsed < 15_000L);

            // The bound is generous to real records: one written at the configured factor still works.
            assertEquals(
                200,
                send(node, basic(ADMIN, ADMIN_PASSWORD), "PUT", "/_serverless/security/users/legit", "{\"password\":\"fine\"}").status()
            );
            assertEquals(200, send(node, basic("legit", "fine")).status());
        }
    }

    /**
     * An address that floods across many names exhausts a budget, which refuses new names from it and
     * still does not refuse the configured account.
     *
     * <p>This is the one place an address is refused as a whole, and it takes a flood to get there: a
     * single bad account stops counting once its pair is refused. The cost is stated in the test rather
     * than hidden -- an ordinary account this node has not cached, arriving from the flooding address,
     * waits out the window too. The configured account does not; it is delayed for at most one cap.
     */
    public void testAFloodingAddressExhaustsItsBudgetWithoutLockingTheRecoveryAccount() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        final Settings settings = nodeSettings("throttle-address-budget").put("serverless.auth.throttle.address_failures", 6)
            .put("serverless.auth.throttle.max_delay", "1s")
            // No cache, so that every attempt below is the uncached kind the budget applies to.
            .put("serverless.auth.cache.ttl", "0s")
            .build();

        try (ServerlessNode node = new ServerlessNode(settings, List.of(authPlugin(settings)))) {
            node.start();
            node.setMetadataPlane(plane);
            ready(node, plane, clock);
            assertEquals(
                200,
                send(node, basic(ADMIN, ADMIN_PASSWORD), "PUT", "/_serverless/security/users/alice", "{\"password\":\"wonder\"}").status()
            );

            // Six names, one failure each: no pair reaches its threshold, the address reaches its budget.
            for (int i = 0; i < 6; i++) {
                assertEquals(401, send(node, basic("name-" + i, "wrong")).status());
            }
            final Response flooded = send(node, basic("name-6", "wrong"));
            assertEquals("a seventh name from the address is refused: " + flooded.body(), 429, flooded.status());
            assertTrue("for the address, not the name: " + flooded.body(), flooded.body().contains("from this address"));

            assertEquals(
                "an uncached ordinary account from the flooding address waits out the window",
                429,
                send(node, basic("alice", "wonder")).status()
            );
            final long started = System.nanoTime();
            final Response recovered = send(node, basic(ADMIN, ADMIN_PASSWORD));
            final long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertEquals("while the configured account is delayed and served: " + recovered.body(), 200, recovered.status());
            assertTrue("within one cap, not the rest of the window: " + elapsed + "ms", elapsed < 10_000L);
        }
    }

    /**
     * Behind a trusted proxy the forwarded address is the one that counts; with no proxy trusted, the
     * header is ignored.
     *
     * <p>A load balancer makes every connection arrive from the balancer, so a throttle that only knew
     * the peer address would count every client as one caller and refuse them together. The header that
     * fixes that is one any client can write, so it is believed only from proxies the operator named --
     * and then the rightmost address no listed proxy owns, the one the nearest proxy actually saw.
     */
    public void testTrustedProxiesAttributeFailuresToTheForwardedAddress() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        // A one-second cap: the account key delays mallory's later checks, and the doubling is not the
        // property under test here.
        final Settings trusting = nodeSettings("throttle-proxy").putList("serverless.auth.throttle.trusted_proxies", "127.0.0.1")
            .put("serverless.auth.throttle.max_delay", "1s")
            .build();

        try (ServerlessNode node = new ServerlessNode(trusting, List.of(authPlugin(trusting)))) {
            node.start();
            node.setMetadataPlane(plane(clock));

            final Map<String, String> fromOne = Map.of("X-Forwarded-For", "10.0.0.1");
            for (int i = 0; i < 5; i++) {
                assertEquals(401, send(node, basic("mallory", "guess-" + i), "GET", "/", null, fromOne).status());
            }
            assertEquals(
                "the forwarded address's pair is refused",
                429,
                send(node, basic("mallory", "guess-5"), "GET", "/", null, fromOne).status()
            );
            assertEquals(
                "a different forwarded address is a different pair",
                401,
                send(node, basic("mallory", "guess-5"), "GET", "/", null, Map.of("X-Forwarded-For", "10.0.0.2")).status()
            );
            assertEquals("and the proxy's own address has no failures of its own", 401, send(node, basic("mallory", "guess-5")).status());
            assertEquals(
                "the rightmost address that is not a trusted proxy is the one attributed",
                429,
                send(node, basic("mallory", "guess-5"), "GET", "/", null, Map.of("X-Forwarded-For", "10.0.0.1, 127.0.0.1")).status()
            );
            assertEquals(
                "a header that cannot be read falls back to the peer",
                401,
                send(node, basic("mallory", "guess-5"), "GET", "/", null, Map.of("X-Forwarded-For", "not-an-address")).status()
            );
        }

        final Settings ignoring = nodeSettings("throttle-no-proxy").put("serverless.auth.throttle.max_delay", "1s").build();
        try (ServerlessNode node = new ServerlessNode(ignoring, List.of(authPlugin(ignoring)))) {
            node.start();
            node.setMetadataPlane(plane(clock));
            for (int i = 0; i < 5; i++) {
                assertEquals(
                    401,
                    send(node, basic("mallory", "guess-" + i), "GET", "/", null, Map.of("X-Forwarded-For", "10.0.0.1")).status()
                );
            }
            assertEquals(
                "with no trusted proxy the header is a client's claim and is ignored",
                429,
                send(node, basic("mallory", "guess-5"), "GET", "/", null, Map.of("X-Forwarded-For", "10.0.0.2")).status()
            );
        }
    }

    /** Creates the account index and gives its shard an owner. */
    private BackgroundReconciler ready(ServerlessNode node, MetadataPlane plane, AtomicLong clock) throws Exception {
        plane.createIndex(new IndexDescriptor(AUTH_INDEX, "uuid-auth-000000000", 1, AUTH_MAPPING, null));
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        loop.want(AUTH_INDEX, 0);
        loop.tick(clock.get());
        return loop;
    }

    private static String basic(String user, String password) {
        return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    private record Response(int status, String body, java.net.http.HttpHeaders headers) {
        String header(String name) {
            return headers.firstValue(name).orElse(null);
        }
    }

    private static Response send(ServerlessNode node, String authorization) throws Exception {
        return send(node, authorization, "GET", "/", null, Map.of());
    }

    private static Response send(ServerlessNode node, String authorization, String method, String path, String body) throws Exception {
        return send(node, authorization, method, path, body, Map.of());
    }

    private static Response send(
        ServerlessNode node,
        String authorization,
        String method,
        String path,
        String body,
        Map<String, String> headers
    ) throws Exception {
        final org.opensearch.core.common.transport.TransportAddress address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json");
            if (authorization != null) {
                builder.header("Authorization", authorization);
            }
            for (Map.Entry<String, String> header : headers.entrySet()) {
                builder.header(header.getKey(), header.getValue());
            }
            builder.method(
                method,
                body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)
            );
            final HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body(), response.headers());
        }
    }
}
