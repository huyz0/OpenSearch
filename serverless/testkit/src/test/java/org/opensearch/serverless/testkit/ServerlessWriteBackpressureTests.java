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
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.WriteBackpressure;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writes are refused early and honestly when the object store is slow to take them.
 *
 * <p>A fleet throttled for a minute had its writes wait out the client's whole timeout -- half a minute at the median,
 * and an answer the client could not tell from a lost write. Now a node lets only so many writes wait on the store,
 * fewer the slower it gets, and refuses the rest with {@code 429} and {@code Retry-After} before applying anything.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessWriteBackpressureTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    /** The limit falls on slow appends, refuses past it, and recovers on fast ones. */
    public void testTheLimitFollowsHowTheStoreAnswers() {
        final WriteBackpressure limiter = new WriteBackpressure(100, 1_000, 2, 16);
        assertEquals(16, limiter.stats().limit());
        // Slow appends, spaced past the once-per-half-second cut.
        for (int i = 0; i < 4; i++) {
            limiter.onAppend(TimeUnit.MILLISECONDS.toNanos(400), true);
            sleepPastACut();
        }
        final int lowered = limiter.stats().limit();
        assertTrue("slow appends lower the limit: " + lowered, lowered < 16);
        final List<Releasable> held = new ArrayList<>();
        for (int i = 0; i < lowered; i++) {
            held.add(limiter.admit());
        }
        final WriteBackpressure.ThrottledException refused = expectThrows(WriteBackpressure.ThrottledException.class, limiter::admit);
        assertTrue("with a retry hint: " + refused.getMessage(), refused.retryAfterSeconds() >= 1);
        held.forEach(Releasable::close);
        for (int i = 0; i < 200; i++) {
            limiter.onAppend(TimeUnit.MILLISECONDS.toNanos(5), true);
        }
        assertEquals("fast appends bring it back", 16, limiter.stats().limit());
    }

    /**
     * One slow append among fast ones does not cut the limit; appends slow on average do. Cutting on every sample past
     * the target kept a fleet's limits at 18 to 27 on a store answering in 300 to 600 ms, refusing writes it could take.
     */
    public void testAnOutlierDoesNotCutTheLimitButASlowStoreDoes() {
        final WriteBackpressure limiter = new WriteBackpressure(1_000, 5_000, 4, 64);
        for (int i = 0; i < 50; i++) {
            limiter.onAppend(TimeUnit.MILLISECONDS.toNanos(400), true);
        }
        limiter.onAppend(TimeUnit.MILLISECONDS.toNanos(2_500), true);
        for (int i = 0; i < 5; i++) {
            limiter.onAppend(TimeUnit.MILLISECONDS.toNanos(400), true);
        }
        assertEquals("one slow append among fast ones changes nothing", 64, limiter.stats().limit());

        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 10; j++) {
                limiter.onAppend(TimeUnit.MILLISECONDS.toNanos(3_000), true);
            }
            sleepPastACut();
        }
        assertTrue("a store slow on average is backed off from: " + limiter.stats(), limiter.stats().limit() < 64);
    }

    /**
     * A limit cut to its floor by a burst is back within a few hundred healthy appends, not a hundred thousand: a fleet
     * shed writes for most of an hour after a minute of SlowDown because growth was additive only.
     */
    public void testALimitCutByABurstRecoversQuicklyOnceTheStoreIs() {
        final WriteBackpressure limiter = new WriteBackpressure(1_000, 5_000, 4, 1024);
        for (int i = 0; i < 20; i++) {
            limiter.onAppend(TimeUnit.MILLISECONDS.toNanos(3_000), false);
            sleepPastACut();
            if (limiter.stats().limit() <= 4) {
                break;
            }
        }
        assertEquals("the burst cut it to the floor", 4, limiter.stats().limit());
        int appends = 0;
        while (limiter.stats().limit() < 1024 && appends < 2_000) {
            limiter.onAppend(TimeUnit.MILLISECONDS.toNanos(100), true);
            appends++;
        }
        assertEquals("back to the ceiling", 1024, limiter.stats().limit());
        assertTrue("within a few hundred healthy appends: " + appends, appends < 600);
    }

    /** Appends averaging past the budget refuse new writes even under the limit. */
    public void testAppendsPastTheBudgetRefuseNewWrites() {
        final WriteBackpressure limiter = new WriteBackpressure(100, 500, 1, 64);
        for (int i = 0; i < 20; i++) {
            limiter.onAppend(TimeUnit.MILLISECONDS.toNanos(2_000), true);
        }
        try (Releasable first = limiter.admit()) {
            expectThrows(WriteBackpressure.ThrottledException.class, limiter::admit);
        }
    }

    private static void sleepPastACut() {
        final long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(510);
        while (System.nanoTime() < until) {
            java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
        }
    }

    /**
     * A burst against a store slow to take log appends: every write is answered promptly, either acknowledged or
     * refused with 429 and a retry hint, and a refused one was never applied.
     */
    public void testABurstAgainstASlowStoreIsShedHonestly() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final HookedBlobStore store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        final Settings settings = Settings.builder()
            .put("node.name", "backpressure")
            .put("cluster.name", "serverless-backpressure")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.write.backpressure.target_millis", 200)
            .put("serverless.write.backpressure.budget_millis", 600)
            .put("serverless.write.backpressure.min_in_flight", 1)
            .put("serverless.write.backpressure.max_in_flight", 4)
            .build();
        try (ServerlessNode node = new ServerlessNode(settings)) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            assertEquals(201, send(node, "PUT", "/alpha/_doc/warm", "{\"msg\":\"m\",\"n\":0}").status());

            store.delayWritesUnder("/wal/", 1_500);
            final int writes = 24;
            final ExecutorService clients = Executors.newFixedThreadPool(writes);
            final List<Future<Response>> sent = new ArrayList<>();
            try {
                for (int i = 0; i < writes; i++) {
                    final String id = "d" + i;
                    sent.add(clients.submit(() -> send(node, "PUT", "/alpha/_doc/" + id, "{\"msg\":\"m\",\"n\":1}")));
                }
                int acknowledged = 0;
                int refused = 0;
                for (int i = 0; i < writes; i++) {
                    final Response response = sent.get(i).get(60, TimeUnit.SECONDS);
                    if (response.status() == 201) {
                        acknowledged++;
                    } else {
                        assertEquals("acknowledged or refused, nothing else: " + response.body(), 429, response.status());
                        assertNotNull("a refusal says when to come back", response.retryAfter());
                        refused++;
                    }
                }
                assertTrue("the burst was shed, not all queued: " + node.writeBackpressure(), refused > 0);
                assertTrue("and some writes still went through", acknowledged > 0);
            } finally {
                clients.shutdownNow();
            }
            store.delayWritesUnder(null, 0);

            for (int i = 0; i < writes; i++) {
                final boolean wasAcknowledged = sent.get(i).get().status() == 201;
                final Response got = send(node, "GET", "/alpha/_doc/d" + i, null);
                assertEquals(
                    "an acknowledged write is there and a refused one never was: d" + i + " " + got.body(),
                    wasAcknowledged,
                    got.body().contains("\"found\":true")
                );
            }
        }
    }

    private record Response(int status, String body, String retryAfter) {
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
            return new Response(response.statusCode(), response.body(), response.headers().firstValue("Retry-After").orElse(null));
        }
    }
}
