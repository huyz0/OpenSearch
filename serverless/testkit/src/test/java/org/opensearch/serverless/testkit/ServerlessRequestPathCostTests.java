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
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What a request costs in metadata, counted by the register it touches rather than in total.
 *
 * <p>A descriptor read was made on every search, multi-search, get and multi-get -- one register GET per
 * request per index named. And every health check, stats call and node listing read the members index,
 * which a load balancer polling health turns into a read per poll per node. These pin both at their
 * bounded cost. That the cache cannot answer from a deleted incarnation is
 * {@link ServerlessRoutingFreshnessTests}.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessRequestPathCostTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-request-cost")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /**
     * Once a name has been resolved, searching it, multi-searching it, getting from it and multi-getting
     * from it read no descriptor at all; once the routing window passes, the next request reads it again.
     *
     * <p>The incarnation fence is widened for the measurement, so what is counted is the routing cache and
     * not how quickly four requests happened to run: the fence costs one descriptor read per index per
     * second of activity, not per request, and is timed separately from the plane's clock.
     */
    public void testAWarmReadRequestReadsNoDescriptor() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final RegisterCounter store = new RegisterCounter(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("request-cost-reads"))) {
            node.start();
            node.setMetadataPlane(plane);
            node.setIncarnationFenceMillis(60_000L);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            assertEquals(201, send(node, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"hello\"}").status());
            assertEquals(200, send(node, "GET", "/alpha/_search?q=msg:hello", null).status());

            store.descriptorReads.set(0);
            final Response search = send(node, "GET", "/alpha/_search?q=msg:hello", null);
            final Response msearch = send(node, "POST", "/_msearch", "{\"index\":\"alpha\"}\n{\"query\":{\"match_all\":{}}}\n");
            final Response get = send(node, "GET", "/alpha/_doc/1", null);
            final Response mget = send(node, "POST", "/alpha/_mget", "{\"ids\":[\"1\"]}");
            assertEquals(search.body(), 200, search.status());
            assertTrue(search.body(), search.body().contains("\"hello\""));
            assertEquals(msearch.body(), 200, msearch.status());
            assertEquals(get.body(), 200, get.status());
            assertEquals(mget.body(), 200, mget.status());
            assertEquals("a warm search, msearch, get and mget must read no descriptor", 0L, store.descriptorReads.get());

            clock.addAndGet(MetadataPlane.DEFAULT_ROUTING_CACHE_MILLIS + 1);
            assertEquals(200, send(node, "GET", "/alpha/_search?q=msg:hello", null).status());
            assertTrue("past the window a search reads the descriptor again", store.descriptorReads.get() >= 1L);
        }
    }

    /**
     * A burst of health checks reads the members index at most once per probe interval, not once each.
     *
     * <p>The health endpoint is what a load balancer polls, and it read the register on every call.
     */
    public void testABurstOfHealthChecksReadsMembershipAtMostOncePerInterval() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final RegisterCounter store = new RegisterCounter(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("request-cost-health"))) {
            node.start();
            node.setMetadataPlane(plane);
            // The first check refreshes an empty snapshot; the burst after it finds a young one.
            assertEquals(200, send(node, "GET", "/_cluster/health", null).status());

            store.membersReads.set(0);
            final int checks = 10;
            final long startedAt = System.nanoTime();
            for (int i = 0; i < checks; i++) {
                assertEquals(200, send(node, "GET", "/_cluster/health", null).status());
            }
            final long elapsedMillis = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
            final long allowed = 1 + elapsedMillis
                / org.opensearch.serverless.membership.BlobLeaseMembership.GENERATION_PROBE_INTERVAL_MILLIS;
            logger.info(
                "request cost: {} health checks in {} ms read the members index {} times",
                checks,
                elapsedMillis,
                store.membersReads.get()
            );
            assertTrue(
                checks + " health checks in " + elapsedMillis + " ms read the members index " + store.membersReads.get() + " times",
                store.membersReads.get() <= allowed && store.membersReads.get() < checks
            );
        }
    }

    /** Counts register reads of index descriptors and of the members index, and nothing else. */
    private static final class RegisterCounter implements BlobStore {

        private final BlobStore delegate;
        final AtomicLong descriptorReads = new AtomicLong();
        final AtomicLong membersReads = new AtomicLong();

        RegisterCounter(BlobStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            final BlobContainer inner = delegate.blobContainer(path);
            final boolean descriptors = path.equals(RegisterMap.indices(BlobPath.cleanPath()));
            if (descriptors == false && path.equals(RegisterMap.members(BlobPath.cleanPath())) == false) {
                return inner;
            }
            return new DelegatingBlobContainer(inner) {
                @Override
                public Optional<BlobRegister> readRegister(String blobName) throws IOException {
                    if (descriptors) {
                        descriptorReads.incrementAndGet();
                    } else if (blobName.equals("members")) {
                        membersReads.incrementAndGet();
                    }
                    return super.readRegister(blobName);
                }
            };
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest.BodyPublisher payload = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", body != null && path.contains("_msearch") ? "application/x-ndjson" : "application/json")
                .method(method, payload)
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
