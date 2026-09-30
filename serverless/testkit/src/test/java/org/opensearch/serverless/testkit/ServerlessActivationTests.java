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
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.ReconcileScheduler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Taking cold shards: one flight per shard, many shards at once, and a write that waits for its shard rather
 * than being sent away to retry.
 */
public class ServerlessActivationTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-activation")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private static ReconcileScheduler scheduler(BackgroundReconciler loop, ServerlessNode node, AtomicLong clock) {
        return new ReconcileScheduler(loop, node.threadPool(), clock::get, TimeValue.timeValueHours(1), TimeValue.timeValueHours(1), null);
    }

    /** The first write to a shard nobody holds is acknowledged in one request: it waits for the activation it caused. */
    public void testAFirstWriteWaitsForItsShardInsteadOfBeingSentAway() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("cold", "uuid-cold-000000000", 1, MAPPING, null));
        try (ServerlessNode node = new ServerlessNode(nodeSettings("first-write"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true);
            try (ReconcileScheduler scheduler = scheduler(loop, node, clock)) {
                node.setSignals(scheduler);
                scheduler.start();
                final Response put = send(node, "PUT", "/cold/_doc/1", "{\"msg\":\"first\"}");
                assertEquals("acknowledged on the first attempt: " + put.body(), 201, put.status());
                assertEquals(node.localNode().getId(), plane.heads().read("cold", 0).orElseThrow().ownerNodeId());
            }
        }
    }

    /** A burst of writes to one cold shard is one activation: one compare-and-swap of its head, every write acknowledged. */
    public void testABurstOfWritesToOneColdShardIsOneActivation() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final AtomicInteger headWrites = new AtomicInteger();
        final String head = RegisterMap.shardHeadBlob("burst", 0);
        final BlobStore counting = gated(new FsBlobStore(1024, createTempDir(), false), (name, proceed) -> {
            if (name.equals(head)) {
                headWrites.incrementAndGet();
            }
            return proceed.get();
        });
        final MetadataPlane plane = new MetadataPlane(counting, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("burst", "uuid-burst-00000000", 1, MAPPING, null));
        try (ServerlessNode node = new ServerlessNode(nodeSettings("burst"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true);
            try (ReconcileScheduler scheduler = scheduler(loop, node, clock)) {
                node.setSignals(scheduler);
                scheduler.start();
                final int writers = 8;
                final CyclicBarrier together = new CyclicBarrier(writers);
                final ExecutorService pool = Executors.newFixedThreadPool(writers);
                try {
                    final List<Future<Response>> puts = new ArrayList<>();
                    for (int i = 0; i < writers; i++) {
                        final int doc = i;
                        puts.add(pool.submit(() -> {
                            together.await(10, TimeUnit.SECONDS);
                            return send(node, "PUT", "/burst/_doc/" + doc, "{\"msg\":\"burst\"}");
                        }));
                    }
                    for (Future<Response> put : puts) {
                        final Response answer = put.get(60, TimeUnit.SECONDS);
                        assertEquals(answer.body(), 201, answer.status());
                    }
                } finally {
                    terminate(pool);
                }
                assertEquals("one activation, so one write of the head", 1, headWrites.get());
            }
        }
    }

    /**
     * Two shards' activations are in flight at once. Each one's head write waits until the other's has begun,
     * so activations run one after another -- the single lock they used to share -- cannot finish.
     */
    public void testDifferentShardsActivateConcurrently() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final String headA = RegisterMap.shardHeadBlob("left", 0);
        final String headB = RegisterMap.shardHeadBlob("right", 0);
        final CountDownLatch aStarted = new CountDownLatch(1);
        final CountDownLatch bStarted = new CountDownLatch(1);
        final BlobStore gated = gated(new FsBlobStore(1024, createTempDir(), false), (name, proceed) -> {
            if (name.equals(headA)) {
                aStarted.countDown();
                if (bStarted.await(10, TimeUnit.SECONDS) == false) {
                    throw new IOException("the other shard's activation never started: activations are serialised");
                }
            } else if (name.equals(headB)) {
                bStarted.countDown();
                if (aStarted.await(10, TimeUnit.SECONDS) == false) {
                    throw new IOException("the other shard's activation never started: activations are serialised");
                }
            }
            return proceed.get();
        });
        final MetadataPlane plane = new MetadataPlane(gated, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("left", "uuid-left-000000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("right", "uuid-right-00000000", 1, MAPPING, null));
        try (ServerlessNode node = new ServerlessNode(nodeSettings("concurrent"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane).setDemandDrivenActivation(true);
            final java.util.Set<ShardId> taken = loop.activateOnDemand(List.of(Map.entry("left", 0), Map.entry("right", 0)));
            assertEquals("both taken, together: " + taken, 2, taken.size());
            assertEquals(2, node.reconciler().openShards().size());
        }
    }

    /**
     * The canary: two nodes reaching for the same cold shards at the same instant leave exactly one owner of each,
     * and the loser holds nothing open.
     */
    public void testTwoNodesRacingForColdShardsLeaveExactlyOneOwnerOfEach() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.nio.file.Path dir = createTempDir();
        final MetadataPlane planeA = new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
        final MetadataPlane planeB = new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
        final int indices = 6;
        for (int i = 0; i < indices; i++) {
            planeA.createIndex(new IndexDescriptor("race-" + i, "uuid-race-" + i + "-0000000", 1, MAPPING, null));
        }
        try (ServerlessNode a = new ServerlessNode(nodeSettings("race-a")); ServerlessNode b = new ServerlessNode(nodeSettings("race-b"))) {
            a.start();
            b.start();
            a.setMetadataPlane(planeA);
            b.setMetadataPlane(planeB);
            final BackgroundReconciler loopA = new BackgroundReconciler(a, planeA).setDemandDrivenActivation(true);
            final BackgroundReconciler loopB = new BackgroundReconciler(b, planeB).setDemandDrivenActivation(true);
            for (int i = 0; i < indices; i++) {
                final String index = "race-" + i;
                final CompletableFuture<Optional<ShardId>> fromA = loopA.activateForRequest(index, 0);
                final CompletableFuture<Optional<ShardId>> fromB = loopB.activateForRequest(index, 0);
                final Optional<ShardId> wonA = fromA.get(60, TimeUnit.SECONDS);
                final Optional<ShardId> wonB = fromB.get(60, TimeUnit.SECONDS);
                assertTrue(index + ": exactly one node takes it, got " + wonA + " and " + wonB, wonA.isPresent() ^ wonB.isPresent());
                final ServerlessNode winner = wonA.isPresent() ? a : b;
                final ServerlessNode loser = wonA.isPresent() ? b : a;
                assertEquals(winner.localNode().getId(), planeA.heads().read(index, 0).orElseThrow().ownerNodeId());
                assertTrue(
                    index + ": the loser holds nothing of it",
                    loser.reconciler().openShards().stream().noneMatch(s -> s.getIndexName().equals(index))
                );
            }
        }
    }

    /** What a head write does after the gate lets it through. */
    @FunctionalInterface
    private interface Gate {
        BlobRegisterCasResult pass(String name, org.opensearch.common.CheckedSupplier<BlobRegisterCasResult, IOException> proceed)
            throws IOException, InterruptedException;
    }

    private static BlobStore gated(BlobStore inner, Gate gate) {
        return new BlobStore() {
            @Override
            public BlobContainer blobContainer(BlobPath path) {
                return new DelegatingBlobContainer(inner.blobContainer(path)) {
                    @Override
                    public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
                        throws IOException {
                        return through(blobName, () -> super.compareAndSwapRegister(blobName, expectedGeneration, newValue));
                    }

                    @Override
                    public BlobRegisterCasResult createRegisterIfAbsent(String blobName, BytesReference value) throws IOException {
                        return through(blobName, () -> super.createRegisterIfAbsent(blobName, value));
                    }

                    private BlobRegisterCasResult through(
                        String blobName,
                        org.opensearch.common.CheckedSupplier<BlobRegisterCasResult, IOException> proceed
                    ) throws IOException {
                        try {
                            return gate.pass(blobName, proceed);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                    }
                };
            }

            @Override
            public void close() throws IOException {
                inner.close();
            }
        };
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
