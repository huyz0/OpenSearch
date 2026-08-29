/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * What the design costs, counted rather than argued.
 *
 * <p>The architecture is justified by a cost model: a steady-state reconciliation pass is a handful of
 * object-store operations, batched leases make that independent of how many shards a node holds, and
 * block-range reads fetch a fraction of a shard. Every one of those numbers was measured against a local
 * filesystem, where an operation is a system call and free. On an object store they are billed HTTP
 * requests, and the first wall-clock reading taken — a three-node fleet test at 69 seconds against a
 * bucket where its filesystem twin takes 6 — was not reassuring.
 *
 * <p><b>Counted in operations, not seconds.</b> Wall-clock on a loopback MinIO says almost nothing about
 * a real network, and it flatters and punishes for reasons that have nothing to do with the design. A
 * request count transfers: it is the same number against S3, and it is the number on the bill.
 *
 * <p>Every scenario runs against both a filesystem and a bucket. Absolute counts are interesting; the
 * <em>difference</em> is what says whether the model measured on a filesystem still holds when the
 * operations are real, and a scenario that costs the same on both is one the transport cannot explain.
 *
 * <p>These tests assert on the shape of the cost — that it does not grow with shard count, that an idle
 * node is cheap — and log the numbers. They are not benchmarks and there is no baseline file to drift
 * against; the assertions are the claims the RFC makes, and they fail if a claim stops being true.
 */
// The AWS SDK keeps event-loop threads alive past the test; they belong to the client, not to us.
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ServerlessCostTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";
    private static final long TTL = 30_000L;
    private static final String DEFAULT_ENDPOINT = "http://127.0.0.1:9000";

    private String endpoint() {
        return System.getProperty(MinioBlobContainerConformanceTests.ENDPOINT, DEFAULT_ENDPOINT);
    }

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-cost")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private CountingBlobStore filesystem() throws Exception {
        return new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false));
    }

    private CountingBlobStore bucket() throws Exception {
        final String name = "cost-" + randomAlphaOfLength(12).toLowerCase(Locale.ROOT);
        final BlobStore store = org.opensearch.repositories.s3.MinioBlobStores.create(
            endpoint(),
            "minioadmin",
            "minioadmin",
            name,
            createTempDir()
        );
        return new CountingBlobStore(store);
    }

    /** What one node holding {@code shards} shards spends on a tick that has nothing to do. */
    private long[] steadyStateTick(CountingBlobStore store, String label, int shards, int ticks) throws Exception {
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", shards, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("cost-" + label + "-" + shards))) {
            node.start();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            for (int shard = 0; shard < shards; shard++) {
                loop.want("alpha", shard);
            }
            loop.tick(System.currentTimeMillis());   // acquire; not what is being measured
            assertEquals("every shard must be held before measuring the idle cost", shards, node.reconciler().openShards().size());

            store.reset();
            final long startedAt = System.nanoTime();
            for (int i = 0; i < ticks; i++) {
                loop.tick(System.currentTimeMillis());
            }
            final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

            logger.info(
                "cost[{} shards={}]: per idle tick -> {} logical, {} implied S3 requests, {}ms  [{}]",
                label,
                shards,
                store.total() / (double) ticks,
                store.impliedS3Requests() / (double) ticks,
                elapsedMillis / (double) ticks,
                store.breakdown()
            );
            return new long[] { store.total(), store.impliedS3Requests(), elapsedMillis, store.registerWrites(), store.registerReads() };
        }
    }

    /**
     * There is only one liveness mode, and a store cannot be built without it.
     *
     * <p>This replaces two earlier tests. One measured per-head liveness and claimed, wrongly, that it
     * would fail when batching became the default — it passed {@code false} explicitly, so it pinned a
     * mode rather than a default and would have gone on passing. The other asked a plane whether it
     * batched, which stopped being a question once the alternative was deleted.
     *
     * <p>What is left to guard is that the alternative cannot come back by accident: a {@code
     * ShardHeadStore} built without a liveness oracle used to silently mean per-head expiry, and now
     * refuses.
     */
    public void testPerHeadLivenessCannotBeReintroducedByPassingNoOracle() {
        final IllegalArgumentException refused = expectThrows(
            IllegalArgumentException.class,
            () -> new org.opensearch.serverless.metadata.ShardHeadStore(
                new FsBlobStore(1024, createTempDir(), false).blobContainer(BlobPath.cleanPath()),
                System::currentTimeMillis,
                TTL,
                null
            )
        );
        assertTrue(
            "the refusal should say what is no longer supported: " + refused.getMessage(),
            refused.getMessage().contains("per-head")
        );
    }

    /**
     * §7's claim, measured: batched leases make the per-tick <b>write</b> cost independent of how many
     * shards a node holds. One lease renewal covers all of them.
     *
     * <p><b>They do not make the tick flat, and the RFC should not be read as saying they do.</b> Each
     * held shard's head is still <em>read</em> every tick, because losing a shard is something only the
     * head can report. So an idle tick stays linear in shard count either way; batching removes the
     * expensive half of it. Measured on a filesystem, at 8 shards: 34 requests per tick without batching,
     * 18 with — better by a constant, not by an order.
     *
     * <p>The assertion is on the compare-and-swap count, because that is the number §7 is actually about
     * and the only one that is genuinely constant.
     */
    public void testBatchedLeasesMakeThePerTickWriteCostIndependentOfShardCount() throws Exception {
        final long[] one = steadyStateTick(filesystem(), "fs-batched", 1, 10);
        final long[] eight = steadyStateTick(filesystem(), "fs-batched", 8, 10);

        final double casAtOne = one[3] / 10.0;
        final double casAtEight = eight[3] / 10.0;
        final double readsAtOne = one[4] / 10.0;
        final double readsAtEight = eight[4] / 10.0;
        logger.info(
            "cost: batched -- compare-and-swaps per tick {} -> {} (1 to 8 shards); register reads per tick {} -> {}",
            casAtOne,
            casAtEight,
            readsAtOne,
            readsAtEight
        );

        assertEquals("one lease renewal must cover every shard, however many there are", casAtOne, casAtEight, 0.001);

        // And the half that is not constant, stated rather than glossed: reads still scale, so the tick
        // is O(shards) even batched. Asserting it keeps the claim honest if someone later fixes it.
        assertTrue(
            "register reads are expected to scale with shard count even when batched: " + readsAtOne + " -> " + readsAtEight,
            readsAtEight > readsAtOne
        );
    }

    /** What a document costs, end to end, including the publish it eventually triggers. */
    public void testWhatADocumentCosts() throws Exception {
        for (boolean onBucket : new boolean[] { false, true }) {
            if (onBucket) {
                assumeEndpoint();
            }
            final CountingBlobStore store = onBucket ? bucket() : filesystem();
            final String label = onBucket ? "s3" : "fs";
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
            plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

            try (ServerlessNode node = new ServerlessNode(nodeSettings("cost-doc-" + label))) {
                node.start();
                final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
                loop.want("alpha", 0);
                loop.tick(System.currentTimeMillis());
                final ShardId shardId = node.reconciler().openShards().iterator().next();

                final int documents = 50;
                store.reset();
                final long startedAt = System.nanoTime();
                for (int i = 0; i < documents; i++) {
                    node.index(shardId, String.valueOf(i), "{\"msg\":\"cost\",\"n\":" + i + "}");
                }
                final long writeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
                final long perDocument = store.impliedS3Requests();
                // Captured before the publish. The first version of this test read the counter after the
                // tick below and then complained that 50 documents cost 54 writes -- four of which were
                // the segment files the publish uploaded, and nothing to do with the per-document cost.
                final long writesBeforePublishing = store.blobWrites();

                loop.tick(System.currentTimeMillis());   // the publish those writes earned
                logger.info(
                    "cost[{}]: {} documents cost {} requests ({} each, {}ms each) before publishing; {} including the publish  [{}]",
                    label,
                    documents,
                    perDocument,
                    perDocument / (double) documents,
                    writeMillis / (double) documents,
                    store.impliedS3Requests(),
                    store.breakdown()
                );

                // One durable write is one log append. Anything more means the write path grew a round trip
                // nobody accounted for, which is the kind of regression a count catches and a wall-clock
                // hides.
                //
                // Measured before the publish, and that distinction is the whole difficulty of this test.
                // Reading the counter after the tick above reports 54 for 50 documents -- 50 log appends
                // plus the four segment files publication uploaded -- and looks exactly like a write path
                // that costs 1.08 operations per document for no reason.
                assertEquals(
                    "on " + label + ", a document should cost exactly one object-store write before publication",
                    documents,
                    writesBeforePublishing
                );
                assertTrue("on " + label + ", no more than about one request per document: " + perDocument, perDocument <= documents * 2L);
            }
        }
    }

    /**
     * What a node with nothing to do costs to keep alive, which is the number that decides whether
     * scale-to-zero is a feature or a necessity.
     */
    public void testWhatAnIdleNodeCostsPerHour() throws Exception {
        assumeEndpoint();
        final CountingBlobStore store = bucket();
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("cost-idle"))) {
            node.start();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(System.currentTimeMillis());

            store.reset();
            final int renewals = 5;
            for (int i = 0; i < renewals; i++) {
                loop.renewLeases();
            }
            final double perRenewal = store.impliedS3Requests() / (double) renewals;

            // The scheduler renews at ttl/3, so an hour of doing nothing is this many renewals.
            final double renewalsPerHour = 3_600_000.0 / (TTL / 3.0);
            final double requestsPerHour = perRenewal * renewalsPerHour;
            logger.info(
                "cost: an idle node holding 1 shard costs {} requests per renewal, {} renewals/hour, {} requests/hour  [{}]",
                perRenewal,
                renewalsPerHour,
                requestsPerHour,
                store.breakdown()
            );
            assertTrue("a renewal should be a small, fixed number of requests: " + perRenewal, perRenewal <= 8);
        }
    }

    private void assumeEndpoint() {
        assumeTrue("no S3-compatible endpoint at " + endpoint() + "; see r11-conformance.md", reachable(endpoint()));
    }

    private static boolean reachable(String endpoint) {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint + "/minio/health/live"))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
            return client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() < 500;
        } catch (Exception e) {
            return false;
        }
    }
}
