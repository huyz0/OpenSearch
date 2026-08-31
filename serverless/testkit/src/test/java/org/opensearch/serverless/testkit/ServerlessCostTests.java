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

    private Settings nodeSettings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-cost")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .build();
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

    /**
     * Reclaiming a dead predecessor's log costs listings once, and on a bucket it does not cost them again.
     *
     * <p>Cross-term reclamation makes every publish list the log's term containers so it can drop the ones
     * below its own. That is a new object-store operation on the publish path, and the question a count
     * answers and an argument does not is whether it is paid once per dead term or once per publish, for
     * the life of the shard.
     *
     * <p><b>The answer differs by store, and both answers are asserted.</b> On a bucket, emptying a prefix
     * makes it stop existing, so a reclaimed term is not there to be listed next time and the second publish
     * is exactly {@code deadTerms} listings cheaper. On a filesystem the emptied directories remain, are
     * re-listed on every publish forever, and the cost does not fall — which is a recurring toll, and
     * tolerable only because the filesystem store is a test fixture (D5) and its listings are system calls
     * rather than billed requests. Pinned here so that if the fixture ever becomes something people run on,
     * this is a measured number rather than a surprise.
     *
     * <p>Both measurements are publishes carrying one document, so the difference between them is the
     * reclamation and nothing else. The first version of this test compared the activation tick against the
     * next tick and found 35 requests against 4 — a difference that was entirely a publish happening versus
     * not happening, since publication is edge-triggered and the second tick had nothing dirty to publish.
     */
    public void testReclaimingTheLogCostsListingsOncePerDeadTerm() throws Exception {
        for (boolean onBucket : new boolean[] { false, true }) {
            if (onBucket) {
                assumeEndpoint();
            }
            final CountingBlobStore store = onBucket ? bucket() : filesystem();
            final String label = onBucket ? "s3" : "fs";
            final java.util.concurrent.atomic.AtomicLong clock = new java.util.concurrent.atomic.AtomicLong(1_000L);
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

            // Three writers that took the shard and died. Simulated at the plane rather than with three
            // real nodes: what is measured is the cost of the containers they leave behind, and a
            // container does not remember how it was filled.
            final int deadTerms = 3;
            final java.util.List<Long> ghostTerms = new java.util.ArrayList<>();
            for (int i = 0; i < deadTerms; i++) {
                clock.addAndGet(TTL + 1);
                ghostTerms.add(plane.activate("alpha", 0, "ghost-" + i, "ephemeral-" + i).head().term());
            }

            clock.addAndGet(TTL + 1);
            try (ServerlessNode node = new ServerlessNode(nodeSettings("cost-reclaim-" + label))) {
                node.start();
                final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
                loop.want("alpha", 0);
                loop.tick(clock.get());   // activates, and publishes in the same pass
                final ShardId shardId = node.reconciler().openShards().iterator().next();

                // The dead terms' records are appended now rather than earlier, because the activation
                // above would otherwise have reclaimed them before anything was being counted. Written
                // at a term below the live one, which is what a zombie that has not noticed it lost the
                // shard produces, and the only state a publish can find to reclaim.
                for (long ghost : ghostTerms) {
                    plane.walStore("alpha", 0)
                        .append(ghost, new org.opensearch.serverless.store.WalRecord("g" + ghost, "{\"msg\":\"ghost\"}"));
                }

                node.index(shardId, "one", "{\"msg\":\"cost\",\"n\":1}");
                store.reset();
                assertEquals(1, loop.tick(clock.addAndGet(1_000)).published().size());
                final long reclaiming = store.listings();

                node.index(shardId, "two", "{\"msg\":\"cost\",\"n\":2}");
                store.reset();
                assertEquals(1, loop.tick(clock.addAndGet(1_000)).published().size());
                final long afterwards = store.listings();

                logger.info(
                    "cost[{}]: the publish that reclaimed {} dead terms did {} listings; the next publish did {}",
                    label,
                    deadTerms,
                    reclaiming,
                    afterwards
                );

                if (onBucket) {
                    assertEquals(
                        "on s3, reclaiming " + deadTerms + " dead terms should stop costing listings once they are gone",
                        reclaiming - deadTerms,
                        afterwards
                    );
                } else {
                    assertEquals(
                        "on fs, emptied term directories remain and are re-listed; if that changed, say so here",
                        reclaiming,
                        afterwards
                    );
                }
            }
        }
    }

    /**
     * What a batch costs, against the number this suite already records for a single document.
     *
     * <p>{@code testWhatADocumentCosts} asserts one object-store write per document, measured before
     * publication. That was the honest price of a write path with no group commit, and it is the number
     * {@code _bulk} exists to change. Measured the same way, on both stores, so the comparison is against
     * this suite's own baseline rather than against an argument.
     */
    public void testWhatABatchCosts() throws Exception {
        for (boolean onBucket : new boolean[] { false, true }) {
            if (onBucket) {
                assumeEndpoint();
            }
            final CountingBlobStore store = onBucket ? bucket() : filesystem();
            final String label = onBucket ? "s3" : "fs";
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
            plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

            try (ServerlessNode node = new ServerlessNode(nodeSettings("cost-bulk-" + label))) {
                node.start();
                node.setMetadataPlane(plane);
                final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
                loop.want("alpha", 0);
                loop.tick(System.currentTimeMillis());
                final ShardId shardId = node.reconciler().openShards().iterator().next();

                final int documents = 50;

                // The baseline, re-measured here rather than quoted: one document at a time.
                store.reset();
                for (int i = 0; i < documents; i++) {
                    node.index(shardId, "single-" + i, "{\"msg\":\"cost\",\"n\":" + i + "}");
                }
                final long oneAtATime = store.blobWrites();

                // The same documents, as one request.
                final StringBuilder body = new StringBuilder();
                for (int i = 0; i < documents; i++) {
                    body.append("{\"index\":{\"_id\":\"batched-").append(i).append("\"}}\n");
                    body.append("{\"msg\":\"cost\",\"n\":").append(i).append("}\n");
                }
                store.reset();
                final var response = bulk(node, body.toString());
                final long batched = store.blobWrites();

                assertEquals("on " + label + ", the batch should be accepted: " + response, 200, statusOf(response));
                // Writes only, and deliberately no wall-clock. The baseline calls the node directly while
                // the batch goes over HTTP, so a time comparison between them would be measuring the
                // presence of a network hop and reporting it as the cost of batching. The request count
                // is the number that transfers anyway -- it is the same against S3, and it is the bill.
                logger.info(
                    "cost[{}]: {} documents cost {} writes one at a time, {} write(s) as one batch",
                    label,
                    documents,
                    oneAtATime,
                    batched
                );

                assertEquals("on " + label + ", the per-document baseline should be one write each", documents, oneAtATime);
                // The whole claim, and it is an equality rather than a ratio: a batch on one shard is one
                // log append, not a smaller number of them.
                assertEquals("on " + label + ", a batch on one shard must cost exactly one object-store write", 1, batched);
            }
        }
    }

    private static int statusOf(String response) {
        return response.startsWith("200 ") ? 200 : -1;
    }

    /** Sends a bulk body over HTTP and returns "status body". */
    private static String bulk(ServerlessNode node, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + "/alpha/_bulk"))
                .timeout(Duration.ofSeconds(120))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() + " " + response.body();
        }
    }

    /**
     * What a query costs, which this suite had never counted.
     *
     * <p>Writes were counted and idle ticks were counted; the number that decides whether search is
     * affordable was not. Three cases, because they are three different prices and conflating them is how
     * the first version of this test went wrong — it called a node "cold" while another node was still the
     * live owner, so it measured forwarding and reported it as the cost of opening a shard.
     *
     * <ol>
     * <li><b>Warm</b> — the shards are open here. Local Lucene; the object store should not be touched
     *     for data at all.</li>
     * <li><b>Forwarded</b> — someone else owns them. Per shard: find the owner, find its lease, ask it.</li>
     * <li><b>Cold reader</b> — nobody owns them, so this node opens each shard from the published commit.
     *     This is the price of the first query after a scale-to-zero, and until now it was a guess.</li>
     * </ol>
     */
    /**
     * What an aggregation costs, against what the same search costs without one.
     *
     * <p>Aggregations were the last thing the search surface refused, and the reason it refused was that
     * combining shards is a reduce rather than a concatenation. That reduce happens on the coordinating
     * node, over answers the shards had to compute anyway — so the claim worth checking is that it adds
     * <em>no object-store requests at all</em>: a terms aggregation reads the same segments the query
     * reads, and the buckets are built in memory from them.
     *
     * <p>Measured against the same search without the aggregation, on the same data, because "42 requests"
     * means nothing on its own and "the same 42" means everything.
     */
    public void testWhatAnAggregationCosts() throws Exception {
        for (boolean onBucket : new boolean[] { false, true }) {
            if (onBucket) {
                assumeEndpoint();
            }
            final CountingBlobStore store = onBucket ? bucket() : filesystem();
            final String label = onBucket ? "s3" : "fs";
            final int shards = 3;
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
            plane.createIndex(
                new IndexDescriptor(
                    "aggs",
                    "uuid-aggs-0000000000",
                    shards,
                    "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"},\"colour\":{\"type\":\"keyword\"}}}",
                    null
                )
            );

            try (ServerlessNode writer = new ServerlessNode(nodeSettings("cost-aggs-" + label))) {
                writer.start();
                writer.setMetadataPlane(plane);
                final BackgroundReconciler loop = new BackgroundReconciler(writer, plane);
                for (int shard = 0; shard < shards; shard++) {
                    loop.want("aggs", shard);
                }
                loop.tick(System.currentTimeMillis());
                for (int i = 0; i < 60; i++) {
                    writer.index(
                        shardOf(writer, "aggs", i),
                        String.valueOf(i),
                        "{\"msg\":\"aggcost\",\"n\":" + i + ",\"colour\":\"c" + (i % 4) + "\"}"
                    );
                }
                loop.tick(System.currentTimeMillis());

                store.reset();
                long startedAt = System.nanoTime();
                final Response plain = search(writer, "/aggs/_search?q=msg:aggcost&size=0");
                final long plainMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
                final long plainRequests = store.impliedS3Requests();
                assertEquals(plain.body(), 200, plain.status());
                assertTrue("the plain search must cover every shard: " + plain.body(), plain.body().contains("\"complete\":true"));

                store.reset();
                startedAt = System.nanoTime();
                final Response aggregated = post(
                    writer,
                    "/aggs/_search",
                    "{\"size\":0,\"query\":{\"match\":{\"msg\":\"aggcost\"}},"
                        + "\"aggs\":{\"by_colour\":{\"terms\":{\"field\":\"colour\"}},\"total\":{\"sum\":{\"field\":\"n\"}}}}"
                );
                final long aggregatedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
                final long aggregatedRequests = store.impliedS3Requests();
                assertEquals(aggregated.body(), 200, aggregated.status());
                assertTrue("the aggregation must cover every shard: " + aggregated.body(), aggregated.body().contains("\"complete\":true"));
                assertTrue("and must have aggregated something: " + aggregated.body(), aggregated.body().contains("by_colour"));

                logger.info(
                    "cost[{}]: over {} locally-held shards, search -> {} requests {}ms; the same search with two "
                        + "aggregations -> {} requests {}ms  [{}]",
                    label,
                    shards,
                    plainRequests,
                    plainMillis,
                    aggregatedRequests,
                    aggregatedMillis,
                    store.breakdown()
                );

                // The claim. A reduce runs on the coordinating node over answers the shards computed from
                // segments they were reading anyway, so it must add nothing to what the deployment pays
                // its object store.
                assertEquals(
                    "on " + label + ", aggregating must not cost object-store requests beyond the search itself",
                    plainRequests,
                    aggregatedRequests
                );
                assertEquals("and must read no data blobs, having held the shards already", 0, store.blobReads());
            }
        }
    }

    public void testWhatASearchCosts() throws Exception {
        for (boolean onBucket : new boolean[] { false, true }) {
            if (onBucket) {
                assumeEndpoint();
            }
            final CountingBlobStore store = onBucket ? bucket() : filesystem();
            final String label = onBucket ? "s3" : "fs";
            final int shards = 3;
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
            plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", shards, MAPPING, null));

            final ServerlessNode writer = new ServerlessNode(nodeSettings("cost-search-" + label));
            writer.start();
            writer.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(writer, plane);
            for (int shard = 0; shard < shards; shard++) {
                loop.want("alpha", shard);
            }
            loop.tick(System.currentTimeMillis());
            assertEquals("the writer must hold every shard", shards, writer.reconciler().openShards().size());
            for (int i = 0; i < 60; i++) {
                writer.index(shardOf(writer, "alpha", i), String.valueOf(i), "{\"msg\":\"searchcost\",\"n\":" + i + "}");
            }
            loop.tick(System.currentTimeMillis());   // publish, so a reader has something to open

            // 1. Warm.
            store.reset();
            long startedAt = System.nanoTime();
            final Response warm = search(writer, "/alpha/_search?q=msg:searchcost&size=10");
            final long warmMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
            final long warmRequests = store.impliedS3Requests();
            final long warmBlobReads = store.blobReads();
            assertEquals("the warm search should have been answered: " + warm.body(), 200, warm.status());
            assertTrue(
                "the warm search must cover every shard, or this measures the cost of not answering: " + warm.body(),
                warm.body().contains("\"complete\":true")
            );
            logger.info(
                "cost[{}]: warm search over {} locally-held shards -> {} requests, {}ms  [{}]",
                label,
                shards,
                warmRequests,
                warmMillis,
                store.breakdown()
            );
            assertEquals(
                "on " + label + ", a query against shards this node already holds must not fetch data from the object store",
                0,
                warmBlobReads
            );

            // 2. Forwarded: a second node while the writer still owns everything.
            try (ServerlessNode idle = new ServerlessNode(nodeSettings("cost-search-fwd-" + label))) {
                idle.start();
                idle.setMetadataPlane(plane);
                store.reset();
                startedAt = System.nanoTime();
                final Response forwarded = search(idle, "/alpha/_search?q=msg:searchcost&size=10");
                final long forwardedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
                assertEquals("the forwarded search should have been answered: " + forwarded.body(), 200, forwarded.status());
                assertTrue(
                    "the forwarded search must cover every shard, or this measures the cost of not answering: " + forwarded.body(),
                    forwarded.body().contains("\"complete\":true")
                );
                logger.info(
                    "cost[{}]: forwarded search over {} shards -> {} requests ({} per shard), {}ms  [{}]",
                    label,
                    shards,
                    store.impliedS3Requests(),
                    store.impliedS3Requests() / (double) shards,
                    forwardedMillis,
                    store.breakdown()
                );
                assertEquals(
                    "on " + label + ", forwarding must not fetch data either -- the owner answers from its own copy",
                    0,
                    store.blobReads()
                );
            }

            // 3. Cold reader: nobody owns the shards at all.
            for (int shard = 0; shard < shards; shard++) {
                assertTrue(plane.heads().release("alpha", shard, writer.localNode().getId()));
            }
            writer.close();

            // The search role, because reader placement only ever chooses among nodes that have it. Without
            // it this node is not a candidate for a shard nobody owns, the fan-out finds no target, and the
            // search returns an empty but honest answer -- which the completeness assertions below catch.
            try (ServerlessNode cold = new ServerlessNode(nodeSettings("cost-search-cold-" + label, "search"))) {
                cold.start();
                cold.setMetadataPlane(plane);
                store.reset();
                startedAt = System.nanoTime();
                final Response first = search(cold, "/alpha/_search?q=msg:searchcost&size=10");
                final long coldMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
                final long coldRequests = store.impliedS3Requests();
                final long coldBlobReads = store.blobReads();
                assertEquals("the cold search should have been answered: " + first.body(), 200, first.status());
                assertTrue(
                    "the cold search must cover every shard, or this measures the cost of not answering: " + first.body(),
                    first.body().contains("\"complete\":true")
                );
                logger.info(
                    "cost[{}]: FIRST search on a node holding nothing, nobody owning -> {} requests ({} per shard), {} of them data reads, {}ms  [{}]",
                    label,
                    coldRequests,
                    coldRequests / (double) shards,
                    coldBlobReads,
                    coldMillis,
                    store.breakdown()
                );
                assertTrue(
                    "on " + label + ", opening a shard from a published commit must actually read it: " + store.breakdown(),
                    coldBlobReads > 0
                );

                // The same query again, with the shards now open here.
                store.reset();
                final Response second = search(cold, "/alpha/_search?q=msg:searchcost&size=10");
                assertEquals(200, second.status());
                assertTrue("the second search must cover every shard: " + second.body(), second.body().contains("\"complete\":true"));
                logger.info(
                    "cost[{}]: second search on the same node -> {} requests, {} of them data reads",
                    label,
                    store.impliedS3Requests(),
                    store.blobReads()
                );
                assertTrue(
                    "on "
                        + label
                        + ", the first query pays for opening and the second must not: "
                        + coldRequests
                        + " then "
                        + store.impliedS3Requests(),
                    store.impliedS3Requests() < coldRequests
                );
            }
        }
    }

    private static ShardId shardOf(ServerlessNode node, String index, int i) {
        final String id = String.valueOf(i);
        for (ShardId shardId : node.reconciler().openShards()) {
            if (shardId.getIndexName().equals(index)
                && shardId.id() == Math.floorMod(org.opensearch.cluster.routing.Murmur3HashFunction.hash(id), 3)) {
                return shardId;
            }
        }
        throw new AssertionError("no open shard for " + id);
    }

    /** One response, status and body. */
    private record Response(int status, String body) {
    }

    private static Response post(ServerlessNode node, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(120))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, java.nio.charset.StandardCharsets.UTF_8))
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }

    private static Response search(ServerlessNode node, String path) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(120))
                .header("Content-Type", "application/json")
                .GET()
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
