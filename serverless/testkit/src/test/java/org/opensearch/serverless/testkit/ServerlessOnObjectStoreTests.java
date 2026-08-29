/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.ReconcileScheduler;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.serverless.store.ObjectStores;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * The shell itself, on an object store.
 *
 * <p>R11 established what an object store must provide and that MinIO provides it. That is a statement
 * about the store, not about the shell: every node this project has ever run has read and written a local
 * directory, so "the design works on object storage" rested on the two halves never having been put
 * together. Segments, the write-ahead log, shard-heads, node leases and index descriptors all move over
 * HTTP here.
 *
 * <p>What that exposes that a filesystem cannot: real latency on every register operation, a listing that
 * is a paginated API call rather than a directory scan, and reads that are HTTP range requests against a
 * server rather than seeks in a file.
 *
 * <p><b>Skipped unless an endpoint is there</b>, loudly, with the command to start one.
 */
// The AWS SDK keeps event-loop threads alive past the test; they belong to the client, not to us.
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ServerlessOnObjectStoreTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";
    private static final String DEFAULT_ENDPOINT = "http://127.0.0.1:9000";

    private String endpoint() {
        return System.getProperty(MinioBlobContainerConformanceTests.ENDPOINT, DEFAULT_ENDPOINT);
    }

    private Settings s3Settings(String name, String bucket) {
        final MockSecureSettings secure = new MockSecureSettings();
        secure.setString("s3.client.default.access_key", "minioadmin");
        secure.setString("s3.client.default.secret_key", "minioadmin");
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-on-s3")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(ObjectStores.TYPE, "s3")
            .put(ObjectStores.BUCKET, bucket)
            .put(ObjectStores.ENDPOINT, endpoint())
            // MinIO is addressed by path. Without this the SDK asks DNS for bucket.127.0.0.1 and nothing
            // resolves -- which surfaces as a timeout rather than as a configuration error.
            .put(ObjectStores.PATH_STYLE, true)
            .put(ObjectStores.REGION, "us-east-1")
            .put(ServerlessBootstrap.LEASE_TTL, 3_000L)
            .setSecureSettings(secure)
            .build();
    }

    private String freshBucket() throws Exception {
        final String bucket = "shell-" + randomAlphaOfLength(12).toLowerCase(Locale.ROOT);
        // Created through the same helper R11 uses, because creating a bucket is not something the shell
        // does or should do -- a serverless node is handed one.
        org.opensearch.repositories.s3.MinioBlobStores.create(endpoint(), "minioadmin", "minioadmin", bucket, createTempDir());
        return bucket;
    }

    private void assumeEndpoint() {
        assumeTrue(
            "no S3-compatible endpoint at "
                + endpoint()
                + "; start one with: docker run -d --name serverless-minio -p 9000:9000 "
                + "-e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin quay.io/minio/minio server /data",
            reachable(endpoint())
        );
    }

    /**
     * A node boots against a bucket, creates an index, writes, and finds the document again — with
     * nothing on local disk that matters.
     */
    public void testANodeRunsWithItsDataInABucket() throws Exception {
        assumeEndpoint();
        final String bucket = freshBucket();

        try (ServerlessBootstrap boot = ServerlessBootstrap.start(s3Settings("s3-node", bucket))) {
            final var http = boot.node().boundHttpAddress().publishAddress();

            assertEquals("index creation must reach the bucket", 200, send(http, "PUT", "/alpha?shards=1", MAPPING).status());
            assertEquals(421, send(http, "PUT", "/alpha/_doc/1", "{\"msg\":\"in-a-bucket\",\"n\":1}").status());
            assertBusy(
                () -> assertFalse("the node should take the shard on its own", boot.node().reconciler().openShards().isEmpty()),
                30,
                TimeUnit.SECONDS
            );

            final Response written = send(http, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"in-a-bucket\",\"n\":1}");
            assertEquals("the write must succeed: " + written.body(), 201, written.status());

            final Response found = send(http, "GET", "/alpha/_search?q=msg:in-a-bucket", null);
            assertEquals(200, found.status());
            assertTrue("the document must come back: " + found.body(), found.body().contains("in-a-bucket"));

            // Everything the node depends on is now in S3, not on its disk.
            assertBusy(
                () -> assertTrue(
                    "the node must publish its commit to the bucket",
                    boot.plane().segmentPublisher("alpha", 0).readManifest().isPresent()
                ),
                30,
                TimeUnit.SECONDS
            );
            assertTrue("and hold its lease there", boot.plane().membership().read(boot.node().localNode().getId()).isPresent());
        }
    }

    /**
     * The property the whole design exists for, on a real object store: a node dies and another serves
     * its data, having never touched the first node's disk.
     *
     * <p>Two nodes in one JVM but two separate {@code path.home} directories and two separate engines.
     * The successor recovers from the bucket alone — the published commit and the write-ahead log — which
     * is exactly the claim that was previously only ever demonstrated against a shared local folder.
     */
    public void testASuccessorRecoversFromTheBucketAlone() throws Exception {
        assumeEndpoint();
        final String bucket = freshBucket();

        final ServerlessBootstrap first = ServerlessBootstrap.start(s3Settings("s3-writer", bucket));
        try {
            final ShardId shardId;
            final var http = first.node().boundHttpAddress().publishAddress();
            assertEquals(200, send(http, "PUT", "/alpha?shards=1", MAPPING).status());
            assertEquals(421, send(http, "PUT", "/alpha/_doc/warm", "{\"msg\":\"survives\",\"n\":0}").status());
            assertBusy(() -> assertFalse(first.node().reconciler().openShards().isEmpty()), 30, TimeUnit.SECONDS);
            shardId = first.node().reconciler().openShards().iterator().next();

            for (int i = 1; i <= 8; i++) {
                assertEquals(
                    201,
                    send(http, "PUT", "/alpha/_doc/" + i + "?refresh=true", "{\"msg\":\"survives\",\"n\":" + i + "}").status()
                );
            }
            // Wait for the node's own edge-triggered publish rather than calling publishAll here. An
            // earlier version reached in and published by hand, which raced the scheduler doing the same
            // thing and closed the engine underneath the next write -- AlreadyClosedException on refresh.
            // Driving a running node from outside its own loop is a way to test something that cannot
            // happen.
            assertBusy(
                () -> assertTrue(
                    "the writer should publish on its own",
                    first.plane().segmentPublisher("alpha", 0).readManifest().isPresent()
                ),
                30,
                TimeUnit.SECONDS
            );

            // These land after that commit, so recovery has to compose a published commit with a log
            // rather than replay everything -- the case that actually happens, and the easy one to get
            // wrong.
            for (int i = 9; i <= 12; i++) {
                assertEquals(
                    201,
                    send(http, "PUT", "/alpha/_doc/" + i + "?refresh=true", "{\"msg\":\"survives\",\"n\":" + i + "}").status()
                );
            }

            // Not a clean handover: drop the shard and the lease the way a crash would, without letting
            // the node publish on its way out.
            first.node().releaseShard(shardId, "simulating a writer that stopped without unwinding");
            first.plane().membership().release(first.node().localNode().getId());
        } finally {
            // A clean stop, and this test says so rather than pretending otherwise: it releases the lease
            // and stops the drivers. Crash semantics -- kill -9, a lease left to lapse, a zombie
            // resuming -- are covered across real processes in ServerlessContentionTests; what is under
            // test here is that a successor can rebuild from the bucket, which is orthogonal to how its
            // predecessor stopped.
            first.close();
        }
        try (ServerlessBootstrap second = ServerlessBootstrap.start(s3Settings("s3-successor", bucket))) {
            final var http = second.node().boundHttpAddress().publishAddress();
            final BackgroundReconciler loop = second.reconciler();
            loop.want("alpha", 0);
            // Retried, because the predecessor's shard-head carries its own expiry and a successor may not
            // steal a lease that has not lapsed yet. Waiting it out is the design working, not a delay to
            // engineer around.
            assertBusy(() -> {
                loop.tick(System.currentTimeMillis());
                assertFalse("the successor must take the shard", second.node().reconciler().openShards().isEmpty());
            }, 60, TimeUnit.SECONDS);
            send(http, "POST", "/alpha/_refresh", null);

            final Response found = send(http, "GET", "/alpha/_search?q=msg:survives&size=50", null);
            assertEquals(200, found.status());
            int hits = 0;
            int at = found.body().indexOf("\"_id\"");
            while (at >= 0) {
                hits++;
                at = found.body().indexOf("\"_id\"", at + 1);
            }
            logger.info("s3 failover: successor recovered {} documents from the bucket", hits);

            // Twelve, not thirteen. The "warm" write was refused with a 421 because no node owned the
            // shard yet, so it was never acknowledged -- and a write nobody was told succeeded must not
            // reappear. Durability is a promise about acknowledged writes in both directions, and the
            // first version of this assertion expected thirteen and was simply wrong about which writes
            // the system had promised.
            assertEquals("every acknowledged write must be recoverable from the bucket alone: " + found.body(), 12, hits);
            assertFalse(
                "a write that was refused must not come back from the dead: " + found.body(),
                found.body().contains("\"_id\":\"warm\"")
            );
        }
    }

    /** The scheduler runs unattended against S3 too, where every renewal is an HTTP round trip. */
    public void testTheDriversRunAgainstABucket() throws Exception {
        assumeEndpoint();
        final String bucket = freshBucket();

        try (ServerlessBootstrap boot = ServerlessBootstrap.start(s3Settings("s3-drivers", bucket))) {
            final ReconcileScheduler scheduler = boot.scheduler();
            assertBusy(() -> assertTrue("the renewal timer must run against S3", scheduler.counts().renewals() >= 1), 60, TimeUnit.SECONDS);
            assertBusy(
                () -> assertTrue(
                    "and the node's lease must be readable from the bucket",
                    boot.plane().membership().read(boot.node().localNode().getId()).isPresent()
                ),
                60,
                TimeUnit.SECONDS
            );
            logger.info("s3 drivers: {}", scheduler.counts());
        }
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

    private record Response(int status, String body) {
    }

    private static Response send(org.opensearch.core.common.transport.TransportAddress address, String method, String path, String body)
        throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build()) {
            final HttpRequest.BodyPublisher payload = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .method(method, payload)
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }

    /**
     * Fencing, on the transport where the fence is an HTTP conditional write.
     *
     * <p>Both halves of §9.6 have only ever been demonstrated on a filesystem, where a compare-and-swap is
     * a rename and a segment upload is a memcpy. The claim the project actually makes is about an object
     * store: that a writer which lost its shard cannot make anyone believe its commit. That rests on
     * {@code compareAndSwapRegister} being linearizable over HTTP, which a filesystem cannot demonstrate —
     * and the precedent for the transport revealing what disk hides is {@code IndexInputStream}, which had
     * no mark support, so publishing a segment was impossible on S3 and perfect on disk.
     *
     * <p>The race window is held open deliberately rather than raced for. See
     * {@link ServerlessFailoverTests#testAWriterThatReadTheManifestBeforeItsSuccessorPublishedIsStillFenced}
     * for why the term check cannot cover this case and the swap must.
     */
    public void testAStaleWriterIsFencedByAConditionalWriteOverHttp() throws Exception {
        assumeEndpoint();
        final String bucket = freshBucket();
        final long ttl = 3_000L;
        final java.util.concurrent.atomic.AtomicLong clock = new java.util.concurrent.atomic.AtomicLong(1_000L);
        final PausingBlobStore store = new PausingBlobStore(
            org.opensearch.repositories.s3.MinioBlobStores.create(endpoint(), "minioadmin", "minioadmin", bucket, createTempDir())
        );
        final var plane = new org.opensearch.serverless.metadata.MetadataPlane(
            store,
            org.opensearch.common.blobstore.BlobPath.cleanPath(),
            clock::get,
            ttl
        );
        plane.createIndex(new org.opensearch.serverless.cluster.IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            org.opensearch.serverless.shell.ServerlessNode loser = new org.opensearch.serverless.shell.ServerlessNode(
                s3Settings("fence-loser", bucket)
            );
            org.opensearch.serverless.shell.ServerlessNode winner = new org.opensearch.serverless.shell.ServerlessNode(
                s3Settings("fence-winner", bucket)
            )
        ) {
            loser.start();
            winner.start();

            final ShardId onLoser = loser.activateWriter(plane, "alpha", 0).orElseThrow();
            final long loserTerm = plane.heads().read("alpha", 0).orElseThrow().term();
            ShardOps.indexDoc(loser.reconciler().shard(onLoser), "1", "{\"msg\":\"from the loser\",\"n\":1}");
            loser.reconciler().shard(onLoser).refresh("fence");

            // The loser begins publishing and parks inside the upload, holding a manifest it read while
            // it was still, genuinely, the newest term.
            store.pauseNextSegmentWrite();
            final java.util.concurrent.atomic.AtomicReference<Throwable> thrown = new java.util.concurrent.atomic.AtomicReference<>();
            final Thread publishing = new Thread(() -> {
                try {
                    loser.publishShard(onLoser, loserTerm);
                } catch (Throwable t) {
                    thrown.set(t);
                }
            }, "fence-loser-publish");
            publishing.start();
            assertTrue("the loser never reached the upload, so no window was opened", store.awaitPaused(2, TimeUnit.MINUTES));

            clock.set(1_000L + ttl);
            final ShardId onWinner = winner.activateWriter(plane, "alpha", 0).orElseThrow();
            final long winnerTerm = plane.heads().read("alpha", 0).orElseThrow().term();
            assertTrue("the successor must hold a higher term", winnerTerm > loserTerm);
            ShardOps.indexDoc(winner.reconciler().shard(onWinner), "2", "{\"msg\":\"from the winner\",\"n\":2}");
            winner.reconciler().shard(onWinner).refresh("fence");
            winner.publishShard(onWinner, winnerTerm);

            store.release();
            publishing.join(TimeUnit.MINUTES.toMillis(3));
            assertNotNull("the loser's publish must be refused, on a bucket as on a disk", thrown.get());
            assertTrue(
                "and refused as a stale writer: " + thrown.get(),
                thrown.get() instanceof org.opensearch.serverless.store.StaleWriterException
            );
            // The conditional write, not the term check. Over HTTP that is a GET for the ETag and a PUT
            // carrying it, which is the pair of requests this whole design rests on.
            assertTrue(
                "the conditional write must be what refused it: " + thrown.get().getMessage(),
                thrown.get().getMessage().contains("changed concurrently")
            );
            assertEquals(
                "the manifest in the bucket must still be the winner's",
                winnerTerm,
                plane.segmentPublisher("alpha", 0).readManifest().orElseThrow().term()
            );
        }

        // Read back out of the bucket by a node that never held the shard: a lost swap would not look
        // like an error here, it would look like the winner's document having never been written.
        try (
            org.opensearch.serverless.shell.ServerlessNode reader = new org.opensearch.serverless.shell.ServerlessNode(
                s3Settings("fence-verify", bucket)
            )
        ) {
            reader.start();
            final ShardId shardId = reader.serveAsReader(plane, "alpha", 0);
            assertEquals("the winner's document must survive", 1L, ShardOps.hits(reader.searchService(), shardId, "msg", "winner"));
        }
    }
}
