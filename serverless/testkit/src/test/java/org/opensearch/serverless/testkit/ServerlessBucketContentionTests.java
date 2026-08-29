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
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.serverless.store.ObjectStores;
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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Real processes contending over a real object store.
 *
 * <p>The last of the halves that had never been put together. Ownership, fencing and recovery were tested
 * across processes on a local filesystem; the object store was tested on its own. On a filesystem a
 * compare-and-swap is instant and the window between reading a shard-head and writing to it is
 * microseconds. Over HTTP it is milliseconds, and that window is precisely where two nodes can both
 * believe they own a shard. Every correctness claim this project makes has been measured on the version
 * of the world where that gap barely exists.
 *
 * <p>It also exercises the part of the design a filesystem hides entirely: a crashed node's data is
 * recovered by a process that has never seen its disk, because there is nothing on its disk to see.
 *
 * <p><b>D5 is finally retired for these tests.</b> Everything here goes over HTTP to an S3 API.
 */
// The AWS SDK keeps event-loop threads alive past the test; they belong to the client, not to us.
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ServerlessBucketContentionTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";
    /**
     * Long enough that a busy machine does not expire a healthy node's lease.
     *
     * <p>Four seconds was too short and the failures it produced were not bugs: under a full concurrent
     * build a node misses a renewal, its lease genuinely lapses, and its peers correctly stop believing
     * in it. Over HTTP to a bucket every renewal is a round trip, so the margin that was already thin on
     * a filesystem is thinner here.
     *
     * <p>Worth stating plainly rather than only fixing: <b>a node that cannot renew within its TTL loses
     * its shards</b>, and a TTL chosen to make tests finish quickly puts the system in a regime where it
     * legitimately thrashes. The production default is 30 seconds for this reason.
     */
    private static final String TTL = "15000";
    private static final String DEFAULT_ENDPOINT = "http://127.0.0.1:9000";

    private String endpoint() {
        return System.getProperty(MinioBlobContainerConformanceTests.ENDPOINT, DEFAULT_ENDPOINT);
    }

    private void assumeEndpoint() {
        assumeTrue(
            "no S3-compatible endpoint at "
                + endpoint()
                + "; start one with: docker run -d --name serverless-minio -p 9000:9000 "
                + "-e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin "
                + "-e \"MINIO_KMS_SECRET_KEY=serverless-key:$(openssl rand -base64 32)\" quay.io/minio/minio server /data",
            reachable(endpoint())
        );
    }

    /** Settings that send a forked node's data to a bucket instead of a directory. */
    private Map<String, String> bucketSettings(String bucket) {
        return Map.of(
            ServerlessBootstrap.LEASE_TTL,
            TTL,
            ObjectStores.TYPE,
            "s3",
            ObjectStores.BUCKET,
            bucket,
            ObjectStores.ENDPOINT,
            endpoint(),
            ObjectStores.PATH_STYLE,
            "true",
            ObjectStores.REGION,
            "us-east-1"
        );
    }

    /** Forks a node whose credentials come from a keystore, as a deployed node's would. */
    private NodeProcess bucketNode(String name, String bucket) throws Exception {
        final Path home = createTempDir();
        NodeProcess.writeKeystore(home, Map.of("s3.client.default.access_key", "minioadmin", "s3.client.default.secret_key", "minioadmin"));
        return NodeProcess.start(name, null, home, bucketSettings(bucket));
    }

    private String freshBucket() throws Exception {
        final String bucket = "contend-" + randomAlphaOfLength(12).toLowerCase(Locale.ROOT);
        org.opensearch.repositories.s3.MinioBlobStores.create(endpoint(), "minioadmin", "minioadmin", bucket, createTempDir());
        return bucket;
    }

    /**
     * Two forked nodes racing for one shard, arbitrated by a compare-and-swap that is now an HTTP request.
     *
     * <p>The same property the filesystem version asserts, on the transport where it can actually fail:
     * the read-then-write of a shard-head is no longer effectively atomic, and if the conditional write
     * were not doing real work both nodes would open the shard and split the writes between them.
     */
    public void testTwoProcessesRacingOverHttpProduceExactlyOneOwner() throws Exception {
        assumeEndpoint();
        final String bucket = freshBucket();

        try (NodeProcess a = bucketNode("bucket-a", bucket); NodeProcess b = bucketNode("bucket-b", bucket)) {
            assertEquals(200, send(a, "PUT", "/alpha?shards=1", MAPPING).status());

            final int perNode = 10;
            writeConcurrently(List.of(a, b), perNode, "contended");

            final MetadataPlane plane = plane(bucket);
            final var head = plane.heads().read("alpha", 0);
            assertTrue("the shard must have an owner", head.isPresent());
            final String owner = head.get().ownerNodeId();
            logger.info("bucket race: owner is {} (a={}, b={})", owner, a.nodeId(), b.nodeId());
            assertTrue("exactly one of the two may own it: " + owner, owner.equals(a.nodeId()) ^ owner.equals(b.nodeId()));

            send(a, "POST", "/alpha/_refresh", null);
            for (NodeProcess reader : List.of(a, b)) {
                assertBusy(() -> {
                    final Response found = search(reader, "msg:contended");
                    assertEquals(200, found.status());
                    assertEquals(
                        "no write may be lost to arbitration over HTTP; " + reader.name() + " returned: " + found.body(),
                        2 * perNode,
                        hitCount(found.body())
                    );
                }, 90, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * A node is killed outright and another rebuilds its shard from the bucket.
     *
     * <p>This is the design's whole claim, and the first time it has been made where it means what it
     * says. On a shared directory a successor reads files its predecessor left on disk; here the dead
     * node's disk is gone and irrelevant, and everything recovered — the published commit and the
     * write-ahead log — came out of object storage.
     */
    public void testAKilledNodesDataIsRebuiltFromTheBucketByANodeThatNeverSawItsDisk() throws Exception {
        assumeEndpoint();
        final String bucket = freshBucket();

        try (NodeProcess b = bucketNode("bucket-survivor", bucket)) {
            final NodeProcess a = bucketNode("bucket-doomed", bucket);
            final String deadNodeId;
            try {
                assertEquals(200, send(a, "PUT", "/alpha?shards=1", MAPPING).status());
                for (int i = 1; i <= 10; i++) {
                    assertEquals(201, putDoc(a, String.valueOf(i), "{\"msg\":\"rebuilt\",\"n\":" + i + "}").status());
                }
                deadNodeId = plane(bucket).heads().read("alpha", 0).orElseThrow().ownerNodeId();
                assertEquals("a should have won the shard, having written first", a.nodeId(), deadNodeId);
            } finally {
                a.killHard();
            }
            assertFalse("the process must actually be dead", a.alive());

            // The negative control: a killed node cannot have released anything, so what follows is
            // driven by the lease lapsing and not by a tidy shutdown.
            assertTrue(
                "a hard-killed node must leave its lease behind in the bucket",
                plane(bucket).membership().read(deadNodeId).isPresent()
            );

            assertEquals(201, putDoc(b, "11", "{\"msg\":\"rebuilt\",\"n\":11}").status());
            assertEquals(
                "the survivor must now own the shard",
                b.nodeId(),
                plane(bucket).heads().read("alpha", 0).orElseThrow().ownerNodeId()
            );

            send(b, "POST", "/alpha/_refresh", null);
            final Response found = search(b, "msg:rebuilt");
            assertEquals(200, found.status());
            assertEquals("every acknowledged write must be recoverable from the bucket alone: " + found.body(), 11, hitCount(found.body()));
        }
    }

    /**
     * Three nodes, four shards, all on one bucket, each capped so the shards cannot land on one node.
     *
     * <p>A search here is a fan-out across processes over a shard set whose segments live in object
     * storage, which is the arrangement the whole design is for and the one nothing had ever run.
     */
    public void testAFleetOnOneBucketAnswersForTheWholeIndex() throws Exception {
        assumeEndpoint();
        final String bucket = freshBucket();

        final List<NodeProcess> fleet = new ArrayList<>();
        try {
            for (String name : List.of("fleet-s3-a", "fleet-s3-b", "fleet-s3-c")) {
                final Path home = createTempDir();
                NodeProcess.writeKeystore(
                    home,
                    Map.of("s3.client.default.access_key", "minioadmin", "s3.client.default.secret_key", "minioadmin")
                );
                final Map<String, String> settings = new java.util.LinkedHashMap<>(bucketSettings(bucket));
                settings.put(ServerlessBootstrap.MAX_SHARDS, "2");
                fleet.add(NodeProcess.start(name, null, home, settings));
            }

            assertEquals(200, send(fleet.get(0), "PUT", "/alpha?shards=4", MAPPING).status());
            final int perNode = 8;
            writeConcurrently(fleet, perNode, "fleet");

            final Set<String> owners = new LinkedHashSet<>();
            for (int shard = 0; shard < 4; shard++) {
                owners.add(plane(bucket).heads().read("alpha", shard).orElseThrow().ownerNodeId());
            }
            logger.info("bucket fleet: 4 shards held by {} distinct nodes", owners.size());
            assertTrue("the cap must have spread the shards, or this tests nothing: " + owners, owners.size() > 1);

            for (NodeProcess node : fleet) {
                send(node, "POST", "/alpha/_refresh", null);
            }
            for (NodeProcess reader : fleet) {
                // Converges rather than instantly correct, for the same reason as the filesystem fleet
                // test: a shard whose owner's lease lapsed is re-acquired and served again. The deadline
                // is relaxed; the property is not, since a fan-out that never completes still times out.
                assertBusy(() -> {
                    final Response found = search(reader, "msg:fleet");
                    assertEquals(200, found.status());
                    assertTrue("a partial answer must not be called complete: " + found.body(), found.body().contains("\"complete\":true"));
                    assertEquals(
                        "every node must answer for the whole index; " + reader.name() + " returned: " + found.body(),
                        fleet.size() * perNode,
                        hitCount(found.body())
                    );
                }, 90, TimeUnit.SECONDS);
            }
        } finally {
            for (NodeProcess node : fleet) {
                node.close();
            }
        }
    }

    // ---------------------------------------------------------------- helpers

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
            thread.join(TimeUnit.MINUTES.toMillis(5));
        }
        assertEquals("every write must be accepted by one node or another: " + failures, List.of(), failures);
    }

    private MetadataPlane plane(String bucket) throws Exception {
        return new MetadataPlane(
            org.opensearch.repositories.s3.MinioBlobStores.create(endpoint(), "minioadmin", "minioadmin", bucket, createTempDir()),
            BlobPath.cleanPath(),
            System::currentTimeMillis,
            Long.parseLong(TTL)
        );
    }

    private Response putDoc(NodeProcess node, String id, String source) throws Exception {
        Response last = null;
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
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
        int count = 0;
        int at = body.indexOf("\"_id\"");
        while (at >= 0) {
            count++;
            at = body.indexOf("\"_id\"", at + 1);
        }
        return count;
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

    private static Response send(NodeProcess node, String method, String path, String body) throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build()) {
            final HttpRequest.BodyPublisher payload = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + node.http() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .method(method, payload)
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
