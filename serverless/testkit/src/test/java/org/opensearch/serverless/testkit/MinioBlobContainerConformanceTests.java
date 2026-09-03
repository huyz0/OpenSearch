/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.repositories.s3.MinioBlobStores;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * R11 against a real S3 API.
 *
 * <p>Every claim this project makes about durability, fencing and ownership rests on properties of an
 * object store, and until now every one of them had only ever been checked against a local filesystem —
 * where they hold almost by construction, because {@code FsBlobContainer} implements compare-and-swap
 * with real filesystem atomicity and ranged reads with a file channel. §12.1 has carried "M12: suite
 * written, not run on a provider" ever since.
 *
 * <p>MinIO is not S3. It is an S3-compatible implementation, and the properties that matter most —
 * conditional-write linearizability under contention, listing bounds, read-after-write — are exactly the
 * ones where a compatible implementation may differ from the real thing. What this establishes is that
 * the suite runs against a genuine S3 <em>API</em>, through the same {@code S3BlobContainer} the shell
 * would use in production, and that the assumptions are not filesystem artefacts. It does not establish
 * anything about AWS S3, GCS, or R2.
 *
 * <p><b>Skipped unless an endpoint is there.</b> Assumed rather than failed, because a developer without
 * a container running should not get a red build for it — but the skip is loud, and
 * {@code r11-conformance.md} says how to start one.
 */
// The AWS SDK keeps event-loop threads alive past the test; they belong to the client, not to us.
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class MinioBlobContainerConformanceTests extends BlobContainerConformanceTestCase {

    /** Where to find an S3-compatible endpoint. */
    public static final String ENDPOINT = "tests.serverless.s3.endpoint";

    private static final String DEFAULT_ENDPOINT = "http://127.0.0.1:9000";

    /** The endpoint this target uses, so a second S3-compatible implementation can subclass. */
    protected String endpoint() {
        return System.getProperty(ENDPOINT, DEFAULT_ENDPOINT);
    }

    /** How to start the endpoint, quoted in the skip when it is not there. */
    protected String howToStart() {
        return "docker run -d --name serverless-minio -p 9000:9000 "
            + "-e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin quay.io/minio/minio server /data";
    }

    /** The access key for this target. */
    protected String accessKey() {
        return "minioadmin";
    }

    /** The secret key for this target. */
    protected String secretKey() {
        return "minioadmin";
    }

    @Override
    protected BlobContainer newContainer() throws Exception {
        final String endpoint = endpoint();
        assumeTrue("no S3-compatible endpoint at " + endpoint + "; start one with: " + howToStart(), reachable(endpoint));

        // A bucket per container. The suite deletes what it writes, but a shared bucket would let one run's
        // leftovers decide another run's listing assertions, and a listing assertion that depends on
        // history is not an assertion.
        final String bucket = "r11-" + randomAlphaOfLength(12).toLowerCase(java.util.Locale.ROOT);
        final var store = MinioBlobStores.create(endpoint, accessKey(), secretKey(), bucket, createTempDir());
        created.add(bucket);
        return store.blobContainer(BlobPath.cleanPath().add("conformance"));
    }

    /** Buckets this test made, so the next run does not inherit them. */
    private final java.util.List<String> created = new java.util.ArrayList<>();

    /**
     * Removes the buckets this test created.
     *
     * <p><b>Why this was not here from the start, and what it cost.</b> A bucket was made per container and
     * none was ever removed. On MinIO an abandoned empty bucket costs nothing, so nothing showed. On
     * SeaweedFS a bucket is a <em>collection</em>, every collection claims volumes from a finite pool, and
     * this suite makes one per test — so a few days of runs exhausted the volume server and every test began
     * failing against a store that was behaving perfectly. That is the worst failure this suite can produce:
     * it exists to tell a real deviation from a fake one, and it was manufacturing fake ones.
     *
     * @throws Exception if the endpoint is unreachable, which is not a failure
     */
    @org.junit.After
    public void removeBucketsThisTestMade() throws Exception {
        for (String bucket : created) {
            MinioBlobStores.deleteBucket(endpoint(), accessKey(), secretKey(), bucket, createTempDir());
        }
        created.clear();
    }

    /**
     * The suite must discriminate <em>on this transport</em>, not merely on a filesystem.
     *
     * <p>{@code ConformanceSuiteSelfTests} already plants these defects over {@code FsBlobContainer},
     * which shows the assertions are sound. It does not show they still bite once the same calls are
     * going over HTTP to a real S3 API, where an error could just as easily surface as an exception, a
     * retry, or a swallowed 412. Seven green tests against a live endpoint mean nothing without this: the
     * question "would this run have failed if MinIO were wrong?" has to be answered by making it wrong.
     */
    public void testTheSuiteWouldCatchACompareAndSwapThatAlwaysWinsOnThisTransport() throws Exception {
        final BlobContainer honest = newContainer();
        final BlobContainer liar = new DelegatingBlobContainer(honest) {
            @Override
            public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue) {
                return BlobRegisterCasResult.applied(expectedGeneration + 1);
            }
        };

        liar.createRegisterIfAbsent("head", new BytesArray("initial".getBytes(StandardCharsets.UTF_8)));
        final long generation = liar.readRegister("head").orElseThrow().generation();

        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger winners = new AtomicInteger();
        final Thread[] threads = new Thread[8];
        for (int i = 0; i < threads.length; i++) {
            final int id = i;
            threads[i] = new Thread(() -> {
                try {
                    start.await();
                    if (liar.compareAndSwapRegister("head", generation, new BytesArray(("w" + id).getBytes(StandardCharsets.UTF_8)))
                        .applied()) {
                        winners.incrementAndGet();
                    }
                } catch (Exception e) {
                    // A thrown exception is not a win, which is the point of counting rather than asserting here.
                }
            });
            threads[i].start();
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        logger.info("r11 self-check on minio: a lying compare-and-swap produced {} winners", winners.get());
        assertTrue(
            "the suite's one-winner assertion must be able to fail against this endpoint; it saw " + winners.get(),
            winners.get() > 1
        );
    }

    /** And the same for ranged reads, which is the other property a compatible implementation may fudge. */
    public void testTheSuiteWouldCatchARangedReadThatIgnoresTheRangeOnThisTransport() throws Exception {
        final BlobContainer honest = newContainer();
        final BlobContainer liar = new DelegatingBlobContainer(honest) {
            @Override
            public java.io.InputStream readBlob(String blobName, long position, long length) throws java.io.IOException {
                return super.readBlob(blobName);
            }
        };

        final byte[] payload = new byte[4096];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }
        liar.writeBlob("blob", new java.io.ByteArrayInputStream(payload), payload.length, true);

        try (var in = liar.readBlob("blob", 100, 64)) {
            final byte[] got = in.readAllBytes();
            logger.info("r11 self-check on minio: a range-ignoring read returned {} bytes for a 64-byte request", got.length);
            assertNotEquals("the suite's exact-length assertion must be able to fail against this endpoint", 64, got.length);
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

    /**
     * And the linearizability check must discriminate on this transport too.
     *
     * <p>The concurrent history running green against a live endpoint says nothing until somebody has
     * shown it would have gone red had the endpoint been wrong — and shown it <em>here</em>, over HTTP,
     * through the S3 client's retries and error handling, rather than only over a filesystem. A stale read
     * is the deviation chosen because it is the one a real provider is most likely to have and the one no
     * other property in this suite asks about.
     *
     * @throws Exception if the endpoint cannot be reached
     */
    public void testTheLinearizabilityCheckWouldCatchAStaleReadOnThisTransport() throws Exception {
        final BlobContainer honest = newContainer();
        final var broken = new MisbehavingBlobContainer(honest);
        broken.createRegisterIfAbsent("stale", new BytesArray("v0".getBytes(StandardCharsets.UTF_8)));
        final long initial = broken.readRegister("stale").orElseThrow().generation();

        final RegisterHistory history = new RegisterHistory();
        long invoked = history.invoke();
        final var applied = broken.compareAndSwapRegister("stale", initial, new BytesArray("v1".getBytes(StandardCharsets.UTF_8)));
        history.completed(new RegisterHistory.Cas(initial, "v1", applied.applied(), applied.currentGeneration()), invoked);
        assertTrue("the write must really have happened", applied.applied());

        broken.staleReadEvery(1);
        invoked = history.invoke();
        final var seen = broken.readRegister("stale").orElseThrow();
        history.completed(new RegisterHistory.Read(seen.generation(), seen.value().utf8ToString()), invoked);
        assertEquals("this test is meaningless unless the read really was stale", "v0", seen.value().utf8ToString());

        assertFalse(
            "a stale read over this transport must be caught, or the green run above proves nothing",
            LinearizabilityChecker.check(history.entries(), initial, "v0").linearizable()
        );
    }
}
