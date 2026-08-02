/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * The two preconditions the whole uniqueness argument rests on, checked against an independent S3
 * implementation rather than against a fixture this project wrote.
 *
 * <h2>Why a fixture is not enough here</h2>
 *
 * {@code S3BlobStoreRepositoryTests} already proves server-side enforcement, and its own javadoc explains
 * why that matters: a unit test asserting {@code ifNoneMatch} is set on the request "proves the
 * request-construction code is right and nothing else... if the header were ignored, every concurrent
 * creator would win and the unit test would still pass".
 *
 * <p>But it proves it against {@code fixture.s3.S3HttpHandler}, which this project extended to enforce
 * If-Match and If-None-Match. A fixture enforces what whoever wrote it believed the semantics to be, so
 * testing against it confirms the code agrees with our own reading of S3 rather than with S3. If that
 * reading is wrong, both sides are wrong together and every test passes.
 *
 * <p>MinIO is an independent implementation of the same API. Agreement between the production code, our
 * fixture, and MinIO is worth considerably more than agreement between the first two.
 *
 * <h2>What this covers, and what it does not</h2>
 *
 * Two behaviours, both load-bearing for {@code createRegisterIfAbsent} and {@code compareAndSwapRegister}:
 * a conditional put with {@code If-None-Match: *} must fail once the key exists, and one with
 * {@code If-Match: <etag>} must fail once the etag has moved. Both must fail with 412 specifically, because
 * that status is what the register code translates into a lost race rather than an error.
 *
 * <p>Not covered: latency, concurrency at scale, or S3 itself. MinIO is closer to S3 than a fixture and is
 * still not S3, and this runs on localhost so it says nothing about round-trip cost.
 *
 * <p>Skipped unless {@code tests.s3.endpoint} is set, so an ordinary build is unaffected. Run with an
 * endpoint pointing at any S3-compatible server:
 * {@code -Dtests.s3.endpoint=http://localhost:19000}.
 */
@ThreadLeakFilters(filters = IdleConnectionReaperThreadFilter.class)
public class S3ConditionalWriteAgainstRealStoreTests extends OpenSearchTestCase {

    private static final String ENDPOINT_PROPERTY = "tests.s3.endpoint";
    private static final String BUCKET = "conditional-write-tests";

    private S3Client clientOrSkip() {
        String endpoint = System.getProperty(ENDPOINT_PROPERTY);
        assumeTrue("set -D" + ENDPOINT_PROPERTY + " to run this against a real S3-compatible store", endpoint != null);
        S3Client client = S3Client.builder()
            .endpointOverride(URI.create(endpoint))
            .region(Region.US_EAST_1)
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        System.getProperty("tests.s3.access_key", "access_key"),
                        System.getProperty("tests.s3.secret_key", "secret_key")
                    )
                )
            )
            // Path style, because a bucket name in the host only resolves against real S3 DNS.
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            .build();
        try {
            client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        } catch (S3Exception e) {
            // Already there, which is the normal case on a second run.
        }
        return client;
    }

    private static String put(S3Client client, String key, String body, String ifNoneMatch, String ifMatch) {
        PutObjectRequest.Builder request = PutObjectRequest.builder().bucket(BUCKET).key(key);
        if (ifNoneMatch != null) {
            request.ifNoneMatch(ifNoneMatch);
        }
        if (ifMatch != null) {
            request.ifMatch(ifMatch);
        }
        return client.putObject(request.build(), RequestBody.fromString(body)).eTag();
    }

    /**
     * A name can be taken exactly once. This is index creation's uniqueness, with no cluster manager
     * serialising anything, so if the store does not enforce it the design has no uniqueness at all.
     */
    public void testIfNoneMatchIsEnforcedByTheStore() {
        S3Client client = clientOrSkip();
        String key = "if-none-match-" + randomAlphaOfLength(8);

        put(client, key, "winner", "*", null);

        S3Exception conflict = expectThrows(S3Exception.class, () -> put(client, key, "loser", "*", null));
        assertEquals(
            "a conditional create against an existing key must be refused with 412, which is what the "
                + "register code reads as a lost race rather than an error",
            412,
            conflict.statusCode()
        );

        String survived = client.getObjectAsBytes(GetObjectRequest.builder().bucket(BUCKET).key(key).build())
            .asString(StandardCharsets.UTF_8);
        assertEquals("the first writer's value must survive, or uniqueness is cosmetic", "winner", survived);
    }

    /**
     * The compare-and-swap half. A writer holding a stale etag must lose, which is what stops two nodes
     * updating one descriptor into an order neither intended.
     */
    public void testIfMatchIsEnforcedByTheStore() {
        S3Client client = clientOrSkip();
        String key = "if-match-" + randomAlphaOfLength(8);

        String firstETag = put(client, key, "generation-1", "*", null);
        String secondETag = put(client, key, "generation-2", null, firstETag);
        assertNotEquals("the etag must move when the object does", firstETag, secondETag);

        S3Exception conflict = expectThrows(S3Exception.class, () -> put(client, key, "generation-3", null, firstETag));
        assertEquals("a swap against a stale etag must be refused with 412", 412, conflict.statusCode());

        String survived = client.getObjectAsBytes(GetObjectRequest.builder().bucket(BUCKET).key(key).build())
            .asString(StandardCharsets.UTF_8);
        assertEquals("and the value must be the one the winner wrote", "generation-2", survived);
    }

    /**
     * Concurrent creators of one name, which is the case the design actually cares about: a hundred million
     * tenants provisioning at once with no lock anywhere.
     */
    public void testExactlyOneConcurrentCreatorWins() throws Exception {
        S3Client client = clientOrSkip();
        String key = "concurrent-" + randomAlphaOfLength(8);
        int writers = 8;

        java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger winners = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger conflicts = new java.util.concurrent.atomic.AtomicInteger();
        Thread[] threads = new Thread[writers];
        for (int i = 0; i < writers; i++) {
            final int id = i;
            threads[i] = new Thread(() -> {
                try {
                    start.await();
                    put(client, key, "writer-" + id, "*", null);
                    winners.incrementAndGet();
                } catch (S3Exception e) {
                    if (e.statusCode() == 412) {
                        conflicts.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            threads[i].start();
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals("exactly one writer may take a name", 1, winners.get());
        assertEquals("and every other must be told it lost, rather than failing some other way", writers - 1, conflicts.get());
    }
}
