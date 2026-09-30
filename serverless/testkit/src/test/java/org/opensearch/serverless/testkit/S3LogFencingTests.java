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
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.serverless.store.WalStore;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The log-fencing history against an S3 API, and what a takeover and an append cost there, counted by the S3
 * client's own metrics.
 *
 * <p>Endpoint and credentials from {@code tests.serverless.s3.endpoint}, {@code tests.serverless.s3.access_key}
 * and {@code tests.serverless.s3.secret_key}; skipped without an endpoint.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class S3LogFencingTests extends LogFencingTestCase {

    private final List<String> created = new ArrayList<>();

    private static String endpoint() {
        return System.getProperty(MinioBlobContainerConformanceTests.ENDPOINT, "http://127.0.0.1:9000");
    }

    private static String accessKey() {
        return System.getProperty("tests.serverless.s3.access_key", "minioadmin");
    }

    private static String secretKey() {
        return System.getProperty("tests.serverless.s3.secret_key", "minioadmin");
    }

    @Override
    protected BlobStore newStore() throws Exception {
        assumeTrue("no S3-compatible endpoint at " + endpoint(), reachable(endpoint()));
        final String bucket = "fence-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        created.add(bucket);
        return org.opensearch.repositories.s3.MinioBlobStores.create(endpoint(), accessKey(), secretKey(), bucket, createTempDir());
    }

    /**
     * Requests for an append, and for a takeover by the old seal against the new fence. The seal still exists
     * for logs sealed before fences did, so both are measured in one run on one store.
     */
    public void testWhatATakeoverAndAnAppendCost() throws Exception {
        final BlobStore store = newStore();
        final BlobPath shard = BlobPath.cleanPath().add("segments").add("alpha#uuid#0");
        final WalStore writer = new WalStore(store, shard);
        writer.establish(1L, null);
        writer.append(1L, List.of(new WalRecord("warm", "{}", 0L, 1L, 1L)));

        Map<String, Long> before = org.opensearch.repositories.s3.MinioBlobStores.requestCounts(store);
        writer.append(1L, List.of(new WalRecord("one", "{}", 1L, 1L, 1L)));
        final Map<String, Long> append = delta(before, org.opensearch.repositories.s3.MinioBlobStores.requestCounts(store));

        // The old takeover: a seal at the swap and another at the open.
        before = org.opensearch.repositories.s3.MinioBlobStores.requestCounts(store);
        new WalStore(store, shard).sealAt(2L);
        new WalStore(store, shard).sealAt(2L);
        final Map<String, Long> sealed = delta(before, org.opensearch.repositories.s3.MinioBlobStores.requestCounts(store));

        // The new one, on a fresh log in the same state: fence at the swap, fence again and begin at the open.
        final BlobPath other = BlobPath.cleanPath().add("segments").add("beta#uuid#0");
        final WalStore predecessor = new WalStore(store, other);
        predecessor.establish(1L, null);
        predecessor.append(1L, List.of(new WalRecord("warm", "{}", 0L, 1L, 1L)));
        predecessor.append(1L, List.of(new WalRecord("one", "{}", 1L, 1L, 1L)));
        before = org.opensearch.repositories.s3.MinioBlobStores.requestCounts(store);
        new WalStore(store, other).fenceOlderTerms(2L);
        final WalStore successor = new WalStore(store, other);
        successor.fenceOlderTerms(2L);
        successor.establish(2L, null);
        final Map<String, Long> fenced = delta(before, org.opensearch.repositories.s3.MinioBlobStores.requestCounts(store));

        logger.info("fencing cost: an append {}; a takeover by seals {}; a takeover by fences {}", append, sealed, fenced);
        assertEquals("an append is one conditional PUT, before and after", Map.of("PutObject", 1L), append);
    }

    private static Map<String, Long> delta(Map<String, Long> before, Map<String, Long> after) {
        final Map<String, Long> delta = new HashMap<>();
        for (Map.Entry<String, Long> entry : after.entrySet()) {
            final long d = entry.getValue() - before.getOrDefault(entry.getKey(), 0L);
            if (d != 0L) {
                delta.put(entry.getKey(), d);
            }
        }
        return delta;
    }

    @org.junit.After
    public void removeBucketsThisTestMade() throws Exception {
        for (String bucket : created) {
            org.opensearch.repositories.s3.MinioBlobStores.deleteBucket(endpoint(), accessKey(), secretKey(), bucket, createTempDir());
        }
        created.clear();
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
