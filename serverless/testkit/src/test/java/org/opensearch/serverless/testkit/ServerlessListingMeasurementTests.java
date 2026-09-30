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
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@code _list/indices} over a large deployment on a real S3 API, a page at a time, end to end through REST.
 *
 * <p>The claim: a walk of {@code P} indices costs one listing request per thousand names plus one descriptor
 * read per index, whatever page the walk is on -- page two hundred costs what page one did, because each
 * page resumes in the store's own listing ({@code StartAfter}) instead of listing from the start. And the
 * walk's semantics under change: an index created or deleted between pages is seen only ahead of the cursor,
 * and every index that exists for the whole walk is returned exactly once.
 *
 * <p>Population from {@code tests.serverless.listing.population} (default {@code 10000}); the figures in
 * {@code STATUS.md} were taken at {@code 100000}. Skipped without an endpoint.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ServerlessListingMeasurementTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";
    private static final int PAGE = 5000;
    private static final Pattern INDEX = Pattern.compile("\"index\":\"([^\"]+)\"");
    private static final Pattern TOKEN = Pattern.compile("\"next_token\":\"([^\"]+)\"");

    private final List<String> created = new ArrayList<>();
    private int port;

    private String endpoint() {
        return System.getProperty(MinioBlobContainerConformanceTests.ENDPOINT, "http://127.0.0.1:9000");
    }

    private static String accessKey() {
        return System.getProperty("tests.serverless.s3.access_key", "minioadmin");
    }

    private static String secretKey() {
        return System.getProperty("tests.serverless.s3.secret_key", "minioadmin");
    }

    private static String name(int i) {
        return String.format(Locale.ROOT, "idle-%07d", i);
    }

    public void testAWalkOfTheWholeDeploymentCostsTheSameOnEveryPage() throws Exception {
        assumeTrue("no S3-compatible endpoint at " + endpoint(), reachable(endpoint()));
        final int population = Integer.parseInt(System.getProperty("tests.serverless.listing.population", "10000"));
        final String bucket = "list-" + population + "-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        final BlobStore s3 = org.opensearch.repositories.s3.MinioBlobStores.create(
            endpoint(),
            accessKey(),
            secretKey(),
            bucket,
            createTempDir()
        );
        created.add(bucket);
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(s3, BlobPath.cleanPath(), clock::get, TTL);

        final long populatingSince = System.nanoTime();
        final ExecutorService creators = Executors.newFixedThreadPool(32);
        try {
            final List<Future<?>> pending = new ArrayList<>();
            for (int from = 0; from < population; from += 500) {
                final int start = from;
                pending.add(creators.submit(() -> {
                    for (int i = start; i < Math.min(population, start + 500); i++) {
                        plane.createIndex(new IndexDescriptor(name(i), "uuid-" + i, 1, MAPPING, null));
                    }
                    return null;
                }));
            }
            for (Future<?> f : pending) {
                f.get();
            }
        } finally {
            creators.shutdown();
            assertTrue(creators.awaitTermination(1, TimeUnit.MINUTES));
        }
        final long populatingSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - populatingSince);

        // The changes the canary makes between two pages, a third of the way through.
        final int changeAfterPage = Math.max(1, population / PAGE / 3);
        final String behindCreated = name(1) + "a";
        final String behindDeleted = name(2);
        final String aheadCreated = name(population - 10) + "a";
        final String aheadDeleted = name(population - 5);

        try (ServerlessNode node = new ServerlessNode(nodeSettings())) {
            node.start();
            node.setMetadataPlane(plane);
            port = node.boundHttpAddress().publishAddress().getPort();

            final List<String> walked = new ArrayList<>();
            final List<Map<String, Long>> costs = new ArrayList<>();
            final List<Long> millis = new ArrayList<>();
            String token = null;
            int pages = 0;
            do {
                final Map<String, Long> before = org.opensearch.repositories.s3.MinioBlobStores.requestCounts(s3);
                final long startedAt = System.nanoTime();
                final String body = get("/_list/indices/idle-*?format=json&size=" + PAGE + (token == null ? "" : "&next_token=" + token));
                millis.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
                costs.add(delta(before, org.opensearch.repositories.s3.MinioBlobStores.requestCounts(s3)));
                final Matcher m = INDEX.matcher(body);
                while (m.find()) {
                    walked.add(m.group(1));
                }
                final Matcher t = TOKEN.matcher(body);
                token = t.find() ? t.group(1) : null;
                pages++;
                if (pages == changeAfterPage) {
                    plane.createIndex(new IndexDescriptor(behindCreated, "uuid-behind", 1, MAPPING, null));
                    plane.createIndex(new IndexDescriptor(aheadCreated, "uuid-ahead", 1, MAPPING, null));
                    assertTrue(plane.deleteIndex(behindDeleted));
                    assertTrue(plane.deleteIndex(aheadDeleted));
                }
                assertTrue("a walk of " + population + " took too many pages", pages <= population / PAGE + 3);
            } while (token != null);

            final Set<String> expected = new TreeSet<>();
            for (int i = 0; i < population; i++) {
                expected.add(name(i));
            }
            expected.remove(aheadDeleted);
            expected.add(aheadCreated);
            assertEquals("every index once: no name is repeated", walked.size(), new HashSet<>(walked).size());
            assertEquals(
                "the walk holds every index that existed throughout, the one created ahead of the cursor, and neither the one "
                    + "created behind it nor the one deleted ahead of it",
                new ArrayList<>(expected),
                walked
            );

            final List<Long> sorted = new ArrayList<>(millis);
            java.util.Collections.sort(sorted);
            logger.info(
                "listing: {} indices in {} pages of {}; first page {} in {}ms, page {} {} in {}ms, last page {} in {}ms; "
                    + "page p50 {}ms, max {}ms; populating took {}s",
                population,
                pages,
                PAGE,
                costs.get(0),
                millis.get(0),
                pages / 2 + 1,
                costs.get(pages / 2),
                millis.get(pages / 2),
                costs.get(pages - 1),
                millis.get(pages - 1),
                sorted.get(sorted.size() / 2),
                sorted.get(sorted.size() - 1),
                populatingSeconds
            );
            // A full page: five listings of a thousand and five thousand reads, early or late. The page the canary
            // changed is exempt; so is the last, which holds what is left.
            final Map<String, Long> first = costs.get(0);
            for (int i = 1; i < pages - 1; i++) {
                if (i == changeAfterPage) {
                    continue;
                }
                assertEquals("page " + (i + 1) + " cost what page 1 did", first, costs.get(i));
            }
            assertEquals(Long.valueOf(PAGE), first.get("GetObject"));
            assertTrue("a page of 5000 lists at most six times: " + first, first.get("ListObjects") <= 6);
        }
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

    private String get(String path) throws Exception {
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).timeout(Duration.ofMinutes(5)).GET().build(),
                HttpResponse.BodyHandlers.ofString()
            );
            assertEquals(response.body(), 200, response.statusCode());
            return response.body();
        }
    }

    private Settings nodeSettings() {
        return Settings.builder()
            .put("node.name", "listing")
            .put("cluster.name", "serverless-listing")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
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
