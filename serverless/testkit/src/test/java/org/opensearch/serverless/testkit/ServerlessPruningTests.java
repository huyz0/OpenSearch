/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.PruningDigest;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Skipping the shards a search provably cannot match, from the digest each publish writes into its manifest --
 * and never one it could.
 */
public class ServerlessPruningTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"n\":{\"type\":\"long\"},"
        + "\"msg\":{\"type\":\"text\"}}}";
    private static final Pattern TOTAL = Pattern.compile("\"total\":\\{\"value\":(\\d+)");
    private static final Pattern SKIPPED = Pattern.compile("\"skipped\":(\\d+)");
    private static final Pattern FAILED = Pattern.compile("\"failed\":(\\d+)");

    private Settings nodeSettings(String name, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-pruning")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put(extra)
            .build();
    }

    private static String day(int d) {
        return String.format(Locale.ROOT, "logs-2026.09.%02d", d);
    }

    private static long at(int d, int hour) {
        return Instant.parse(String.format(Locale.ROOT, "2026-09-%02dT%02d:00:00Z", d, hour)).toEpochMilli();
    }

    /**
     * Writes each index's documents, publishes, and lets go of every shard, so each is a published commit with
     * nobody holding it: what a day-partitioned log's older days are.
     */
    private void seed(ServerlessNode node, MetadataPlane plane, AtomicLong clock, Map<String, List<long[]>> docs) throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (String index : docs.keySet()) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index, 1, MAPPING, null));
            loop.want(index, 0);
        }
        loop.tick(clock.get());
        for (Map.Entry<String, List<long[]>> index : docs.entrySet()) {
            int id = 0;
            for (long[] doc : index.getValue()) {
                final Response put = send(
                    node,
                    "PUT",
                    "/" + index.getKey() + "/_doc/" + (id++),
                    "{\"@timestamp\":" + doc[0] + ",\"n\":" + doc[1] + ",\"msg\":\"log line\"}"
                );
                assertEquals(put.body(), 201, put.status());
            }
        }
        loop.publishAll();
        loop.setIdleAfterMillis(1);
        loop.releaseIdle(clock.get() + 60_000L);
        assertTrue("every shard let go", node.reconciler().openShards().isEmpty());
    }

    private Map<String, List<long[]>> days(int count) {
        final Map<String, List<long[]>> docs = new java.util.LinkedHashMap<>();
        for (int d = 1; d <= count; d++) {
            final List<long[]> day = new ArrayList<>();
            for (int h = 0; h < 4; h++) {
                day.add(new long[] { at(d, h), d * 100L + h });
            }
            docs.put(day(d), day);
        }
        return docs;
    }

    public void testThePublishedManifestCarriesTheCommitsDigest() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("digest", Settings.EMPTY))) {
            node.start();
            node.setMetadataPlane(plane);
            seed(node, plane, clock, days(1));
            final CommitManifest manifest = plane.segmentPublisher(day(1), "uuid-" + day(1), 0).readManifest().orElseThrow();
            final PruningDigest.FieldRange timestamp = manifest.digest().fields().get("@timestamp");
            assertEquals("date", timestamp.type());
            assertEquals(at(1, 0), timestamp.min().longValue());
            assertEquals(at(1, 3), timestamp.max().longValue());
            final PruningDigest.FieldRange n = manifest.digest().fields().get("n");
            assertEquals(new PruningDigest.FieldRange("long", 100L, 103L, null, null), n);
            assertNull("a text field has no points and is not digested", manifest.digest().fields().get("msg"));
        }
    }

    /** A two-day range over eight days: six shards skipped, and the answer the same as with pruning off. */
    public void testARangeSearchSkipsTheShardsItCannotMatch() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.nio.file.Path dir = createTempDir();
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
        final String query = "{\"size\":100,\"query\":{\"bool\":{\"filter\":[{\"range\":{\"@timestamp\":"
            + "{\"gte\":\"2026-09-03\",\"lte\":\"2026-09-04T12:00:00Z\"}}}]}},\"aggs\":{\"most\":{\"max\":{\"field\":\"n\"}}}}";
        final String pruned;
        try (ServerlessNode node = new ServerlessNode(nodeSettings("prune", Settings.EMPTY))) {
            node.start();
            node.setMetadataPlane(plane);
            seed(node, plane, clock, days(8));
            final Response answer = send(node, "POST", "/logs-*/_search", query);
            assertEquals(answer.body(), 200, answer.status());
            assertEquals("two days of four documents: " + answer.body(), 8L, first(TOTAL, answer.body()));
            assertEquals("six days skipped: " + answer.body(), 6L, first(SKIPPED, answer.body()));
            assertTrue("and counted as successful: " + answer.body(), answer.body().contains("\"successful\":8"));
            assertTrue("aggregations over what matched: " + answer.body(), answer.body().contains("\"value\":403.0"));
            assertTrue("only the unskipped shards were opened", node.reconciler().openShards().size() <= 2);
            pruned = answer.body();
        }
        final MetadataPlane again = new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
        try (
            ServerlessNode node = new ServerlessNode(
                nodeSettings("no-prune", Settings.builder().put("serverless.search.prune", false).build())
            )
        ) {
            node.start();
            node.setMetadataPlane(again);
            final Response unpruned = send(node, "POST", "/logs-*/_search", query);
            assertEquals(unpruned.body(), 200, unpruned.status());
            assertEquals(0L, first(SKIPPED, unpruned.body()));
            assertEquals(first(TOTAL, pruned), first(TOTAL, unpruned.body()));
            assertTrue(unpruned.body().contains("\"value\":403.0"));
        }
    }

    /**
     * The canary: random data, random ranges -- inclusive and exclusive, on the boundaries and off them, dates and
     * numbers -- and every total must equal what brute force counts. A digest that pruned one shard it should not
     * have loses that shard's documents from the total.
     */
    public void testTheDigestNeverSkipsAShardThatCouldMatch() throws Exception {
        canary(Settings.EMPTY);
    }

    /** The same canary with the digest rollups consulted for every pattern, however few indices it matches. */
    public void testTheRollupsNeverSkipAShardThatCouldMatch() throws Exception {
        canary(Settings.builder().put("serverless.search.rollup.min_indices", 1).build());
    }

    private void canary(Settings extra) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final Map<String, List<long[]>> docs = new java.util.LinkedHashMap<>();
        final List<long[]> all = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final List<long[]> index = new ArrayList<>();
            final long base = randomLongBetween(0, 1_000);
            for (int d = 0; d < randomIntBetween(1, 5); d++) {
                final long[] doc = new long[] {
                    at(1 + (int) (base % 20), randomIntBetween(0, 23)) + randomIntBetween(0, 59_999),
                    base + randomIntBetween(0, 50) };
                index.add(doc);
                all.add(doc);
            }
            docs.put(String.format(Locale.ROOT, "logs-canary-%02d", i), index);
        }
        long skippedOverall = 0;
        try (ServerlessNode node = new ServerlessNode(nodeSettings("canary", extra))) {
            node.start();
            node.setMetadataPlane(plane);
            seed(node, plane, clock, docs);
            for (int q = 0; q < 40; q++) {
                final boolean onDate = randomBoolean();
                final long[] pick = randomFrom(all);
                final long pivot = onDate ? pick[0] : pick[1];
                final long width = onDate ? randomLongBetween(0, 3 * 86_400_000L) : randomLongBetween(0, 200);
                final long lower = pivot - (randomBoolean() ? 0 : randomLongBetween(0, width));
                final long upper = lower + width;
                final boolean includeLower = randomBoolean();
                final boolean includeUpper = randomBoolean();
                final String field = onDate ? "@timestamp" : "n";
                final String from = onDate && randomBoolean() ? "\"" + Instant.ofEpochMilli(lower) + "\"" : Long.toString(lower);
                final String to = onDate && randomBoolean() ? "\"" + Instant.ofEpochMilli(upper) + "\"" : Long.toString(upper);
                final String body = "{\"size\":0,\"track_total_hits\":true,\"query\":{\"range\":{\""
                    + field
                    + "\":{\""
                    + (includeLower ? "gte" : "gt")
                    + "\":"
                    + from
                    + ",\""
                    + (includeUpper ? "lte" : "lt")
                    + "\":"
                    + to
                    + "}}}}";
                long expected = 0;
                for (long[] doc : all) {
                    final long v = onDate ? doc[0] : doc[1];
                    if ((includeLower ? v >= lower : v > lower) && (includeUpper ? v <= upper : v < upper)) {
                        expected++;
                    }
                }
                final Response answer = send(node, "POST", "/logs-canary-*/_search", body);
                assertEquals(body + " -> " + answer.body(), 200, answer.status());
                assertEquals("query " + body, expected, first(TOTAL, answer.body()));
                skippedOverall += first(SKIPPED, answer.body());
            }
        }
        assertTrue("some queries must have pruned something, or this proves nothing", skippedOverall > 0);
    }

    private static final Settings ROLLUPS_ALWAYS = Settings.builder().put("serverless.search.rollup.min_indices", 1).build();

    /** An index deleted and recreated with documents inside the range is found, whatever its old incarnation's entry said. */
    public void testARecreatedIndexIsNotRuledOutByItsPredecessorsEntry() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("recreate", ROLLUPS_ALWAYS))) {
            node.start();
            node.setMetadataPlane(plane);
            seed(node, plane, clock, days(4));
            final String query = "{\"size\":0,\"track_total_hits\":true,\"query\":{\"range\":{\"@timestamp\":{\"gte\":\"2026-09-20\"}}}}";
            assertEquals(0L, first(TOTAL, send(node, "POST", "/logs-*/_search", query).body()));

            // Day 2 deleted and recreated, and given a document on the 21st.
            assertTrue(plane.deleteIndex(day(2)));
            final Map<String, List<long[]>> again = new java.util.LinkedHashMap<>();
            again.put(day(2), List.of(new long[] { at(21, 5), 2_105L }));
            seedAgain(node, plane, clock, again);

            final Response answer = send(node, "POST", "/logs-*/_search", query);
            assertEquals(answer.body(), 200, answer.status());
            assertEquals("the new incarnation's document is found: " + answer.body(), 1L, first(TOTAL, answer.body()));
        }
    }

    /**
     * The canary's own canary: an entry planted narrower than its shard -- the one thing the invariant forbids --
     * makes the total wrong, so a rollup that ever did that would be caught.
     */
    public void testAPlantedNarrowEntryIsCaughtByTheTotal() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("planted", ROLLUPS_ALWAYS))) {
            node.start();
            node.setMetadataPlane(plane);
            seed(node, plane, clock, days(4));
            final String query =
                "{\"size\":0,\"track_total_hits\":true,\"query\":{\"range\":{\"@timestamp\":{\"gte\":\"2026-09-03\",\"lt\":\"2026-09-04\"}}}}";
            assertEquals("honest rollups: day 3's four documents", 4L, first(TOTAL, send(node, "POST", "/logs-*/_search", query).body()));

            // Day 3's entry rewritten to claim it holds only day 1.
            plane.rollups()
                .plantForTest(
                    day(3),
                    new org.opensearch.serverless.metadata.DigestRollups.Entry(
                        "uuid-" + day(3),
                        1,
                        List.of(
                            new org.opensearch.serverless.metadata.DigestRollups.ShardState(
                                0L,
                                new PruningDigest(
                                    Map.of(
                                        "@timestamp",
                                        new PruningDigest.FieldRange(
                                            "date",
                                            at(1, 0),
                                            at(1, 3),
                                            "strict_date_optional_time||epoch_millis",
                                            "und"
                                        )
                                    )
                                )
                            )
                        )
                    )
                );
            assertEquals(
                "a narrow entry loses day 3's documents -- which is what the canary's totals would catch",
                0L,
                first(TOTAL, send(node, "POST", "/logs-*/_search", query).body())
            );
        }
    }

    /** A shard held by a writer is not ruled out by the rollups, whatever its published digest says. */
    public void testAShardWithAWriterIsNotRuledOutByTheRollups() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("writer-mark", ROLLUPS_ALWAYS))) {
            node.start();
            node.setMetadataPlane(plane);
            seed(node, plane, clock, days(3));
            final String query = "{\"size\":0,\"query\":{\"range\":{\"@timestamp\":{\"gte\":\"2026-09-20\"}}}}";
            // Everything ruled out, and one kept for the aggregations' shape.
            assertEquals(2L, first(SKIPPED, send(node, "POST", "/logs-*/_search", query).body()));

            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want(day(2), 0);
            loop.tick(clock.get());
            final var entry = plane.rollups().readGroup("logs-", new org.opensearch.serverless.metadata.DescriptorStore.Reads() {
                @Override
                public <T> List<T> runAll(List<java.util.concurrent.Callable<T>> tasks) throws InterruptedException {
                    final List<T> out = new ArrayList<>();
                    for (java.util.concurrent.Callable<T> task : tasks) {
                        try {
                            out.add(task.call());
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    }
                    return out;
                }
            }).get(day(2));
            assertTrue("the writer is recorded: " + entry, entry.states().get(0).ownerTerm() > 0);
            assertFalse(entry.rulesOut(org.opensearch.index.query.QueryBuilders.rangeQuery("@timestamp").gte("2026-09-20"), clock.get()));
        }
    }

    /** Writes more documents into indices that may exist already, publishes and lets go. */
    private void seedAgain(ServerlessNode node, MetadataPlane plane, AtomicLong clock, Map<String, List<long[]>> docs) throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (String index : docs.keySet()) {
            if (plane.describe(index).isEmpty()) {
                plane.createIndex(new IndexDescriptor(index, "uuid-" + index + "-again", 1, MAPPING, null));
            }
            loop.want(index, 0);
        }
        loop.tick(clock.get());
        for (Map.Entry<String, List<long[]>> index : docs.entrySet()) {
            int id = 100;
            for (long[] doc : index.getValue()) {
                final Response put = send(
                    node,
                    "PUT",
                    "/" + index.getKey() + "/_doc/" + (id++),
                    "{\"@timestamp\":" + doc[0] + ",\"n\":" + doc[1] + ",\"msg\":\"log line\"}"
                );
                assertEquals(put.body(), 201, put.status());
            }
        }
        loop.publishAll();
        loop.setIdleAfterMillis(1);
        loop.releaseIdle(clock.get() + 60_000L);
    }

    /**
     * A shard with an owner is never skipped, whatever its digest says: a writer can hold refreshed documents the
     * published commit does not describe.
     */
    public void testAShardWithAnOwnerIsNeverSkipped() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.nio.file.Path dir = createTempDir();
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
        final MetadataPlane coordinatorPlane = new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
        try (
            ServerlessNode writer = new ServerlessNode(nodeSettings("owner", Settings.builder().put("serverless.roles", "ingest").build()));
            ServerlessNode coordinator = new ServerlessNode(nodeSettings("coordinator", Settings.EMPTY))
        ) {
            writer.start();
            writer.setMetadataPlane(plane);
            coordinator.start();
            coordinator.setMetadataPlane(coordinatorPlane);
            seed(writer, plane, clock, days(2));
            // Day 2 taken back by a writer, which then holds it: its head has an owner again.
            final BackgroundReconciler loop = new BackgroundReconciler(writer, plane);
            loop.want(day(2), 0);
            loop.tick(clock.get());
            new BackgroundReconciler(coordinator, coordinatorPlane).tick(clock.get());

            final Response answer = send(
                coordinator,
                "POST",
                "/logs-*/_search",
                "{\"size\":0,\"query\":{\"range\":{\"@timestamp\":{\"gte\":\"2026-09-20\"}}}}"
            );
            assertEquals(answer.body(), 200, answer.status());
            assertEquals("day 1 skipped, day 2 asked because it is owned: " + answer.body(), 1L, first(SKIPPED, answer.body()));
        }
    }

    /** Past the activation budget a search is refused, unless the caller asked for what fits. */
    public void testASearchPastTheActivationBudgetIsRefusedUnlessPartialIsAskedFor() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        try (
            ServerlessNode node = new ServerlessNode(
                nodeSettings("budget", Settings.builder().put("serverless.search.max_activations_per_query", 2).build())
            )
        ) {
            node.start();
            node.setMetadataPlane(plane);
            seed(node, plane, clock, days(5));
            final Response refused = send(node, "POST", "/logs-*/_search", "{\"query\":{\"match_all\":{}}}");
            assertEquals(refused.body(), 400, refused.status());
            assertTrue(refused.body(), refused.body().contains("allow_partial_activation"));
            assertTrue("refused before anything was opened", node.reconciler().openShards().isEmpty());

            final Response partial = send(node, "POST", "/logs-*/_search?allow_partial_activation=true", "{\"query\":{\"match_all\":{}}}");
            assertEquals(partial.body(), 200, partial.status());
            assertEquals("two searched, three reported failed: " + partial.body(), 3L, first(FAILED, partial.body()));
            assertEquals("the two searched days' documents: " + partial.body(), 8L, first(TOTAL, partial.body()));

            // Pruning comes first: a range that leaves two shards to search fits the budget.
            final Response narrowed = send(
                node,
                "POST",
                "/logs-*/_search",
                "{\"query\":{\"range\":{\"@timestamp\":{\"gte\":\"2026-09-04\"}}}}"
            );
            assertEquals(narrowed.body(), 200, narrowed.status());
            assertEquals(3L, first(SKIPPED, narrowed.body()));
        }
    }

    private static long first(Pattern pattern, String body) {
        final Matcher m = pattern.matcher(body);
        assertTrue("no " + pattern + " in " + body, m.find());
        return Long.parseLong(m.group(1));
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
