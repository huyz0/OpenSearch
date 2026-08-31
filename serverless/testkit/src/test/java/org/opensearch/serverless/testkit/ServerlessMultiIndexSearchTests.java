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
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Searching several indices at once, and refusing to guess which ones.
 *
 * <p>A comma-separated list is resolved by looking each name up: one register read per name, no listing.
 * A <b>prefix</b> pattern costs one bounded listing on top of that — a single {@code ListObjectsV2} with a
 * maximum key count, whose cost is set by the cap rather than by the population. Anything else stays
 * refused: {@code *-2026} cannot be answered by a listing at all, only by reading every name in the
 * deployment, which is the operation &sect;6.3 declines to offer on a request path.
 *
 * <p>The rule was never "listings are forbidden". It was that a request must not cost the size of the
 * deployment and must not answer from a subset while looking complete — so a pattern matching more than the
 * cap is refused rather than truncated.
 *
 * <p><b>One fan-out, not one search per index.</b> Six shards across three indices are asked at once, and
 * the window is cut once over everything — so page two of a search over three indices is the page two it
 * would be over one.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessMultiIndexSearchTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"rank\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-multi-index")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** Two indices, one search, and every hit says where it came from. */
    public void testASearchCoversEveryIndexNamed() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("logs-a", "uuid-logs-a-0000000", 2, MAPPING, null));
        plane.createIndex(new IndexDescriptor("logs-b", "uuid-logs-b-0000000", 2, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("multi-both"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 2, "logs-a", "logs-b");

            assertEquals(201, send(node, "PUT", "/logs-a/_doc/1?refresh=true", "{\"msg\":\"shared\",\"rank\":1}").status());
            assertEquals(201, send(node, "PUT", "/logs-b/_doc/2?refresh=true", "{\"msg\":\"shared\",\"rank\":2}").status());

            final Response both = send(node, "POST", "/logs-a,logs-b/_search", "{\"size\":10,\"query\":{\"match\":{\"msg\":\"shared\"}}}");
            assertEquals(both.body(), 200, both.status());
            assertTrue("both documents must be found: " + both.body(), both.body().contains("\"value\":2"));
            assertEquals("every shard of both indices must be reported: " + both.body(), 4, shardTotal(both.body()));
            assertTrue("and all of them answered: " + both.body(), both.body().contains("\"complete\":true"));

            // Each hit names its own index, which is the difference between a multi-index search and a
            // single-index search with a longer name.
            final List<String> indices = indicesIn(both.body());
            assertTrue("a hit from each index: " + both.body(), indices.contains("logs-a") && indices.contains("logs-b"));

            // And one index alone still means one index.
            final Response one = send(node, "POST", "/logs-a/_search", "{\"size\":10,\"query\":{\"match\":{\"msg\":\"shared\"}}}");
            assertTrue("naming one index must not search the other: " + one.body(), one.body().contains("\"value\":1"));
            assertEquals(2, shardTotal(one.body()));
        }
    }

    /**
     * The page is cut once, over everything.
     *
     * <p>The assertion that catches a search which ran per index and concatenated: sorted by rank, the
     * second page of a search over both indices interleaves them.
     */
    public void testThePageIsCutAcrossTheWholeSet() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("even", "uuid-even-000000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("odd", "uuid-odd-0000000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("multi-page"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 1, "even", "odd");

            for (int rank = 1; rank <= 10; rank++) {
                final String index = rank % 2 == 0 ? "even" : "odd";
                assertEquals(201, send(node, "PUT", "/" + index + "/_doc/d" + rank + "?refresh=true", body(rank)).status());
            }

            final Response page = send(node, "POST", "/even,odd/_search", "{\"from\":3,\"size\":4,\"sort\":[{\"rank\":\"asc\"}]}");
            assertEquals(page.body(), 200, page.status());
            assertEquals("the fourth through seventh documents overall: " + page.body(), List.of(4L, 5L, 6L, 7L), ranksIn(page.body()));
        }
    }

    /**
     * A prefix pattern is answered, by one bounded listing.
     *
     * <p><b>What changed, and why the old refusal was right until it was not.</b> &sect;6.3 refuses
     * enumeration on a request path, and resolving {@code logs-*} by reading every index name in the
     * deployment is exactly that. But the rule was never "listings are forbidden" — it was that a request
     * must not cost the size of the deployment and must not answer from a subset while looking complete.
     * A prefix listing with a maximum key count satisfies both: one round trip whose cost is set by the
     * cap, and a refusal rather than a truncation when more match than the cap allows.
     */
    public void testAPrefixPatternMatchesTheIndicesThatShareIt() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("logs-a", "uuid-logs-a-0000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("logs-b", "uuid-logs-b-0000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("metrics-a", "uuid-metrics-a-000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("multi-prefix"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 1, "logs-a", "logs-b", "metrics-a");

            assertEquals(201, send(node, "PUT", "/logs-a/_doc/1?refresh=true", "{\"msg\":\"shared\",\"rank\":1}").status());
            assertEquals(201, send(node, "PUT", "/logs-b/_doc/2?refresh=true", "{\"msg\":\"shared\",\"rank\":2}").status());
            assertEquals(201, send(node, "PUT", "/metrics-a/_doc/3?refresh=true", "{\"msg\":\"shared\",\"rank\":3}").status());

            final Response matched = send(node, "POST", "/logs-*/_search", "{\"size\":10,\"query\":{\"match\":{\"msg\":\"shared\"}}}");
            assertEquals(matched.body(), 200, matched.status());
            assertTrue("both logs indices must be searched: " + matched.body(), matched.body().contains("\"value\":2"));
            assertEquals("and only those two: " + matched.body(), 2, shardTotal(matched.body()));
            final List<String> indices = indicesIn(matched.body());
            assertTrue("a hit from each: " + matched.body(), indices.contains("logs-a") && indices.contains("logs-b"));
            assertFalse("and nothing from outside the prefix: " + matched.body(), indices.contains("metrics-a"));

            // A pattern beside a name, which is where the two paths have to agree.
            final Response mixed = send(
                node,
                "POST",
                "/logs-*,metrics-a/_search",
                "{\"size\":10,\"query\":{\"match\":{\"msg\":\"shared\"}}}"
            );
            assertEquals(mixed.body(), 200, mixed.status());
            assertEquals("all three: " + mixed.body(), 3, shardTotal(mixed.body()));

            // And a name matched by both the pattern and the list is one index, not two.
            final Response overlapping = send(
                node,
                "POST",
                "/logs-*,logs-a/_search",
                "{\"size\":10,\"query\":{\"match\":{\"msg\":\"shared\"}}}"
            );
            assertEquals("an index named twice is still one index: " + overlapping.body(), 2, shardTotal(overlapping.body()));
        }
    }

    /**
     * A pattern that is not a prefix is still refused, and the reason is unchanged.
     *
     * <p>{@code *-2026} cannot be answered by a listing at all — only by reading every name in the
     * deployment and matching each one, which is the operation this system does not offer on a request
     * path. Offering the prefix case and refusing this one is the honest split, rather than supporting a
     * wildcard syntax whose cost depends on where the caller put the star.
     */
    public void testANonPrefixPatternIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("logs-a", "uuid-logs-a-0000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("multi-pattern"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 1, "logs-a");

            // The single-character wildcard is percent-encoded, because a raw ? in a URL starts the query
            // string and never reaches the handler as part of the index name.
            for (String pattern : List.of("*-a", "lo*s-a", "logs-%3F")) {
                final Response refused = send(node, "POST", "/" + pattern + "/_search", "{\"query\":{\"match_all\":{}}}");
                assertEquals("[" + pattern + "] must be refused: " + refused.body(), 501, refused.status());
                assertTrue("and say what it would have cost: " + refused.body(), refused.body().contains("reading every index name"));
            }

            // Inside a list, where it would be easiest to let one through.
            assertEquals(501, send(node, "POST", "/logs-a,*-a/_search", "{\"query\":{\"match_all\":{}}}").status());
        }
    }

    /**
     * A pattern matching nothing is an error, not an empty answer.
     *
     * <p>The same rule a named index gets: a search that covered no indices at all and reported itself
     * complete is the confident empty answer this surface exists to avoid. A caller who means "whatever is
     * there, possibly nothing" says so with {@code ignore_unavailable}.
     */
    public void testAPatternMatchingNothingIsRefusedUnlessAsked() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("logs-a", "uuid-logs-a-0000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("multi-empty-pattern"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 1, "logs-a");
            assertEquals(201, send(node, "PUT", "/logs-a/_doc/1?refresh=true", "{\"msg\":\"shared\",\"rank\":1}").status());

            final Response nothing = send(node, "POST", "/nosuch-*/_search", "{\"query\":{\"match_all\":{}}}");
            assertEquals("a pattern matching nothing must not answer emptily: " + nothing.body(), 404, nothing.status());
            assertTrue("and must name the pattern: " + nothing.body(), nothing.body().contains("nosuch-*"));

            // Asked for explicitly, it is allowed -- and the indices that did match are still searched.
            final Response asked = send(
                node,
                "POST",
                "/logs-*,nosuch-*/_search?ignore_unavailable=true",
                "{\"query\":{\"match\":{\"msg\":\"shared\"}}}"
            );
            assertEquals(asked.body(), 200, asked.status());
            assertTrue("what did match must still be searched: " + asked.body(), asked.body().contains("\"value\":1"));
        }
    }

    /**
     * A pattern matching more than the cap is refused, not truncated.
     *
     * <p><b>This is the assertion the whole permission rests on.</b> A prefix listing is only bounded
     * because it stops at a maximum, and an answer that stopped at a maximum and said nothing about it
     * would be a partial result wearing the shape of a complete one — worse than the refusal that used to
     * be there, because at least the refusal was honest.
     *
     * <p>The listing asks for the cap plus one, which is what makes "there are more than this" a fact
     * rather than a guess: a listing that returns exactly the cap is indistinguishable from one that was
     * cut off there.
     */
    public void testAPatternMatchingMoreThanTheCapIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        for (String suffix : List.of("a", "b", "c")) {
            plane.createIndex(new IndexDescriptor("logs-" + suffix, "uuid-logs-" + suffix + "-0000000", 1, MAPPING, null));
        }

        final Settings capped = Settings.builder()
            .put(nodeSettings("multi-capped"))
            .put("serverless.search.pattern.max_indices", 3)
            .build();
        try (ServerlessNode node = new ServerlessNode(capped)) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 1, "logs-a", "logs-b", "logs-c");
            assertEquals(201, send(node, "PUT", "/logs-a/_doc/1?refresh=true", "{\"msg\":\"shared\",\"rank\":1}").status());

            // Exactly the cap is allowed: the boundary is "more than", not "as many as".
            final Response atTheCap = send(node, "POST", "/logs-*/_search", "{\"query\":{\"match\":{\"msg\":\"shared\"}}}");
            assertEquals("three indices with a cap of three must be searched: " + atTheCap.body(), 200, atTheCap.status());

            plane.createIndex(new IndexDescriptor("logs-d", "uuid-logs-d-0000000", 1, MAPPING, null));

            final Response overTheCap = send(node, "POST", "/logs-*/_search", "{\"query\":{\"match\":{\"msg\":\"shared\"}}}");
            assertEquals("one more than the cap must be refused: " + overTheCap.body(), 400, overTheCap.status());
            assertTrue("and say what the limit was: " + overTheCap.body(), overTheCap.body().contains("more than 3 indices"));
            assertTrue(
                "and why it is a refusal rather than a truncation: " + overTheCap.body(),
                overTheCap.body().contains("partial result that looks complete")
            );
        }
    }

    /**
     * A pattern steps over what a deletion left behind.
     *
     * <p>Deleting an index swaps its descriptor to a tombstone and then removes it, so a listing can
     * return a name whose register describes nothing. A pattern is a filter over what exists, so that is
     * simply not a match — where a <em>named</em> index that is not there is a mistake and stays an error.
     *
     * <p>Reporting it as skipped would be wrong for the same reason: skipped means "you asked for this and
     * it is missing from the answer", and nobody asked for it.
     */
    public void testAPatternDoesNotTripOverADeletedIndex() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("logs-a", "uuid-logs-a-0000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("logs-b", "uuid-logs-b-0000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("multi-deleted"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 1, "logs-a", "logs-b");
            assertEquals(201, send(node, "PUT", "/logs-a/_doc/1?refresh=true", "{\"msg\":\"shared\",\"rank\":1}").status());

            assertEquals(200, send(node, "DELETE", "/logs-b", null).status());

            final Response afterDelete = send(node, "POST", "/logs-*/_search", "{\"query\":{\"match\":{\"msg\":\"shared\"}}}");
            assertEquals("a pattern must survive a deleted index: " + afterDelete.body(), 200, afterDelete.status());
            assertEquals("and search only what is left: " + afterDelete.body(), 1, shardTotal(afterDelete.body()));
            assertFalse("with nothing reported as skipped: " + afterDelete.body(), afterDelete.body().contains("\"skipped\""));

            // And with a tombstone still in place -- the window between the swap and the removal, which a
            // failed removal makes permanent.
            final var descriptors = plane.blobStore()
                .blobContainer(org.opensearch.serverless.metadata.RegisterMap.indices(BlobPath.cleanPath()));
            final var tombstone = new org.opensearch.core.common.bytes.BytesArray("{\"tombstone\":true}");
            assertTrue(descriptors.createRegisterIfAbsent("logs-c", tombstone).applied());

            final Response withTombstone = send(node, "POST", "/logs-*/_search", "{\"query\":{\"match\":{\"msg\":\"shared\"}}}");
            assertEquals("a tombstone in the listing must not become an error: " + withTombstone.body(), 200, withTombstone.status());
            assertEquals("nor a shard to search: " + withTombstone.body(), 1, shardTotal(withTombstone.body()));
        }
    }

    /** An index in the list that does not exist is refused, not quietly dropped. */
    public void testAMissingIndexInTheListIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("present", "uuid-present-000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("multi-missing"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 1, "present");
            assertEquals(201, send(node, "PUT", "/present/_doc/1?refresh=true", body(1)).status());

            final Response got = send(node, "POST", "/present,absent/_search", "{\"query\":{\"match_all\":{}}}");
            // A result that looked complete while silently covering two of the three indices asked for is
            // the confident empty answer this whole surface exists to avoid.
            assertEquals("a search naming an index that does not exist must be refused: " + got.body(), 404, got.status());
            assertTrue("and name the one that is missing: " + got.body(), got.body().contains("absent"));
        }
    }

    /**
     * {@code ignore_unavailable} lets a caller say they expect an index to be missing — and names what was.
     *
     * <p>Refusing stays the default, because a typo is far more likely than an absence somebody planned for
     * and a search that answers 200 while covering two of the three indices asked for is the confident
     * empty answer this surface exists to avoid. Somebody searching yesterday's and today's index on a day
     * that has only just started is saying something different, and the flag is how they say it.
     *
     * <p><b>What is skipped is named in the answer.</b> A flag that turned a visible absence into an
     * invisible one would be worse than the refusal it replaced: the caller allowed for a gap, they did not
     * ask to be unable to see it.
     */
    public void testIgnoreUnavailableSkipsWhatIsMissingAndSaysWhat() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("today", "uuid-today-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("multi-ignore"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, 1, "today");
            assertEquals(201, send(node, "PUT", "/today/_doc/1?refresh=true", body(1)).status());

            // Without the flag, unchanged.
            assertEquals(404, send(node, "POST", "/today,yesterday/_search", "{\"query\":{\"match_all\":{}}}").status());

            final Response ignored = send(
                node,
                "POST",
                "/today,yesterday/_search?ignore_unavailable=true",
                "{\"query\":{\"match_all\":{}}}"
            );
            assertEquals(ignored.body(), 200, ignored.status());
            assertTrue("the index that exists is searched: " + ignored.body(), ignored.body().contains("\"value\":1"));
            assertEquals("and only its shards are counted: " + ignored.body(), 1, shardTotal(ignored.body()));
            assertTrue("and the one that does not is named: " + ignored.body(), ignored.body().contains("\"skipped\":[\"yesterday\"]"));

            // Nothing skipped, nothing said. A field that appeared on every answer would be noise.
            assertFalse(send(node, "POST", "/today/_search", "{\"query\":{\"match_all\":{}}}").body().contains("skipped"));

            // And the flag does not extend to "none of them exist": the caller allowed for a gap, not for
            // the whole thing to be missing, and an empty answer over nothing is indistinguishable from an
            // empty answer over everything.
            final Response allMissing = send(
                node,
                "POST",
                "/gone,also-gone/_search?ignore_unavailable=true",
                "{\"query\":{\"match_all\":{}}}"
            );
            assertEquals("every name absent must still be refused: " + allMissing.body(), 404, allMissing.status());
            assertTrue(allMissing.body().contains("none of the indices named exist"));
        }
    }

    private MetadataPlane plane(AtomicLong clock) throws java.io.IOException {
        return new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
    }

    private void hold(ServerlessNode node, MetadataPlane plane, AtomicLong clock, int shards, String... indices) throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (String index : indices) {
            for (int shard = 0; shard < shards; shard++) {
                loop.want(index, shard);
            }
        }
        loop.tick(clock.get());
    }

    private static String body(int rank) {
        return "{\"msg\":\"doc\",\"rank\":" + rank + "}";
    }

    private static final Pattern RANK = Pattern.compile("\"rank\":(\\d+)");
    private static final Pattern INDEX = Pattern.compile("\"_index\":\"([a-z-]+)\"");
    private static final Pattern SHARD_TOTAL = Pattern.compile("\"_shards\":\\{\"total\":(\\d+)");

    private static List<Long> ranksIn(String body) {
        final List<Long> ranks = new ArrayList<>();
        final Matcher matcher = RANK.matcher(body);
        while (matcher.find()) {
            ranks.add(Long.parseLong(matcher.group(1)));
        }
        return ranks;
    }

    private static List<String> indicesIn(String body) {
        final List<String> indices = new ArrayList<>();
        final Matcher matcher = INDEX.matcher(body);
        while (matcher.find()) {
            indices.add(matcher.group(1));
        }
        return indices;
    }

    private static int shardTotal(String body) {
        final Matcher matcher = SHARD_TOTAL.matcher(body);
        assertTrue("no shard coverage in " + body, matcher.find());
        return Integer.parseInt(matcher.group(1));
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
