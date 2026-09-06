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
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * M53: discovery — what indices exist, and what can be queried in them.
 *
 * <p><b>Both endpoints answer from descriptors alone, and that is the design.</b> Neither reads a shard, so
 * both work against an index whose shards are all dormant — the normal resting state here, and precisely when
 * a client doing discovery needs an answer.
 *
 * <p><b>The scaling question, and why the answer is a different endpoint rather than a smaller one.</b>
 * {@code _cat/indices} returns everything by contract. The tempting fix — return the first hundred — is the
 * confident partial answer this design refuses everywhere else, in {@code TooManyMatchesException}'s own
 * words: an answer cut off at a limit looks exactly like a complete one. OpenSearch reached the same
 * conclusion about its own {@code _cat} APIs and added {@code _list/indices}, whose contract <em>is</em> a
 * page at a time. Serving that contract involves no lying, so that is what is served.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessDiscoveryTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private int port;

    private Settings nodeSettings(String name) {
        return nodeSettings(name, 0);
    }

    /**
     * @param cap the pattern cap, or 0 for the default -- a low one is how the refusal past the cap is
     *            reachable at all, since the real default of five hundred is out of a test's reach
     */
    private Settings nodeSettings(String name, int cap) {
        final Settings.Builder settings = Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-discovery")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest");
        if (cap > 0) {
            settings.put("serverless.search.pattern.max_indices", cap);
        }
        return settings.build();
    }

    private record Answer(int status, String body) {
        boolean has(String fragment) {
            return body.contains(fragment);
        }
    }

    private Answer call(String method, String path, String body) throws Exception {
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                    .header("Content-Type", "application/json")
                    .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(30))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
            return new Answer(response.statusCode(), response.body());
        }
    }

    private static int occurrences(String body, String fragment) {
        int count = 0;
        int at = body.indexOf(fragment);
        while (at >= 0) {
            count++;
            at = body.indexOf(fragment, at + fragment.length());
        }
        return count;
    }

    private static MetadataPlane plane(AtomicLong clock, Path store) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, store, false), BlobPath.cleanPath(), clock::get, TTL);
    }

    private ServerlessNode running(MetadataPlane plane, String name) throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings(name));
        node.start();
        node.setMetadataPlane(plane);
        port = node.boundHttpAddress().publishAddress().getPort();
        call(
            "PUT",
            "/logs-a",
            "{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}}"
        );
        call("PUT", "/logs-b", "{\"settings\":{\"number_of_shards\":2},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}}");
        call("PUT", "/other", "{\"settings\":{\"number_of_shards\":1}}");
        return node;
    }

    /** A prefix lists what matches it, as a table for a person and as JSON for a program. */
    public void testListIndicesAnswersForAPrefix() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "list-prefix")) {
            assertNotNull(node);

            final Answer json = call("GET", "/_list/indices/logs-*?format=json", null);
            assertEquals(json.body(), 200, json.status());
            assertTrue("the matching indices are listed: " + json.body(), json.has("logs-a") && json.has("logs-b"));
            assertFalse("and nothing outside the prefix is: " + json.body(), json.has("other"));
            assertTrue("with the shard count the index was made with: " + json.body(), json.has("\"pri\":\"2\""));

            final Answer text = call("GET", "/_list/indices/logs-*?v=true", null);
            assertTrue("a header row for a person: " + text.body(), text.has("index"));
            assertFalse("text, not JSON: " + text.body(), text.has("{"));

            // A literal name is not a pattern, and a name that is not there is a mistake rather than a
            // filter that matched nothing.
            assertTrue(call("GET", "/_list/indices/other?format=json", null).has("other"));
            assertEquals(404, call("GET", "/_list/indices/ghost", null).status());
        }
    }

    /**
     * The bare forms answer under the cap and are refused past it, rather than truncating.
     *
     * <p>They used to be refused outright as enumeration. That was stricter than the machinery needed:
     * an empty prefix is the same one bounded listing every other prefix takes, capped and refusing past
     * the cap, so a deployment small enough to answer can be answered without lying to anyone. What must
     * never happen is the middle case -- returning the first hundred of a larger deployment, which looks
     * exactly like returning all of them and gives a client no way to tell.
     */
    public void testTheBareListingsAnswerUnderTheCapAndAreRefusedPastIt() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "list-bare")) {
            assertNotNull(node);

            for (String path : new String[] { "/_list/indices", "/_cat/indices" }) {
                final Answer served = call("GET", path + "?format=json", null);
                assertEquals(path + " must answer under the cap: " + served.body(), 200, served.status());
                assertTrue(
                    path + " must list every index: " + served.body(),
                    served.has("logs-a") && served.has("logs-b") && served.has("other")
                );
            }
        }

        // And past the cap, refused rather than cut off. Two allowed, three present.
        final AtomicLong tight = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(tight, createTempDir());
        try (ServerlessNode node = new ServerlessNode(nodeSettings("list-over-cap", 2))) {
            node.start();
            node.setMetadataPlane(plane);
            port = node.boundHttpAddress().publishAddress().getPort();
            call("PUT", "/logs-a", "{\"settings\":{\"number_of_shards\":1}}");
            call("PUT", "/logs-b", "{\"settings\":{\"number_of_shards\":1}}");
            call("PUT", "/other", "{\"settings\":{\"number_of_shards\":1}}");

            for (String path : new String[] { "/_list/indices", "/_cat/indices" }) {
                final Answer refused = call("GET", path + "?format=json", null);
                assertEquals(path + " must refuse past the cap: " + refused.body(), 400, refused.status());
                assertFalse("and must not answer partially: " + refused.body(), refused.has("logs-a"));
            }
        }
    }

    /** A cursor is still refused, and still names the primitive core does not expose. */
    public void testACursorIsRefusedRatherThanSilentlyRestartingTheWalk() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "list-cursor")) {
            assertNotNull(node);
            final Answer token = call("GET", "/_list/indices/logs-*?next_token=abc", null);
            assertEquals(token.body(), 501, token.status());
            assertTrue("naming the missing primitive: " + token.body(), token.has("start-after"));
        }
    }

    /**
     * A prefix matching more than the cap is refused, not cut off.
     *
     * <p><b>This is the assertion the endpoint's whole argument rests on</b>, and the first version of this
     * suite did not make it: the other tests exercise the <em>unscoped</em> refusal, which is a registration,
     * while this exercises the <em>bounded</em> one, which is the behaviour. A canary that truncated instead
     * of refusing passed everything else.
     */
    public void testAPrefixMatchingMoreThanTheCapIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        final Settings capped = Settings.builder().put(nodeSettings("list-capped")).put("serverless.search.pattern.max_indices", 2).build();
        try (ServerlessNode node = new ServerlessNode(capped)) {
            node.start();
            node.setMetadataPlane(plane);
            port = node.boundHttpAddress().publishAddress().getPort();
            for (String name : new String[] { "logs-a", "logs-b", "logs-c" }) {
                call("PUT", "/" + name, "{\"settings\":{\"number_of_shards\":1}}");
            }

            final Answer refused = call("GET", "/_list/indices/logs-*", null);
            assertEquals("three matches against a cap of two must refuse: " + refused.body(), 400, refused.status());
            assertTrue("as too_many_indices: " + refused.body(), refused.has("too_many_indices"));

            // Field capabilities resolve through the same expander, so they refuse the same way.
            final Answer caps = call("GET", "/logs-*/_field_caps", null);
            assertEquals("and so must field capabilities: " + caps.body(), 400, caps.status());
            assertTrue(caps.body(), caps.has("too_many_indices"));

            // A prefix inside the cap still answers, which is what makes the refusal a bound rather than a
            // blanket rejection of patterns.
            final Answer narrow = call("GET", "/_list/indices/logs-a*?format=json", null);
            assertEquals(narrow.body(), 200, narrow.status());
            assertTrue(narrow.body(), narrow.has("logs-a"));
        }
    }

    /** Field capabilities come from the mapping, so an index with no shard open still answers. */
    public void testFieldCapabilitiesAnswerWithoutAShard() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "caps-dormant")) {
            // Not one shard has been activated: nothing has been written to any of these indices.
            assertTrue("no shard is open", node.reconciler().openShards().isEmpty());

            final Answer caps = call("GET", "/logs-a/_field_caps?fields=msg,n", null);
            assertEquals(caps.body(), 200, caps.status());
            assertTrue("the text field is reported as text: " + caps.body(), caps.has("\"text\":{\"type\":\"text\""));
            assertTrue("the long field as long: " + caps.body(), caps.has("\"long\":{\"type\":\"long\""));
            assertTrue("a long is aggregatable: " + caps.body(), caps.has("\"type\":\"long\",\"searchable\":true,\"aggregatable\":true"));
            assertTrue(
                "a text field is searchable but not aggregatable: " + caps.body(),
                caps.has("\"type\":\"text\",\"searchable\":true,\"aggregatable\":false")
            );
        }
    }

    /**
     * Where two indices map a field differently, both types are reported and attributed.
     *
     * <p>Collapsing the disagreement to one answer would be choosing for the caller. OpenSearch names the
     * indices under each type when they disagree and omits the key when they do not, which is how a client
     * detects a conflict at all.
     */
    public void testAFieldMappedTwoWaysIsReportedBothWays() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "caps-conflict")) {
            assertNotNull(node);

            final Answer conflict = call("GET", "/logs-*/_field_caps?fields=msg", null);
            assertEquals(conflict.body(), 200, conflict.status());
            assertTrue("the text mapping is attributed: " + conflict.body(), conflict.has("\"indices\":[\"logs-a\"]"));
            assertTrue("and the keyword mapping: " + conflict.body(), conflict.has("\"indices\":[\"logs-b\"]"));

            // Counted rather than pattern-matched, because every response carries a top-level "indices"
            // array and an assertion that did not distinguish it from the per-type one would pass on the
            // wrong thing -- as the first version of this assertion did. A conflict adds one key per
            // disagreeing type; agreement adds none.
            assertEquals(
                "conflict: the top-level array plus one per type: " + conflict.body(),
                3,
                occurrences(conflict.body(), "\"indices\"")
            );

            final Answer agreed = call("GET", "/logs-a/_field_caps?fields=msg", null);
            assertEquals("agreement: the top-level array alone: " + agreed.body(), 1, occurrences(agreed.body(), "\"indices\""));
        }
    }

    /** Metadata fields are flagged, so a client asking for everything can tell them from its own fields. */
    public void testMetadataFieldsAreFlagged() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "caps-metadata")) {
            assertNotNull(node);
            final Answer all = call("GET", "/logs-a/_field_caps", null);
            assertTrue("core's own metadata fields come back for fields=*: " + all.body(), all.has("_seq_no"));
            assertTrue("and are marked as such: " + all.body(), all.has("\"metadata_field\":true"));

            final Answer mine = call("GET", "/logs-a/_field_caps?fields=n", null);
            assertFalse("a field the caller declared is not flagged: " + mine.body(), mine.has("metadata_field"));
        }
    }

    /** Asking every index in the deployment what fields it has is the inventory operation, and is refused. */
    public void testUnscopedFieldCapabilitiesAreRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "caps-unscoped")) {
            assertNotNull(node);
            final Answer refused = call("GET", "/_field_caps", null);
            assertEquals(refused.body(), 501, refused.status());
            assertTrue("pointing at the scoped form: " + refused.body(), refused.has("/{index}/_field_caps"));
        }
    }

    /**
     * The refusals that used to share one wrong reason now each carry their own.
     *
     * <p>A caller told that a listing is unavailable because there is no cluster state would reasonably
     * conclude the data does not exist. It does exist, is readable one index at a time, and is refused because
     * reading all of it at once is unbounded — a different thing. Three others are refused because there is no
     * allocator, which is different again.
     */
    public void testEachRefusalCarriesItsOwnReason() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "reasons")) {
            assertNotNull(node);
            for (String[] expected : new String[][] {
                // /_cat/indices is no longer here: it is served now, bounded by the pattern cap and
                // refused past it. See testTheBareListingsAnswerUnderTheCapAndAreRefusedPastIt.
                { "/_cat/shards", "_cluster/health/{index}?level=shards" },
                { "/_cat/aliases", "found by name here, not enumerated" },
                { "/_cat/count", "/{index}/_count answers for one" },
                { "/_cluster/reroute", "there is no allocator here" },
                { "/_cluster/allocation/explain", "there is no allocator here" },
                { "/_cat/allocation", "there is no allocator here" },
                { "/_cat/recovery", "there is no allocator here" },
                { "/_cat/pending_tasks", "no cluster manager and no task queue" },
                { "/_cat/thread_pool", "GET /_serverless/stats answers for the node" } }) {
                final Answer refused = call("GET", expected[0], null);
                assertEquals(expected[0] + " must still refuse: " + refused.body(), 501, refused.status());
                assertTrue(expected[0] + " must carry its own reason: " + refused.body(), refused.has(expected[1]));
                assertFalse(
                    expected[0] + " must not still blame missing cluster state: " + refused.body(),
                    refused.has("no cluster-wide state")
                );
            }

            // What the shared reason is actually true of keeps it.
            assertTrue(call("GET", "/_cluster/state", null).has("no cluster-wide state"));
        }
    }
}
