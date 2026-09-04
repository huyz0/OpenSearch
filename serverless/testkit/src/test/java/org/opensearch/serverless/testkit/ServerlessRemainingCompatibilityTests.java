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
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The items M61 left, each now served and pinned over HTTP.
 *
 * <p>Creation metadata on an index, core's alias shapes and the reverse lookup, stored scripts, search
 * templates, ranking evaluation, search pipelines, rollover, and data streams. Each was listed as
 * "deliberately still not done" at the end of M61; none of them turned out to need what this design does
 * not have, once the thing it does have was looked at again.
 */
public class ServerlessRemainingCompatibilityTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING =
        "{\"properties\":{\"msg\":{\"type\":\"text\"},\"tag\":{\"type\":\"keyword\"},\"n\":{\"type\":\"integer\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-remaining-compat")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private record Fixture(ServerlessNode node, MetadataPlane plane, AtomicLong clock, BackgroundReconciler loop) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            node.close();
        }
    }

    /** A node with an empty plane; indices are created over HTTP so they carry creation metadata. */
    private Fixture fixture(String name) throws Exception {
        final AtomicLong clock = new AtomicLong(1_700_000_000_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final ServerlessNode node = new ServerlessNode(nodeSettings(name));
        node.start();
        node.setMetadataPlane(plane);
        return new Fixture(node, plane, clock, new BackgroundReconciler(node, plane));
    }

    /** Creates an index over HTTP, holds its shard, and writes two documents. */
    private void alpha(Fixture f) throws Exception {
        assertEquals(200, send(f.node(), "PUT", "/alpha", "{\"mappings\":" + MAPPING + "}").status());
        f.loop().want("alpha", 0);
        f.loop().tick(f.clock().get());
        assertEquals(201, send(f.node(), "PUT", "/alpha/_doc/1", "{\"msg\":\"hello world\",\"tag\":\"a\",\"n\":1}").status());
        assertEquals(201, send(f.node(), "PUT", "/alpha/_doc/2?refresh=true", "{\"msg\":\"hello again\",\"tag\":\"b\",\"n\":2}").status());
    }

    /** An index created over HTTP records when and by what; one made before that has neither and says so. */
    public void testCreationMetadataAndAliasShapes() throws Exception {
        try (Fixture f = fixture("creation")) {
            alpha(f);
            final Response got = send(f.node(), "GET", "/alpha", null);
            assertEquals(got.body(), 200, got.status());
            assertTrue("creation_date from the plane's clock: " + got.body(), got.body().contains("\"creation_date\":\"1700000000000\""));
            assertTrue("version.created nested as core renders it: " + got.body(), got.body().contains("\"version\":{\"created\":\""));
            assertTrue("aliases keyed by name, empty when none: " + got.body(), got.body().contains("\"aliases\":{}"));

            assertEquals(200, send(f.node(), "PUT", "/alpha/_alias/first", null).status());
            assertEquals(
                200,
                send(f.node(), "POST", "/_aliases", "{\"actions\":[{\"add\":{\"index\":\"alpha\",\"alias\":\"second\"}}]}").status()
            );
            // Core's shape for GET /_alias/{name}: keyed by index.
            final Response byName = send(f.node(), "GET", "/_alias/first", null);
            assertTrue(byName.body(), byName.body().contains("\"alpha\":{\"aliases\":{\"first\":{}}}"));
            // The reverse question, verified against each alias record.
            final Response reverse = send(f.node(), "GET", "/alpha/_alias", null);
            assertEquals(reverse.body(), 200, reverse.status());
            assertTrue(reverse.body(), reverse.body().contains("\"first\":{}") && reverse.body().contains("\"second\":{}"));
            assertTrue(
                "and on GET /{index}: " + send(f.node(), "GET", "/alpha", null).body(),
                send(f.node(), "GET", "/alpha", null).body().contains("\"second\":{}")
            );
            final Response resolved = send(f.node(), "GET", "/_resolve/index/alpha", null);
            assertTrue(
                "resolve lists an index's aliases: " + resolved.body(),
                resolved.body().contains("\"aliases\":[\"first\",\"second\"]")
            );
            // Removed from the alias, gone from the reverse answer -- the hint is verified, not trusted.
            assertEquals(200, send(f.node(), "DELETE", "/alpha/_alias/first", null).status());
            assertFalse(send(f.node(), "GET", "/alpha/_alias", null).body().contains("\"first\""));
            assertTrue(send(f.node(), "GET", "/alpha/_alias", null).body().contains("\"second\""));
        }
    }

    /** Stored scripts live in a register, and resolve everywhere a script id is accepted. */
    public void testStoredScripts() throws Exception {
        try (Fixture f = fixture("scripts")) {
            alpha(f);
            final Response put = send(
                f.node(),
                "PUT",
                "/_scripts/bump",
                "{\"script\":{\"lang\":\"painless\",\"source\":\"ctx._source.n += params.by\"}}"
            );
            assertEquals(put.body(), 200, put.status());
            final Response got = send(f.node(), "GET", "/_scripts/bump", null);
            assertTrue(
                "core's get shape: " + got.body(),
                got.body().contains("\"_id\":\"bump\",\"found\":true,\"script\":{\"lang\":\"painless\"")
            );
            final Response updated = send(
                f.node(),
                "POST",
                "/alpha/_update/1?refresh=true",
                "{\"script\":{\"id\":\"bump\",\"params\":{\"by\":10}}}"
            );
            assertEquals(updated.body(), 200, updated.status());
            assertTrue(send(f.node(), "GET", "/alpha/_doc/1", null).body().contains("\"n\":11"));
            // A stored script inside the query DSL resolves through the same store.
            assertEquals(
                200,
                send(f.node(), "PUT", "/_scripts/big", "{\"script\":{\"lang\":\"painless\",\"source\":\"doc['n'].value > params.min\"}}")
                    .status()
            );
            final Response searched = send(
                f.node(),
                "POST",
                "/alpha/_search",
                "{\"query\":{\"script\":{\"script\":{\"id\":\"big\",\"params\":{\"min\":5}}}}}"
            );
            assertEquals(searched.body(), 200, searched.status());
            assertTrue(searched.body(), searched.body().contains("\"value\":1") && searched.body().contains("\"_id\":\"1\""));
            assertEquals(200, send(f.node(), "DELETE", "/_scripts/big", null).status());
            assertEquals(404, send(f.node(), "GET", "/_scripts/big", null).status());
            final Response compiled = send(
                f.node(),
                "PUT",
                "/_scripts/broken/score",
                "{\"script\":{\"lang\":\"painless\",\"source\":\"this is not painless\"}}"
            );
            assertEquals(
                "a script that does not compile in its context is refused where it is stored: " + compiled.body(),
                400,
                compiled.status()
            );
        }
    }

    /** Search templates render through mustache and run through the same search a plain body does. */
    public void testSearchTemplates() throws Exception {
        try (Fixture f = fixture("templates")) {
            alpha(f);
            final Response rendered = send(
                f.node(),
                "POST",
                "/_render/template",
                "{\"source\":{\"query\":{\"match\":{\"msg\":\"{{word}}\"}}},\"params\":{\"word\":\"again\"}}"
            );
            assertEquals(rendered.body(), 200, rendered.status());
            assertTrue(rendered.body(), rendered.body().contains("\"template_output\":{\"query\":{\"match\":{\"msg\":\"again\"}}}"));
            final Response searched = send(
                f.node(),
                "POST",
                "/alpha/_search/template?typed_keys=true",
                "{\"source\":{\"query\":{\"match\":{\"msg\":\"{{word}}\"}}},\"params\":{\"word\":\"again\"}}"
            );
            assertEquals(searched.body(), 200, searched.status());
            assertTrue(
                "one hit, the second document: " + searched.body(),
                searched.body().contains("\"value\":1") && searched.body().contains("\"_id\":\"2\"")
            );
            // A stored template, by id.
            assertEquals(
                200,
                send(
                    f.node(),
                    "PUT",
                    "/_scripts/by-tag",
                    "{\"script\":{\"lang\":\"mustache\",\"source\":{\"query\":{\"term\":{\"tag\":\"{{t}}\"}}}}}"
                ).status()
            );
            final Response stored = send(f.node(), "POST", "/alpha/_search/template", "{\"id\":\"by-tag\",\"params\":{\"t\":\"a\"}}");
            assertEquals(stored.body(), 200, stored.status());
            assertTrue(stored.body(), stored.body().contains("\"_id\":\"1\""));
            final Response batch = send(
                f.node(),
                "POST",
                "/_msearch/template",
                "{\"index\":\"alpha\"}\n{\"id\":\"by-tag\",\"params\":{\"t\":\"a\"}}\n{\"index\":\"alpha\"}\n{\"source\":{\"query\":{\"match_all\":{}}}}\n"
            );
            assertEquals(batch.body(), 200, batch.status());
            assertTrue(batch.body(), batch.body().startsWith("{\"took\":") && batch.body().contains("\"status\":200"));
        }
    }

    /** Ranking evaluation, with the module's own precision metric. */
    public void testRankEval() throws Exception {
        try (Fixture f = fixture("rank-eval")) {
            alpha(f);
            final Response evaluated = send(
                f.node(),
                "POST",
                "/alpha/_rank_eval",
                "{\"requests\":[{\"id\":\"hello\",\"request\":{\"query\":{\"match\":{\"msg\":\"hello\"}}},"
                    + "\"ratings\":[{\"_index\":\"alpha\",\"_id\":\"1\",\"rating\":1},{\"_index\":\"alpha\",\"_id\":\"2\",\"rating\":0}]}],"
                    + "\"metric\":{\"precision\":{\"k\":2,\"relevant_rating_threshold\":1}}}"
            );
            assertEquals(evaluated.body(), 200, evaluated.status());
            assertTrue("one of two hits is relevant: " + evaluated.body(), evaluated.body().contains("\"metric_score\":0.5"));
            assertTrue(evaluated.body(), evaluated.body().contains("\"details\":{\"hello\":{"));
        }
    }

    /** A search pipeline's request processors narrow every shard's question; its response processors reshape the answer. */
    public void testSearchPipelines() throws Exception {
        try (Fixture f = fixture("pipelines")) {
            alpha(f);
            final Response put = send(
                f.node(),
                "PUT",
                "/_search/pipeline/only-b",
                "{\"request_processors\":[{\"filter_query\":{\"query\":{\"term\":{\"tag\":\"b\"}}}}],"
                    + "\"response_processors\":[{\"rename_field\":{\"field\":\"msg\",\"target_field\":\"text\"}}]}"
            );
            assertEquals(put.body(), 200, put.status());
            assertTrue(send(f.node(), "GET", "/_search/pipeline/only-b", null).body().contains("\"only-b\":{"));
            final Response searched = send(f.node(), "POST", "/alpha/_search?search_pipeline=only-b", "{\"query\":{\"match_all\":{}}}");
            assertEquals(searched.body(), 200, searched.status());
            assertTrue(
                "filtered to tag b: " + searched.body(),
                searched.body().contains("\"value\":1") && searched.body().contains("\"_id\":\"2\"")
            );
            assertTrue("and the field renamed: " + searched.body(), searched.body().contains("\"text\":\"hello again\""));
            final Response inline = send(
                f.node(),
                "POST",
                "/alpha/_search",
                "{\"query\":{\"match_all\":{}},\"search_pipeline\":{\"request_processors\":[{\"filter_query\":{\"query\":{\"term\":{\"tag\":\"a\"}}}}]}}"
            );
            assertEquals(inline.body(), 200, inline.status());
            assertTrue(
                "an inline pipeline too: " + inline.body(),
                inline.body().contains("\"value\":1") && inline.body().contains("\"_id\":\"1\"")
            );
            final Response unknown = send(f.node(), "PUT", "/_search/pipeline/bad", "{\"request_processors\":[{\"no_such\":{}}]}");
            assertEquals("an unknown processor is refused at PUT, naming what exists: " + unknown.body(), 400, unknown.status());
            assertTrue(unknown.body(), unknown.body().contains("filter_query"));
            assertEquals(200, send(f.node(), "DELETE", "/_search/pipeline/only-b", null).status());
            assertEquals(400, send(f.node(), "POST", "/alpha/_search?search_pipeline=only-b", "{\"query\":{\"match_all\":{}}}").status());
        }
    }

    /** Rollover: a new index behind the name, one compare-and-swap, conditions against what is recorded. */
    public void testRollover() throws Exception {
        try (Fixture f = fixture("rollover")) {
            assertEquals(200, send(f.node(), "PUT", "/logs-000001", "{\"mappings\":" + MAPPING + "}").status());
            f.loop().want("logs-000001", 0);
            f.loop().tick(f.clock().get());
            assertEquals(200, send(f.node(), "PUT", "/logs-000001/_alias/logs", null).status());
            assertEquals(201, send(f.node(), "PUT", "/logs-000001/_doc/1?refresh=true", "{\"msg\":\"one\"}").status());

            final Response notYet = send(f.node(), "POST", "/logs/_rollover", "{\"conditions\":{\"max_docs\":5}}");
            assertEquals(notYet.body(), 200, notYet.status());
            assertTrue(
                "condition not met, nothing rolled: " + notYet.body(),
                notYet.body().contains("\"rolled_over\":false") && notYet.body().contains("\"[max_docs: 5]\":false")
            );
            final Response dry = send(f.node(), "POST", "/logs/_rollover?dry_run=true", "{\"conditions\":{\"max_docs\":1}}");
            assertTrue(
                "a dry run names the index it would make: " + dry.body(),
                dry.body().contains("\"new_index\":\"logs-000002\"") && dry.body().contains("\"dry_run\":true")
            );
            assertEquals(404, send(f.node(), "GET", "/logs-000002", null).status());

            f.clock().addAndGet(3_600_000L);
            final Response rolled = send(f.node(), "POST", "/logs/_rollover", "{\"conditions\":{\"max_docs\":1,\"max_age\":\"30m\"}}");
            assertEquals(rolled.body(), 200, rolled.status());
            assertTrue(rolled.body(), rolled.body().contains("\"rolled_over\":true") && rolled.body().contains("\"[max_age: 30m]\":true"));
            assertEquals(200, send(f.node(), "GET", "/logs-000002", null).status());
            assertTrue(
                "the alias moved: " + send(f.node(), "GET", "/_alias/logs", null).body(),
                send(f.node(), "GET", "/_alias/logs", null).body().contains("\"logs-000002\"")
            );
            assertFalse(send(f.node(), "GET", "/_alias/logs", null).body().contains("\"logs-000001\""));
            assertEquals(
                "a size condition is refused, not answered as never met",
                501,
                send(f.node(), "POST", "/logs/_rollover", "{\"conditions\":{\"max_size\":\"1gb\"}}").status()
            );
        }
    }

    /** Data streams: an alias with a generation, written through to its newest backing index. */
    public void testDataStreams() throws Exception {
        try (Fixture f = fixture("streams")) {
            assertEquals(
                "a data stream needs a template declaring it, as in core",
                400,
                send(f.node(), "PUT", "/_data_stream/events", null).status()
            );
            assertEquals(
                200,
                send(
                    f.node(),
                    "PUT",
                    "/_index_template/events",
                    "{\"index_patterns\":[\"events*\"],\"data_stream\":{},\"template\":{\"mappings\":" + MAPPING + "}}"
                ).status()
            );
            final Response created = send(f.node(), "PUT", "/_data_stream/events", null);
            assertEquals(created.body(), 200, created.status());
            final Response described = send(f.node(), "GET", "/_data_stream/events", null);
            assertEquals(described.body(), 200, described.status());
            assertTrue(
                "core's shape: " + described.body(),
                described.body().contains("\"name\":\"events\"")
                    && described.body().contains("\"index_name\":\".ds-events-000001\"")
                    && described.body().contains("\"generation\":1")
            );
            assertTrue(
                "the timestamp field is mapped on the backing index: " + send(f.node(), "GET", "/.ds-events-000001/_mapping", null).body(),
                send(f.node(), "GET", "/.ds-events-000001/_mapping", null).body().contains("\"@timestamp\":{\"type\":\"date\"}")
            );

            f.loop().want(".ds-events-000001", 0);
            f.loop().tick(f.clock().get());
            final Response written = send(
                f.node(),
                "POST",
                "/events/_create/1?refresh=true",
                "{\"@timestamp\":\"2026-09-04T00:00:00Z\",\"msg\":\"first\"}"
            );
            assertEquals(written.body(), 201, written.status());
            assertEquals(
                "only creates through the name, as in core",
                400,
                send(f.node(), "PUT", "/events/_doc/2", "{\"msg\":\"x\"}").status()
            );
            final Response bulk = send(
                f.node(),
                "POST",
                "/_bulk?refresh=true",
                "{\"create\":{\"_index\":\"events\",\"_id\":\"3\"}}\n{\"@timestamp\":\"2026-09-04T00:00:01Z\",\"msg\":\"third\"}\n"
            );
            assertTrue(bulk.body(), bulk.body().contains("\"status\":201"));
            final Response searched = send(f.node(), "GET", "/events/_search?q=msg:first", null);
            assertTrue("searched through the name: " + searched.body(), searched.body().contains("\"_index\":\".ds-events-000001\""));

            final Response rolled = send(f.node(), "POST", "/events/_rollover", null);
            assertEquals(rolled.body(), 200, rolled.status());
            assertTrue(
                rolled.body(),
                rolled.body().contains("\"new_index\":\".ds-events-000002\"") && rolled.body().contains("\"rolled_over\":true")
            );
            assertTrue(send(f.node(), "GET", "/_data_stream/events", null).body().contains("\"generation\":2"));
            f.loop().want(".ds-events-000002", 0);
            f.loop().tick(f.clock().get());
            final Response afterRoll = send(
                f.node(),
                "POST",
                "/events/_create/9?refresh=true",
                "{\"@timestamp\":\"2026-09-04T00:00:02Z\",\"msg\":\"ninth\"}"
            );
            assertTrue(
                "a write after rollover lands in the new backing index: " + afterRoll.body(),
                afterRoll.body().contains("\"_index\":\".ds-events-000002\"")
            );
            assertTrue(send(f.node(), "GET", "/.ds-events-000002/_doc/9", null).body().contains("\"found\":true"));
            assertTrue(
                "and a search covers both: " + send(f.node(), "GET", "/events/_search?q=msg:first%20OR%20msg:ninth", null).body(),
                send(f.node(), "GET", "/events/_search?q=msg:first%20OR%20msg:ninth", null).body().contains("\"value\":2")
            );

            assertEquals(200, send(f.node(), "DELETE", "/_data_stream/events", null).status());
            assertEquals(404, send(f.node(), "GET", "/_data_stream/events", null).status());
            assertEquals(404, send(f.node(), "GET", "/.ds-events-000001", null).status());
        }
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
