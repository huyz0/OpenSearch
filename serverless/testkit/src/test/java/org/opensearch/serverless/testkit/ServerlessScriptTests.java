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
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * M57: scripts, and the endpoints that used to look like typos.
 *
 * <p><b>"No modules are loaded" was never "no module code is used."</b> The shell has always depended on
 * {@code modules:transport-netty4} directly — it picks its transport by name rather than discovering it from
 * disk. Scripting is the same choice: {@code modules:lang-painless} is now a dependency and its engine is
 * registered directly. Nothing about the no-discovery decision changed; a second thing was chosen by name.
 *
 * <p><b>Inline scripts work; stored scripts do not, and the reason is structural.</b> {@code ScriptService}
 * is a {@code ClusterStateApplier} and reads stored scripts out of cluster metadata. There is no cluster
 * state here, so a stored script has nowhere to be looked up from — which is the same call AWS OpenSearch
 * Serverless makes, supporting inline scripts and refusing {@code /_scripts}.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessScriptTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private int port;

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-scripts")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
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

    private static MetadataPlane plane(AtomicLong clock, Path store) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, store, false), BlobPath.cleanPath(), clock::get, TTL);
    }

    private ServerlessNode running(MetadataPlane plane, AtomicLong clock, String name) throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings(name));
        node.start();
        node.setMetadataPlane(plane);
        port = node.boundHttpAddress().publishAddress().getPort();
        call(
            "PUT",
            "/alpha",
            "{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}}"
        );
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());
        call("PUT", "/alpha/_doc/1", "{\"msg\":\"one\",\"n\":10}");
        call("PUT", "/alpha/_doc/2?refresh=true", "{\"msg\":\"two\",\"n\":20}");
        return node;
    }

    /** A script decides which documents match, and the answer is the documents it chose. */
    public void testAScriptCanDecideWhatMatches() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "script-query")) {
            assertNotNull(node);
            final Answer filtered = call(
                "POST",
                "/alpha/_search",
                "{\"query\":{\"script\":{\"script\":{\"source\":\"doc['n'].value > 15\"}}}}"
            );
            assertEquals(filtered.body(), 200, filtered.status());
            assertTrue("only the document the script chose: " + filtered.body(), filtered.has("\"value\":1"));
            assertTrue(filtered.body(), filtered.has("\"_id\":\"2\""));
        }
    }

    /** A script decides the score, and the ordering follows it. */
    public void testAScriptCanDecideTheScore() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "script-score")) {
            assertNotNull(node);
            final Answer scored = call(
                "POST",
                "/alpha/_search",
                "{\"query\":{\"script_score\":{\"query\":{\"match_all\":{}},\"script\":{\"source\":\"doc['n'].value * 1.0\"}}}}"
            );
            assertEquals(scored.body(), 200, scored.status());
            assertTrue("the score is the script's value: " + scored.body(), scored.has("\"max_score\":20.0"));
            // Highest first, which is only true if the script actually drove the ranking.
            assertTrue(scored.body(), scored.body().indexOf("\"_id\":\"2\"") < scored.body().indexOf("\"_id\":\"1\""));
        }
    }

    /**
     * A computed field is returned, not just computed.
     *
     * <p>The first version of this work registered the engine and left the renderer alone: {@code script_fields}
     * ran, produced values, and the response carried hits with nothing in them. That looks like the script
     * returned nothing rather than like the renderer dropped it — a wrong answer wearing the shape of an
     * empty one.
     */
    public void testAComputedFieldComesBack() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "script-fields")) {
            assertNotNull(node);
            final Answer computed = call(
                "POST",
                "/alpha/_search",
                "{\"query\":{\"match_all\":{}},\"script_fields\":{\"double_n\":{\"script\":{\"source\":\"doc['n'].value * 2\"}}}}"
            );
            assertEquals(computed.body(), 200, computed.status());
            assertTrue("the computed value must reach the client: " + computed.body(), computed.has("\"double_n\":[20]"));
            assertTrue(computed.body(), computed.has("\"double_n\":[40]"));
        }
    }

    /** An aggregation over a script. */
    public void testAnAggregationCanRunAScript() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "script-agg")) {
            assertNotNull(node);
            final Answer summed = call(
                "POST",
                "/alpha/_search",
                "{\"size\":0,\"aggs\":{\"total\":{\"sum\":{\"script\":{\"source\":\"doc['n'].value\"}}}}}"
            );
            assertEquals(summed.body(), 200, summed.status());
            assertTrue("10 + 20: " + summed.body(), summed.has("\"value\":30.0"));
        }
    }

    /**
     * A scripted update edits the document in place, and {@code ctx.op} decides what happens.
     *
     * <p>{@code op} is the reason a caller reaches for a scripted update rather than a partial document: the
     * decision to write, to skip or to delete is made against the document as it is, inside the same
     * operation, rather than in a client that has to read first and race with everyone else.
     */
    public void testAScriptedUpdateEditsDecidesAndCanDelete() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "script-update")) {
            assertNotNull(node);

            final Answer edited = call("POST", "/alpha/_update/1", "{\"script\":{\"source\":\"ctx._source.n += 5\"}}");
            assertEquals(edited.body(), 200, edited.status());
            assertTrue(edited.body(), edited.has("\"result\":\"updated\""));
            assertTrue("the edit must reach the document: ", call("GET", "/alpha/_doc/1", null).has("\"n\":15"));

            // params, so the same script serves many calls.
            call("POST", "/alpha/_update/1", "{\"script\":{\"source\":\"ctx._source.n += params.by\",\"params\":{\"by\":100}}}");
            assertTrue("params must reach the script: ", call("GET", "/alpha/_doc/1", null).has("\"n\":115"));

            final Answer skipped = call("POST", "/alpha/_update/1", "{\"script\":{\"source\":\"ctx.op = 'noop'\"}}");
            assertTrue("ctx.op=noop writes nothing: " + skipped.body(), skipped.has("\"result\":\"noop\""));

            final Answer removed = call("POST", "/alpha/_update/2", "{\"script\":{\"source\":\"ctx.op = 'delete'\"}}");
            assertTrue("ctx.op=delete removes it: " + removed.body(), removed.has("\"result\":\"deleted\""));
            assertEquals("and it is gone", 404, call("GET", "/alpha/_doc/2", null).status());
        }
    }

    /** A script that will not compile is the caller's mistake, reported as core words it. */
    public void testABadScriptIsTheCallersMistake() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "script-bad")) {
            assertNotNull(node);
            final Answer broken = call(
                "POST",
                "/alpha/_search",
                "{\"query\":{\"script\":{\"script\":{\"source\":\"this is not painless\"}}}}"
            );
            assertEquals(broken.body(), 400, broken.status());
            assertTrue("core's own compile error, pointing at the offending source: " + broken.body(), broken.has("compile error"));
            assertTrue(broken.body(), broken.has("script_stack"));
        }
    }

    /**
     * Stored scripts are refused, and the reason is the one that is true now.
     *
     * <p>The refusal used to say no engine was registered. One is. What is missing is somewhere for
     * {@code ScriptService} to look a stored script up, which is cluster metadata.
     */
    public void testStoredScriptsRefuseForTheStructuralReason() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "script-stored")) {
            assertNotNull(node);
            // Served since M62: stored scripts live in a register of their own, and the script service
            // resolves an id from it rather than from cluster state.
            final Answer stored = call("PUT", "/_scripts/mine", "{\"script\":{\"lang\":\"painless\",\"source\":\"ctx._source.n = 5\"}}");
            assertEquals(stored.body(), 200, stored.status());

            // Referring to one from an update is refused by name rather than compiled into nothing.
            final Answer byId = call("POST", "/alpha/_update/1", "{\"script\":{\"id\":\"mine\"}}");
            assertEquals(byId.body(), 200, byId.status());
            final Answer missing = call("POST", "/alpha/_update/1", "{\"script\":{\"id\":\"nowhere\"}}");
            assertEquals(missing.body(), 404, missing.status());
            assertTrue(missing.body(), missing.has("unable to find script [nowhere]"));
        }
    }

    /**
     * An API this shell does not implement says so, rather than reporting a missing index, node or repository.
     *
     * <p><b>This is the class of bug, not one bug.</b> {@code GET /{index}} matches any single-segment path,
     * so every unrouted top-level API was answered "no such index: _stats" — a confident wrong answer about
     * something that was never an index, sending a caller to look for a missing index instead of a missing
     * endpoint. M50 fixed it for {@code /_search} by routing {@code /_search}, which fixes one path and leaves
     * the next. The same shape existed for node ids and repository names.
     */
    public void testAnUnimplementedApiIsNotReportedAsAMissingIndex() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "no-typos")) {
            assertNotNull(node);

            for (String path : new String[] {
                "/_stats",
                "/_data_stream",
                "/_dangling",
                "/_remote/info",
                "/alpha/_stats",
                "/alpha/_close",
                "/alpha/_open",
                "/alpha/_recovery",
                "/alpha/_shrink/beta",
                "/_cat/master",
                "/_cat/plugins",
                "/_cat/snapshots",
                "/_cluster/pending_tasks",
                "/_scripts/painless/_execute",
                "/_index_template/_simulate_index/alpha" }) {
                final Answer answer = call("GET", path, null);
                assertEquals(path + " must refuse explicitly: " + answer.body(), 501, answer.status());
                assertFalse(path + " must not be read as an index: " + answer.body(), answer.has("no such index"));
                assertFalse(path + " must not look like a typo: " + answer.body(), answer.has("no handler found"));
            }

            // Node ids and repository names had the same shape.
            final Answer selector = call("GET", "/_nodes/_local", null);
            assertEquals(selector.body(), 501, selector.status());
            assertTrue("a selector is not a node: " + selector.body(), selector.has("node selector"));

            final Answer subApi = call("GET", "/_nodes/hot_threads", null);
            assertEquals(subApi.body(), 501, subApi.status());
            // Its own refusal now, with the same substance: not "no live node matches [hot_threads]".
            assertTrue("a node API is not a node id: " + subApi.body(), subApi.has("not_implemented") && subApi.has("_serverless/stats"));

            final Answer repo = call("GET", "/_snapshot/_status", null);
            assertEquals(repo.body(), 501, repo.status());
            assertFalse("not a missing repository: " + repo.body(), repo.has("no such repository"));

            // And a real index that is not there is still a 404, which is the distinction the whole change
            // exists to preserve.
            assertEquals("a genuinely missing index is still a 404", 404, call("GET", "/no-such-index", null).status());
        }
    }
}
