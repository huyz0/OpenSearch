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
 * M59: the classic alias spellings, and two questions that never needed a cluster.
 *
 * <p><b>The alias API was the last invented shape on this surface.</b> Standard OpenSearch and AWS both use
 * {@code PUT /{index}/_alias/{name}} and {@code POST /_aliases}; this shell served a name-scoped API of its
 * own and refused those — the category M47 cleared out everywhere else, surviving because M47's audit
 * predated noticing it.
 *
 * <p><b>{@code POST /_aliases} is served for the case it exists to serve.</b> Its purpose is the atomic move:
 * remove an alias from yesterday's index, add it to today's, so a caller searching it sees one or the other
 * and never neither. Every action on <em>one</em> alias touches one register, so the whole move is a single
 * compare-and-swap and is genuinely atomic. Actions spanning several aliases are several registers with no
 * transaction over them, and serving that would be a different operation wearing the same name.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessAliasShapeTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private int port;

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-alias-shapes")
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

    private ServerlessNode running(MetadataPlane plane, String name) throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings(name));
        node.start();
        node.setMetadataPlane(plane);
        port = node.boundHttpAddress().publishAddress().getPort();
        call("PUT", "/logs-a", "{\"settings\":{\"number_of_shards\":1}}");
        call("PUT", "/logs-b", "{\"settings\":{\"number_of_shards\":1}}");
        return node;
    }

    /** The index-scoped spellings: attach, ask, detach — and the name-scoped ones still work. */
    public void testTheIndexScopedAliasSpellings() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "alias-scoped")) {
            assertNotNull(node);

            assertEquals(200, call("PUT", "/logs-a/_alias/current", null).status());

            final Answer membership = call("GET", "/logs-a/_alias/current", null);
            assertEquals(membership.body(), 200, membership.status());
            assertTrue(
                "keyed by index, as OpenSearch keys it: " + membership.body(),
                membership.has("\"logs-a\":{\"aliases\":{\"current\"")
            );

            assertEquals("HEAD answers with a status", 200, call("HEAD", "/logs-a/_alias/current", null).status());
            assertEquals("an index the alias does not cover is a 404", 404, call("HEAD", "/logs-b/_alias/current", null).status());

            // Attaching a second index adds to the set rather than replacing it: an alias is a set.
            assertEquals(200, call("PUT", "/logs-b/_alias/current", null).status());
            final Answer both = call("GET", "/_alias/current", null);
            assertTrue(both.body(), both.has("logs-a") && both.has("logs-b"));

            assertEquals(200, call("DELETE", "/logs-a/_alias/current", null).status());
            final Answer one = call("GET", "/_alias/current", null);
            assertTrue("the detached index is gone: " + one.body(), one.has("logs-b"));
            assertFalse(one.body(), one.has("logs-a"));

            assertEquals("aliasing an index that is not there is a 404", 404, call("PUT", "/ghost/_alias/x", null).status());
        }
    }

    /**
     * Removing the last index removes the alias.
     *
     * <p>An alias standing for nothing resolves to nothing, which reads exactly like an empty index — the
     * failure this whole surface is arranged to avoid, and one that would sit there until somebody noticed.
     */
    public void testAnAliasOverNothingIsRemoved() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "alias-empty")) {
            assertNotNull(node);
            call("PUT", "/logs-a/_alias/only", null);
            assertEquals(200, call("DELETE", "/logs-a/_alias/only", null).status());
            assertEquals("the alias must be gone, not standing for nothing", 404, call("GET", "/_alias/only", null).status());
        }
    }

    /**
     * The atomic move, which is what {@code POST /_aliases} is for.
     *
     * <p>Both actions touch one register, so the move is one compare-and-swap: a caller searching the alias
     * throughout sees yesterday's index or today's and never neither.
     */
    public void testTheAtomicMoveIsServed() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "alias-move")) {
            assertNotNull(node);
            call("PUT", "/logs-a/_alias/current", null);

            final Answer moved = call(
                "POST",
                "/_aliases",
                "{\"actions\":[{\"remove\":{\"index\":\"logs-a\",\"alias\":\"current\"}},"
                    + "{\"add\":{\"index\":\"logs-b\",\"alias\":\"current\"}}]}"
            );
            assertEquals(moved.body(), 200, moved.status());

            final Answer after = call("GET", "/_alias/current", null);
            assertTrue("the alias points at the new index: " + after.body(), after.has("logs-b"));
            assertFalse("and not the old one: " + after.body(), after.has("logs-a"));
        }
    }

    /**
     * Actions spanning several aliases are refused, because they could apply partly.
     *
     * <p>Serving them would be a different operation wearing the same name — the objection that also keeps
     * {@code PUT /_settings} off patterns.
     */
    public void testActionsSpanningSeveralAliasesAreRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "alias-multi")) {
            assertNotNull(node);
            final Answer refused = call(
                "POST",
                "/_aliases",
                "{\"actions\":[{\"add\":{\"index\":\"logs-a\",\"alias\":\"one\"}},{\"add\":{\"index\":\"logs-b\",\"alias\":\"two\"}}]}"
            );
            assertEquals(refused.body(), 501, refused.status());
            assertTrue("naming why: " + refused.body(), refused.has("could apply partly"));
            assertTrue("and what to do instead: " + refused.body(), refused.has("one request per alias"));

            // Nothing was applied, which is what "could apply partly" is refusing to risk.
            assertEquals(404, call("GET", "/_alias/one", null).status());
            assertEquals(404, call("GET", "/_alias/two", null).status());
        }
    }

    /** Validating a query parses it, and says that is all it did. */
    public void testValidatingAQueryParsesIt() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "validate")) {
            assertNotNull(node);

            final Answer good = call("POST", "/logs-a/_validate/query", "{\"query\":{\"match_all\":{}}}");
            assertEquals(good.body(), 200, good.status());
            assertTrue(good.body(), good.has("\"valid\":true"));
            // Said plainly, because "valid" is narrower here than a caller may assume.
            assertTrue("the limit of the answer is stated: " + good.body(), good.has("parsing only"));

            final Answer bad = call("POST", "/logs-a/_validate/query?explain=true", "{\"query\":{\"nope\":{}}}");
            assertEquals("an unparseable query is still a 200 saying invalid: " + bad.body(), 200, bad.status());
            assertTrue(bad.body(), bad.has("\"valid\":false"));
            assertTrue("and explain says what was wrong: " + bad.body(), bad.has("unknown query [nope]"));

            final Answer without = call("POST", "/logs-a/_validate/query", "{\"query\":{\"nope\":{}}}");
            assertFalse("without explain, no explanation: " + without.body(), without.has("explanations"));

            assertEquals(404, call("POST", "/ghost/_validate/query", "{\"query\":{\"match_all\":{}}}").status());
        }
    }

    /** Resolving names says which are indices and which are aliases. */
    public void testResolvingNames() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "resolve")) {
            assertNotNull(node);
            call("PUT", "/logs-a/_alias/current", null);

            final Answer byPrefix = call("GET", "/_resolve/index/logs-*", null);
            assertEquals(byPrefix.body(), 200, byPrefix.status());
            assertTrue(byPrefix.body(), byPrefix.has("\"name\":\"logs-a\""));
            assertTrue(byPrefix.body(), byPrefix.has("\"name\":\"logs-b\""));

            final Answer alias = call("GET", "/_resolve/index/current", null);
            assertTrue(
                "an alias is reported as one, with what it covers: " + alias.body(),
                alias.has("\"aliases\":[{\"name\":\"current\"")
            );
            assertTrue(alias.body(), alias.has("logs-a"));

            final Answer nothing = call("GET", "/_resolve/index/ghost", null);
            assertEquals(nothing.body(), 200, nothing.status());
            assertTrue("a name that is neither is simply absent from both: " + nothing.body(), nothing.has("\"indices\":[]"));
            // Present and empty rather than absent, so a client walking all three does not branch.
            assertTrue(nothing.body(), nothing.has("\"data_streams\":[]"));
        }
    }

    /** What is left refused says what it actually lacks. */
    public void testWhatIsStillRefusedSaysWhy() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "alias-refused")) {
            assertNotNull(node);

            // Served since M62: a body-less evaluation is a bad request, not a refusal and not a missing index.
            final Answer rankEval = call("GET", "/logs-a/_rank_eval", null);
            assertEquals(rankEval.body(), 400, rankEval.status());
            assertTrue(rankEval.body(), rankEval.has("could not parse the evaluation"));

            // Enumerating every alias is still the inventory operation this design refuses.
            assertEquals(501, call("GET", "/_alias", null).status());
        }
    }
}
