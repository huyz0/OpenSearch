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
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * M50: what an ordinary OpenSearch client does, done against this shell.
 *
 * <p><b>What the audit these tests came from found.</b> The shell's stated posture is an explicit
 * allowlist: an unimplemented endpoint returns 501 with a reason, and nothing silently returns a wrong
 * answer. Reading every handler's routes and then driving a running node over HTTP found three groups of
 * things that had drifted off it — a create-index call that succeeded while discarding most of what it was
 * asked for, refusals whose stated reasons had stopped being true when M48 shipped conditional writes, and
 * a dozen endpoints that fell through to core's default 400 instead of the promised 501.
 *
 * <p><b>Why these tests go over HTTP.</b> Every finding was about what a client sees, and a test that calls
 * the handler directly cannot see a route that is not registered, a parameter that is not consumed, or a
 * status that core rewrites on the way out. The previous audit's own lesson was that assuming from names
 * gets it wrong; this suite refuses to assume from method signatures for the same reason.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessClientCompatibilityTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private int port;

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-client-compat")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** One HTTP exchange, as a status and a body. */
    private record Answer(int status, String body) {
        boolean has(String fragment) {
            return body.contains(fragment);
        }
    }

    private Answer call(String method, String path, String body) throws Exception {
        final HttpRequest.BodyPublisher payload = body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(body);
        // A client per call, closed with the call. The suite's thread-leak detector counts an HttpClient's
        // workers as leaked threads, which is what the rest of this testkit already works around this way.
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                    .header("Content-Type", "application/json")
                    .method(method, payload)
                    .timeout(Duration.ofSeconds(30))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
            return new Answer(response.statusCode(), response.body());
        }
    }

    private ServerlessNode serving(MetadataPlane plane, String name) throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings(name));
        node.start();
        node.setMetadataPlane(plane);
        port = node.boundHttpAddress().publishAddress().getPort();
        return node;
    }

    private static MetadataPlane plane(AtomicLong clock, java.nio.file.Path store) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, store, false), BlobPath.cleanPath(), clock::get, TTL);
    }

    /**
     * The create-index body an OpenSearch client actually sends is honoured, shard count included.
     *
     * <p><b>This was the one place on the surface where a caller was misled rather than refused.</b> The
     * shard count came from a {@code ?shards=} query parameter and the whole body was taken as the mapping,
     * so this request produced a one-shard index whose mapping was the envelope — and answered
     * {@code acknowledged} and {@code shards_acknowledged}, both true. Shard count is fixed at creation, so
     * the loss was not recoverable without deleting the index and its data.
     */
    public void testTheStandardCreateIndexEnvelopeIsHonoured() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = serving(plane(clock, createTempDir()), "compat-create")) {
            assertNotNull(node);
            final Answer created = call(
                "PUT",
                "/alpha",
                "{\"settings\":{\"number_of_shards\":3},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"}}}}"
            );
            assertEquals(created.body(), 200, created.status());

            final Answer read = call("GET", "/alpha", null);
            assertTrue("the shard count asked for must be the shard count made: " + read.body(), read.has("\"number_of_shards\":\"3\""));
            assertTrue("the mapping must be the mapping, not the envelope: " + read.body(), read.has("\"msg\":{\"type\":\"text\"}"));
            assertFalse(
                "the envelope must not have been stored as a mapping: " + read.body(),
                read.has("\"settings\":{\"number_of_shards\"")
            );
        }
    }

    /**
     * A replica count is refused with the reason it is refused for, rather than accepted and ignored.
     *
     * <p>Recording a replica count nothing acts on would report a redundancy this deployment does not
     * provide that way, which is the same failure as losing the shard count — quieter, and no more honest.
     */
    public void testAReplicaCountIsRefusedRatherThanIgnored() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = serving(plane(clock, createTempDir()), "compat-replicas")) {
            assertNotNull(node);
            final Answer refused = call("PUT", "/beta", "{\"settings\":{\"number_of_replicas\":1}}");
            assertEquals(refused.body(), 501, refused.status());
            assertTrue("the refusal must say why: " + refused.body(), refused.has("durability comes from"));
            assertTrue("and render the real error object: " + refused.body(), refused.has("\"root_cause\""));

            // Zero is not a refusal: a client that spells out what this deployment already provides is
            // asking for nothing it cannot have.
            final Answer allowed = call("PUT", "/beta", "{\"settings\":{\"number_of_replicas\":0,\"number_of_shards\":2}}");
            assertEquals(allowed.body(), 200, allowed.status());
        }
    }

    /** The body shape this handler has always taken — a bare mapping, with {@code ?shards=} — still works. */
    public void testTheOlderBareMappingBodyStillWorks() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = serving(plane(clock, createTempDir()), "compat-legacy")) {
            assertNotNull(node);
            assertEquals(200, call("PUT", "/gamma?shards=2", "{\"properties\":{\"msg\":{\"type\":\"text\"}}}").status());
            final Answer read = call("GET", "/gamma", null);
            assertTrue("the query parameter must still set the shard count: " + read.body(), read.has("\"number_of_shards\":\"2\""));
            assertTrue("and the bare body must still be the mapping: " + read.body(), read.has("\"msg\":{\"type\":\"text\"}"));
        }
    }

    /**
     * An index reports its mapping and settings, on {@code GET /{index}} and on the two narrower paths.
     *
     * <p>It used to answer {@code "has_mapping": true} — a flag saying a mapping existed without letting the
     * caller see it — and with {@code _mapping} and {@code _settings} both unrouted there was no path by
     * which a client could read back anything it had configured.
     */
    public void testAnIndexReportsWhatItWasConfiguredWith() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = serving(plane(clock, createTempDir()), "compat-read-back")) {
            assertNotNull(node);
            call("PUT", "/alpha", "{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"}}}}");

            final Answer full = call("GET", "/alpha", null);
            assertTrue("the answer must be keyed by index name, as OpenSearch keys it: " + full.body(), full.has("{\"alpha\":{"));
            assertFalse("the invented flag must be gone: " + full.body(), full.has("has_mapping"));

            final Answer mapping = call("GET", "/alpha/_mapping", null);
            assertEquals(mapping.body(), 200, mapping.status());
            assertTrue(mapping.has("\"msg\":{\"type\":\"text\"}"));
            assertFalse("_mapping must not carry settings: " + mapping.body(), mapping.has("number_of_shards"));

            final Answer settings = call("GET", "/alpha/_settings", null);
            assertEquals(settings.body(), 200, settings.status());
            assertTrue(settings.has("\"number_of_shards\":\"1\""));
            assertFalse("_settings must not carry the mapping: " + settings.body(), settings.has("\"msg\""));
        }
    }

    /** {@code HEAD /{index}} answers the question it exists to answer. It used to be a 405. */
    public void testHeadSaysWhetherAnIndexExists() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = serving(plane(clock, createTempDir()), "compat-head")) {
            assertNotNull(node);
            call("PUT", "/alpha", "{\"settings\":{\"number_of_shards\":1}}");
            assertEquals("an index that exists is a 200", 200, call("HEAD", "/alpha", null).status());
            assertEquals("one that does not is a 404, not a 405", 404, call("HEAD", "/nope", null).status());
        }
    }

    /**
     * Documents written the four ways a client writes them: with an id, without one, create-if-absent, and
     * create-when-present.
     *
     * <p>{@code create} is one constant on the call the write path already makes — {@code MATCH_DELETED}
     * rather than {@code MATCH_ANY} — so the engine does the comparison under the per-document lock it
     * already holds. Nothing reads first, which would be a race rather than a check.
     */
    public void testTheFourWaysAClientWritesADocument() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = serving(plane, "compat-write")) {
            call("PUT", "/alpha", "{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"}}}}");
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            final Answer withId = call("PUT", "/alpha/_doc/k1", "{\"msg\":\"one\"}");
            assertEquals(withId.body(), 201, withId.status());

            final Answer generated = call("POST", "/alpha/_doc", "{\"msg\":\"auto\"}");
            assertEquals("an id the caller did not supply must be generated: " + generated.body(), 201, generated.status());
            assertTrue("and reported, since it is the only way the caller learns it: " + generated.body(), generated.has("\"_id\":\""));

            final Answer created = call("PUT", "/alpha/_create/k2", "{\"msg\":\"new\"}");
            assertEquals(created.body(), 201, created.status());

            final Answer again = call("PUT", "/alpha/_create/k2", "{\"msg\":\"again\"}");
            assertEquals("creating a document that exists is a conflict, not an overwrite: " + again.body(), 409, again.status());
            assertTrue(again.has("version_conflict_engine_exception"));

            // And the refusal was a refusal: the first write is still what is stored.
            assertTrue(call("GET", "/alpha/_doc/k2", null).has("\"msg\":\"new\""));
        }
    }

    /**
     * {@code _bulk} does create and per-item conditions, and says something true about what it still cannot do.
     *
     * <p><b>The refusals here were the ones that had gone false.</b> M48 gave the single-document path
     * conditional writes; {@code _bulk} went on telling callers that {@code create} "requires
     * version-conditional writes, which this system does not have" and that "every write is a plain
     * overwrite". Both were true when written and neither was true afterwards. The obstacle was never a
     * missing capability — it was that the batch path took the write-ahead <em>log</em> record as its input
     * type, and a log record describes what happened, so there was nowhere in it for a condition.
     */
    public void testBulkDoesCreateAndConditions() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = serving(plane, "compat-bulk")) {
            call("PUT", "/alpha", "{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"}}}}");
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            final Answer creates = call(
                "POST",
                "/_bulk",
                "{\"create\":{\"_index\":\"alpha\",\"_id\":\"b1\"}}\n{\"msg\":\"first\"}\n"
                    + "{\"create\":{\"_index\":\"alpha\",\"_id\":\"b1\"}}\n{\"msg\":\"second\"}\n"
            );
            assertEquals(creates.body(), 200, creates.status());
            assertTrue("the action key must be the one the caller sent: " + creates.body(), creates.has("\"create\":{"));
            assertTrue("the first create must succeed: " + creates.body(), creates.has("\"status\":201"));
            assertTrue("the second must lose, as a conflict: " + creates.body(), creates.has("\"status\":409"));
            assertTrue(creates.has("version_conflict_engine_exception"));
            assertTrue("the item that lost must not have overwritten: ", call("GET", "/alpha/_doc/b1", null).has("\"msg\":\"first\""));

            final Answer stale = call(
                "POST",
                "/_bulk",
                "{\"index\":{\"_index\":\"alpha\",\"_id\":\"b1\",\"if_seq_no\":999,\"if_primary_term\":1}}\n{\"msg\":\"stale\"}\n"
            );
            assertTrue("a stale condition must be refused, not ignored: " + stale.body(), stale.has("\"status\":409"));
            assertTrue("and the document must be untouched", call("GET", "/alpha/_doc/b1", null).has("\"msg\":\"first\""));

            // The condition that does hold goes through, which is what makes the refusal above meaningful
            // rather than a blanket rejection of anything carrying a condition.
            final String current = call("GET", "/alpha/_doc/b1", null).body();
            final long seqNo = Long.parseLong(current.split("\"_seq_no\":")[1].split("[,}]")[0]);
            final long term = Long.parseLong(current.split("\"_primary_term\":")[1].split("[,}]")[0]);
            final Answer fresh = call(
                "POST",
                "/_bulk",
                "{\"index\":{\"_index\":\"alpha\",\"_id\":\"b1\",\"if_seq_no\":"
                    + seqNo
                    + ",\"if_primary_term\":"
                    + term
                    + "}}\n"
                    + "{\"msg\":\"conditioned\"}\n"
            );
            assertTrue("a condition that holds must be honoured: " + fresh.body(), fresh.has("\"status\":200"));
            assertTrue(call("GET", "/alpha/_doc/b1", null).has("\"msg\":\"conditioned\""));
        }
    }

    /**
     * The two things {@code _bulk} still refuses say what is actually true of them.
     *
     * <p>{@code update} is a partial merge, which has to read the current document before it can write one;
     * the batch path applies without reading. External versioning asks for a version model this system does
     * not keep, which is the same refusal the single-document path makes.
     */
    public void testWhatBulkStillRefusesSaysWhy() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = serving(plane, "compat-bulk-refused")) {
            call("PUT", "/alpha", "{\"settings\":{\"number_of_shards\":1}}");
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            final Answer update = call("POST", "/_bulk", "{\"update\":{\"_index\":\"alpha\",\"_id\":\"u1\"}}\n{\"doc\":{\"msg\":\"x\"}}\n");
            assertTrue("the refusal must name the real obstacle: " + update.body(), update.has("has to read the current"));
            assertFalse("and must not repeat the claim that went false: " + update.body(), update.has("does not have"));
            assertTrue("and must point at what does work: " + update.body(), update.has("_update/{id}"));

            final Answer external = call(
                "POST",
                "/_bulk",
                "{\"index\":{\"_index\":\"alpha\",\"_id\":\"u1\",\"_version\":4}}\n{\"msg\":\"x\"}\n"
            );
            assertTrue("external versioning is still refused: " + external.body(), external.has("external versioning"));
            assertTrue("pointing at the conditions that do work: " + external.body(), external.has("if_seq_no"));
        }
    }

    /**
     * An endpoint this shell does not implement says so, with a reason, at 501.
     *
     * <p>Ten paths did this. Everything else a client reaches for fell through to core's default handler and
     * came back as {@code 400 {"error":"no handler found for uri ..."}} — a bare string, the wrong status,
     * and indistinguishable from a typo. The reason matters as much as the status: "not here" and "not here
     * <em>because</em>" are different answers, and only the second tells a caller what to do instead.
     */
    public void testUnimplementedEndpointsRefuseWithAReason() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = serving(plane(clock, createTempDir()), "compat-501")) {
            assertNotNull(node);
            call("PUT", "/alpha", "{\"settings\":{\"number_of_shards\":1}}");

            for (String[] expected : new String[][] {
                { "/alpha/_count", "hits.total" },
                { "/alpha/_refresh", "refresh=true" },
                { "/_msearch", "one search per request" },
                { "/_reindex", "search_after" },
                { "/alpha/_update_by_query", "search_after" },
                { "/_search/scroll", "point in time" },
                { "/_cluster/state", "no cluster-wide state" },
                { "/_cat/indices", "no cluster-wide state" } }) {
                final Answer answer = call("GET", expected[0], null);
                assertEquals(expected[0] + " must be a 501: " + answer.body(), 501, answer.status());
                assertTrue(expected[0] + " must say why: " + answer.body(), answer.has(expected[1]));
                assertTrue(expected[0] + " must use the real error object: " + answer.body(), answer.has("\"root_cause\""));
            }
        }
    }

    /**
     * Searching without naming an index, and searching with a bare query string.
     *
     * <p>{@code GET /_search} used to fall through to {@code GET /{index}} and answer "no such index:
     * _search", and {@code POST /_search} answered 405 by the same cause. {@code q=} was a hand-rolled split
     * on the first colon, so {@code q=field:value} worked and {@code q=hello} — the simplest thing a caller
     * can type — was a 400.
     */
    public void testSearchWithoutAnIndexAndWithABareQueryString() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = serving(plane, "compat-search")) {
            call("PUT", "/alpha", "{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"}}}}");
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            call("PUT", "/alpha/_doc/k1?refresh=true", "{\"msg\":\"findable\"}");

            final Answer everywhere = call("POST", "/_search", "{\"query\":{\"match_all\":{}}}");
            assertEquals("a search naming no index must search, not 405: " + everywhere.body(), 200, everywhere.status());
            assertTrue("and find what is there: " + everywhere.body(), everywhere.has("\"value\":1"));

            final Answer bare = call("GET", "/alpha/_search?q=findable", null);
            assertEquals("a bare term must be a query, not a 400: " + bare.body(), 200, bare.status());
            assertTrue("and must match: " + bare.body(), bare.has("\"value\":1"));

            // The colon form still parses -- as the query-string syntax it always looked like.
            final Answer fielded = call("GET", "/alpha/_search?q=msg:findable", null);
            assertEquals(fielded.body(), 200, fielded.status());
            assertTrue("the shorthand this handler was born with must not have broken: " + fielded.body(), fielded.has("\"value\":1"));
        }
    }
}
