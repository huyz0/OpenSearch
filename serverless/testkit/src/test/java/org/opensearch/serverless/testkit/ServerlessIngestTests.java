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
 * M58: ingest pipelines, and the dynamic mapping that turned out to be missing underneath them.
 *
 * <p><b>Pipelines are the last item from the vendor comparison.</b> M56 refused them because every processor
 * lives in the {@code ingest-common} module and this shell loads none, and recorded that enabling them was
 * "a decision about loading modules, not about adding an endpoint". M57 made that decision by choosing
 * {@code lang-painless} the way the transport had always been chosen. This is that decision applied a third
 * time.
 *
 * <p><b>The bigger find is underneath.</b> Probing a pipeline that adds a field turned up a pre-existing bug:
 * an index created without an explicit mapping could not accept a single document. Core returns
 * {@code MAPPING_UPDATE_REQUIRED} with the mapping addition a write needs; a classic node sends that to the
 * cluster manager and retries, and nothing here was doing the equivalent — so every write of an undeclared
 * field answered 500. <b>Every test in this repository happened to declare its mappings</b>, which is why it
 * survived fifty-seven milestones unseen.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessIngestTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private int port;

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-ingest")
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

    /** An index with <em>no</em> mapping at all, which is the state the dynamic-mapping bug made unusable. */
    private ServerlessNode running(MetadataPlane plane, AtomicLong clock, String name) throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings(name));
        node.start();
        node.setMetadataPlane(plane);
        port = node.boundHttpAddress().publishAddress().getPort();
        call("PUT", "/alpha", "{\"settings\":{\"number_of_shards\":1}}");
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());
        return node;
    }

    /**
     * A document with a field nobody declared is indexed, and the mapping grows to describe it.
     *
     * <p><b>This is the test that would have caught the bug.</b> Every other test in this repository creates
     * its index with a complete mapping, so the write path's mapping-update branch was never taken. An index
     * created without one could not accept a single document — a 500 on the most ordinary request there is.
     */
    public void testADocumentWithAnUndeclaredFieldIsIndexed() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "dynamic")) {
            assertNotNull(node);

            final Answer written = call("PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"hello\",\"n\":7}");
            assertEquals("an index with no mapping must still take a document: " + written.body(), 201, written.status());

            final Answer read = call("GET", "/alpha/_doc/1", null);
            assertTrue("and keep it whole: " + read.body(), read.has("\"msg\":\"hello\""));
            assertTrue(read.body(), read.has("\"n\":7"));

            // The mapping grew to describe what arrived, which is what makes the field queryable rather than
            // merely stored.
            final Answer mapping = call("GET", "/alpha/_mapping", null);
            assertTrue("the mapping must have grown: " + mapping.body(), mapping.has("\"msg\""));
            assertTrue(mapping.body(), mapping.has("\"n\""));

            final Answer ranged = call("POST", "/alpha/_search", "{\"query\":{\"range\":{\"n\":{\"gte\":5}}}}");
            assertTrue("and the field must be queryable as its inferred type: " + ranged.body(), ranged.has("\"value\":1"));
        }
    }

    /** A second document adding another field grows the mapping again, without losing the first. */
    public void testTheMappingKeepsGrowing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "dynamic-more")) {
            assertNotNull(node);
            assertEquals(201, call("PUT", "/alpha/_doc/1", "{\"first\":\"a\"}").status());
            assertEquals(201, call("PUT", "/alpha/_doc/2?refresh=true", "{\"second\":\"b\"}").status());

            final Answer mapping = call("GET", "/alpha/_mapping", null);
            assertTrue("the earlier field must survive: " + mapping.body(), mapping.has("\"first\""));
            assertTrue("and the later one must be there: " + mapping.body(), mapping.has("\"second\""));
        }
    }

    /** A pipeline is stored, read back verbatim, and removed. */
    public void testAPipelineRoundTrips() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "pipeline-crud")) {
            assertNotNull(node);
            final Answer stored = call(
                "PUT",
                "/_ingest/pipeline/tag",
                "{\"description\":\"adds a tag\",\"processors\":[{\"set\":{\"field\":\"tag\",\"value\":\"seen\"}}]}"
            );
            assertEquals(stored.body(), 200, stored.status());

            final Answer read = call("GET", "/_ingest/pipeline/tag", null);
            assertEquals(read.body(), 200, read.status());
            assertTrue("keyed by id, as OpenSearch keys it: " + read.body(), read.has("\"tag\":{"));
            assertTrue("stored verbatim: " + read.body(), read.has("\"description\":\"adds a tag\""));

            assertEquals(200, call("DELETE", "/_ingest/pipeline/tag", null).status());
            assertEquals(404, call("GET", "/_ingest/pipeline/tag", null).status());
        }
    }

    /**
     * A pipeline shapes the document before it is written, and several processors run in order.
     *
     * <p>The assertion is the stored document, not the response: a pipeline that ran and was then discarded
     * would answer 201 exactly the same way.
     */
    public void testAPipelineShapesTheDocument() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "pipeline-run")) {
            assertNotNull(node);
            call(
                "PUT",
                "/_ingest/pipeline/chain",
                "{\"processors\":[{\"set\":{\"field\":\"a\",\"value\":1}},{\"uppercase\":{\"field\":\"msg\"}},"
                    + "{\"rename\":{\"field\":\"msg\",\"target_field\":\"message\"}}]}"
            );

            assertEquals(201, call("PUT", "/alpha/_doc/1?pipeline=chain&refresh=true", "{\"msg\":\"shout\"}").status());

            final Answer read = call("GET", "/alpha/_doc/1", null);
            assertTrue("the processors ran, in order: " + read.body(), read.has("\"message\":\"SHOUT\""));
            assertTrue("and the added field is there: " + read.body(), read.has("\"a\":1"));
            assertFalse("the renamed field must be gone: " + read.body(), read.has("\"msg\""));

            // No pipeline named, no pipeline run.
            assertEquals(201, call("PUT", "/alpha/_doc/2?refresh=true", "{\"msg\":\"quiet\"}").status());
            assertTrue("an unpiped write is untouched: ", call("GET", "/alpha/_doc/2", null).has("\"msg\":\"quiet\""));
        }
    }

    /**
     * A pipeline may drop a document, and dropping is reported rather than looking like a write.
     *
     * <p>A caller that cannot tell "dropped" from "written" cannot tell whether its pipeline works, which is
     * the whole reason for having one.
     */
    public void testAPipelineCanDropADocument() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "pipeline-drop")) {
            assertNotNull(node);
            call("PUT", "/_ingest/pipeline/dropper", "{\"processors\":[{\"drop\":{\"if\":\"ctx.msg == 'skip'\"}}]}");

            final Answer dropped = call("PUT", "/alpha/_doc/1?pipeline=dropper", "{\"msg\":\"skip\"}");
            assertEquals(dropped.body(), 200, dropped.status());
            assertTrue("said, not implied: " + dropped.body(), dropped.has("\"dropped_by_pipeline\":\"dropper\""));
            assertEquals("and nothing was written", 404, call("GET", "/alpha/_doc/1", null).status());

            final Answer kept = call("PUT", "/alpha/_doc/2?pipeline=dropper&refresh=true", "{\"msg\":\"keep\"}");
            assertEquals("a document the condition does not match is written: " + kept.body(), 201, kept.status());
            assertEquals(200, call("GET", "/alpha/_doc/2", null).status());
        }
    }

    /**
     * A pipeline that could not run is refused when it is stored, not when it is used.
     *
     * <p>Accepting it and failing on the first write that referenced it would surface the mistake days later,
     * in somebody else's request, with nothing connecting it to the change that caused it.
     */
    public void testAnUnrunnablePipelineIsRefusedWhenStored() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "pipeline-invalid")) {
            assertNotNull(node);

            final Answer unknown = call("PUT", "/_ingest/pipeline/bad", "{\"processors\":[{\"no_such_processor\":{\"field\":\"x\"}}]}");
            assertEquals(unknown.body(), 400, unknown.status());
            assertTrue(unknown.body(), unknown.has("No processor type exists with name [no_such_processor]"));
            // The list matters: "unknown processor" without one sends an operator to another product's docs.
            assertTrue("and says what is available: " + unknown.body(), unknown.has("Available processors:"));
            assertTrue(unknown.body(), unknown.has("set"));

            final Answer misconfigured = call("PUT", "/_ingest/pipeline/bad2", "{\"processors\":[{\"set\":{}}]}");
            assertEquals("a processor configured wrongly is caught too: " + misconfigured.body(), 400, misconfigured.status());

            assertEquals("and neither was stored", 404, call("GET", "/_ingest/pipeline/bad", null).status());
        }
    }

    /** Naming a pipeline that is not there, and a processor failing at write time, are both the caller's 400. */
    public void testPipelineFailuresAtWriteTime() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), clock, "pipeline-failures")) {
            assertNotNull(node);

            final Answer missing = call("PUT", "/alpha/_doc/1?pipeline=ghost", "{\"msg\":\"x\"}");
            assertEquals(missing.body(), 400, missing.status());
            assertTrue(missing.body(), missing.has("no such pipeline: ghost"));
            assertEquals("and nothing was written", 404, call("GET", "/alpha/_doc/1", null).status());

            call("PUT", "/_ingest/pipeline/failer", "{\"processors\":[{\"rename\":{\"field\":\"absent\",\"target_field\":\"x\"}}]}");
            final Answer failed = call("PUT", "/alpha/_doc/2?pipeline=failer", "{\"msg\":\"x\"}");
            assertEquals("a processor that fails fails the write: " + failed.body(), 400, failed.status());
            assertTrue(failed.body(), failed.has("field [absent] doesn't exist"));
            assertEquals("and the document is not there", 404, call("GET", "/alpha/_doc/2", null).status());
        }
    }

    /**
     * {@code index.default_pipeline} and {@code index.final_pipeline}, which used to be refused.
     *
     * <p>They were refused honestly: nothing on the write path read them, so storing one would have
     * reported a pipeline that never ran. Both are read now, with core's precedence, and this pins the
     * three things about that precedence a client can actually depend on:
     *
     * <ul>
     *   <li><b>Order.</b> The default runs, then the final. Each processor appends its own letter to the
     *       same array, so the document itself carries the order it was shaped in — an assertion on two
     *       separate fields would pass whichever way round they ran.</li>
     *   <li><b>A request pipeline displaces the default, not the final.</b> That is the point of a final
     *       pipeline: it is the one a caller cannot opt out of.</li>
     *   <li><b>{@code _none} opts out of the default</b> and still leaves the final in place.</li>
     * </ul>
     *
     * <p>Through {@code _doc} and through {@code _bulk}, because they resolve pipelines on different code
     * paths and an index-level default that worked on only one of them would be worse than none.
     */
    public void testTheIndexPipelineSettingsRunInOrderAndOnlyTheDefaultCanBeDisplaced() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = running(plane, clock, "index-pipelines")) {
            assertNotNull(node);
            for (String each : new String[] { "dflt", "fin", "req" }) {
                final Answer stored = call(
                    "PUT",
                    "/_ingest/pipeline/" + each,
                    "{\"processors\":[{\"append\":{\"field\":\"trail\",\"value\":\"" + each + "\"}}]}"
                );
                assertEquals(stored.body(), 200, stored.status());
            }

            final Answer created = call(
                "PUT",
                "/beta",
                "{\"settings\":{\"number_of_shards\":1,\"index.default_pipeline\":\"dflt\",\"index.final_pipeline\":\"fin\"}}"
            );
            assertEquals("the settings that used to be refused: " + created.body(), 200, created.status());
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("beta", 0);
            loop.tick(clock.get());

            // Nothing named on the request: the index's own two, in order.
            assertEquals(201, call("PUT", "/beta/_doc/1?refresh=true", "{\"msg\":\"a\"}").status());
            assertTrue(
                "default then final, in that order: " + call("GET", "/beta/_doc/1", null).body(),
                call("GET", "/beta/_doc/1", null).has("\"trail\":[\"dflt\",\"fin\"]")
            );

            // A request pipeline takes the default's place and leaves the final alone.
            assertEquals(201, call("PUT", "/beta/_doc/2?pipeline=req&refresh=true", "{\"msg\":\"b\"}").status());
            assertTrue(
                "the request pipeline displaces the default, not the final: " + call("GET", "/beta/_doc/2", null).body(),
                call("GET", "/beta/_doc/2", null).has("\"trail\":[\"req\",\"fin\"]")
            );

            // _none opts out of the default. The final is not opt-out-able, which is what it is for.
            assertEquals(201, call("PUT", "/beta/_doc/3?pipeline=_none&refresh=true", "{\"msg\":\"c\"}").status());
            assertTrue(
                "_none drops the default and keeps the final: " + call("GET", "/beta/_doc/3", null).body(),
                call("GET", "/beta/_doc/3", null).has("\"trail\":[\"fin\"]")
            );

            // And the same three, through the batch path, which resolves them somewhere else entirely.
            final Answer batch = call(
                "POST",
                "/_bulk?refresh=true",
                "{\"index\":{\"_index\":\"beta\",\"_id\":\"b1\"}}\n{\"msg\":\"d\"}\n"
                    + "{\"index\":{\"_index\":\"beta\",\"_id\":\"b2\",\"pipeline\":\"req\"}}\n{\"msg\":\"e\"}\n"
                    + "{\"index\":{\"_index\":\"beta\",\"_id\":\"b3\",\"pipeline\":\"_none\"}}\n{\"msg\":\"f\"}\n"
            );
            assertEquals(batch.body(), 200, batch.status());
            assertTrue("the batch must not have failed: " + batch.body(), batch.has("\"errors\":false"));
            assertTrue(call("GET", "/beta/_doc/b1", null).has("\"trail\":[\"dflt\",\"fin\"]"));
            assertTrue(call("GET", "/beta/_doc/b2", null).has("\"trail\":[\"req\",\"fin\"]"));
            assertTrue(call("GET", "/beta/_doc/b3", null).has("\"trail\":[\"fin\"]"));
        }
    }
}
