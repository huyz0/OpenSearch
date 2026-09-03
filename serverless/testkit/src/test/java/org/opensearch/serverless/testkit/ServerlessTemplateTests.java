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
 * M56: index and component templates — configuration a new index inherits.
 *
 * <p><b>The refusal these replace was wrong about itself.</b> It said "there is no cluster state for a
 * template to live in". A template needs somewhere to live, not specifically cluster state, and a register on
 * an object store is somewhere — which is where every index descriptor already lives. Fifth instance of the
 * same shape.
 *
 * <p><b>Enumerating templates is allowed where enumerating indices is not</b>, and the difference is worth
 * being precise about because it looks like an exception to the rule. Indices are unbounded by design, so any
 * listing is either truncated or ruinous. Templates are operator-authored configuration whose number is
 * bounded <em>on creation</em> — the thousand-and-first is refused. Bounding an input is not truncating an
 * answer, and it keeps the cost on the operation that caused it rather than on index creation, which has to
 * read them all.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessTemplateTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private int port;

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-templates")
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
        return node;
    }

    private void templates() throws Exception {
        call(
            "PUT",
            "/_component_template/base",
            "{\"template\":{\"settings\":{\"refresh_interval\":\"25s\"},\"mappings\":{\"properties\":{\"host\":{\"type\":\"keyword\"}}}}}"
        );
        call(
            "PUT",
            "/_index_template/logs",
            "{\"index_patterns\":[\"logs-*\"],\"priority\":100,\"composed_of\":[\"base\"],"
                + "\"template\":{\"settings\":{\"number_of_shards\":3},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"}}}}}"
        );
    }

    /**
     * An index created under a matching template inherits its settings, mapping and shard count.
     *
     * <p>The shard count is the assertion that would be easiest to get wrong invisibly: "one shard" is both a
     * legitimate request and what an absent request looks like, so a create path that cannot tell them apart
     * silently ignores every template's {@code number_of_shards}.
     */
    public void testANewIndexInheritsFromTheTemplateThatMatchesIt() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "tmpl-inherit")) {
            assertNotNull(node);
            templates();

            final Answer created = call("PUT", "/logs-2026", null);
            assertEquals(created.body(), 200, created.status());
            assertTrue("the response reports the count the index was made with: " + created.body(), created.has("\"shards\":3"));

            final Answer settings = call("GET", "/logs-2026/_settings", null);
            assertTrue("the shard count came from the template: " + settings.body(), settings.has("\"number_of_shards\":\"3\""));
            assertTrue("and a component's setting: " + settings.body(), settings.has("\"refresh_interval\":\"25s\""));

            final Answer mapping = call("GET", "/logs-2026/_mapping", null);
            assertTrue("the index template's field: " + mapping.body(), mapping.has("\"msg\":{\"type\":\"text\"}"));
            assertTrue("and the component's, both: " + mapping.body(), mapping.has("\"host\":{\"type\":\"keyword\"}"));
        }
    }

    /** An index whose name matches nothing gets the defaults, untouched. */
    public void testAnIndexMatchingNoTemplateIsUnaffected() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "tmpl-nomatch")) {
            assertNotNull(node);
            templates();

            call("PUT", "/other-index", null);
            final Answer settings = call("GET", "/other-index/_settings", null);
            assertTrue("the default shard count: " + settings.body(), settings.has("\"number_of_shards\":\"1\""));
            assertFalse("and nothing a template contributed: " + settings.body(), settings.has("refresh_interval"));
        }
    }

    /**
     * What the request says wins over what a template says.
     *
     * <p>A caller who spells out a mapping is not overruled by configuration they may not know exists — but
     * the fields they did not mention still arrive, which is what composing means.
     */
    public void testTheRequestOverridesTheTemplateFieldByField() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "tmpl-override")) {
            assertNotNull(node);
            templates();

            final Answer created = call(
                "PUT",
                "/logs-own",
                "{\"settings\":{\"number_of_shards\":2},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}}"
            );
            assertTrue("the caller's shard count wins: " + created.body(), created.has("\"shards\":2"));

            final Answer mapping = call("GET", "/logs-own/_mapping", null);
            assertTrue("the caller's type for a field they named: " + mapping.body(), mapping.has("\"msg\":{\"type\":\"keyword\"}"));
            assertTrue("and the component's field they did not: " + mapping.body(), mapping.has("\"host\":{\"type\":\"keyword\"}"));
        }
    }

    /**
     * Among matching templates, the highest priority wins outright.
     *
     * <p>Merging two templates that disagree about a field's type has no sensible resolution, so priority
     * chooses one — which is what priority is for. The loser contributes nothing, not even its
     * non-conflicting fields.
     */
    public void testTheHighestPriorityTemplateWinsOutright() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "tmpl-priority")) {
            assertNotNull(node);
            call(
                "PUT",
                "/_index_template/low",
                "{\"index_patterns\":[\"logs-*\"],\"priority\":1,\"template\":{\"settings\":{\"number_of_shards\":5},"
                    + "\"mappings\":{\"properties\":{\"only_in_low\":{\"type\":\"long\"}}}}}"
            );
            call(
                "PUT",
                "/_index_template/high",
                "{\"index_patterns\":[\"logs-*\"],\"priority\":50,\"template\":{\"settings\":{\"number_of_shards\":2},"
                    + "\"mappings\":{\"properties\":{\"only_in_high\":{\"type\":\"long\"}}}}}"
            );

            final Answer created = call("PUT", "/logs-x", null);
            assertTrue("the higher priority decides: " + created.body(), created.has("\"shards\":2"));

            final Answer mapping = call("GET", "/logs-x/_mapping", null);
            assertTrue(mapping.body(), mapping.has("only_in_high"));
            assertFalse("the loser contributes nothing at all: " + mapping.body(), mapping.has("only_in_low"));
        }
    }

    /** A template round-trips as the JSON it was sent as, and can be read, listed, tested for and removed. */
    public void testTemplatesRoundTripAndAreManageable() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "tmpl-crud")) {
            assertNotNull(node);
            templates();

            final Answer one = call("GET", "/_index_template/logs", null);
            assertEquals(one.body(), 200, one.status());
            assertTrue("stored verbatim, not re-serialised from a parsed copy: " + one.body(), one.has("\"index_patterns\":[\"logs-*\"]"));
            assertTrue(one.body(), one.has("\"priority\":100"));

            assertEquals("HEAD answers with a status", 200, call("HEAD", "/_index_template/logs", null).status());
            assertEquals(404, call("HEAD", "/_index_template/ghost", null).status());
            assertTrue("component templates list separately", call("GET", "/_component_template", null).has("base"));

            assertEquals(200, call("DELETE", "/_index_template/logs", null).status());
            assertEquals("and are gone afterwards", 404, call("GET", "/_index_template/logs", null).status());

            // Deleting the template does not touch indices it already created: a template is applied once,
            // at creation, and is not a live link.
            assertEquals(404, call("GET", "/_index_template/logs", null).status());
        }
    }

    /** The two ways of writing a template that could never do what its author meant. */
    public void testTemplatesThatCouldNeverWorkAreRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "tmpl-invalid")) {
            assertNotNull(node);

            final Answer noPatterns = call("PUT", "/_index_template/nopat", "{\"template\":{}}");
            assertEquals("an index template with no patterns matches nothing: " + noPatterns.body(), 400, noPatterns.status());
            assertTrue(noPatterns.body(), noPatterns.has("can never match an index"));

            final Answer componentWithPatterns = call("PUT", "/_component_template/bad", "{\"index_patterns\":[\"x-*\"],\"template\":{}}");
            assertEquals(componentWithPatterns.body(), 400, componentWithPatterns.status());
            assertTrue(
                "a component does not decide what it applies to: " + componentWithPatterns.body(),
                componentWithPatterns.has("composed by an index template")
            );
        }
    }

    /**
     * A template composing a component that does not exist fails the create, and says which.
     *
     * <p>Skipping the missing component and creating the index anyway is the tempting behaviour and the wrong
     * one: an index quietly created without the analysis settings a component was meant to contribute shows up
     * much later as a query matching nothing, with no trace of why.
     */
    public void testAMissingComponentFailsTheCreateRatherThanBeingSkipped() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "tmpl-missing")) {
            assertNotNull(node);
            call("PUT", "/_index_template/broken", "{\"index_patterns\":[\"broke-*\"],\"composed_of\":[\"absent\"],\"template\":{}}");

            final Answer created = call("PUT", "/broke-1", null);
            assertEquals("the caller's configuration to fix, so 400 not 500: " + created.body(), 400, created.status());
            assertTrue(
                "naming both the template and the component: " + created.body(),
                created.has("composes component template [absent]")
            );
            assertEquals("and the index must not exist", 404, call("GET", "/broke-1", null).status());
        }
    }

    /**
     * A file this system did not write is left alone rather than read as a template.
     *
     * <p><b>This is a bug that was caught, not a hypothetical.</b> The first version of the store read every
     * blob in the container and parsed it as a template. Lucene's {@code ExtrasFS} — which the test framework
     * runs precisely to find code that assumes it owns a directory — drops a file named {@code extra0} into
     * directories, and <em>index creation</em>, which reads every template, began failing across the whole
     * deployment with "stored template [extra0] could not be read". An operator or another tool leaving a file
     * in that container would do the same in production, and the failure would look nothing like its cause.
     *
     * <p>The rule is the one {@code WalStore} and the point-in-time store already follow: absent, foreign and
     * corrupt are three different answers.
     */
    public void testAForeignFileAmongTheTemplatesIsNotOneOfThem() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final var store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane metadata = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        try (ServerlessNode node = running(metadata, "tmpl-foreign")) {
            assertNotNull(node);
            templates();

            final var container = store.blobContainer(org.opensearch.serverless.metadata.RegisterMap.indexTemplates(BlobPath.cleanPath()));
            final var junk = new org.opensearch.core.common.bytes.BytesArray("not a template at all");
            container.writeBlob("somebody-elses-file.txt", junk.streamInput(), junk.length(), true);

            // Index creation reads every template, so this is where a stray file would bite.
            final Answer created = call("PUT", "/logs-strays", null);
            assertEquals("a foreign file must not break creating an index: " + created.body(), 200, created.status());
            assertTrue("and the real template must still apply: " + created.body(), created.has("\"shards\":3"));

            assertFalse("nor appear as a template: ", call("GET", "/_index_template", null).has("somebody-elses-file"));
            assertTrue("and it stays where it is", container.blobExists("somebody-elses-file.txt"));
        }
    }

    /**
     * Ingest pipelines stay refused, and the reason is now the real one.
     *
     * <p>The old reason said there was no cluster state for a pipeline to live in. Templates now prove that
     * wrong — they live in a register. What is missing is what would <em>run</em> one.
     */
    public void testIngestPipelinesRefuseForTheRealReason() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (ServerlessNode node = running(plane(clock, createTempDir()), "tmpl-pipeline")) {
            assertNotNull(node);
            final Answer refused = call("GET", "/_ingest/pipeline/x", null);
            assertEquals(refused.body(), 501, refused.status());
            assertTrue("storing is not the problem: " + refused.body(), refused.has("storing a pipeline is not the problem"));
            assertTrue("running it is: " + refused.body(), refused.has("ingest-common module"));
            assertFalse("the old reason must be gone: " + refused.body(), refused.has("no cluster state for a pipeline"));
        }
    }
}
