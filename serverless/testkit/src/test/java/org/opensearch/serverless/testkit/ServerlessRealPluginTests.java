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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * A real plugin, assembled by the build, installed, and doing its actual job.
 *
 * <p>The previous test compiled a plugin of its own, which proved the loader on a class that exists
 * nowhere else. This one goes the other way: {@code analysis-icu} exactly as the build assembles it for a
 * distribution, dragged in with a fourteen-megabyte {@code icu4j} and a Lucene analysis jar, so the loader
 * is exercised on a plugin that brings dependencies rather than a single class.
 *
 * <p><b>And it checks the thing that actually matters, which is not loading.</b> Loading a plugin and then
 * ignoring what it declares is not hosting it. The shell used to build its analysis registry, its mapper
 * registry and its search module from empty lists, so an installed {@code AnalysisPlugin} would load,
 * log a cheerful line, and contribute nothing. The assertion here is therefore behavioural: an index
 * configured to use the plugin's tokenizer must tokenize differently from one that is not, and the
 * difference must be visible in what a search finds.
 *
 * <p>The difference chosen is {@code icu_folding}: a document holding {@code Résumé}, a query for
 * {@code resume}. The standard analyzer lowercases and keeps the diacritics, so the plain spelling finds
 * nothing; ICU's folding filter strips them at index time, so it does. One index with the plugin's filter
 * and one without, the same document in both, the same query against both — the plugin either ran or it
 * did not, and there is no way to pass this by accident.
 *
 * <p><b>Folding rather than {@code icu_tokenizer} on Thai</b>, which was the first attempt and the more
 * dramatic demonstration: word segmentation depends on ICU's dictionary break rules, which did not split
 * in this build, and an assertion that depends on optional data is an assertion that fails for reasons
 * that have nothing to do with the shell. Folding is table-driven and unconditional.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
/*
 * ExtrasFS, the test framework's mock filesystem, plants a stray entry in every directory it creates --
 * including this test's plugins/ directory. Core then finds a directory there with no descriptor and
 * refuses to start, which is exactly the behaviour testAnIncompleteInstallationIsRefused asserts and
 * exactly what we do not want randomly injected. Core's own PluginsServiceTests suppresses it for the
 * same reason.
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class ServerlessRealPluginTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    /** Written with diacritics, as somebody's name actually is. */
    private static final String WITH_ACCENTS = "Résumé";
    /** Typed without them, as somebody searching actually types. */
    private static final String WITHOUT_ACCENTS = "resume";

    private static final String WITH_ICU = "{\"properties\":{\"msg\":{\"type\":\"text\",\"analyzer\":\"icu_text\"}}}";
    private static final String PLAIN = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name, Path home) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-real-plugin")
            .put("path.home", home)
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** An installed analysis plugin changes what a search finds, which is the only proof that counts. */
    public void testAnInstalledAnalysisPluginActuallyAnalyses() throws Exception {
        final Path home = createTempDir();
        install(home);

        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(
            new IndexDescriptor(
                "with_icu",
                "uuid-with-icu-0000000",
                1,
                WITH_ICU,
                Settings.builder()
                    .put("index.analysis.analyzer.icu_text.type", "custom")
                    .put("index.analysis.analyzer.icu_text.tokenizer", "standard")
                    .putList("index.analysis.analyzer.icu_text.filter", "icu_folding")
                    .build()
            )
        );
        plane.createIndex(new IndexDescriptor("without_icu", "uuid-without-icu-000", 1, PLAIN, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("real-plugin", home))) {
            node.start();
            node.setMetadataPlane(plane);

            assertEquals("the plugin must be loaded", 1, node.plugins().plugins().size());
            assertTrue(
                "and it must be the real one: " + node.plugins().plugins().get(0).getClass().getName(),
                node.plugins().plugins().get(0).getClass().getName().contains("ICU")
            );

            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("with_icu", 0);
            loop.want("without_icu", 0);
            loop.tick(clock.get());

            for (String index : new String[] { "with_icu", "without_icu" }) {
                final Response written = send(node, "PUT", "/" + index + "/_doc/1?refresh=true", "{\"msg\":\"" + WITH_ACCENTS + "\"}");
                assertEquals("the document must be written to " + index + ": " + written.body(), 201, written.status());
            }

            final String query = "{\"query\":{\"match\":{\"msg\":\"" + WITHOUT_ACCENTS + "\"}}}";

            // The plugin's token filter folded the diacritics away at index time, so the plain spelling
            // reaches the accented one.
            final Response found = send(node, "POST", "/with_icu/_search", query);
            assertEquals(found.body(), 200, found.status());
            assertTrue("an installed analysis plugin must actually analyse: " + found.body(), found.body().contains("\"value\":1"));

            // Without it, the same document under the same query keeps its accents and matches nothing.
            // This half is what makes the half above evidence rather than a coincidence: the two indices
            // differ in one thing, and that thing is the plugin.
            final Response missing = send(node, "POST", "/without_icu/_search", query);
            assertEquals(missing.body(), 200, missing.status());
            assertTrue(
                "and the control index must not match, or the first assertion proves nothing: " + missing.body(),
                missing.body().contains("\"value\":0")
            );

            // Both indices do hold the document, so the miss above is analysis and not an empty index.
            final String exact = "{\"query\":{\"match\":{\"msg\":\"" + WITH_ACCENTS + "\"}}}";
            assertTrue(send(node, "POST", "/without_icu/_search", exact).body().contains("\"value\":1"));
        }
    }

    /** Unpacks the assembled plugin zip into {@code home/plugins/analysis-icu}, as the plugin CLI would. */
    private void install(Path home) throws Exception {
        final String dist = System.getProperty("tests.serverless.plugin.dist");
        assumeTrue("this test needs the assembled analysis-icu zip; run it through Gradle", dist != null);
        final Path distributions = Path.of(dist);
        assumeTrue("no distributions directory at " + distributions, Files.isDirectory(distributions));

        final Path zip;
        try (var files = Files.list(distributions)) {
            zip = files.filter(f -> f.getFileName().toString().endsWith(".zip")).findFirst().orElse(null);
        }
        assumeTrue("no plugin zip in " + distributions, zip != null);

        final Path target = home.resolve("plugins").resolve("analysis-icu");
        Files.createDirectories(target);
        try (ZipInputStream in = new ZipInputStream(Files.newInputStream(zip))) {
            ZipEntry entry;
            while ((entry = in.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                // Flattened deliberately: an OpenSearch plugin zip is flat, and resolving entry names
                // against the target without checking would be the classic zip-slip. Taking only the file
                // name cannot escape the directory.
                final Path file = target.resolve(Path.of(entry.getName()).getFileName().toString());
                // Not try-with-resources: closing the entry's bytes closes the ZipInputStream itself, and
                // the next getNextEntry then fails with "Stream closed".
                Files.write(file, in.readAllBytes());
            }
        }
        assertTrue("the installation must have a descriptor", Files.exists(target.resolve("plugin-descriptor.properties")));
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
