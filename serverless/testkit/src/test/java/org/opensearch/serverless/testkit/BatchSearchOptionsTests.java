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
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

public class BatchSearchOptionsTests extends OpenSearchTestCase {
    private void assertItemRefused(String endpoint, String header, String source) throws Exception {
        withNode(new org.opensearch.plugins.Plugin() {
        }, node -> {
            final var answer = send(node, endpoint, header + "\n" + source + "\n");
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"status\":501"));
            assertFalse(answer.body(), answer.body().contains("\"hits\":"));
        });
    }

    public void testMsearchHeaderDfsIsNotIgnored() throws Exception {
        assertItemRefused("/docs/_msearch", "{\"search_type\":\"dfs_query_then_fetch\"}", "{}");
    }

    public void testMsearchCamelCaseHeaderDfsIsNotIgnored() throws Exception {
        assertItemRefused("/docs/_msearch", "{\"searchType\":\"dfs_query_then_fetch\"}", "{}");
    }

    public void testMsearchUrlDfsIsNotIgnored() throws Exception {
        assertItemRefused("/docs/_msearch?search_type=dfs_query_then_fetch", "{}", "{}");
    }

    public void testMsearchShardPreferenceIsNotIgnored() throws Exception {
        assertItemRefused("/docs/_msearch", "{\"preference\":\"_shards:0\"}", "{}");
    }

    public void testMsearchHeaderCanSelectSupportedTypeOverUrlDefault() throws Exception {
        withNode(new org.opensearch.plugins.Plugin() {
        }, node -> {
            final var answer = send(
                node,
                "/docs/_msearch?search_type=dfs_query_then_fetch",
                "{\"search_type\":\"query_then_fetch\"}\n{}\n"
            );
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"status\":200"));
            assertTrue(answer.body(), answer.body().contains("\"value\":2"));
        });
    }

    public void testMultiTemplateDfsIsNotIgnored() throws Exception {
        assertItemRefused(
            "/docs/_msearch/template",
            "{\"search_type\":\"dfs_query_then_fetch\"}",
            "{\"source\":{\"query\":{\"match_all\":{}}}}"
        );
    }

    public void testMultiTemplateShardPreferenceIsNotIgnored() throws Exception {
        assertItemRefused("/docs/_msearch/template", "{\"preference\":\"_shards:0\"}", "{\"source\":{\"query\":{\"match_all\":{}}}}");
    }

    public void testMultiTemplateRoutingIsNotIgnored() throws Exception {
        assertItemRefused("/docs/_msearch/template", "{\"routing\":\"selected\"}", "{\"source\":{\"query\":{\"match_all\":{}}}}");
    }

    public void testRankEvalDfsIsNotIgnored() throws Exception {
        withNode(new org.opensearch.plugins.Plugin() {
        }, node -> {
            final var answer = send(
                node,
                "/docs/_rank_eval?search_type=dfs_query_then_fetch",
                "{\"requests\":[{\"id\":\"test\",\"request\":{\"query\":{\"match_all\":{}}},\"ratings\":[{\"_index\":\"docs\",\"_id\":\"doc-0\",\"rating\":1}]}],\"metric\":{\"precision\":{\"k\":2}}}"
            );
            assertEquals(answer.body(), 501, answer.statusCode());
        });
    }

    public void testStoredMultiTemplateReadsOffHttpWorker() throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "batch-template-worker")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest,search")
                .build();
            try (var node = new ServerlessNode(settings)) {
                node.start();
                node.setMetadataPlane(metadata);
                metadata.scripts().put("cold", "{\"script\":{\"lang\":\"mustache\",\"source\":{\"query\":{\"match_all\":{}}}}}");
                final var reader = new java.util.concurrent.atomic.AtomicReference<Thread>();
                store.onNextRegisterRead("scripts", "_version", () -> reader.set(Thread.currentThread()));
                final var answer = send(node, "/ghost/_msearch/template", "{}\n{\"id\":\"cold\"}\n");
                assertEquals(answer.body(), 200, answer.statusCode());
                assertNotNull("stored template must consult its marker", reader.get());
                assertFalse(
                    "script IO ran on " + reader.get().getName(),
                    org.opensearch.transport.Transports.isTransportThread(reader.get())
                );
            }
        }
    }

    private void withNode(org.opensearch.plugins.Plugin plugin, org.opensearch.common.CheckedConsumer<ServerlessNode, Exception> work)
        throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "search-execution-options")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest,search")
                .build();
            try (var node = new ServerlessNode(settings, List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                metadata.createIndex(new IndexDescriptor("docs", "uuid-docs", 2, "{\"properties\":{\"msg\":{\"type\":\"text\"}}}", null));
                for (int i = 0; i < 2; i++) {
                    final var shard = node.activateWriter(metadata, "docs", i).orElseThrow();
                    node.index(shard, "doc-" + i, "{\"msg\":\"visible\"}");
                    node.reconciler().shard(shard).refresh("search-options-test");
                }
                work.accept(node);
            }
        }
    }

    private static HttpResponse<String> send(ServerlessNode node, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final var request = HttpRequest.newBuilder(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }
}
