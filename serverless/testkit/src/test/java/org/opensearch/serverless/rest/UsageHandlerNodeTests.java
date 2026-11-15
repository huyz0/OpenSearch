/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.serverless.store.ObjectStoreSettings;
import org.opensearch.serverless.usage.Guardrails;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/** A real node's {@code _serverless/usage}: each index's requests, documents and searches, and its tenant's level. */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class UsageHandlerNodeTests extends OpenSearchTestCase {

    @SuppressWarnings("unchecked")
    public void testUsageReportsEachIndexAndTenant() throws Exception {
        final Settings settings = Settings.builder()
            .put("node.name", "usage-endpoint")
            .put("cluster.name", "serverless-usage-endpoint")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(ObjectStoreSettings.PATH.getKey(), createTempDir().toString())
            .build();
        try (
            ServerlessBootstrap boot = ServerlessBootstrap.start(settings);
            HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(10)).build()
        ) {
            final int port = boot.node().boundHttpAddress().publishAddress().getPort();
            send(http, port, "PUT", "/billed", "{\"settings\":{\"number_of_shards\":1,\"index.serverless.cost.tier\":\"small\"}}");
            send(
                http,
                port,
                "POST",
                "/_bulk?refresh=true",
                "{\"index\":{\"_index\":\"billed\"}}\n{\"n\":1}\n{\"index\":{\"_index\":\"billed\"}}\n{\"n\":2}\n"
                    + "{\"index\":{\"_index\":\"billed\"}}\n{\"n\":3}\n"
            );
            send(http, port, "PUT", "/billed/_doc/4?refresh=true", "{\"n\":4}");
            send(http, port, "POST", "/billed/_search", "{\"query\":{\"match_all\":{}}}");
            send(http, port, "POST", "/billed/_search", "{\"query\":{\"match_all\":{}}}");
            final Guardrails guardrails = boot.node().part(Guardrails.class);
            assertNotNull(guardrails);
            guardrails.tick();

            final Map<String, Object> body = send(http, port, "GET", "/_serverless/usage", null);
            assertEquals(true, body.get("metered"));
            assertEquals("observe", body.get("guardrails"));
            assertTrue(((Number) body.get("started_millis")).longValue() > 0L);
            final Map<String, Object> indices = (Map<String, Object>) body.get("indices");
            final Map<String, Object> billed = (Map<String, Object>) indices.get("billed");
            assertNotNull(indices.keySet().toString(), billed);
            assertEquals(4, ((Number) billed.get("docs")).intValue());
            assertEquals(2, ((Number) billed.get("queries")).intValue());
            assertEquals("billed", billed.get("tenant"));
            assertEquals("small", billed.get("tier"));
            assertEquals(20, ((Number) billed.get("budget_per_minute")).intValue());
            assertTrue(billed.toString(), ((Number) billed.get("cost_units")).doubleValue() > 0d);
            assertTrue(((Map<String, Object>) billed.get("requests")).containsKey("blob_write"));
            assertTrue(indices.containsKey("_node"));
            final Map<String, Object> tenants = (Map<String, Object>) body.get("tenants");
            assertEquals(0, ((Map<String, Object>) tenants.get("billed")).get("level"));

            final Map<String, Object> top = send(http, port, "GET", "/_serverless/usage?top=0", null);
            assertEquals(java.util.Set.of("_node", "_other"), ((Map<String, Object>) top.get("indices")).keySet());
        }
    }

    private static Map<String, Object> send(HttpClient http, int port, String method, String path, String body) throws Exception {
        final HttpResponse<String> response = http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", path.contains("_bulk") ? "application/x-ndjson" : "application/json")
                .timeout(Duration.ofSeconds(60))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );
        assertTrue(path + ": " + response.body(), response.statusCode() == 200 || response.statusCode() == 201);
        assertFalse(path + ": " + response.body(), path.contains("_bulk") && response.body().contains("\"errors\":true"));
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response.body(), false);
    }
}
