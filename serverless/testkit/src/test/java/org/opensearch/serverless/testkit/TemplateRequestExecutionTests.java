/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.admin.indices.template.delete.DeleteComponentTemplateAction;
import org.opensearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.opensearch.action.admin.indices.template.get.GetComponentTemplateAction;
import org.opensearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.opensearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.ComponentTemplate;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class TemplateRequestExecutionTests extends OpenSearchTestCase {
    private static String component(String field) {
        return "{\"version\":7,\"template\":{\"mappings\":{\"properties\":{\"" + field + "\":{\"type\":\"keyword\"}}}}}";
    }

    private static String composable(String field) {
        return "{\"index_patterns\":[\"logs-*\"],\"priority\":10,\"version\":7,\"template\":{\"mappings\":{\"properties\":{\""
            + field
            + "\":{\"type\":\"keyword\"}}}}}";
    }

    private static ComponentTemplate parseComponent(String body) throws Exception {
        try (
            var parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, body)
        ) {
            return ComponentTemplate.parse(parser);
        }
    }

    private static ComposableIndexTemplate parseComposable(String body) throws Exception {
        try (
            var parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, body)
        ) {
            return ComposableIndexTemplate.parse(parser);
        }
    }

    private void withNode(PolicyFilterPlugin plugin, org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work)
        throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "template-filter-contract")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings, List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                work.accept(node, metadata);
            }
        }
    }

    public void testComponentPutExposesItsNativeBodyAndCreateFlag() throws Exception {
        withNode(PolicyFilterPlugin.request(PutComponentTemplateAction.NAME, request -> {
            final var put = (PutComponentTemplateAction.Request) request;
            assertEquals("original", put.name());
            assertTrue(put.create());
            assertEquals(Long.valueOf(7L), put.componentTemplate().version());
            return request;
        }), (node, metadata) -> {
            final var response = send(node, "PUT", "/_component_template/original?create=true", component("original_field"));
            assertEquals(response.body(), 200, response.statusCode());
        });
    }

    public void testComposablePutExposesItsNativeBodyAndCreateFlag() throws Exception {
        withNode(PolicyFilterPlugin.request(PutComposableIndexTemplateAction.NAME, request -> {
            final var put = (PutComposableIndexTemplateAction.Request) request;
            assertEquals("original", put.name());
            assertTrue(put.create());
            assertEquals(Long.valueOf(10L), put.indexTemplate().priority());
            return request;
        }), (node, metadata) -> {
            final var response = send(node, "PUT", "/_index_template/original?create=true", composable("original_field"));
            assertEquals(response.body(), 200, response.statusCode());
        });
    }

    public void testComponentPutUsesAdmittedNameBodyAndCreateFlag() throws Exception {
        final var selected = new PutComponentTemplateAction.Request("selected").create(false)
            .componentTemplate(parseComponent(component("selected_field")));
        withNode(PolicyFilterPlugin.request(PutComponentTemplateAction.NAME, request -> selected), (node, metadata) -> {
            metadata.componentTemplates().put("selected", component("old_field"));
            final var response = send(node, "PUT", "/_component_template/original?create=true", component("original_field"));
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(metadata.componentTemplates().get("original").isEmpty());
            assertTrue(metadata.componentTemplates().get("selected").orElseThrow().contains("selected_field"));
        });
    }

    public void testComposablePutUsesAdmittedNameBodyAndCreateFlag() throws Exception {
        final var selected = new PutComposableIndexTemplateAction.Request("selected").create(false)
            .indexTemplate(parseComposable(composable("selected_field")));
        withNode(PolicyFilterPlugin.request(PutComposableIndexTemplateAction.NAME, request -> selected), (node, metadata) -> {
            metadata.indexTemplates().put("selected", composable("old_field"));
            final var response = send(node, "PUT", "/_index_template/original?create=true", composable("original_field"));
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(metadata.indexTemplates().get("original").isEmpty());
            assertTrue(metadata.indexTemplates().get("selected").orElseThrow().contains("selected_field"));
        });
    }

    public void testComponentDeleteUsesNativeAdmittedName() throws Exception {
        withNode(PolicyFilterPlugin.request(DeleteComponentTemplateAction.NAME, request -> {
            assertEquals("original", ((DeleteComponentTemplateAction.Request) request).name());
            return new DeleteComponentTemplateAction.Request("selected");
        }), (node, metadata) -> {
            metadata.componentTemplates().put("original", component("original_field"));
            metadata.componentTemplates().put("selected", component("selected_field"));
            final var response = send(node, "DELETE", "/_component_template/original", null);
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(metadata.componentTemplates().get("original").isPresent());
            assertTrue(metadata.componentTemplates().get("selected").isEmpty());
        });
    }

    public void testComposableDeleteUsesNativeAdmittedName() throws Exception {
        withNode(PolicyFilterPlugin.request(DeleteComposableIndexTemplateAction.NAME, request -> {
            assertEquals("original", ((DeleteComposableIndexTemplateAction.Request) request).name());
            return new DeleteComposableIndexTemplateAction.Request("selected");
        }), (node, metadata) -> {
            metadata.indexTemplates().put("original", composable("original_field"));
            metadata.indexTemplates().put("selected", composable("selected_field"));
            final var response = send(node, "DELETE", "/_index_template/original", null);
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(metadata.indexTemplates().get("original").isPresent());
            assertTrue(metadata.indexTemplates().get("selected").isEmpty());
        });
    }

    public void testComponentGetUsesNativeAdmittedName() throws Exception {
        withNode(PolicyFilterPlugin.request(GetComponentTemplateAction.NAME, request -> {
            assertEquals("original", ((GetComponentTemplateAction.Request) request).name());
            return new GetComponentTemplateAction.Request("selected");
        }), (node, metadata) -> {
            metadata.componentTemplates().put("original", component("original_field"));
            metadata.componentTemplates().put("selected", component("selected_field"));
            final var response = send(node, "GET", "/_component_template/original", null);
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("selected_field"));
            assertFalse(response.body(), response.body().contains("original_field"));
        });
    }

    public void testComposableGetUsesAdmittedName() throws Exception {
        withNode(
            PolicyFilterPlugin.request(
                GetComposableIndexTemplateAction.NAME,
                request -> new GetComposableIndexTemplateAction.Request("selected")
            ),
            (node, metadata) -> {
                metadata.indexTemplates().put("original", composable("original_field"));
                metadata.indexTemplates().put("selected", composable("selected_field"));
                final var response = send(node, "GET", "/_index_template/original", null);
                assertEquals(response.body(), 200, response.statusCode());
                assertTrue(response.body(), response.body().contains("selected_field"));
                assertFalse(response.body(), response.body().contains("original_field"));
            }
        );
    }

    public void testReadWaitsForResponseAuthorization() throws Exception {
        withNode(PolicyFilterPlugin.denyAfter(GetComponentTemplateAction.NAME), (node, metadata) -> {
            metadata.componentTemplates().put("private", component("private_field"));
            final var response = send(node, "GET", "/_component_template/private", null);
            assertEquals(response.body(), 403, response.statusCode());
            assertFalse(response.body(), response.body().contains("private_field"));
        });
    }

    public void testPutUsesRewrittenNativeAcknowledgement() throws Exception {
        withNode(PolicyFilterPlugin.response(PutComponentTemplateAction.NAME, response -> {
            assertTrue(((AcknowledgedResponse) response).isAcknowledged());
            return new AcknowledgedResponse(false);
        }), (node, metadata) -> {
            final var response = send(node, "PUT", "/_component_template/original", component("original_field"));
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"acknowledged\":false"));
            assertTrue(metadata.componentTemplates().get("original").isPresent());
        });
    }

    public void testCachedAcknowledgementSkipsPut() throws Exception {
        withNode(PolicyFilterPlugin.cached(PutComposableIndexTemplateAction.NAME, new AcknowledgedResponse(false)), (node, metadata) -> {
            final var response = send(node, "PUT", "/_index_template/original", composable("original_field"));
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"acknowledged\":false"));
            assertTrue(metadata.indexTemplates().get("original").isEmpty());
        });
    }

    public void testComponentReadUsesRewrittenNativeResponse() throws Exception {
        final var filtered = new GetComponentTemplateAction.Response(Map.of("selected", parseComponent(component("selected_field"))));
        withNode(PolicyFilterPlugin.response(GetComponentTemplateAction.NAME, response -> {
            assertTrue(((GetComponentTemplateAction.Response) response).getComponentTemplates().containsKey("original"));
            return filtered;
        }), (node, metadata) -> {
            metadata.componentTemplates().put("original", component("private_field"));
            final var response = send(node, "GET", "/_component_template/original", null);
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("selected_field"));
            assertFalse(response.body(), response.body().contains("private_field"));
        });
    }

    public void testCachedComposableReadCanSkipStorage() throws Exception {
        final var cached = new GetComposableIndexTemplateAction.Response(Map.of("selected", parseComposable(composable("selected_field"))));
        withNode(PolicyFilterPlugin.cached(GetComposableIndexTemplateAction.NAME, cached), (node, metadata) -> {
            final var response = send(node, "GET", "/_index_template/missing", null);
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("selected_field"));
        });
    }

    public void testAdmittedNegativePriorityIsRefusedBeforeStorage() throws Exception {
        final var invalid = new PutComposableIndexTemplateAction.Request("selected").indexTemplate(
            parseComposable(composable("selected_field").replace("\"priority\":10", "\"priority\":-1"))
        );
        withNode(PolicyFilterPlugin.request(PutComposableIndexTemplateAction.NAME, request -> invalid), (node, metadata) -> {
            final var response = send(node, "PUT", "/_index_template/original", composable("original_field"));
            assertEquals(response.body(), 400, response.statusCode());
            assertTrue(metadata.indexTemplates().get("original").isEmpty());
            assertTrue(metadata.indexTemplates().get("selected").isEmpty());
        });
    }

    public void testOriginalNegativePriorityIsRefusedBeforeStorage() throws Exception {
        withNode(PolicyFilterPlugin.request(PutComposableIndexTemplateAction.NAME, request -> request), (node, metadata) -> {
            final var response = send(
                node,
                "PUT",
                "/_index_template/original",
                composable("original_field").replace("\"priority\":10", "\"priority\":-1")
            );
            assertEquals(response.body(), 400, response.statusCode());
            assertTrue(metadata.indexTemplates().get("original").isEmpty());
        });
    }

    public void testNativeNumericStringPriorityControlsInheritance() throws Exception {
        final String higher = composable("selected_field").replace("\"priority\":10", "\"priority\":\"20\"");
        assertEquals(Long.valueOf(20L), parseComposable(higher).priority());
        withNode(PolicyFilterPlugin.request(PutComposableIndexTemplateAction.NAME, request -> request), (node, metadata) -> {
            metadata.indexTemplates().put("lower", composable("lower_field"));
            final var response = send(node, "PUT", "/_index_template/higher", higher);
            assertEquals(response.body(), 200, response.statusCode());
            assertEquals(
                "higher",
                org.opensearch.serverless.metadata.TemplateResolver.resolve(
                    metadata.indexTemplates().all(),
                    metadata.componentTemplates().all(),
                    "logs-new"
                ).from()
            );
        });
    }

    public void testHeadUsesAdmittedReadSelection() throws Exception {
        withNode(
            PolicyFilterPlugin.request(GetComponentTemplateAction.NAME, request -> new GetComponentTemplateAction.Request("selected")),
            (node, metadata) -> {
                metadata.componentTemplates().put("selected", component("selected_field"));
                final var response = send(node, "HEAD", "/_component_template/missing", null);
                assertEquals(response.body(), 200, response.statusCode());
                assertEquals("", response.body());
            }
        );
    }

    public void testTemplateAliasesRemainExplicitlyUnsupported() throws Exception {
        withNode(PolicyFilterPlugin.request(PutComponentTemplateAction.NAME, request -> request), (node, metadata) -> {
            final var response = send(node, "PUT", "/_component_template/original", "{\"template\":{\"aliases\":{\"alias\":{}}}}");
            assertEquals(response.body(), 501, response.statusCode());
            assertTrue(metadata.componentTemplates().get("original").isEmpty());
        });
    }

    private static HttpResponse<String> send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final var request = HttpRequest.newBuilder(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }
}
