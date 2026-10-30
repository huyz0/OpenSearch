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

public class FieldCapabilitiesContractTests extends OpenSearchTestCase {
    @SuppressWarnings("unchecked")
    private static java.util.Map<String, Object> object(Object value) {
        return (java.util.Map<String, Object>) value;
    }

    private static java.util.Map<String, Object> json(String source) {
        return org.opensearch.common.xcontent.XContentHelper.convertToMap(
            new org.opensearch.core.common.bytes.BytesArray(source),
            false,
            org.opensearch.common.xcontent.XContentType.JSON
        ).v2();
    }

    private static java.util.Map<String, Object> keyword(HttpResponse<String> answer) {
        assertEquals(answer.body(), 200, answer.statusCode());
        return object(object(object(json(answer.body()).get("fields")).get("title")).get("keyword"));
    }

    public void testMixedCapabilitiesNameTheDisabledIndices() throws Exception {
        withNode(node -> {
            final var caps = keyword(send(node, "/enabled,disabled/_field_caps?fields=title", "{}"));
            assertEquals(false, caps.get("searchable"));
            assertEquals(false, caps.get("aggregatable"));
            assertEquals(List.of("disabled"), caps.get("non_searchable_indices"));
            assertEquals(List.of("disabled"), caps.get("non_aggregatable_indices"));
        });
    }

    public void testUniformlyDisabledCapabilitiesDoNotInventMixedLists() throws Exception {
        withNode(node -> {
            final var caps = keyword(send(node, "/disabled/_field_caps?fields=title", "{}"));
            assertEquals(false, caps.get("searchable"));
            assertFalse(caps.containsKey("non_searchable_indices"));
            assertFalse(caps.containsKey("non_aggregatable_indices"));
        });
    }

    public void testFieldAliasesHaveTheirOwnCapabilitiesEntry() throws Exception {
        withNode(node -> {
            final var answer = send(node, "/enabled/_field_caps?fields=title_alias", "{}");
            assertEquals(answer.body(), 200, answer.statusCode());
            final var fields = object(json(answer.body()).get("fields"));
            assertTrue(answer.body(), fields.containsKey("title_alias"));
            assertFalse(answer.body(), fields.containsKey("title"));
            assertEquals(true, object(object(fields.get("title_alias")).get("keyword")).get("searchable"));
        });
    }

    public void testCapabilitiesMergeFieldMetadataAcrossIndices() throws Exception {
        withNode(node -> {
            final var caps = keyword(send(node, "/enabled,disabled/_field_caps?fields=title", "{}"));
            final var meta = object(caps.get("meta"));
            assertNotNull(caps.toString(), meta);
            assertEquals(java.util.Set.of("search", "archive"), java.util.Set.copyOf((List<?>) meta.get("owner")));
        });
    }

    public void testEmptyBodyFieldsCannotSilentlySelectEveryField() throws Exception {
        withNode(node -> {
            final var answer = send(node, "/enabled/_field_caps", "{\"fields\":[]}");
            assertEquals(answer.body(), 400, answer.statusCode());
        });
    }

    public void testWildcardIncludesMatchingAliases() throws Exception {
        withNode(node -> {
            final var answer = send(node, "/enabled/_field_caps?fields=*itle*", "{}");
            assertEquals(answer.body(), 200, answer.statusCode());
            assertEquals(java.util.Set.of("title", "title_alias"), object(json(answer.body()).get("fields")).keySet());
        });
    }

    public static final class HiddenTitlePlugin extends org.opensearch.plugins.Plugin implements org.opensearch.plugins.MapperPlugin {
        @Override
        public java.util.function.Function<String, java.util.function.Predicate<String>> getFieldFilter() {
            return index -> field -> field.equals("title") == false;
        }
    }

    public void testMapperFieldFilterHidesFieldsAndTheirAliases() throws Exception {
        withNode(new HiddenTitlePlugin(), node -> {
            final var answer = send(node, "/enabled/_field_caps?fields=*", "{}");
            assertEquals(answer.body(), 200, answer.statusCode());
            final var fields = object(json(answer.body()).get("fields"));
            assertFalse(answer.body(), fields.containsKey("title"));
            assertFalse(answer.body(), fields.containsKey("title_alias"));
            assertTrue(answer.body(), fields.containsKey("_id"));
        });
    }

    private void withNode(org.opensearch.common.CheckedConsumer<ServerlessNode, Exception> work) throws Exception {
        withNode(new org.opensearch.plugins.Plugin() {
        }, work);
    }

    private void withNode(org.opensearch.plugins.Plugin plugin, org.opensearch.common.CheckedConsumer<ServerlessNode, Exception> work)
        throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "field-capabilities-contract")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest,search")
                .build();
            try (var node = new ServerlessNode(settings, List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                metadata.createIndex(
                    new IndexDescriptor(
                        "enabled",
                        "uuid-enabled",
                        3,
                        "{\"properties\":{\"title\":{\"type\":\"keyword\",\"meta\":{\"owner\":\"search\"}},\"title_alias\":{\"type\":\"alias\",\"path\":\"title\"}}}",
                        null
                    )
                );
                metadata.createIndex(
                    new IndexDescriptor(
                        "disabled",
                        "uuid-disabled",
                        3,
                        "{\"properties\":{\"title\":{\"type\":\"keyword\",\"index\":false,\"doc_values\":false,\"meta\":{\"owner\":\"archive\"}}}}",
                        null
                    )
                );
                work.accept(node);
                assertTrue("capabilities must not activate dormant shards", node.reconciler().heldShards().isEmpty());
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
