/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.cluster.metadata.Context;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.function.Function;

public class NativeCreateRequestExecutionTests extends OpenSearchTestCase {
    public static final class CreateFilterPlugin extends org.opensearch.plugins.Plugin implements org.opensearch.plugins.ActionPlugin {
        private final Function<ActionRequest, ActionRequest> rewrite;
        private final Function<ActionResponse, ActionResponse> after;
        private final CreateIndexResponse cached;

        CreateFilterPlugin(
            Function<ActionRequest, ActionRequest> rewrite,
            Function<ActionResponse, ActionResponse> after,
            CreateIndexResponse cached
        ) {
            this.rewrite = rewrite;
            this.after = after;
            this.cached = cached;
        }

        @Override
        public List<org.opensearch.action.support.ActionFilter> getActionFilters() {
            return List.of(new org.opensearch.action.support.ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                @SuppressWarnings("unchecked")
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    org.opensearch.tasks.Task task,
                    String action,
                    Request request,
                    org.opensearch.action.support.ActionRequestMetadata<Request, Response> metadata,
                    org.opensearch.core.action.ActionListener<Response> listener,
                    org.opensearch.action.support.ActionFilterChain<Request, Response> chain
                ) {
                    if (CreateIndexAction.NAME.equals(action) == false) {
                        chain.proceed(task, action, request, listener);
                    } else if (cached != null) {
                        listener.onResponse((Response) cached);
                    } else {
                        chain.proceed(
                            task,
                            action,
                            rewrite == null ? request : (Request) rewrite.apply(request),
                            org.opensearch.core.action.ActionListener.wrap(
                                response -> listener.onResponse((Response) (after == null ? response : after.apply(response))),
                                listener::onFailure
                            )
                        );
                    }
                }
            });
        }
    }

    public void testNativeCreatePersistsSettings() throws Exception {
        withNode(new org.opensearch.plugins.Plugin() {
        }, (node, metadata) -> {
            node.client()
                .admin()
                .indices()
                .create(
                    new CreateIndexRequest("configured").settings(
                        Settings.builder().put("index.refresh_interval", "30s").put("index.max_result_window", 333)
                    )
                )
                .actionGet();
            final var descriptor = metadata.describe("configured").orElseThrow();
            assertNotNull(descriptor.extraSettings());
            assertEquals("30s", descriptor.extraSettings().get("index.refresh_interval"));
            assertEquals("333", descriptor.extraSettings().get("index.max_result_window"));
        });
    }

    public void testNativeCreateNormalizesBareShardSettings() throws Exception {
        withNode(new org.opensearch.plugins.Plugin() {
        }, (node, metadata) -> {
            node.client()
                .admin()
                .indices()
                .create(new CreateIndexRequest("configured").settings(Settings.builder().put("number_of_shards", 2)))
                .actionGet();
            assertEquals(2, metadata.describe("configured").orElseThrow().numberOfShards());
        });
    }

    public void testNativeCreateInheritsTemplates() throws Exception {
        withNode(new org.opensearch.plugins.Plugin() {
        }, (node, metadata) -> {
            metadata.indexTemplates()
                .put(
                    "events",
                    "{\"index_patterns\":[\"events*\"],\"priority\":1,\"template\":{\"settings\":{\"number_of_shards\":2,\"refresh_interval\":\"30s\"},\"mappings\":{\"properties\":{\"tag\":{\"type\":\"keyword\"}}}}}"
                );
            node.client().admin().indices().create(new CreateIndexRequest("events-1")).actionGet();
            final var descriptor = metadata.describe("events-1").orElseThrow();
            assertEquals(2, descriptor.numberOfShards());
            assertEquals("30s", descriptor.extraSettings().get("index.refresh_interval"));
            assertTrue(descriptor.mapping(), descriptor.mapping().contains("\"tag\""));
        });
    }

    public void testNativeCreateRefusesUnsupportedReplicas() throws Exception {
        assertRefused(
            new CreateIndexRequest("refused").settings(Settings.builder().put("index.number_of_replicas", 1)),
            RestStatus.NOT_IMPLEMENTED
        );
    }

    public void testNativeCreateRefusesPrivateIdentitySettings() throws Exception {
        assertRefused(new CreateIndexRequest("refused").settings(Settings.builder().put("index.uuid", "forged")), RestStatus.BAD_REQUEST);
    }

    public void testNativeCreateRefusesIgnoredAliases() throws Exception {
        assertRefused(new CreateIndexRequest("refused").alias(new Alias("ignored")), RestStatus.NOT_IMPLEMENTED);
    }

    public void testNativeCreateRefusesIgnoredContexts() throws Exception {
        assertRefused(new CreateIndexRequest("refused").context(new Context("ignored")), RestStatus.NOT_IMPLEMENTED);
    }

    private void assertRefused(CreateIndexRequest request, RestStatus status) throws Exception {
        withNode(new org.opensearch.plugins.Plugin() {
        }, (node, metadata) -> {
            final var failure = expectThrows(
                org.opensearch.OpenSearchStatusException.class,
                () -> node.client().admin().indices().create(request).actionGet()
            );
            assertEquals(failure.toString(), status, failure.status());
            assertTrue(metadata.describe(request.index()).isEmpty());
        });
    }

    public void testNativeCreateUsesTheAdmittedIndexAndSettings() throws Exception {
        final var plugin = new CreateFilterPlugin(
            request -> new CreateIndexRequest("selected").settings(Settings.builder().put("index.refresh_interval", "30s")),
            null,
            null
        );
        withNode(plugin, (node, metadata) -> {
            final var response = node.client().admin().indices().create(new CreateIndexRequest("original")).actionGet();
            assertEquals("selected", response.index());
            assertTrue(metadata.describe("original").isEmpty());
            assertEquals("30s", metadata.describe("selected").orElseThrow().extraSettings().get("index.refresh_interval"));
        });
    }

    public void testNativeCreateHonorsTheFilteredNativeResponse() throws Exception {
        final var plugin = new CreateFilterPlugin(null, response -> {
            final var created = (CreateIndexResponse) response;
            if (created.isAcknowledged() == false) {
                throw new IllegalArgumentException("the filter must see the actual creation response");
            }
            return new CreateIndexResponse(false, false, "filtered");
        }, null);
        withNode(plugin, (node, metadata) -> {
            final var response = node.client().admin().indices().create(new CreateIndexRequest("original")).actionGet();
            assertFalse(response.isAcknowledged());
            assertEquals("filtered", response.index());
            assertTrue(metadata.describe("original").isPresent());
        });
    }

    public void testNativeCreateReturnsACachedResponseWithoutCreatingMetadata() throws Exception {
        final var plugin = new CreateFilterPlugin(null, null, new CreateIndexResponse(false, false, "cached"));
        withNode(plugin, (node, metadata) -> {
            final var response = node.client().admin().indices().create(new CreateIndexRequest("original")).actionGet();
            assertNotNull(response);
            assertFalse(response.isAcknowledged());
            assertEquals("cached", response.index());
            assertTrue(metadata.describe("original").isEmpty());
        });
    }

    private void withNode(
        org.opensearch.plugins.Plugin plugin,
        org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work
    ) throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "native-create-contract")
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

    public void testWrappedNativeMappingsComposeWithTemplates() throws Exception {
        withNode(new org.opensearch.plugins.Plugin() {
        }, (node, metadata) -> {
            metadata.indexTemplates()
                .put(
                    "events",
                    "{\"index_patterns\":[\"events*\"],\"template\":{\"mappings\":{\"properties\":{\"tag\":{\"type\":\"keyword\"}}}}}"
                );
            final var request = new CreateIndexRequest("events-1").mapping(
                java.util.Map.of("properties", java.util.Map.of("n", java.util.Map.of("type", "long")))
            );
            node.client().admin().indices().create(request).actionGet();
            final var shard = node.activateWriter(metadata, "events-1", 0).orElseThrow();
            node.index(shard, "doc", "{\"n\":9,\"tag\":\"valid\"}");
            assertTrue(node.get(shard, "doc").found());
        });
    }

    public void testNativeCreateRefusesMalformedMappings() throws Exception {
        assertRefused(new CreateIndexRequest("refused").mapping("{\"properties\":"), RestStatus.BAD_REQUEST);
    }

    public void testNativeCreateRefusesTrailingMappingContent() throws Exception {
        assertRefused(new CreateIndexRequest("refused").mapping("{\"properties\":{}} {}"), RestStatus.BAD_REQUEST);
    }

    public void testNativeCreateRefusesInvalidSettings() throws Exception {
        assertRefused(
            new CreateIndexRequest("refused").settings(Settings.builder().put("index.refresh_interval", "not-a-duration")),
            RestStatus.BAD_REQUEST
        );
    }

    public void testExplicitNativeSettingsOverrideTemplateDefaults() throws Exception {
        withNode(new org.opensearch.plugins.Plugin() {
        }, (node, metadata) -> {
            metadata.indexTemplates()
                .put(
                    "events",
                    "{\"index_patterns\":[\"events*\"],\"template\":{\"settings\":{\"number_of_shards\":5,\"refresh_interval\":\"60s\"}}}"
                );
            node.client()
                .admin()
                .indices()
                .create(
                    new CreateIndexRequest("events-1").settings(
                        Settings.builder().put("number_of_shards", 1).put("refresh_interval", "30s")
                    )
                )
                .actionGet();
            final var descriptor = metadata.describe("events-1").orElseThrow();
            assertEquals(1, descriptor.numberOfShards());
            assertEquals("30s", descriptor.extraSettings().get("index.refresh_interval"));
        });
    }
}
