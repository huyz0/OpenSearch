/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.fieldcaps.FieldCapabilities;
import org.opensearch.action.fieldcaps.FieldCapabilitiesAction;
import org.opensearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
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
import java.util.Map;
import java.util.function.UnaryOperator;

public class FieldCapabilitiesRequestExecutionTests extends OpenSearchTestCase {
    public static final class CapFilterPlugin extends org.opensearch.plugins.Plugin implements org.opensearch.plugins.ActionPlugin {
        private final UnaryOperator<FieldCapabilitiesRequest> rewrite;
        private final UnaryOperator<FieldCapabilitiesResponse> responseRewrite;
        private final boolean cached;
        private final boolean denyAfter;

        CapFilterPlugin(
            UnaryOperator<FieldCapabilitiesRequest> rewrite,
            UnaryOperator<FieldCapabilitiesResponse> responseRewrite,
            boolean cached,
            boolean denyAfter
        ) {
            this.rewrite = rewrite;
            this.responseRewrite = responseRewrite;
            this.cached = cached;
            this.denyAfter = denyAfter;
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
                public <Request extends ActionRequest, Response extends org.opensearch.core.action.ActionResponse> void apply(
                    org.opensearch.tasks.Task task,
                    String action,
                    Request request,
                    org.opensearch.action.support.ActionRequestMetadata<Request, Response> metadata,
                    org.opensearch.core.action.ActionListener<Response> listener,
                    org.opensearch.action.support.ActionFilterChain<Request, Response> chain
                ) {
                    if (FieldCapabilitiesAction.NAME.equals(action) == false) {
                        chain.proceed(task, action, request, listener);
                        return;
                    }
                    if (cached) {
                        listener.onResponse((Response) cachedResponse());
                        return;
                    }
                    final Request selected = rewrite == null ? request : (Request) rewrite.apply((FieldCapabilitiesRequest) request);
                    chain.proceed(task, action, selected, org.opensearch.core.action.ActionListener.wrap(response -> {
                        if (denyAfter) {
                            listener.onFailure(
                                new org.opensearch.OpenSearchSecurityException(
                                    "field capabilities denied",
                                    org.opensearch.core.rest.RestStatus.FORBIDDEN
                                )
                            );
                        } else {
                            listener.onResponse(
                                responseRewrite == null ? response : (Response) responseRewrite.apply((FieldCapabilitiesResponse) response)
                            );
                        }
                    }, listener::onFailure));
                }
            });
        }
    }

    private static FieldCapabilitiesResponse cachedResponse() {
        return new FieldCapabilitiesResponse(
            new String[] { "cached" },
            Map.of("selected", Map.of("keyword", new FieldCapabilities("selected", "keyword", true, true, null, null, null, Map.of())))
        );
    }

    private void withNode(CapFilterPlugin plugin, org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work)
        throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            metadata.createIndex(
                new IndexDescriptor(
                    "docs",
                    "uuid-docs",
                    1,
                    "{\"properties\":{\"public\":{\"type\":\"keyword\"},\"private\":{\"type\":\"long\"}}}",
                    null
                )
            );
            metadata.createIndex(
                new IndexDescriptor("other", "uuid-other", 1, "{\"properties\":{\"selected\":{\"type\":\"keyword\"}}}", null)
            );
            final var settings = Settings.builder()
                .put("node.name", "field-caps-filter-contract")
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

    public void testFilterSeesActualNativeFieldsAndIndices() throws Exception {
        withNode(new CapFilterPlugin(request -> {
            assertArrayEquals(new String[] { "docs" }, request.indices());
            assertArrayEquals(new String[] { "public" }, request.fields());
            return request;
        }, null, false, false), (node, metadata) -> {
            final var response = send(node, "/docs/_field_caps?fields=public");
            assertEquals(response.body(), 200, response.statusCode());
        });
    }

    public void testUsesAdmittedIndexSelection() throws Exception {
        withNode(new CapFilterPlugin(request -> request.indices("other"), null, false, false), (node, metadata) -> {
            final var response = send(node, "/docs/_field_caps?fields=*");
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"selected\""));
            assertFalse(response.body(), response.body().contains("\"private\""));
            assertFalse(response.body(), response.body().contains("\"docs\""));
        });
    }

    public void testUsesAdmittedFields() throws Exception {
        withNode(new CapFilterPlugin(request -> request.fields("public"), null, false, false), (node, metadata) -> {
            final var response = send(node, "/docs/_field_caps?fields=*");
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"public\""));
            assertFalse(response.body(), response.body().contains("\"private\""));
        });
    }

    public void testResponseWaitsForPostFilterAuthorization() throws Exception {
        withNode(new CapFilterPlugin(null, null, false, true), (node, metadata) -> {
            final var response = send(node, "/docs/_field_caps?fields=*");
            assertEquals(response.body(), 403, response.statusCode());
            assertFalse(response.body(), response.body().contains("\"private\""));
        });
    }

    public void testUsesRedactedNativeResponse() throws Exception {
        withNode(new CapFilterPlugin(null, response -> {
            assertNotNull(response.getField("private"));
            return new FieldCapabilitiesResponse(response.getIndices(), Map.of("public", response.getField("public")));
        }, false, false), (node, metadata) -> {
            final var response = send(node, "/docs/_field_caps?fields=*");
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"public\""));
            assertFalse(response.body(), response.body().contains("\"private\""));
        });
    }

    public void testCachedNativeResponseCanBypassStorage() throws Exception {
        withNode(new CapFilterPlugin(null, null, true, false), (node, metadata) -> {
            final var response = send(node, "/missing/_field_caps?fields=*");
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"selected\""));
            assertTrue(response.body(), response.body().contains("\"cached\""));
        });
    }

    public void testRefusesAdmittedUnmappedMode() throws Exception {
        withNode(new CapFilterPlugin(request -> request.includeUnmapped(true), null, false, false), (node, metadata) -> {
            final var response = send(node, "/docs/_field_caps?fields=*");
            assertEquals(response.body(), 501, response.statusCode());
        });
    }

    public void testRefusesAdmittedIndexFilter() throws Exception {
        withNode(
            new CapFilterPlugin(request -> request.indexFilter(new org.opensearch.index.query.MatchNoneQueryBuilder()), null, false, false),
            (node, metadata) -> {
                final var response = send(node, "/docs/_field_caps?fields=*");
                assertEquals(response.body(), 501, response.statusCode());
            }
        );
    }

    private static HttpResponse<String> send(ServerlessNode node, String path) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final var request = HttpRequest.newBuilder(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build();
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }
}
