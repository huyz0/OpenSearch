/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.get.GetResult;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class MultiGetRequestExecutionTests extends OpenSearchTestCase {
    public static final class MultiFilterPlugin extends org.opensearch.plugins.Plugin implements org.opensearch.plugins.ActionPlugin {
        private final Function<ActionRequest, ActionRequest> rewrite;
        private final Function<ActionResponse, ActionResponse> after;
        private final ActionResponse cached;
        private final AtomicInteger calls = new AtomicInteger();

        MultiFilterPlugin(
            Function<ActionRequest, ActionRequest> rewrite,
            Function<ActionResponse, ActionResponse> after,
            ActionResponse cached
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
                    if (MultiGetAction.NAME.equals(action) == false) {
                        chain.proceed(task, action, request, listener);
                        return;
                    }
                    calls.incrementAndGet();
                    if (cached != null) {
                        listener.onResponse((Response) cached);
                        return;
                    }
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
            });
        }
    }

    public void testFilterSeesActualNativeSourceAndVersionSelection() throws Exception {
        final var plugin = new MultiFilterPlugin(request -> {
            final var ask = (MultiGetRequest) request;
            if (ask.getItems().size() != 1
                || ask.getItems().get(0).version() != 1L
                || List.of(ask.getItems().get(0).fetchSourceContext().includes()).equals(List.of("msg")) == false) {
                throw new IllegalArgumentException("filter did not receive the actual source and version selection");
            }
            return request;
        }, null, null);
        withNode(plugin, true, (node, metadata) -> {
            final var response = send(node, "{\"docs\":[{\"_id\":\"private\",\"version\":1,\"_source\":\"msg\"}]}");
            assertEquals(response.body(), 200, response.statusCode());
            assertEquals(1, plugin.calls.get());
        });
    }

    public void testMultiGetUsesTheAdmittedDocumentSelection() throws Exception {
        final var plugin = new MultiFilterPlugin(request -> new MultiGetRequest().add("docs", "public"), null, null);
        withNode(plugin, true, (node, metadata) -> {
            final var response = send(node, "{\"ids\":[\"private\"]}");
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("public-value"));
            assertFalse(response.body(), response.body().contains("private-value"));
        });
    }

    public void testMultiGetHonorsNativeResponseRedaction() throws Exception {
        final var plugin = new MultiFilterPlugin(null, response -> {
            final var nativeResponse = (MultiGetResponse) response;
            if (nativeResponse.getResponses().length != 1 || nativeResponse.getResponses()[0].isFailed()) {
                throw new IllegalArgumentException("the filter must receive the actual read result");
            }
            return new MultiGetResponse(new MultiGetItemResponse[0]);
        }, null);
        withNode(plugin, true, (node, metadata) -> {
            final var response = send(node, "{\"ids\":[\"private\"]}");
            assertEquals(response.body(), 200, response.statusCode());
            assertEquals("{\"docs\":[]}", response.body());
        });
    }

    public void testMultiGetWaitsForAfterWorkDenial() throws Exception {
        final var plugin = new MultiFilterPlugin(null, response -> {
            throw new org.opensearch.OpenSearchSecurityException("read denied", org.opensearch.core.rest.RestStatus.FORBIDDEN);
        }, null);
        withNode(plugin, true, (node, metadata) -> {
            final var response = send(node, "{\"ids\":[\"private\"]}");
            assertEquals(response.body(), 403, response.statusCode());
            assertFalse(response.body(), response.body().contains("private-value"));
        });
    }

    public void testMultiGetUsesACachedNativeResponseWithoutReading() throws Exception {
        final var cached = new MultiGetResponse(
            new MultiGetItemResponse[] {
                new MultiGetItemResponse(
                    new GetResponse(
                        new GetResult(
                            "cached",
                            "cached",
                            0L,
                            1L,
                            1L,
                            true,
                            new BytesArray("{\"msg\":\"cached-value\"}"),
                            Map.of(),
                            Map.of()
                        )
                    ),
                    null
                ) }
        );
        final var plugin = new MultiFilterPlugin(null, null, cached);
        withNode(plugin, false, (node, metadata) -> {
            final var response = send(node, "{\"ids\":[\"private\"]}");
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("cached-value"));
            assertTrue(metadata.describe("docs").isEmpty());
        });
    }

    private void withNode(
        MultiFilterPlugin plugin,
        boolean populate,
        org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work
    ) throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "mget-request-contract")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings, List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                if (populate) {
                    metadata.createIndex(
                        new IndexDescriptor("docs", "uuid-docs", 1, "{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}", null)
                    );
                    final var shard = node.activateWriter(metadata, "docs", 0).orElseThrow();
                    node.index(shard, "private", "{\"msg\":\"private-value\"}");
                    node.index(shard, "public", "{\"msg\":\"public-value\"}");
                }
                work.accept(node, metadata);
            }
        }
    }

    private static HttpResponse<String> send(ServerlessNode node, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final var request = HttpRequest.newBuilder(
                URI.create("http://" + address.getAddress() + ":" + address.getPort() + "/docs/_mget")
            )
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }
}
