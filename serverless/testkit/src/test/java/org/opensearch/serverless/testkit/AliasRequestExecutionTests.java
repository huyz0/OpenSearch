/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.cluster.AliasRecord;
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
import java.util.concurrent.atomic.AtomicReference;

/** Alias administration must finish authorization before returning data and keep store IO off HTTP workers. */
public class AliasRequestExecutionTests extends OpenSearchTestCase {
    private Settings settings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "alias-execution")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane seed(BlobStore store) throws Exception {
        final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1_000L, 30_000L);
        metadata.createIndex(new IndexDescriptor("protected", "uuid-protected", 1, "{}", null));
        metadata.createAlias(new AliasRecord("a", List.of("protected")));
        return metadata;
    }

    public static final class AliasReadFilterPlugin extends org.opensearch.plugins.Plugin implements org.opensearch.plugins.ActionPlugin {
        private enum Behavior {
            DENY_BEFORE,
            DENY_AFTER,
            REWRITE
        }

        private final Behavior behavior;

        AliasReadFilterPlugin(boolean after) {
            this(after ? Behavior.DENY_AFTER : Behavior.DENY_BEFORE);
        }

        AliasReadFilterPlugin(Behavior behavior) {
            this.behavior = behavior;
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
                public <
                    Request extends org.opensearch.action.ActionRequest,
                    Response extends org.opensearch.core.action.ActionResponse> void apply(
                        org.opensearch.tasks.Task task,
                        String action,
                        Request request,
                        org.opensearch.action.support.ActionRequestMetadata<Request, Response> metadata,
                        org.opensearch.core.action.ActionListener<Response> listener,
                        org.opensearch.action.support.ActionFilterChain<Request, Response> chain
                    ) {
                    if (org.opensearch.action.admin.indices.alias.get.GetAliasesAction.NAME.equals(action)) {
                        if (behavior == Behavior.REWRITE) {
                            chain.proceed(
                                task,
                                action,
                                (Request) new org.opensearch.action.admin.indices.alias.get.GetAliasesRequest("public").indices(
                                    "public-index"
                                ),
                                listener
                            );
                        } else if (behavior == Behavior.DENY_AFTER) {
                            chain.proceed(
                                task,
                                action,
                                request,
                                org.opensearch.core.action.ActionListener.wrap(response -> deny(listener), listener::onFailure)
                            );
                        } else {
                            deny(listener);
                        }
                    } else {
                        chain.proceed(task, action, request, listener);
                    }
                }
            });
        }

        private static void deny(org.opensearch.core.action.ActionListener<?> listener) {
            listener.onFailure(
                new org.opensearch.OpenSearchSecurityException("alias access denied", org.opensearch.core.rest.RestStatus.FORBIDDEN)
            );
        }
    }

    public void testEveryAliasReadFormRunsItsAuthorizationFilter() throws Exception {
        assertAliasReadsDenied(false);
    }

    public void testPostFilterDenialCannotLeakAnAliasResponse() throws Exception {
        assertAliasReadsDenied(true);
    }

    private void assertAliasReadsDenied(boolean after) throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = seed(store);
            try (var node = new ServerlessNode(settings("alias-denial-" + after), List.of(new AliasReadFilterPlugin(after)))) {
                node.start();
                node.setMetadataPlane(metadata);
                for (String path : List.of("/_alias/a", "/protected/_alias/a", "/protected/_alias")) {
                    final var response = send(node, "GET", path, null);
                    assertEquals(response.body(), 403, response.statusCode());
                    assertTrue(response.body(), response.body().contains("alias access denied"));
                    assertFalse(response.body(), response.body().contains("\"aliases\":{"));
                }
            }
        }
    }

    public void testAliasStoreReadsRunAwayFromTheHttpWorker() throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = seed(store);
            try (var node = new ServerlessNode(settings("alias-worker"))) {
                node.start();
                node.setMetadataPlane(metadata);
                record Call(String method, String path, String body, String key) {
                }
                for (var call : List.of(
                    new Call("GET", "/_alias/a", null, "a"),
                    new Call("GET", "/protected/_alias", null, "protected"),
                    new Call("PUT", "/protected/_alias/a", null, "protected"),
                    new Call("POST", "/_aliases", "{\"actions\":[{\"add\":{\"index\":\"protected\",\"alias\":\"a\"}}]}", "a")
                )) {
                    final var observed = new AtomicReference<Thread>();
                    store.onNextRegisterRead("indices", call.key(), () -> observed.set(Thread.currentThread()));
                    final var response = send(node, call.method(), call.path(), call.body());
                    assertEquals(response.body(), 200, response.statusCode());
                    assertTrue("the request must read its metadata", store.fired());
                    assertFalse(
                        "store IO on " + observed.get().getName(),
                        org.opensearch.transport.Transports.isTransportThread(observed.get())
                    );
                }
            }
        }
    }

    public void testAliasReadFiltersCanReplaceTheSelection() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = seed(store);
            metadata.createIndex(new IndexDescriptor("public-index", "uuid-public", 1, "{}", null));
            metadata.createAlias(new AliasRecord("public", List.of("public-index")));
            try (
                var node = new ServerlessNode(
                    settings("alias-read-rewrite"),
                    List.of(new AliasReadFilterPlugin(AliasReadFilterPlugin.Behavior.REWRITE))
                )
            ) {
                node.start();
                node.setMetadataPlane(metadata);
                for (String path : List.of("/_alias/a", "/protected/_alias/a", "/protected/_alias")) {
                    final var response = send(node, "GET", path, null);
                    assertEquals(response.body(), 200, response.statusCode());
                    assertTrue(response.body(), response.body().contains("\"public-index\""));
                    assertTrue(response.body(), response.body().contains("\"public\""));
                    assertFalse(response.body(), response.body().contains("\"protected\""));
                }
            }
        }
    }

    public void testReverseAliasReadDoesNotRenderDataStreamsAsAliases() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = seed(store);
            metadata.createAlias(new AliasRecord("stream", List.of("protected"), true, 1L, "@timestamp"));
            try (var node = new ServerlessNode(settings("alias-read-stream"))) {
                node.start();
                node.setMetadataPlane(metadata);
                final var response = send(node, "GET", "/protected/_alias", null);
                assertEquals(response.body(), 200, response.statusCode());
                assertTrue(response.body(), response.body().contains("\"a\""));
                assertFalse(response.body(), response.body().contains("\"stream\""));
            }
        }
    }

    private static HttpResponse<String> send(ServerlessNode node, String method, String path, String body) throws Exception {
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final var request = HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + node.boundHttpAddress().publishAddress().getPort() + path)
            )
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }
}
