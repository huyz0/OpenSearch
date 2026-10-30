/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.GetAllPitsAction;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/** PIT HTTP responses and operations must reflect the completed action-filter chain. */
public class PointInTimeRequestExecutionTests extends OpenSearchTestCase {
    public static final class PitFilterPlugin extends org.opensearch.plugins.Plugin
        implements
            org.opensearch.plugins.ActionPlugin,
            org.opensearch.plugins.SystemIndexPlugin {
        private final String action;
        private final Function<ActionRequest, ActionRequest> rewrite;

        PitFilterPlugin(String action, Function<ActionRequest, ActionRequest> rewrite) {
            this.action = action;
            this.rewrite = rewrite;
        }

        @Override
        public java.util.Collection<org.opensearch.indices.SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(new org.opensearch.indices.SystemIndexDescriptor(".pit_owned", "a plugin's private PIT source"));
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
                    String name,
                    Request request,
                    org.opensearch.action.support.ActionRequestMetadata<Request, Response> metadata,
                    org.opensearch.core.action.ActionListener<Response> listener,
                    org.opensearch.action.support.ActionFilterChain<Request, Response> chain
                ) {
                    if (action.equals(name) == false) {
                        chain.proceed(task, name, request, listener);
                    } else if (rewrite != null) {
                        chain.proceed(task, name, (Request) rewrite.apply(request), listener);
                    } else {
                        chain.proceed(
                            task,
                            name,
                            request,
                            org.opensearch.core.action.ActionListener.wrap(
                                response -> listener.onFailure(
                                    new org.opensearch.OpenSearchSecurityException(
                                        "PIT access denied after work",
                                        org.opensearch.core.rest.RestStatus.FORBIDDEN
                                    )
                                ),
                                listener::onFailure
                            )
                        );
                    }
                }
            });
        }
    }

    private void withNode(
        org.opensearch.plugins.Plugin plugin,
        org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work
    ) throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "pit-filter-contract")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest,search")
                .build();
            try (var node = new ServerlessNode(settings, List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                for (String index : List.of("private-index", "public-index", ".pit_owned")) {
                    metadata.createIndex(new IndexDescriptor(index, "uuid-" + index, 1, "{}", null));
                    final var shard = node.activateWriter(metadata, index, 0).orElseThrow();
                    node.index(shard, "doc", "{}");
                    node.publishShard(shard, metadata.heads().read(index, 0).orElseThrow().term());
                }
                work.accept(node, metadata);
            }
        }
    }

    public static final class NativeResponsePlugin extends org.opensearch.plugins.Plugin implements org.opensearch.plugins.ActionPlugin {
        private final boolean cached;

        NativeResponsePlugin(boolean cached) {
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
                public <Request extends ActionRequest, Response extends org.opensearch.core.action.ActionResponse> void apply(
                    org.opensearch.tasks.Task task,
                    String action,
                    Request request,
                    org.opensearch.action.support.ActionRequestMetadata<Request, Response> metadata,
                    org.opensearch.core.action.ActionListener<Response> listener,
                    org.opensearch.action.support.ActionFilterChain<Request, Response> chain
                ) {
                    if (CreatePitAction.NAME.equals(action) == false) {
                        chain.proceed(task, action, request, listener);
                        return;
                    }
                    final Response replacement = (Response) new org.opensearch.action.search.CreatePitResponse(
                        "filtered-id",
                        1000L,
                        1,
                        1,
                        0,
                        0,
                        org.opensearch.action.search.ShardSearchFailure.EMPTY_ARRAY
                    );
                    if (cached) {
                        listener.onResponse(replacement);
                    } else {
                        chain.proceed(
                            task,
                            action,
                            request,
                            org.opensearch.core.action.ActionListener.wrap(ignored -> listener.onResponse(replacement), listener::onFailure)
                        );
                    }
                }
            });
        }
    }

    public void testCachedNativeResponseIsRefusedWithoutHanging() throws Exception {
        assertNativeResponseRefused(true);
    }

    public void testRewrittenNativeResponseCannotExposeTheOriginalBody() throws Exception {
        assertNativeResponseRefused(false);
    }

    private void assertNativeResponseRefused(boolean cached) throws Exception {
        withNode(new NativeResponsePlugin(cached), (node, metadata) -> {
            final var response = send(node, "POST", "/private-index/_pit?keep_alive=30s", null);
            assertEquals(response.body(), 501, response.statusCode());
            assertFalse("the unfiltered body must not reach HTTP: " + response.body(), response.body().contains("\"pit_id\""));
            if (cached) {
                assertTrue("a short-circuited request did not create a pin", metadata.livePointsInTime(1000L).isEmpty());
            }
            assertBusy(
                () -> assertEquals(
                    0L,
                    node.circuitBreakerService().getBreaker(org.opensearch.core.common.breaker.CircuitBreaker.REQUEST).getUsed()
                )
            );
        });
    }

    public void testCreateDenialAfterWorkReachesHttp() throws Exception {
        withNode(new PitFilterPlugin(CreatePitAction.NAME, null), (node, metadata) -> {
            final var response = send(node, "POST", "/private-index/_pit?keep_alive=30s", null);
            assertEquals(response.body(), 403, response.statusCode());
            assertFalse(response.body(), response.body().contains("\"pit_id\""));
        });
    }

    public void testListDenialAfterWorkDoesNotExposePitIds() throws Exception {
        withNode(new PitFilterPlugin(GetAllPitsAction.NAME, null), (node, metadata) -> {
            final String id = create(node, "private-index");
            final var response = send(node, "GET", "/_search/point_in_time/_all", null);
            assertEquals(response.body(), 403, response.statusCode());
            assertFalse(response.body(), response.body().contains(id));
        });
    }

    public void testDeleteDenialAfterWorkReachesHttp() throws Exception {
        withNode(new PitFilterPlugin(DeletePitAction.NAME, null), (node, metadata) -> {
            final String id = create(node, "private-index");
            final var response = send(node, "DELETE", "/_search/point_in_time", "{\"pit_id\":[\"" + id + "\"]}");
            assertEquals(response.body(), 403, response.statusCode());
        });
    }

    public void testCreateUsesTheAdmittedIndexAndKeepAlive() throws Exception {
        withNode(
            new PitFilterPlugin(
                CreatePitAction.NAME,
                request -> new CreatePitRequest(TimeValue.timeValueSeconds(10), false, "public-index")
            ),
            (node, metadata) -> {
                final var response = send(node, "POST", "/private-index/_pit?keep_alive=30s", null);
                assertEquals(response.body(), 200, response.statusCode());
                final var body = json(response.body());
                assertEquals("public-index", body.get("index"));
                assertEquals(10_000L, ((Number) body.get("keep_alive_millis")).longValue());
                final var pit = metadata.pointInTime((String) body.get("pit_id")).orElseThrow();
                assertEquals("public-index", pit.index());
                assertEquals(11_000L, pit.expiresAtMillis());
            }
        );
    }

    public void testAdmittedCreateCannotBypassTheSystemIndexGuard() throws Exception {
        withNode(
            new PitFilterPlugin(CreatePitAction.NAME, request -> new CreatePitRequest(TimeValue.timeValueSeconds(10), false, ".pit_owned")),
            (node, metadata) -> {
                final var response = send(node, "POST", "/private-index/_pit?keep_alive=30s", null);
                assertEquals(response.body(), 403, response.statusCode());
                assertTrue("a rewritten HTTP request cannot pin a plugin's private index", metadata.livePointsInTime(1000L).isEmpty());
            }
        );
    }

    public void testDeleteUsesTheAdmittedIds() throws Exception {
        final var admitted = new AtomicReference<String>();
        withNode(new PitFilterPlugin(DeletePitAction.NAME, request -> new DeletePitRequest(List.of(admitted.get()))), (node, metadata) -> {
            final String requested = create(node, "private-index");
            final String selected = create(node, "public-index");
            admitted.set(selected);
            final var response = send(node, "DELETE", "/_search/point_in_time", "{\"pit_id\":[\"" + requested + "\"]}");
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue("the original request was not admitted", metadata.pointInTime(requested).isPresent());
            assertTrue("the selected view was released", metadata.pointInTime(selected).isEmpty());
        });
    }

    private static String create(ServerlessNode node, String index) throws Exception {
        final var response = send(node, "POST", "/" + index + "/_pit?keep_alive=30s", null);
        assertEquals(response.body(), 200, response.statusCode());
        return (String) json(response.body()).get("pit_id");
    }

    private static Map<String, Object> json(String body) throws Exception {
        try (
            var parser = org.opensearch.common.xcontent.json.JsonXContent.jsonXContent.createParser(
                org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS,
                body
            )
        ) {
            return parser.map();
        }
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
