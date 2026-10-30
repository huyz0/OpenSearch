/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.opensearch.action.admin.cluster.storedscripts.PutStoredScriptAction;
import org.opensearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;

/** Native request fidelity, admitted selection and response authorization for stored scripts. */
public class StoredScriptRequestExecutionTests extends OpenSearchTestCase {
    private static String script(String value) {
        return "{\"script\":{\"lang\":\"mustache\",\"source\":\"" + value + "\"}}";
    }

    public static final class ScriptFilterPlugin extends org.opensearch.plugins.Plugin implements org.opensearch.plugins.ActionPlugin {
        private final String target;
        private final Function<ActionRequest, ActionRequest> rewrite;
        private final boolean shortCircuit;
        private final boolean replaceResponse;

        ScriptFilterPlugin(String target, Function<ActionRequest, ActionRequest> rewrite) {
            this(target, rewrite, false, false);
        }

        ScriptFilterPlugin(String target, Function<ActionRequest, ActionRequest> rewrite, boolean shortCircuit, boolean replaceResponse) {
            this.target = target;
            this.rewrite = rewrite;
            this.shortCircuit = shortCircuit;
            this.replaceResponse = replaceResponse;
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
                    if (target.equals(action) == false) {
                        chain.proceed(task, action, request, listener);
                    } else if (shortCircuit) {
                        listener.onResponse(
                            (Response) (target.equals(GetStoredScriptAction.NAME)
                                ? filteredGet()
                                : new org.opensearch.action.support.clustermanager.AcknowledgedResponse(false))
                        );
                    } else if (replaceResponse) {
                        chain.proceed(task, action, request, org.opensearch.core.action.ActionListener.wrap(response -> {
                            if (target.equals(GetStoredScriptAction.NAME)) {
                                final var get = (org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptResponse) response;
                                assertEquals("private", get.getId());
                                assertEquals("private-value", get.getSource().getSource());
                                listener.onResponse((Response) filteredGet());
                            } else {
                                final var acknowledged = (org.opensearch.action.support.clustermanager.AcknowledgedResponse) response;
                                assertTrue(acknowledged.isAcknowledged());
                                listener.onResponse(
                                    (Response) new org.opensearch.action.support.clustermanager.AcknowledgedResponse(false)
                                );
                            }
                        }, listener::onFailure));
                    } else if (rewrite != null) {
                        chain.proceed(task, action, (Request) rewrite.apply(request), listener);
                    } else {
                        chain.proceed(
                            task,
                            action,
                            request,
                            org.opensearch.core.action.ActionListener.wrap(
                                response -> listener.onFailure(
                                    new org.opensearch.OpenSearchSecurityException(
                                        "pipeline access denied",
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

    private void withNode(ScriptFilterPlugin plugin, org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work)
        throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "pipeline-filter-contract")
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

    public void testPutExposesNativeBodyAndContext() throws Exception {
        withNode(new ScriptFilterPlugin(PutStoredScriptAction.NAME, request -> {
            final var put = (PutStoredScriptRequest) request;
            assertEquals("original", put.id());
            assertEquals("template", put.context());
            assertEquals("original-value", put.source().getSource());
            return request;
        }), (node, metadata) -> {
            final var response = send(node, "PUT", "/_scripts/original/template", script("original-value"));
            assertEquals(response.body(), 200, response.statusCode());
        });
    }

    public void testPutUsesAdmittedIdBodyAndContext() throws Exception {
        withNode(
            new ScriptFilterPlugin(
                PutStoredScriptAction.NAME,
                request -> new PutStoredScriptRequest().id("selected")
                    .context("template")
                    .content(new BytesArray(script("selected-value")), XContentType.JSON)
            ),
            (node, metadata) -> {
                final var response = send(node, "PUT", "/_scripts/original", script("original-value"));
                assertEquals(response.body(), 200, response.statusCode());
                assertTrue(metadata.scripts().get("original").isEmpty());
                assertTrue(metadata.scripts().get("selected").orElseThrow().contains("selected-value"));
            }
        );
    }

    public void testDeleteUsesTheNativeAdmittedId() throws Exception {
        withNode(new ScriptFilterPlugin(DeleteStoredScriptAction.NAME, request -> {
            assertEquals("original", ((DeleteStoredScriptRequest) request).id());
            return new DeleteStoredScriptRequest("selected");
        }), (node, metadata) -> {
            metadata.scripts().put("original", script("original-value"));
            metadata.scripts().put("selected", script("selected-value"));
            final var response = send(node, "DELETE", "/_scripts/original", null);
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(metadata.scripts().get("original").isPresent());
            assertTrue(metadata.scripts().get("selected").isEmpty());
        });
    }

    public void testGetUsesTheAdmittedId() throws Exception {
        withNode(
            new ScriptFilterPlugin(GetStoredScriptAction.NAME, request -> new GetStoredScriptRequest("selected")),
            (node, metadata) -> {
                metadata.scripts().put("original", script("original-value"));
                metadata.scripts().put("selected", script("selected-value"));
                final var response = send(node, "GET", "/_scripts/original", null);
                assertEquals(response.body(), 200, response.statusCode());
                assertTrue(response.body(), response.body().contains("selected-value"));
                assertFalse(response.body(), response.body().contains("original-value"));
            }
        );
    }

    public void testGetWaitsForPostFilterAuthorization() throws Exception {
        withNode(new ScriptFilterPlugin(GetStoredScriptAction.NAME, null), (node, metadata) -> {
            metadata.scripts().put("private", script("private-value"));
            final var response = send(node, "GET", "/_scripts/private", null);
            assertEquals(response.body(), 403, response.statusCode());
            assertFalse(response.body(), response.body().contains("private-value"));
        });
    }

    public void testPutUsesTheReplacedNativeAcknowledgement() throws Exception {
        withNode(new ScriptFilterPlugin(PutStoredScriptAction.NAME, null, false, true), (node, metadata) -> {
            final var response = send(node, "PUT", "/_scripts/original", script("original-value"));
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"acknowledged\":false"));
            assertTrue(metadata.scripts().get("original").isPresent());
        });
    }

    public void testShortCircuitedPutDoesNotStoreAScript() throws Exception {
        withNode(new ScriptFilterPlugin(PutStoredScriptAction.NAME, null, true, false), (node, metadata) -> {
            final var response = send(node, "PUT", "/_scripts/original", script("not-stored"));
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"acknowledged\":false"));
            assertTrue(metadata.scripts().get("original").isEmpty());
        });
    }

    private static org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptResponse filteredGet() {
        try (
            var parser = org.opensearch.common.xcontent.XContentType.JSON.xContent()
                .createParser(
                    org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                    org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    "{\"_id\":\"filtered\",\"found\":true,\"script\":{\"lang\":\"mustache\",\"source\":\"filtered-value\"}}"
                )
        ) {
            return org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptResponse.fromXContent(parser);
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }

    public void testGetExposesNativeResponseAndUsesItsReplacement() throws Exception {
        withNode(new ScriptFilterPlugin(GetStoredScriptAction.NAME, null, false, true), (node, metadata) -> {
            metadata.scripts().put("private", script("private-value"));
            final var response = send(node, "GET", "/_scripts/private", null);
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("filtered-value"));
            assertFalse(response.body(), response.body().contains("private-value"));
        });
    }

    public void testCachedNativeGetResponseCanShortCircuitStorage() throws Exception {
        withNode(new ScriptFilterPlugin(GetStoredScriptAction.NAME, null, true, false), (node, metadata) -> {
            final var response = send(node, "GET", "/_scripts/missing", null);
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("filtered-value"));
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
