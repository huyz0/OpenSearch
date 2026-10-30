/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ingest.DeletePipelineAction;
import org.opensearch.action.ingest.DeletePipelineRequest;
import org.opensearch.action.ingest.GetPipelineAction;
import org.opensearch.action.ingest.GetPipelineRequest;
import org.opensearch.action.ingest.PutPipelineAction;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.ingest.SimulatePipelineAction;
import org.opensearch.action.ingest.SimulatePipelineRequest;
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

/** Native filter request types, admitted selection and deferred HTTP responses for ingest pipelines. */
public class PipelineRequestExecutionTests extends OpenSearchTestCase {
    private static String pipeline(String value) {
        return "{\"processors\":[{\"set\":{\"field\":\"tag\",\"value\":\"" + value + "\"}}]}";
    }

    public static final class PipelineFilterPlugin extends org.opensearch.plugins.Plugin implements org.opensearch.plugins.ActionPlugin {
        private final String target;
        private final Function<ActionRequest, ActionRequest> rewrite;
        private final boolean acknowledgeOnly;
        private final boolean replaceAcknowledgement;

        PipelineFilterPlugin(String target, Function<ActionRequest, ActionRequest> rewrite) {
            this(target, rewrite, false, false);
        }

        PipelineFilterPlugin(
            String target,
            Function<ActionRequest, ActionRequest> rewrite,
            boolean acknowledgeOnly,
            boolean replaceAcknowledgement
        ) {
            this.target = target;
            this.rewrite = rewrite;
            this.acknowledgeOnly = acknowledgeOnly;
            this.replaceAcknowledgement = replaceAcknowledgement;
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
                    } else if (acknowledgeOnly) {
                        listener.onResponse((Response) new org.opensearch.action.support.clustermanager.AcknowledgedResponse(false));
                    } else if (replaceAcknowledgement) {
                        chain.proceed(task, action, request, org.opensearch.core.action.ActionListener.wrap(response -> {
                            final var acknowledged = (org.opensearch.action.support.clustermanager.AcknowledgedResponse) response;
                            if (acknowledged.isAcknowledged() == false) {
                                throw new IllegalArgumentException("the write was acknowledged before the filter changed it");
                            }
                            listener.onResponse((Response) new org.opensearch.action.support.clustermanager.AcknowledgedResponse(false));
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

    private void withNode(
        PipelineFilterPlugin plugin,
        org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work
    ) throws Exception {
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

    public void testPutShowsTheNativeAcknowledgementAndUsesItsReplacement() throws Exception {
        withNode(new PipelineFilterPlugin(PutPipelineAction.NAME, null, false, true), (node, metadata) -> {
            final var response = send(node, "PUT", "/_ingest/pipeline/original", pipeline("stored-value"));
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"acknowledged\":false"));
            assertTrue(metadata.pipelines().get("original").isPresent());
        });
    }

    public void testShortCircuitedAcknowledgementDoesNotRunTheWrite() throws Exception {
        withNode(new PipelineFilterPlugin(PutPipelineAction.NAME, null, true, false), (node, metadata) -> {
            final var response = send(node, "PUT", "/_ingest/pipeline/original", pipeline("not-stored"));
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"acknowledged\":false"));
            assertTrue(metadata.pipelines().get("original").isEmpty());
        });
    }

    public void testPutFilterReceivesTheNativeRequest() throws Exception {
        withNode(new PipelineFilterPlugin(PutPipelineAction.NAME, request -> {
            final var put = (PutPipelineRequest) request;
            assertEquals("original", put.getId());
            assertTrue(put.getSource().utf8ToString(), put.getSource().utf8ToString().contains("original-value"));
            return request;
        }), (node, metadata) -> {
            final var response = send(node, "PUT", "/_ingest/pipeline/original", pipeline("original-value"));
            assertEquals(response.body(), 200, response.statusCode());
        });
    }

    public void testPutUsesTheAdmittedIdAndBody() throws Exception {
        withNode(
            new PipelineFilterPlugin(
                PutPipelineAction.NAME,
                request -> new PutPipelineRequest("selected", new BytesArray(pipeline("selected-value")), XContentType.JSON)
            ),
            (node, metadata) -> {
                final var response = send(node, "PUT", "/_ingest/pipeline/original", pipeline("original-value"));
                assertEquals(response.body(), 200, response.statusCode());
                assertTrue(metadata.pipelines().get("original").isEmpty());
                assertTrue(metadata.pipelines().get("selected").orElseThrow().contains("selected-value"));
            }
        );
    }

    public void testDeleteUsesTheNativeAdmittedRequest() throws Exception {
        withNode(new PipelineFilterPlugin(DeletePipelineAction.NAME, request -> {
            final var delete = (DeletePipelineRequest) request;
            if (delete.getId().equals("original") == false) {
                throw new IllegalArgumentException("the filter must see the actual pipeline ID");
            }
            return new DeletePipelineRequest("selected");
        }), (node, metadata) -> {
            metadata.pipelines().put("original", pipeline("original-value"));
            metadata.pipelines().put("selected", pipeline("selected-value"));
            final var response = send(node, "DELETE", "/_ingest/pipeline/original", null);
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(metadata.pipelines().get("original").isPresent());
            assertTrue(metadata.pipelines().get("selected").isEmpty());
        });
    }

    public void testGetUsesTheAdmittedSelector() throws Exception {
        withNode(new PipelineFilterPlugin(GetPipelineAction.NAME, request -> new GetPipelineRequest("public")), (node, metadata) -> {
            metadata.pipelines().put("private", pipeline("private-value"));
            metadata.pipelines().put("public", pipeline("public-value"));
            final var response = send(node, "GET", "/_ingest/pipeline", null);
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("public-value"));
            assertFalse(response.body(), response.body().contains("private-value"));
        });
    }

    public void testGetDenialAfterWorkReachesHttp() throws Exception {
        withNode(new PipelineFilterPlugin(GetPipelineAction.NAME, null), (node, metadata) -> {
            metadata.pipelines().put("private", pipeline("private-value"));
            final var response = send(node, "GET", "/_ingest/pipeline/private", null);
            assertEquals(response.body(), 403, response.statusCode());
            assertFalse(response.body(), response.body().contains("private-value"));
        });
    }

    public void testSimulateUsesTheAdmittedSourceAndId() throws Exception {
        withNode(new PipelineFilterPlugin(SimulatePipelineAction.NAME, request -> {
            final var original = (SimulatePipelineRequest) request;
            if ("original".equals(original.getId()) == false) {
                throw new IllegalArgumentException("the filter must see the simulation's stored pipeline ID");
            }
            final var selected = new SimulatePipelineRequest(new BytesArray("{\"docs\":[{\"_source\":{}}]}"), XContentType.JSON);
            selected.setId("selected");
            return selected;
        }), (node, metadata) -> {
            metadata.pipelines().put("original", pipeline("original-value"));
            metadata.pipelines().put("selected", pipeline("selected-value"));
            final var response = send(node, "POST", "/_ingest/pipeline/original/_simulate", "{\"docs\":[{\"_source\":{}}]}");
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("selected-value"));
            assertFalse(response.body(), response.body().contains("original-value"));
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
