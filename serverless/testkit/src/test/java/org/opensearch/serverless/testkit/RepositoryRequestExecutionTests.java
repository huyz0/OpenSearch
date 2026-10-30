/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RepositoryDescriptor;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class RepositoryRequestExecutionTests extends OpenSearchTestCase {
    private void withNode(PolicyFilterPlugin plugin, org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work)
        throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "repository-filter-contract")
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

    private static void put(MetadataPlane metadata, String name) throws Exception {
        metadata.putRepository(new RepositoryDescriptor(name, "fs", Map.of("location", name), 1000L));
    }

    private static GetRepositoriesResponse repositories(String body) throws Exception {
        try (
            var parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, body)
        ) {
            return GetRepositoriesResponse.fromXContent(parser);
        }
    }

    public void testPutExposesTheNativeBodyAndVerifyFlag() throws Exception {
        withNode(PolicyFilterPlugin.request(PutRepositoryAction.NAME, request -> {
            final var put = (PutRepositoryRequest) request;
            assertEquals("original", put.name());
            assertEquals("fs", put.type());
            assertEquals("true", put.settings().get("remote_store_index_shallow_copy"));
            assertFalse(put.verify());
            return request;
        }), (node, metadata) -> {
            var response = send(
                node,
                "PUT",
                "/_snapshot/original?verify=false",
                "{\"type\":\"fs\",\"settings\":{\"remote_store_index_shallow_copy\":true}}"
            );
            assertEquals(response.body(), 200, response.statusCode());
        });
    }

    public void testPutUsesTheAdmittedNameAndSettings() throws Exception {
        withNode(
            PolicyFilterPlugin.request(
                PutRepositoryAction.NAME,
                request -> new PutRepositoryRequest("selected").type("fs").settings(Map.of("remote_store_index_shallow_copy", true))
            ),
            (node, metadata) -> {
                var response = send(node, "PUT", "/_snapshot/original", "{\"type\":\"fs\"}");
                assertEquals(response.body(), 200, response.statusCode());
                assertTrue(metadata.describeRepository("original").isEmpty());
                assertTrue(metadata.describeRepository("selected").orElseThrow().shallowByDefault());
            }
        );
    }

    public void testGetUsesTheAdmittedSelection() throws Exception {
        withNode(
            PolicyFilterPlugin.request(GetRepositoriesAction.NAME, request -> new GetRepositoriesRequest(new String[] { "selected" })),
            (node, metadata) -> {
                put(metadata, "selected");
                var response = send(node, "GET", "/_snapshot/original", null);
                assertEquals(response.body(), 200, response.statusCode());
                assertTrue(response.body(), response.body().contains("\"selected\""));
                assertFalse(response.body(), response.body().contains("\"original\""));
            }
        );
    }

    public void testGetHonorsCachedNativeResponses() throws Exception {
        final var cached = repositories("{\"cached\":{\"type\":\"fs\",\"settings\":{\"location\":\"safe\"}}}");
        withNode(PolicyFilterPlugin.cached(GetRepositoriesAction.NAME, cached), (node, metadata) -> {
            var response = send(node, "GET", "/_snapshot/missing", null);
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"cached\""));
        });
    }

    public void testGetExposesTypedResponsesAndHonorsRedaction() throws Exception {
        final var redacted = repositories("{}");
        withNode(PolicyFilterPlugin.response(GetRepositoriesAction.NAME, response -> {
            assertTrue(response.toString(), response instanceof GetRepositoriesResponse);
            assertEquals(1, ((GetRepositoriesResponse) response).repositories().size());
            return redacted;
        }), (node, metadata) -> {
            put(metadata, "original");
            var response = send(node, "GET", "/_snapshot/original", null);
            assertEquals(response.body(), 200, response.statusCode());
            assertEquals("{}", response.body());
        });
    }

    public void testDeleteUsesTheAdmittedName() throws Exception {
        withNode(
            PolicyFilterPlugin.request(DeleteRepositoryAction.NAME, request -> new DeleteRepositoryRequest("selected")),
            (node, metadata) -> {
                put(metadata, "original");
                put(metadata, "selected");
                var response = send(node, "DELETE", "/_snapshot/original", null);
                assertEquals(response.body(), 200, response.statusCode());
                assertTrue(metadata.describeRepository("original").isPresent());
                assertTrue(metadata.describeRepository("selected").isEmpty());
            }
        );
    }

    public void testPutHonorsCachedAcknowledgementsWithoutMutation() throws Exception {
        withNode(PolicyFilterPlugin.cached(PutRepositoryAction.NAME, new AcknowledgedResponse(false)), (node, metadata) -> {
            var response = send(node, "PUT", "/_snapshot/original", "{\"type\":\"fs\"}");
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"acknowledged\":false"));
            assertTrue(metadata.describeRepository("original").isEmpty());
        });
    }

    public void testVerifyPassesThroughAuthorizationBeforeResponding() throws Exception {
        withNode(PolicyFilterPlugin.denyAfter(VerifyRepositoryAction.NAME), (node, metadata) -> {
            put(metadata, "original");
            var response = send(node, "POST", "/_snapshot/original/_verify", null);
            assertEquals(response.body(), 403, response.statusCode());
        });
    }

    public void testVerifyUsesTheAdmittedRepository() throws Exception {
        withNode(
            PolicyFilterPlugin.request(VerifyRepositoryAction.NAME, request -> new VerifyRepositoryRequest("selected")),
            (node, metadata) -> {
                put(metadata, "selected");
                var response = send(node, "POST", "/_snapshot/missing/_verify", null);
                assertEquals(response.body(), 200, response.statusCode());
                assertTrue(response.body(), response.body().contains(node.localNode().getId()));
            }
        );
    }

    public void testVerifyHonorsCachedNativeResponses() throws Exception {
        withNode(PolicyFilterPlugin.cached(VerifyRepositoryAction.NAME, new VerifyRepositoryResponse(List.of())), (node, metadata) -> {
            var response = send(node, "POST", "/_snapshot/missing/_verify", null);
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"nodes\":{}"));
        });
    }

    public void testMalformedSettingsAreRejectedBeforeMutation() throws Exception {
        withNode(PolicyFilterPlugin.request(PutRepositoryAction.NAME, request -> request), (node, metadata) -> {
            var response = send(node, "PUT", "/_snapshot/original", "{\"type\":\"fs\",\"settings\":[]}");
            assertEquals(response.body(), 400, response.statusCode());
            assertTrue(metadata.describeRepository("original").isEmpty());
        });
    }

    public void testUnsupportedAdmittedRepositoryTypesAreRefused() throws Exception {
        withNode(
            PolicyFilterPlugin.request(PutRepositoryAction.NAME, request -> new PutRepositoryRequest("selected").type("s3")),
            (node, metadata) -> {
                var response = send(node, "PUT", "/_snapshot/original", "{\"type\":\"fs\"}");
                assertEquals(response.body(), 501, response.statusCode());
                assertTrue(metadata.describeRepository("original").isEmpty());
                assertTrue(metadata.describeRepository("selected").isEmpty());
            }
        );
    }

    public void testAdmittedRepositoryNamesAreValidatedBeforeWriting() throws Exception {
        withNode(
            PolicyFilterPlugin.request(PutRepositoryAction.NAME, request -> new PutRepositoryRequest("../unsafe").type("fs")),
            (node, metadata) -> {
                var response = send(node, "PUT", "/_snapshot/original", "{\"type\":\"fs\"}");
                assertEquals(response.body(), 400, response.statusCode());
                assertTrue(metadata.listRepositories().isEmpty());
            }
        );
    }

    public void testVerifyExposesNativeResponsesAndHonorsRedaction() throws Exception {
        withNode(PolicyFilterPlugin.response(VerifyRepositoryAction.NAME, response -> {
            final var verified = (VerifyRepositoryResponse) response;
            assertEquals(1, verified.getNodes().size());
            return new VerifyRepositoryResponse(List.of());
        }), (node, metadata) -> {
            put(metadata, "original");
            var response = send(node, "POST", "/_snapshot/original/_verify", null);
            assertEquals(response.body(), 200, response.statusCode());
            assertTrue(response.body(), response.body().contains("\"nodes\":{}"));
        });
    }

    public void testNativeSettingsListsSurviveRegistration() throws Exception {
        withNode(PolicyFilterPlugin.request(PutRepositoryAction.NAME, request -> request), (node, metadata) -> {
            var response = send(node, "PUT", "/_snapshot/original", "{\"type\":\"fs\",\"settings\":{\"labels\":[\"one\",\"two\"]}}");
            assertEquals(response.body(), 200, response.statusCode());
            assertEquals(List.of("one", "two"), metadata.describeRepository("original").orElseThrow().settings().get("labels"));
        });
    }

    public void testReservedRepositoryApiNamesRemainUnsupported() throws Exception {
        withNode(PolicyFilterPlugin.request(GetRepositoriesAction.NAME, request -> request), (node, metadata) -> {
            var response = send(node, "GET", "/_snapshot/_unsupported", null);
            assertEquals(response.body(), 501, response.statusCode());
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
