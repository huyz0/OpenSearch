/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.admin.indices.datastream.CreateDataStreamAction;
import org.opensearch.action.admin.indices.datastream.DeleteDataStreamAction;
import org.opensearch.action.admin.indices.datastream.GetDataStreamAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.blobstore.BlobPath;
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

public class DataStreamRequestExecutionTests extends OpenSearchTestCase {
    private record Fixture(ServerlessNode node, MetadataPlane metadata) {
    }

    private void withNode(PolicyFilterPlugin plugin, org.opensearch.common.CheckedConsumer<Fixture, Exception> work) throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "data-stream-policy")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings, List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                metadata.indexTemplates().put("streams", "{\"index_patterns\":[\"stream-*\"],\"data_stream\":{}}");
                for (String name : List.of("stream-original", "stream-selected")) {
                    final var backing = org.opensearch.cluster.metadata.DataStream.getDefaultBackingIndexName(name, 1L);
                    metadata.createIndex(
                        new IndexDescriptor(
                            backing,
                            "uuid-" + name,
                            1,
                            "{\"properties\":{\"@timestamp\":{\"type\":\"date\"}},\"_data_stream_timestamp\":{\"enabled\":true,\"timestamp_field\":{\"name\":\"@timestamp\"}}}",
                            null
                        )
                    );
                    metadata.createAlias(new AliasRecord(name, List.of(backing), true, 1L, "@timestamp"));
                }
                work.accept(new Fixture(node, metadata));
            }
        }
    }

    private static HttpResponse<String> send(ServerlessNode node, String method, String name) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            return client.send(
                HttpRequest.newBuilder(URI.create("http://" + address.getAddress() + ":" + address.getPort() + "/_data_stream/" + name))
                    .timeout(Duration.ofSeconds(10))
                    .method(method, HttpRequest.BodyPublishers.noBody())
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
        }
    }

    public void testCreateUsesTheAdmittedName() throws Exception {
        withNode(
            PolicyFilterPlugin.request(CreateDataStreamAction.NAME, request -> new CreateDataStreamAction.Request("stream-chosen")),
            fixture -> {
                final var result = send(fixture.node(), "PUT", "stream-new");
                assertEquals(result.body(), 200, result.statusCode());
                assertTrue(fixture.metadata().aliasWithGeneration("stream-chosen").isPresent());
                assertTrue(fixture.metadata().aliasWithGeneration("stream-new").isEmpty());
            }
        );
    }

    public void testCreateResponsePoliciesSeeTheNativeAcknowledgement() throws Exception {
        withNode(PolicyFilterPlugin.response(CreateDataStreamAction.NAME, response -> {
            assertTrue(((AcknowledgedResponse) response).isAcknowledged());
            return new AcknowledgedResponse(false);
        }), fixture -> {
            final var result = send(fixture.node(), "PUT", "stream-new");
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"acknowledged\":false"));
            assertTrue(fixture.metadata().aliasWithGeneration("stream-new").isPresent());
        });
    }

    public void testCachedCreateAcknowledgementDoesNotCreateAStream() throws Exception {
        withNode(PolicyFilterPlugin.cached(CreateDataStreamAction.NAME, new AcknowledgedResponse(false)), fixture -> {
            final var result = send(fixture.node(), "PUT", "stream-new");
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"acknowledged\":false"));
            assertTrue(fixture.metadata().aliasWithGeneration("stream-new").isEmpty());
        });
    }

    public void testCreateResponseDenialPrecedesTheHttpAnswer() throws Exception {
        withNode(PolicyFilterPlugin.denyAfter(CreateDataStreamAction.NAME), fixture -> {
            final var result = send(fixture.node(), "PUT", "stream-new");
            assertEquals(result.body(), 403, result.statusCode());
        });
    }

    public void testDescribeUsesTheAdmittedNames() throws Exception {
        withNode(
            PolicyFilterPlugin.request(
                GetDataStreamAction.NAME,
                request -> new GetDataStreamAction.Request(new String[] { "stream-selected" })
            ),
            fixture -> {
                final var result = send(fixture.node(), "GET", "stream-original");
                assertEquals(result.body(), 200, result.statusCode());
                assertTrue(result.body(), result.body().contains("\"name\":\"stream-selected\""));
                assertFalse(result.body(), result.body().contains("stream-original"));
            }
        );
    }

    public void testDescribeResponseRedactionReachesTheCaller() throws Exception {
        withNode(PolicyFilterPlugin.response(GetDataStreamAction.NAME, response -> {
            final var described = (GetDataStreamAction.Response) response;
            assertEquals("stream-original", described.getDataStreams().get(0).getDataStream().getName());
            assertEquals("uuid-stream-original", described.getDataStreams().get(0).getDataStream().getIndices().get(0).getUUID());
            return new GetDataStreamAction.Response(List.of());
        }), fixture -> {
            final var result = send(fixture.node(), "GET", "stream-original");
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"data_streams\":[]"));
            assertFalse(result.body(), result.body().contains("uuid-stream-original"));
        });
    }

    public void testCachedDescribeResponseBypassesMissingStreamLookup() throws Exception {
        withNode(PolicyFilterPlugin.cached(GetDataStreamAction.NAME, new GetDataStreamAction.Response(List.of())), fixture -> {
            final var result = send(fixture.node(), "GET", "stream-missing");
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"data_streams\":[]"));
        });
    }

    public void testDescribeResponseDenialPrecedesTheHttpAnswer() throws Exception {
        withNode(PolicyFilterPlugin.denyAfter(GetDataStreamAction.NAME), fixture -> {
            final var result = send(fixture.node(), "GET", "stream-original");
            assertEquals(result.body(), 403, result.statusCode());
        });
    }

    public void testDeleteUsesTheAdmittedName() throws Exception {
        withNode(
            PolicyFilterPlugin.request(
                DeleteDataStreamAction.NAME,
                request -> new DeleteDataStreamAction.Request(new String[] { "stream-selected" })
            ),
            fixture -> {
                final var result = send(fixture.node(), "DELETE", "stream-original");
                assertEquals(result.body(), 200, result.statusCode());
                assertTrue(fixture.metadata().aliasWithGeneration("stream-original").isPresent());
                assertTrue(fixture.metadata().aliasWithGeneration("stream-selected").isEmpty());
            }
        );
    }

    public void testDeleteResponsePoliciesSeeTheNativeAcknowledgement() throws Exception {
        withNode(PolicyFilterPlugin.response(DeleteDataStreamAction.NAME, response -> {
            assertTrue(((AcknowledgedResponse) response).isAcknowledged());
            return new AcknowledgedResponse(false);
        }), fixture -> {
            final var result = send(fixture.node(), "DELETE", "stream-original");
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"acknowledged\":false"));
            assertTrue(fixture.metadata().aliasWithGeneration("stream-original").isEmpty());
        });
    }

    public void testCachedDeleteAcknowledgementDoesNotDeleteAStream() throws Exception {
        withNode(PolicyFilterPlugin.cached(DeleteDataStreamAction.NAME, new AcknowledgedResponse(false)), fixture -> {
            final var result = send(fixture.node(), "DELETE", "stream-original");
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"acknowledged\":false"));
            assertTrue(fixture.metadata().aliasWithGeneration("stream-original").isPresent());
        });
    }

    public void testAdmittedMultipleDeletesAreRefusedBeforeMutatingAnyStream() throws Exception {
        withNode(
            PolicyFilterPlugin.request(
                DeleteDataStreamAction.NAME,
                request -> new DeleteDataStreamAction.Request(new String[] { "stream-original", "stream-selected" })
            ),
            fixture -> {
                final var result = send(fixture.node(), "DELETE", "stream-original");
                assertEquals(result.body(), 501, result.statusCode());
                assertTrue(fixture.metadata().aliasWithGeneration("stream-original").isPresent());
                assertTrue(fixture.metadata().aliasWithGeneration("stream-selected").isPresent());
            }
        );
    }

    public void testStoredStreamWithLegacyBackingNamesRemainsReadable() throws Exception {
        withNode(PolicyFilterPlugin.request(GetDataStreamAction.NAME, request -> request), fixture -> {
            fixture.metadata().createIndex(new IndexDescriptor("restored-backing", "uuid-restored", 1, "{}", null));
            fixture.metadata().createAlias(new AliasRecord("stream-restored", List.of("restored-backing"), true, 7L, "@timestamp"));
            final var result = send(fixture.node(), "GET", "stream-restored");
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"index_name\":\"restored-backing\""));
            assertTrue(result.body(), result.body().contains("\"generation\":7"));
        });
    }

    public void testDescribeRequestExposesEachRequestedName() throws Exception {
        withNode(PolicyFilterPlugin.request(GetDataStreamAction.NAME, request -> {
            final var names = ((GetDataStreamAction.Request) request).indices();
            if (java.util.Arrays.equals(new String[] { "stream-original", "stream-selected" }, names) == false) {
                throw new IllegalArgumentException("native selection must contain each requested data stream name");
            }
            return request;
        }), fixture -> {
            final var result = send(fixture.node(), "GET", "stream-original,stream-selected");
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("stream-original"));
            assertTrue(result.body(), result.body().contains("stream-selected"));
        });
    }

    public void testDeleteRequestExposesEachNameBeforeRefusingMultipleDeletes() throws Exception {
        withNode(PolicyFilterPlugin.request(DeleteDataStreamAction.NAME, request -> {
            final var names = ((DeleteDataStreamAction.Request) request).indices();
            if (java.util.Arrays.equals(new String[] { "stream-original", "stream-selected" }, names) == false) {
                throw new IllegalArgumentException("native selection must contain each requested data stream name");
            }
            return request;
        }), fixture -> {
            final var result = send(fixture.node(), "DELETE", "stream-original,stream-selected");
            assertEquals(result.body(), 501, result.statusCode());
            assertTrue(fixture.metadata().aliasWithGeneration("stream-original").isPresent());
            assertTrue(fixture.metadata().aliasWithGeneration("stream-selected").isPresent());
        });
    }

    public void testInPlaceDescribeRedactionReachesTheCaller() throws Exception {
        withNode(PolicyFilterPlugin.response(GetDataStreamAction.NAME, response -> {
            ((GetDataStreamAction.Response) response).getDataStreams().clear();
            return response;
        }), fixture -> {
            final var result = send(fixture.node(), "GET", "stream-original");
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"data_streams\":[]"));
        });
    }

    public void testCachedDescribeUsesItsNativeSerializer() throws Exception {
        final var cached = new GetDataStreamAction.Response(List.of()) {
            @Override
            public org.opensearch.core.xcontent.XContentBuilder toXContent(
                org.opensearch.core.xcontent.XContentBuilder builder,
                org.opensearch.core.xcontent.ToXContent.Params params
            ) throws java.io.IOException {
                return builder.startObject().field("policy_result", "safe").endObject();
            }
        };
        withNode(PolicyFilterPlugin.cached(GetDataStreamAction.NAME, cached), fixture -> {
            final var result = send(fixture.node(), "GET", "stream-missing");
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"policy_result\":\"safe\""));
        });
    }

    public void testDeleteResponseDenialPrecedesTheHttpAnswer() throws Exception {
        withNode(PolicyFilterPlugin.denyAfter(DeleteDataStreamAction.NAME), fixture -> {
            final var result = send(fixture.node(), "DELETE", "stream-original");
            assertEquals(result.body(), 403, result.statusCode());
            assertTrue(fixture.metadata().aliasWithGeneration("stream-original").isEmpty());
        });
    }

    public void testAdmittedNullNamesDescribeAllStreams() throws Exception {
        withNode(
            PolicyFilterPlugin.request(GetDataStreamAction.NAME, request -> new GetDataStreamAction.Request((String[]) null)),
            fixture -> {
                final var result = send(fixture.node(), "GET", "stream-original");
                assertEquals(result.body(), 200, result.statusCode());
                assertTrue(result.body(), result.body().contains("stream-original"));
                assertTrue(result.body(), result.body().contains("stream-selected"));
            }
        );
    }

    public void testMixedPrefixAndLiteralSelectorsIgnoreOrdinaryIndicesAndDeduplicateStreams() throws Exception {
        withNode(PolicyFilterPlugin.request(GetDataStreamAction.NAME, request -> request), fixture -> {
            fixture.metadata().createIndex(new IndexDescriptor("stream-ordinary", "uuid-ordinary", 1, "{}", null));
            final var result = send(fixture.node(), "GET", "stream-*,stream-selected");
            assertEquals(result.body(), 200, result.statusCode());
            final var body = org.opensearch.common.xcontent.XContentHelper.convertToMap(
                new org.opensearch.core.common.bytes.BytesArray(result.body()),
                false,
                org.opensearch.common.xcontent.XContentType.JSON
            ).v2();
            assertEquals(2, ((List<?>) body.get("data_streams")).size());
            assertFalse(result.body(), result.body().contains("stream-ordinary"));
        });
    }

}
