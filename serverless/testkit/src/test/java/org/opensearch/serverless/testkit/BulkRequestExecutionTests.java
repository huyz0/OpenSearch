/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.VersionType;
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

public class BulkRequestExecutionTests extends OpenSearchTestCase {
    private void withNode(PolicyFilterPlugin plugin, org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work)
        throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "bulk-filter-contract")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings, List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                metadata.createIndex(new IndexDescriptor("original", "uuid-original", 1, "{}", null));
                metadata.createIndex(new IndexDescriptor("selected", "uuid-selected", 1, "{}", null));
                node.activateWriter(metadata, "original", 0).orElseThrow();
                node.activateWriter(metadata, "selected", 0).orElseThrow();
                work.accept(node, metadata);
            }
        }
    }

    private static String indexBody(String index, String id) {
        return "{\"index\":{\"_index\":\"" + index + "\",\"_id\":\"" + id + "\"}}\n{\"value\":\"original\"}\n";
    }

    public void testNativeBulkRequestCarriesSourcesOperationTypesConditionsAndPipelines() throws Exception {
        withNode(PolicyFilterPlugin.request(BulkAction.NAME, request -> {
            final var bulk = (BulkRequest) request;
            assertEquals(3, bulk.numberOfActions());
            final var create = (IndexRequest) bulk.requests().get(0);
            assertEquals(DocWriteRequest.OpType.CREATE, create.opType());
            assertNotNull(create.source());
            assertTrue(create.source().utf8ToString().contains("original"));
            assertEquals("shape", create.getPipeline());
            final var index = (IndexRequest) bulk.requests().get(1);
            assertEquals(10L, index.ifSeqNo());
            assertEquals(3L, index.ifPrimaryTerm());
            final var delete = (DeleteRequest) bulk.requests().get(2);
            assertEquals(11L, delete.ifSeqNo());
            assertEquals(4L, delete.ifPrimaryTerm());
            assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, bulk.getRefreshPolicy());
            return request;
        }), (node, metadata) -> {
            metadata.pipelines().put("shape", "{\"processors\":[]}");
            final var answer = send(
                node,
                "POST",
                "/_bulk?pipeline=shape&refresh=true",
                "{\"create\":{\"_index\":\"original\",\"_id\":\"create\"}}\n{\"value\":\"original\"}\n"
                    + "{\"index\":{\"_index\":\"original\",\"_id\":\"replace\",\"if_seq_no\":10,\"if_primary_term\":3}}\n{\"value\":\"original\"}\n"
                    + "{\"delete\":{\"_index\":\"original\",\"_id\":\"delete\",\"if_seq_no\":11,\"if_primary_term\":4}}\n"
            );
            assertEquals(answer.body(), 200, answer.statusCode());
        });
    }

    public void testBulkUsesAdmittedItemIndicesIdsAndSources() throws Exception {
        withNode(PolicyFilterPlugin.request(BulkAction.NAME, request -> {
            ((BulkRequest) request).requests().set(0, new IndexRequest("selected").id("chosen").source(Map.of("value", "safe")));
            return request;
        }), (node, metadata) -> {
            var answer = send(node, "POST", "/_bulk?refresh=true", indexBody("original", "original"));
            assertEquals(answer.body(), 200, answer.statusCode());
            answer = send(node, "GET", "/selected/_doc/chosen", null);
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"value\":\"safe\""));
            assertEquals(404, send(node, "GET", "/original/_doc/original", null).statusCode());
        });
    }

    public void testBulkUsesReplacementRequestsAndOperationTypes() throws Exception {
        withNode(
            PolicyFilterPlugin.request(BulkAction.NAME, request -> new BulkRequest().add(new DeleteRequest("selected", "victim"))),
            (node, metadata) -> {
                var answer = send(node, "PUT", "/selected/_doc/victim?refresh=true", "{\"value\":\"old\"}");
                assertEquals(answer.body(), 201, answer.statusCode());
                answer = send(node, "POST", "/_bulk?refresh=true", indexBody("original", "original"));
                assertEquals(answer.body(), 200, answer.statusCode());
                assertEquals(404, send(node, "GET", "/selected/_doc/victim", null).statusCode());
                assertEquals(404, send(node, "GET", "/original/_doc/original", null).statusCode());
            }
        );
    }

    public void testBulkHonorsCachedNativeResponsesWithoutWriting() throws Exception {
        withNode(PolicyFilterPlugin.cached(BulkAction.NAME, new BulkResponse(new BulkItemResponse[0], 7L)), (node, metadata) -> {
            final var answer = send(node, "POST", "/_bulk", indexBody("original", "original"));
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"items\":[]"));
            assertEquals(404, send(node, "GET", "/original/_doc/original", null).statusCode());
        });
    }

    public void testBulkNativeResponseRedactionReachesTheCaller() throws Exception {
        withNode(PolicyFilterPlugin.response(BulkAction.NAME, response -> {
            final var bulk = (BulkResponse) response;
            assertEquals(1, bulk.getItems().length);
            return new BulkResponse(new BulkItemResponse[0], bulk.getTook().millis());
        }), (node, metadata) -> {
            final var answer = send(node, "POST", "/_bulk", indexBody("original", "original"));
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"items\":[]"));
        });
    }

    public void testBulkHonorsAdmittedSequenceConditions() throws Exception {
        withNode(PolicyFilterPlugin.request(BulkAction.NAME, request -> {
            final var index = (IndexRequest) ((BulkRequest) request).requests().get(0);
            index.setIfSeqNo(12345L).setIfPrimaryTerm(1L);
            return request;
        }), (node, metadata) -> {
            final var answer = send(node, "POST", "/_bulk", indexBody("original", "original"));
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"status\":409"));
            assertEquals(404, send(node, "GET", "/original/_doc/original", null).statusCode());
        });
    }

    public void testBulkRefusesUnsupportedAdmittedExternalVersioning() throws Exception {
        withNode(PolicyFilterPlugin.request(BulkAction.NAME, request -> {
            final var index = (IndexRequest) ((BulkRequest) request).requests().get(0);
            index.version(4L).versionType(VersionType.EXTERNAL);
            return request;
        }), (node, metadata) -> {
            final var answer = send(node, "POST", "/_bulk", indexBody("original", "original"));
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"status\":501"));
            assertEquals(404, send(node, "GET", "/original/_doc/original", null).statusCode());
        });
    }

    public void testDeniedBulkDoesNotLoadOrExecuteIngestPipelines() throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            metadata.createIndex(new IndexDescriptor("original", "uuid-original", 1, "{}", null));
            metadata.pipelines().put("shape", "{\"processors\":[]}");
            final var plugin = PolicyFilterPlugin.request(BulkAction.NAME, request -> {
                throw new org.opensearch.OpenSearchSecurityException("denied", org.opensearch.core.rest.RestStatus.FORBIDDEN);
            });
            final var settings = Settings.builder()
                .put("node.name", "bulk-denied-before-ingest")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings, List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                node.activateWriter(metadata, "original", 0).orElseThrow();
                final var read = new java.util.concurrent.atomic.AtomicBoolean();
                store.onNextRegisterRead("pipelines", "_version", () -> read.set(true));
                final var answer = send(node, "POST", "/_bulk?pipeline=shape", indexBody("original", "original"));
                assertEquals(answer.body(), 403, answer.statusCode());
                assertFalse("authorization must precede ingest configuration IO and processing", read.get());
            }
        }
    }

    public void testBulkExposesWaitForRefreshAndTimeoutHints() throws Exception {
        withNode(PolicyFilterPlugin.request(BulkAction.NAME, request -> {
            final var bulk = (BulkRequest) request;
            assertEquals(WriteRequest.RefreshPolicy.WAIT_UNTIL, bulk.getRefreshPolicy());
            assertEquals(123L, bulk.timeout().millis());
            assertEquals(org.opensearch.action.support.ActiveShardCount.ONE, bulk.waitForActiveShards());
            return request;
        }), (node, metadata) -> {
            final var answer = send(
                node,
                "POST",
                "/_bulk?refresh=wait_for&timeout=123ms&wait_for_active_shards=1",
                indexBody("original", "original")
            );
            assertEquals(answer.body(), 200, answer.statusCode());
        });
    }

    public void testInPlaceActiveShardRequirementsAreNotIgnored() throws Exception {
        withNode(PolicyFilterPlugin.request(BulkAction.NAME, request -> {
            ((BulkRequest) request).waitForActiveShards(2);
            return request;
        }), (node, metadata) -> {
            final var answer = send(node, "POST", "/_bulk", indexBody("original", "original"));
            assertEquals(answer.body(), 501, answer.statusCode());
            assertEquals(404, send(node, "GET", "/original/_doc/original", null).statusCode());
        });
    }

    public void testBulkHonorsInPlaceRefreshPolicyRewrites() throws Exception {
        withNode(PolicyFilterPlugin.request(BulkAction.NAME, request -> {
            ((BulkRequest) request).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            return request;
        }), (node, metadata) -> {
            final var answer = send(node, "POST", "/_bulk", indexBody("original", "original"));
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"forced_refresh\":true"));
        });
    }

    private static BulkResponse wireCopy(BulkResponse response) {
        try (var out = new org.opensearch.common.io.stream.BytesStreamOutput()) {
            response.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                return new BulkResponse(in);
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    public void testMixedParsingFailuresAndSuccessfulItemsKeepTheirOrder() throws Exception {
        withNode(PolicyFilterPlugin.response(BulkAction.NAME, response -> wireCopy((BulkResponse) response)), (node, metadata) -> {
            final var answer = send(
                node,
                "POST",
                "/_bulk",
                "{\"update\":{\"_index\":\"original\",\"_id\":\"unsupported\"}}\n{\"doc\":{\"value\":\"changed\"}}\n"
                    + "{\"create\":{\"_index\":\"original\",\"_id\":\"conditional\",\"if_seq_no\":0,\"if_primary_term\":1}}\n{\"value\":\"bad\"}\n"
                    + indexBody("original", "success")
            );
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"update\":{\"_index\":\"original\""));
            assertTrue(answer.body(), answer.body().contains("\"status\":501"));
            assertTrue(answer.body(), answer.body().contains("\"status\":400"));
            assertEquals(200, send(node, "GET", "/original/_doc/success", null).statusCode());
            assertEquals(404, send(node, "GET", "/original/_doc/conditional", null).statusCode());
            assertEquals(404, send(node, "GET", "/original/_doc/unsupported", null).statusCode());
        });
    }

    public void testDroppedItemsUseSerializableNativeNoOpResponses() throws Exception {
        withNode(PolicyFilterPlugin.response(BulkAction.NAME, response -> {
            final var bulk = (BulkResponse) response;
            assertEquals(org.opensearch.action.DocWriteResponse.Result.NOOP, bulk.getItems()[0].getResponse().getResult());
            return wireCopy(bulk);
        }), (node, metadata) -> {
            metadata.pipelines().put("drop", "{\"processors\":[{\"drop\":{}}]}");
            final var answer = send(node, "POST", "/_bulk?pipeline=drop", indexBody("original", "dropped"));
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"result\":\"noop\""));
            assertEquals(404, send(node, "GET", "/original/_doc/dropped", null).statusCode());
        });
    }

    public void testInPlaceNativeResponseItemRedactionReachesTheCaller() throws Exception {
        withNode(PolicyFilterPlugin.response(BulkAction.NAME, response -> {
            ((BulkResponse) response).getItems()[0] = new BulkItemResponse(
                0,
                DocWriteRequest.OpType.INDEX,
                new org.opensearch.action.index.IndexResponse(
                    new org.opensearch.core.index.shard.ShardId("redacted", "uuid-redacted", 0),
                    "safe",
                    0L,
                    1L,
                    1L,
                    true
                )
            );
            return response;
        }), (node, metadata) -> {
            final var answer = send(node, "POST", "/_bulk", indexBody("original", "original"));
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("redacted"));
            assertFalse(answer.body(), answer.body().contains("original"));
        });
    }

    public void testCachedNativeResponseSerializersArePreserved() throws Exception {
        final var cached = new BulkResponse(new BulkItemResponse[0], 7L) {
            @Override
            public org.opensearch.core.xcontent.XContentBuilder toXContent(
                org.opensearch.core.xcontent.XContentBuilder builder,
                org.opensearch.core.xcontent.ToXContent.Params params
            ) throws java.io.IOException {
                builder.startObject();
                builder.field("took", 7L);
                builder.field("errors", false);
                builder.startArray("items").endArray();
                builder.field("policy", "cached-native-response");
                return builder.endObject();
            }
        };
        withNode(PolicyFilterPlugin.cached(BulkAction.NAME, cached), (node, metadata) -> {
            final var answer = send(node, "POST", "/_bulk", indexBody("original", "original"));
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("cached-native-response"));
            assertEquals(404, send(node, "GET", "/original/_doc/original", null).statusCode());
        });
    }

    public void testNativeCreateDefaultsPreserveCreateAndConflictSemantics() throws Exception {
        withNode(PolicyFilterPlugin.request(BulkAction.NAME, request -> request), (node, metadata) -> {
            final String create = "{\"create\":{\"_index\":\"original\",\"_id\":\"created\"}}\n{\"value\":\"created\"}\n";
            final var answer = send(node, "POST", "/_bulk?refresh=true", create + create);
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"status\":201"));
            assertTrue(answer.body(), answer.body().contains("\"status\":409"));
            assertFalse(answer.body(), answer.body().contains("\"status\":501"));
            assertEquals(200, send(node, "GET", "/original/_doc/created", null).statusCode());
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
