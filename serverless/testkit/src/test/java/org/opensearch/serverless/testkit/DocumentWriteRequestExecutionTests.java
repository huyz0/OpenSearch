/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteAction;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexAction;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shard.ShardOperations;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class DocumentWriteRequestExecutionTests extends OpenSearchTestCase {
    private record Fixture(ServerlessNode node, MetadataPlane metadata, HookedBlobStore store) {
        ShardOperations operations() {
            return new ShardOperations(node, metadata);
        }

        void seed(String index, String id) throws Exception {
            final var shard = new ShardId(index, "uuid-" + index, 0);
            node.index(shard, id, "{\"value\":\"before\"}");
            node.reconciler().shard(shard).refresh("seed-policy-test");
        }
    }

    private void withNode(org.opensearch.plugins.Plugin plugin, org.opensearch.common.CheckedConsumer<Fixture, Exception> work)
        throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "document-write-policy")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings, List.of(plugin))) {
                node.start();
                node.setMetadataPlane(metadata);
                for (String index : List.of("original", "selected")) {
                    metadata.createIndex(new IndexDescriptor(index, "uuid-" + index, 1, "{}", null));
                    node.activateWriter(metadata, index, 0).orElseThrow();
                }
                work.accept(new Fixture(node, metadata, store));
            }
        }
    }

    private static HttpResponse<String> send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            return client.send(
                HttpRequest.newBuilder(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
        }
    }

    public void testIndexRequestCarriesConditionsPipelineAndRefreshPolicy() throws Exception {
        withNode(PolicyFilterPlugin.request(IndexAction.NAME, request -> {
            final var index = (IndexRequest) request;
            assertEquals("original", index.index());
            assertEquals("item", index.id());
            assertEquals(12345L, index.ifSeqNo());
            assertEquals(1L, index.ifPrimaryTerm());
            assertEquals("shape", index.getPipeline());
            assertEquals(WriteRequest.RefreshPolicy.WAIT_UNTIL, index.getRefreshPolicy());
            return request;
        }), fixture -> {
            fixture.metadata().pipelines().put("shape", "{\"processors\":[]}");
            final var result = send(
                fixture.node(),
                "PUT",
                "/original/_doc/item?if_seq_no=12345&if_primary_term=1&pipeline=shape&refresh=wait_for",
                "{\"value\":1}"
            );
            assertEquals(result.body(), 409, result.statusCode());
        });
    }

    public void testIndexExecutesTheAdmittedIndexIdAndSource() throws Exception {
        withNode(
            PolicyFilterPlugin.request(
                IndexAction.NAME,
                request -> new IndexRequest("selected").id("chosen")
                    .source(Map.of("value", "safe"))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            ),
            fixture -> {
                final var result = send(fixture.node(), "PUT", "/original/_doc/original", "{\"value\":\"original\"}");
                assertEquals(result.body(), 201, result.statusCode());
                assertTrue(result.body(), result.body().contains("\"_index\":\"selected\""));
                assertTrue(result.body(), result.body().contains("\"_id\":\"chosen\""));
                assertTrue(fixture.operations().get("selected", "chosen").document().source().contains("safe"));
                assertFalse(fixture.operations().get("original", "original").document().found());
            }
        );
    }

    public void testAdmittedSequenceConditionsPreventAnUnconditionalWrite() throws Exception {
        withNode(PolicyFilterPlugin.request(IndexAction.NAME, request -> {
            ((IndexRequest) request).setIfSeqNo(12345L).setIfPrimaryTerm(1L);
            return request;
        }), fixture -> {
            final var result = send(fixture.node(), "PUT", "/original/_doc/item", "{\"value\":1}");
            assertEquals(result.body(), 409, result.statusCode());
            assertFalse(fixture.operations().get("original", "item").document().found());
        });
    }

    public void testCreateOperationIsExposedAndEnforced() throws Exception {
        withNode(PolicyFilterPlugin.request(IndexAction.NAME, request -> {
            final var index = (IndexRequest) request;
            assertEquals(DocWriteRequest.OpType.CREATE, index.opType());
            return request;
        }), fixture -> {
            final var result = send(fixture.node(), "PUT", "/original/_create/new", "{\"value\":1}");
            assertEquals(result.body(), 201, result.statusCode());
        });
    }

    public void testFilterCanRequireCreateInsteadOfOverwriting() throws Exception {
        withNode(PolicyFilterPlugin.request(IndexAction.NAME, request -> {
            ((IndexRequest) request).create(true);
            return request;
        }), fixture -> {
            fixture.seed("original", "keep");
            final var result = send(fixture.node(), "PUT", "/original/_doc/keep", "{\"value\":\"after\"}");
            assertEquals(result.body(), 409, result.statusCode());
            assertTrue(fixture.operations().get("original", "keep").document().source().contains("before"));
        });
    }

    public void testAdmittedPipelineRunsBeforeWriting() throws Exception {
        withNode(PolicyFilterPlugin.request(IndexAction.NAME, request -> {
            ((IndexRequest) request).setPipeline("shape");
            return request;
        }), fixture -> {
            fixture.metadata().pipelines().put("shape", "{\"processors\":[{\"set\":{\"field\":\"policy\",\"value\":\"safe\"}}]}");
            final var result = send(fixture.node(), "PUT", "/original/_doc/item", "{\"value\":1}");
            assertEquals(result.body(), 201, result.statusCode());
            assertTrue(fixture.operations().get("original", "item").document().source().contains("safe"));
        });
    }

    public void testDenialPrecedesEvenADroppingPipeline() throws Exception {
        withNode(PolicyFilterPlugin.request(IndexAction.NAME, request -> {
            throw new org.opensearch.OpenSearchSecurityException("denied", org.opensearch.core.rest.RestStatus.FORBIDDEN);
        }), fixture -> {
            fixture.metadata().pipelines().put("drop", "{\"processors\":[{\"drop\":{}}]}");
            final var read = new AtomicBoolean();
            fixture.store().onNextRegisterRead("pipelines", "_version", () -> read.set(true));
            final var result = send(fixture.node(), "PUT", "/original/_doc/item?pipeline=drop", "{\"value\":1}");
            assertEquals(result.body(), 403, result.statusCode());
            assertFalse("denial must precede pipeline configuration IO", read.get());
        });
    }

    public void testResponseFiltersSeeTheActualNativeIndexResponse() throws Exception {
        withNode(PolicyFilterPlugin.response(IndexAction.NAME, response -> {
            final var index = (IndexResponse) response;
            assertEquals("original", index.getIndex());
            assertEquals("item", index.getId());
            assertTrue(index.getSeqNo() >= 0L);
            return response;
        }), fixture -> {
            final var result = send(fixture.node(), "PUT", "/original/_doc/item", "{\"value\":1}");
            assertEquals(result.body(), 201, result.statusCode());
        });
    }

    public void testResponseReplacementControlsTheHttpResult() throws Exception {
        final var redacted = new IndexResponse(new ShardId("redacted", "uuid-redacted", 0), "hidden", 10L, 2L, 3L, false);
        redacted.setShardInfo(new org.opensearch.action.support.replication.ReplicationResponse.ShardInfo(1, 1));
        withNode(PolicyFilterPlugin.response(IndexAction.NAME, response -> redacted), fixture -> {
            final var result = send(fixture.node(), "PUT", "/original/_doc/item", "{\"value\":1}");
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"_index\":\"redacted\""));
            assertTrue(result.body(), result.body().contains("\"_id\":\"hidden\""));
            assertFalse(result.body(), result.body().contains("\"_node\""));
            assertTrue(fixture.operations().get("original", "item").document().found());
        });
    }

    public void testCachedIndexResponseDoesNotWrite() throws Exception {
        final var cached = new IndexResponse(new ShardId("cached", "uuid-cached", 0), "cached", 10L, 2L, 3L, false);
        cached.setShardInfo(new org.opensearch.action.support.replication.ReplicationResponse.ShardInfo(1, 1));
        withNode(PolicyFilterPlugin.cached(IndexAction.NAME, cached), fixture -> {
            final var result = send(fixture.node(), "PUT", "/original/_doc/item", "{\"value\":1}");
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"_index\":\"cached\""));
            assertFalse(fixture.operations().get("original", "item").document().found());
        });
    }

    public void testDeleteExecutesTheAdmittedIndexAndId() throws Exception {
        withNode(PolicyFilterPlugin.request(DeleteAction.NAME, request -> new DeleteRequest("selected", "victim")), fixture -> {
            fixture.seed("original", "keep");
            fixture.seed("selected", "victim");
            final var result = send(fixture.node(), "DELETE", "/original/_doc/keep", null);
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"_index\":\"selected\""));
            assertTrue(fixture.operations().get("original", "keep").document().found());
            assertFalse(fixture.operations().get("selected", "victim").document().found());
        });
    }

    public void testCachedDeleteResponseDoesNotDelete() throws Exception {
        final var cached = new DeleteResponse(new ShardId("redacted", "uuid-redacted", 0), "hidden", 10L, 2L, 3L, false);
        cached.setShardInfo(new org.opensearch.action.support.replication.ReplicationResponse.ShardInfo(1, 1));
        withNode(PolicyFilterPlugin.cached(DeleteAction.NAME, cached), fixture -> {
            fixture.seed("original", "keep");
            final var result = send(fixture.node(), "DELETE", "/original/_doc/keep", null);
            assertEquals(result.body(), 404, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"_id\":\"hidden\""));
            assertTrue(fixture.operations().get("original", "keep").document().found());
        });
    }

    public void testResponseDenialPrecedesTheHttpAnswer() throws Exception {
        withNode(PolicyFilterPlugin.denyAfter(IndexAction.NAME), fixture -> {
            final var result = send(fixture.node(), "PUT", "/original/_doc/item", "{\"value\":1}");
            assertEquals(result.body(), 403, result.statusCode());
        });
    }

    public void testCachedIndexResponseBypassesRoutingAndUsesItsOwnSerializer() throws Exception {
        final var cached = new IndexResponse(new ShardId("cached", "uuid-cached", 0), "cached", 1L, 1L, 1L, false) {
            @Override
            public org.opensearch.core.xcontent.XContentBuilder innerToXContent(
                org.opensearch.core.xcontent.XContentBuilder builder,
                org.opensearch.core.xcontent.ToXContent.Params params
            ) throws java.io.IOException {
                return builder.field("policy_result", "safe");
            }
        };
        cached.setShardInfo(new org.opensearch.action.support.replication.ReplicationResponse.ShardInfo(1, 1));
        withNode(PolicyFilterPlugin.cached(IndexAction.NAME, cached), fixture -> {
            final var result = send(fixture.node(), "PUT", "/missing/_doc/item?pipeline=missing", "{\"value\":1}");
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"policy_result\":\"safe\""));
            assertFalse(result.body(), result.body().contains("\"durable\""));
        });
    }

    public void testInPlaceResponseChangesRetainTruthfulRoutingExtensions() throws Exception {
        withNode(PolicyFilterPlugin.response(IndexAction.NAME, response -> {
            ((IndexResponse) response).setShardInfo(new org.opensearch.action.support.replication.ReplicationResponse.ShardInfo(7, 3));
            return response;
        }), fixture -> {
            final var result = send(fixture.node(), "PUT", "/original/_doc/item", "{\"value\":1}");
            assertEquals(result.body(), 201, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"total\":7"));
            assertTrue(result.body(), result.body().contains("\"successful\":3"));
            assertTrue(result.body(), result.body().contains(fixture.node().localNode().getId()));
            assertTrue(result.body(), result.body().contains("\"durable\":\"write-ahead log\""));
        });
    }

    public void testDroppedDocumentsHaveSerializableNativeNoOpResponses() throws Exception {
        withNode(PolicyFilterPlugin.response(IndexAction.NAME, response -> {
            final var dropped = (org.opensearch.action.update.UpdateResponse) response;
            assertEquals(org.opensearch.action.DocWriteResponse.Result.NOOP, dropped.getResult());
            assertEquals(0, dropped.getShardInfo().getTotal());
            try (var out = new org.opensearch.common.io.stream.BytesStreamOutput()) {
                dropped.writeTo(out);
                try (var in = out.bytes().streamInput()) {
                    return new org.opensearch.action.update.UpdateResponse(in);
                }
            } catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        }), fixture -> {
            fixture.metadata().pipelines().put("drop", "{\"processors\":[{\"drop\":{}}]}");
            final var result = send(fixture.node(), "PUT", "/original/_doc/item?pipeline=drop", "{\"value\":1}");
            assertEquals(result.body(), 200, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"result\":\"noop\""));
            assertFalse(fixture.operations().get("original", "item").document().found());
        });
    }

    public void testAnAdmittedMissingIdIsGeneratedOnTheAcceptedRequest() throws Exception {
        final var selected = new java.util.concurrent.atomic.AtomicReference<IndexRequest>();
        withNode(PolicyFilterPlugin.request(IndexAction.NAME, request -> {
            final var replacement = new IndexRequest("selected").source(Map.of("value", "safe"));
            selected.set(replacement);
            return replacement;
        }), fixture -> {
            final var result = send(fixture.node(), "PUT", "/original/_doc/item", "{\"value\":1}");
            assertEquals(result.body(), 201, result.statusCode());
            final var body = org.opensearch.common.xcontent.XContentHelper.convertToMap(
                new org.opensearch.core.common.bytes.BytesArray(result.body()),
                false,
                org.opensearch.common.xcontent.XContentType.JSON
            ).v2();
            assertNotNull(selected.get().id());
            assertEquals(selected.get().id(), body.get("_id"));
            assertTrue(fixture.operations().get("selected", selected.get().id()).document().source().contains("safe"));
        });
    }

    public void testDataStreamAdmissionUsesTheLogicalNameAndReportsTheBackingIndex() throws Exception {
        withNode(PolicyFilterPlugin.request(IndexAction.NAME, request -> {
            assertEquals("events", ((IndexRequest) request).index());
            assertEquals(DocWriteRequest.OpType.CREATE, ((IndexRequest) request).opType());
            return request;
        }), fixture -> {
            final var template = send(
                fixture.node(),
                "PUT",
                "/_index_template/events",
                "{\"index_patterns\":[\"events*\"],\"data_stream\":{},\"template\":{\"mappings\":{\"properties\":{\"@timestamp\":{\"type\":\"date\"}}}}}"
            );
            assertEquals(template.body(), 200, template.statusCode());
            final var created = send(fixture.node(), "PUT", "/_data_stream/events", "{}");
            assertEquals(created.body(), 200, created.statusCode());
            final var backing = fixture.metadata().writeTarget("events").orElseThrow().index();
            fixture.node().activateWriter(fixture.metadata(), backing.name(), 0).orElseThrow();
            final var result = send(
                fixture.node(),
                "PUT",
                "/events/_create/event",
                "{\"@timestamp\":\"2026-10-30T00:00:00Z\",\"value\":1}"
            );
            assertEquals(result.body(), 201, result.statusCode());
            assertTrue(result.body(), result.body().contains("\"_index\":\"" + backing.name() + "\""));
        });
    }

    public void testAdmittedUnsupportedWriteOptionsAreRefusedBeforeWriting() throws Exception {
        for (String option : List.of("routing", "require_alias", "replicas")) {
            withNode(PolicyFilterPlugin.request(IndexAction.NAME, request -> {
                final var selected = (IndexRequest) request;
                switch (option) {
                    case "routing" -> selected.routing("elsewhere");
                    case "require_alias" -> selected.setRequireAlias(true);
                    case "replicas" -> selected.waitForActiveShards(org.opensearch.action.support.ActiveShardCount.from(2));
                    default -> throw new AssertionError(option);
                }
                return selected;
            }), fixture -> {
                final var result = send(fixture.node(), "PUT", "/original/_doc/item", "{\"value\":1}");
                assertEquals(result.body(), 501, result.statusCode());
                assertFalse(fixture.operations().get("original", "item").document().found());
            });
        }
    }

    public void testAdmittedDeleteConditionsPreventAnUnconditionalRemoval() throws Exception {
        withNode(PolicyFilterPlugin.request(DeleteAction.NAME, request -> {
            ((DeleteRequest) request).setIfSeqNo(12345L).setIfPrimaryTerm(1L);
            return request;
        }), fixture -> {
            fixture.seed("original", "keep");
            final var result = send(fixture.node(), "DELETE", "/original/_doc/keep", null);
            assertEquals(result.body(), 409, result.statusCode());
            assertTrue(fixture.operations().get("original", "keep").document().found());
        });
    }

    private static final class NetworkContinuationPlugin extends org.opensearch.plugins.Plugin
        implements
            org.opensearch.plugins.ActionPlugin {
        private final java.util.List<Thread> workers = new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override
        public List<org.opensearch.action.support.ActionFilter> getActionFilters() {
            return List.of(new org.opensearch.action.support.ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
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
                    if (IndexAction.NAME.equals(action) == false) {
                        chain.proceed(task, action, request, listener);
                        return;
                    }
                    final var worker = new Thread(
                        () -> chain.proceed(task, action, request, listener),
                        org.opensearch.transport.Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX + "-policy-continuation"
                    );
                    workers.add(worker);
                    worker.start();
                }
            });
        }

        @Override
        public void close() throws java.io.IOException {
            for (Thread worker : workers) {
                try {
                    worker.join(10_000L);
                    if (worker.isAlive()) {
                        throw new java.io.IOException("policy continuation did not terminate");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new java.io.IOException(e);
                }
            }
        }
    }

    public void testNetworkFilterContinuationDoesNotRunBlockingPipelineReads() throws Exception {
        withNode(new NetworkContinuationPlugin(), fixture -> {
            fixture.metadata().pipelines().put("shape", "{\"processors\":[]}");
            final var read = new AtomicBoolean();
            final var networkRead = new AtomicBoolean();
            fixture.store().onNextRegisterRead("pipelines", "_version", () -> {
                read.set(true);
                networkRead.set(org.opensearch.transport.Transports.isTransportThread(Thread.currentThread()));
            });
            final var result = send(fixture.node(), "PUT", "/original/_doc/item?pipeline=shape", "{\"value\":1}");
            assertEquals(result.body(), 201, result.statusCode());
            assertTrue("the continuation must actually read the pipeline", read.get());
            assertFalse("a network continuation must be moved to a worker before storage IO", networkRead.get());
        });
    }

}
