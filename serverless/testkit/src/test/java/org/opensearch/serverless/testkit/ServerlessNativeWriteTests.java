/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ServerlessNativeWriteTests extends OpenSearchTestCase {
    private Settings settings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane plane(AtomicLong clock) throws Exception {
        final var plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, 30_000L);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-native-writes", 1, "{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}", null));
        return plane;
    }

    private static void attach(ServerlessNode node, MetadataPlane plane) throws Exception {
        node.start();
        node.setMetadataPlane(plane);
    }

    private static void own(ServerlessNode owner, MetadataPlane plane, AtomicLong clock) throws Exception {
        final var loop = new BackgroundReconciler(owner, plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());
    }

    private static IndexRequest index(String id, String message) {
        return new IndexRequest("alpha").id(id).source("{\"msg\":\"" + message + "\"}", XContentType.JSON);
    }

    public void testConditionsAndActualWriteResultsLocallyAndForwarded() throws Exception {
        final var clock = new AtomicLong(1_000L);
        final var plane = plane(clock);
        try (var owner = new ServerlessNode(settings("native-owner")); var peer = new ServerlessNode(settings("native-peer"))) {
            attach(owner, plane);
            attach(peer, plane);
            own(owner, plane, clock);
            int ordinal = 0;
            for (var via : List.of(owner, peer)) {
                final var client = via.client();
                final String id = "doc-" + ordinal++;
                final var created = client.index(index(id, "created")).actionGet();
                assertEquals(DocWriteResponse.Result.CREATED, created.getResult());
                assertTrue("the assigned sequence number must travel back", created.getSeqNo() >= 0L);
                assertTrue(created.getPrimaryTerm() > 0L);
                assertEquals(1L, created.getVersion());
                assertEquals(1, created.getShardInfo().getSuccessful());
                assertEquals("uuid-native-writes", created.getShardId().getIndex().getUUID());
                final var updated = client.index(index(id, "updated")).actionGet();
                assertEquals(DocWriteResponse.Result.UPDATED, updated.getResult());
                assertEquals(2L, updated.getVersion());
                assertTrue(updated.getSeqNo() > created.getSeqNo());
                expectThrows(
                    VersionConflictEngineException.class,
                    () -> client.index(index(id, "create-overwrite").opType(org.opensearch.action.DocWriteRequest.OpType.CREATE))
                        .actionGet()
                );
                expectThrows(
                    VersionConflictEngineException.class,
                    () -> client.index(
                        index(id, "stale-overwrite").setIfSeqNo(created.getSeqNo()).setIfPrimaryTerm(created.getPrimaryTerm())
                    ).actionGet()
                );
                final var conditional = client.index(
                    index(id, "conditional").setIfSeqNo(updated.getSeqNo()).setIfPrimaryTerm(updated.getPrimaryTerm())
                ).actionGet();
                assertEquals(3L, conditional.getVersion());
                expectThrows(
                    VersionConflictEngineException.class,
                    () -> client.delete(
                        new DeleteRequest("alpha", id).setIfSeqNo(updated.getSeqNo()).setIfPrimaryTerm(updated.getPrimaryTerm())
                    ).actionGet()
                );
                final var deleted = client.delete(
                    new DeleteRequest("alpha", id).setIfSeqNo(conditional.getSeqNo()).setIfPrimaryTerm(conditional.getPrimaryTerm())
                ).actionGet();
                assertEquals(DocWriteResponse.Result.DELETED, deleted.getResult());
                assertEquals(4L, deleted.getVersion());
                assertTrue(deleted.getSeqNo() > conditional.getSeqNo());
                final var absent = client.delete(new DeleteRequest("alpha", id)).actionGet();
                assertEquals(DocWriteResponse.Result.NOT_FOUND, absent.getResult());
                assertTrue(absent.getSeqNo() > deleted.getSeqNo());
            }
        }
    }

    public void testGeneratedIdsStayOnTheRequestForReuseAndRetry() throws Exception {
        final var clock = new AtomicLong(1_000L);
        final var plane = plane(clock);
        try (var owner = new ServerlessNode(settings("native-id-owner")); var peer = new ServerlessNode(settings("native-id-peer"))) {
            attach(owner, plane);
            attach(peer, plane);
            own(owner, plane, clock);
            for (var via : List.of(owner, peer)) {
                final var request = new IndexRequest("alpha").source("{\"msg\":\"generated\"}", XContentType.JSON);
                final var created = via.client().index(request).actionGet();
                assertNotNull(created.getId());
                assertEquals("the caller must know the id even if it later needs to retry", created.getId(), request.id());
                final var repeated = via.client().index(request).actionGet();
                assertEquals(created.getId(), repeated.getId());
                assertEquals(DocWriteResponse.Result.UPDATED, repeated.getResult());
                assertEquals(2L, repeated.getVersion());
            }
        }
    }

    public void testUnsupportedWriteDirectivesAreRefusedBeforeApplying() throws Exception {
        final var clock = new AtomicLong(1_000L);
        final var plane = plane(clock);
        try (var node = new ServerlessNode(settings("native-refusals"))) {
            attach(node, plane);
            own(node, plane, clock);
            final var client = node.client();
            for (IndexRequest request : List.of(
                index("routing", "blocked").routing("custom"),
                index("version", "blocked").version(7).versionType(org.opensearch.index.VersionType.EXTERNAL),
                index("pipeline", "blocked").setPipeline("transform"),
                index("alias", "blocked").setRequireAlias(true),
                index("replicas", "blocked").waitForActiveShards(2)
            )) {
                expectThrows(UnsupportedOperationException.class, () -> client.index(request).actionGet());
                assertFalse(client.get(new org.opensearch.action.get.GetRequest("alpha", request.id())).actionGet().isExists());
            }
            final var written = client.index(index("retained", "original")).actionGet();
            assertTrue(written.getSeqNo() >= 0L);
            for (DeleteRequest request : List.of(
                new DeleteRequest("alpha", "retained").routing("custom"),
                new DeleteRequest("alpha", "retained").version(7).versionType(org.opensearch.index.VersionType.EXTERNAL),
                new DeleteRequest("alpha", "retained").waitForActiveShards(2)
            )) {
                expectThrows(UnsupportedOperationException.class, () -> client.delete(request).actionGet());
                assertTrue(client.get(new org.opensearch.action.get.GetRequest("alpha", "retained")).actionGet().isExists());
            }
            plane.updateDescriptor(
                plane.describe("alpha").orElseThrow().withSettings(Settings.builder().put("index.final_pipeline", "protect").build()),
                plane.descriptorGeneration("alpha")
            );
            expectThrows(UnsupportedOperationException.class, () -> client.index(index("final-pipeline", "unprocessed")).actionGet());
            assertFalse(client.get(new org.opensearch.action.get.GetRequest("alpha", "final-pipeline")).actionGet().isExists());
        }
    }

    public static final class NativeWriteContractPlugin extends org.opensearch.plugins.Plugin
        implements
            org.opensearch.plugins.ActionPlugin {
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
                    Request admitted = request;
                    if (request instanceof org.opensearch.action.get.GetRequest get) {
                        if ("cached".equals(get.id())) {
                            listener.onResponse(
                                (Response) new org.opensearch.action.get.GetResponse(
                                    new org.opensearch.index.get.GetResult(
                                        "alpha",
                                        "cached",
                                        5L,
                                        1L,
                                        7L,
                                        true,
                                        new org.opensearch.core.common.bytes.BytesArray("{\"msg\":\"cached\"}"),
                                        java.util.Map.of(),
                                        java.util.Map.of()
                                    )
                                )
                            );
                            return;
                        }
                        if ("redirect".equals(get.id())) {
                            admitted = (Request) new org.opensearch.action.get.GetRequest(get.index(), "public");
                        }
                    }
                    if (request instanceof IndexRequest indexed && "rewrite".equals(indexed.id())) {
                        admitted = (Request) index(indexed.id(), "approved");
                    }
                    chain.proceed(task, action, admitted, org.opensearch.core.action.ActionListener.wrap(response -> {
                        if (org.opensearch.action.index.IndexAction.NAME.equals(action)
                            && (response instanceof org.opensearch.action.index.IndexResponse) == false) {
                            throw new IllegalStateException("an index filter must receive IndexResponse");
                        }
                        if (org.opensearch.action.delete.DeleteAction.NAME.equals(action)
                            && (response instanceof org.opensearch.action.delete.DeleteResponse) == false) {
                            throw new IllegalStateException("a delete filter must receive DeleteResponse");
                        }
                        listener.onResponse(response);
                    }, listener::onFailure));
                }
            });
        }
    }

    public void testNativeWriteFiltersSeeTheAdmittedRequestAndNativeResponses() throws Exception {
        final var clock = new AtomicLong(1_000L);
        final var plane = plane(clock);
        try (var node = new ServerlessNode(settings("native-filter-contract"), List.of(new NativeWriteContractPlugin()))) {
            attach(node, plane);
            own(node, plane, clock);
            final var created = node.client().index(index("rewrite", "original")).actionGet();
            assertTrue(created.getSeqNo() >= 0L);
            assertEquals(
                "approved",
                node.client().get(new org.opensearch.action.get.GetRequest("alpha", "rewrite")).actionGet().getSourceAsMap().get("msg")
            );
            assertEquals(
                DocWriteResponse.Result.DELETED,
                node.client().delete(new DeleteRequest("alpha", "rewrite")).actionGet().getResult()
            );
            try (var builder = org.opensearch.common.xcontent.XContentFactory.cborBuilder()) {
                builder.startObject().field("msg", "binary-source").endObject();
                final var binary = new IndexRequest("alpha").id("binary")
                    .source(org.opensearch.core.common.bytes.BytesReference.bytes(builder), XContentType.CBOR);
                assertEquals(DocWriteResponse.Result.CREATED, node.client().index(binary).actionGet().getResult());
                assertEquals(
                    "binary-source",
                    node.client().get(new org.opensearch.action.get.GetRequest("alpha", "binary")).actionGet().getSourceAsMap().get("msg")
                );
            }
        }
    }

    public void testNativeGetConditionsAndRefusalsLocallyAndForwarded() throws Exception {
        final var clock = new AtomicLong(1_000L);
        final var plane = plane(clock);
        try (
            var owner = new ServerlessNode(settings("native-get-contract-owner"));
            var peer = new ServerlessNode(settings("native-get-contract-peer"))
        ) {
            attach(owner, plane);
            attach(peer, plane);
            own(owner, plane, clock);
            owner.client().index(index("versioned", "first")).actionGet();
            final var current = owner.client().index(index("versioned", "current")).actionGet();
            for (var via : List.of(owner, peer)) {
                for (var type : org.opensearch.index.VersionType.values()) {
                    final var request = new org.opensearch.action.get.GetRequest("alpha", "versioned").version(current.getVersion())
                        .versionType(type);
                    assertEquals(current.getVersion(), via.client().get(request).actionGet().getVersion());
                    expectThrows(
                        VersionConflictEngineException.class,
                        () -> via.client()
                            .get(
                                new org.opensearch.action.get.GetRequest("alpha", "versioned").version(current.getVersion() - 1)
                                    .versionType(type)
                            )
                            .actionGet()
                    );
                }
                assertFalse(
                    via.client().get(new org.opensearch.action.get.GetRequest("alpha", "absent").version(1)).actionGet().isExists()
                );
                for (var request : List.of(
                    new org.opensearch.action.get.GetRequest("alpha", "versioned").routing("custom"),
                    new org.opensearch.action.get.GetRequest("alpha", "versioned").storedFields("msg")
                )) {
                    expectThrows(UnsupportedOperationException.class, () -> via.client().get(request).actionGet());
                }
            }
        }
    }

    public void testNativeGetFiltersCanReplaceRequestsAndSupplyCachedResponses() throws Exception {
        final var clock = new AtomicLong(1_000L);
        final var plane = plane(clock);
        try (var node = new ServerlessNode(settings("native-get-filter-contract"), List.of(new NativeWriteContractPlugin()))) {
            attach(node, plane);
            own(node, plane, clock);
            node.client().index(index("redirect", "private")).actionGet();
            node.client().index(index("public", "visible")).actionGet();
            final var redirected = node.client().get(new org.opensearch.action.get.GetRequest("alpha", "redirect")).actionGet();
            assertEquals("public", redirected.getId());
            assertEquals("visible", redirected.getSourceAsMap().get("msg"));
            final var cached = node.client().get(new org.opensearch.action.get.GetRequest("alpha", "cached")).actionGet();
            assertEquals("cached", cached.getSourceAsMap().get("msg"));
            assertEquals(7L, cached.getVersion());
            final var operations = new org.opensearch.serverless.shard.ShardOperations(node, plane);
            assertEquals("public", operations.get("alpha", "redirect").document().id());
            assertEquals(7L, operations.get("alpha", "cached").document().version());
            final var address = node.boundHttpAddress().publishAddress();
            try (var http = java.net.http.HttpClient.newHttpClient()) {
                for (String id : List.of("redirect", "cached")) {
                    final var response = http.send(
                        java.net.http.HttpRequest.newBuilder(
                            java.net.URI.create("http://" + address.getAddress() + ":" + address.getPort() + "/alpha/_doc/" + id)
                        ).GET().build(),
                        java.net.http.HttpResponse.BodyHandlers.ofString()
                    );
                    assertEquals(response.body(), 200, response.statusCode());
                    assertFalse(response.body(), response.body().contains("private"));
                    assertTrue(response.body(), response.body().contains(id.equals("redirect") ? "visible" : "cached"));
                }
            }

        }
    }

}
