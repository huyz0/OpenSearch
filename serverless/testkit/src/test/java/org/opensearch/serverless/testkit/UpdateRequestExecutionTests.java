/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateAction;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
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

public class UpdateRequestExecutionTests extends OpenSearchTestCase {
    private void withNode(PolicyFilterPlugin plugin, org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work)
        throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "update-filter-contract")
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

    private static void seed(ServerlessNode node, String index, String id, String value) {
        node.client().index(new IndexRequest(index).id(id).source(Map.of("value", value))).actionGet();
    }

    private static String value(ServerlessNode node, String index, String id) {
        final var response = node.client().get(new org.opensearch.action.get.GetRequest(index, id)).actionGet();
        return response.isExists() ? String.valueOf(response.getSourceAsMap().get("value")) : null;
    }

    public void testNativeUpdateRequestExposesItsActualBodyAndFlags() throws Exception {
        withNode(PolicyFilterPlugin.request(UpdateAction.NAME, request -> {
            final var update = (UpdateRequest) request;
            assertEquals("changed", update.doc().sourceAsMap().get("value"));
            assertTrue(update.docAsUpsert());
            assertFalse(update.detectNoop());
            assertEquals(2, update.retryOnConflict());
            assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, update.getRefreshPolicy());
            return request;
        }), (node, metadata) -> {
            final var answer = send(
                node,
                "POST",
                "/original/_update/original?refresh=true&retry_on_conflict=2",
                "{\"doc\":{\"value\":\"changed\"},\"doc_as_upsert\":true,\"detect_noop\":false}"
            );
            assertEquals(answer.body(), 201, answer.statusCode());
        });
    }

    public void testUpdateUsesTheAdmittedIndexIdAndDocument() throws Exception {
        withNode(
            PolicyFilterPlugin.request(UpdateAction.NAME, request -> new UpdateRequest("selected", "chosen").doc(Map.of("value", "safe"))),
            (node, metadata) -> {
                seed(node, "original", "original", "private");
                seed(node, "selected", "chosen", "old");
                final var answer = send(node, "POST", "/original/_update/original", "{\"doc\":{\"value\":\"unsafe\"}}");
                assertEquals(answer.body(), 200, answer.statusCode());
                assertEquals("private", value(node, "original", "original"));
                assertEquals("safe", value(node, "selected", "chosen"));
                assertTrue(answer.body(), answer.body().contains("\"_index\":\"selected\""));
                assertTrue(answer.body(), answer.body().contains("\"_id\":\"chosen\""));
            }
        );
    }

    public void testUpdateResponseRewritesReachTheCaller() throws Exception {
        withNode(PolicyFilterPlugin.response(UpdateAction.NAME, response -> {
            final var update = (UpdateResponse) response;
            assertEquals("original", update.getIndex());
            return new UpdateResponse(new ShardId("visible", "uuid-visible", 0), "safe", 0L, 1L, 1L, DocWriteResponse.Result.NOOP);
        }), (node, metadata) -> {
            seed(node, "original", "original", "old");
            final var answer = send(node, "POST", "/original/_update/original", "{\"doc\":{\"value\":\"changed\"}}");
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"_index\":\"visible\""));
            assertFalse(answer.body(), answer.body().contains("original"));
        });
    }

    public void testCachedNativeUpdateResponsesDoNotWrite() throws Exception {
        final var cached = new UpdateResponse(new ShardId("cached", "uuid-cached", 0), "safe", 0L, 1L, 1L, DocWriteResponse.Result.CREATED);
        withNode(PolicyFilterPlugin.cached(UpdateAction.NAME, cached), (node, metadata) -> {
            seed(node, "original", "original", "old");
            final var answer = send(node, "POST", "/original/_update/original", "{\"doc\":{\"value\":\"changed\"}}");
            assertEquals(answer.body(), 201, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"_index\":\"cached\""));
            assertEquals("old", value(node, "original", "original"));
        });
    }

    public void testAdmittedConditionsAreCheckedBeforeNoOpUpdates() throws Exception {
        withNode(PolicyFilterPlugin.request(UpdateAction.NAME, request -> {
            ((UpdateRequest) request).setIfSeqNo(12345L).setIfPrimaryTerm(1L);
            return request;
        }), (node, metadata) -> {
            seed(node, "original", "original", "old");
            final var answer = send(node, "POST", "/original/_update/original", "{\"doc\":{\"value\":\"old\"}}");
            assertEquals(answer.body(), 409, answer.statusCode());
            assertEquals("old", value(node, "original", "original"));
        });
    }

    public void testUnsupportedAdmittedSourceFetchingIsRefused() throws Exception {
        withNode(PolicyFilterPlugin.request(UpdateAction.NAME, request -> {
            ((UpdateRequest) request).fetchSource(true);
            return request;
        }), (node, metadata) -> {
            seed(node, "original", "original", "old");
            final var answer = send(node, "POST", "/original/_update/original", "{\"doc\":{\"value\":\"changed\"}}");
            assertEquals(answer.body(), 501, answer.statusCode());
            assertEquals("old", value(node, "original", "original"));
        });
    }

    public void testUnsupportedAdmittedRoutingIsRefused() throws Exception {
        withNode(PolicyFilterPlugin.request(UpdateAction.NAME, request -> {
            ((UpdateRequest) request).routing("elsewhere");
            return request;
        }), (node, metadata) -> {
            seed(node, "original", "original", "old");
            final var answer = send(node, "POST", "/original/_update/original", "{\"doc\":{\"value\":\"changed\"}}");
            assertEquals(answer.body(), 501, answer.statusCode());
            assertEquals("old", value(node, "original", "original"));
        });
    }

    public void testUpdateResponseAuthorizationStillPrecedesHttpSuccess() throws Exception {
        withNode(PolicyFilterPlugin.denyAfter(UpdateAction.NAME), (node, metadata) -> {
            seed(node, "original", "original", "old");
            final var answer = send(node, "POST", "/original/_update/original", "{\"doc\":{\"value\":\"changed\"}}");
            assertEquals(answer.body(), 403, answer.statusCode());
        });
    }

    public void testScriptedUpsertRequestsExposeTheActualScriptAndUpsert() throws Exception {
        withNode(PolicyFilterPlugin.request(UpdateAction.NAME, request -> {
            final var update = (UpdateRequest) request;
            assertNotNull(update.script());
            assertTrue(update.scriptedUpsert());
            assertEquals("seed", update.upsertRequest().sourceAsMap().get("value"));
            return request;
        }), (node, metadata) -> {
            final var answer = send(
                node,
                "POST",
                "/original/_update/scripted",
                "{\"script\":{\"source\":\"ctx._source.value=params.value\",\"params\":{\"value\":\"safe\"}},"
                    + "\"upsert\":{\"value\":\"seed\"},\"scripted_upsert\":true}"
            );
            assertEquals(answer.body(), 201, answer.statusCode());
            assertEquals("safe", value(node, "original", "scripted"));
        });
    }

    public void testNegativeAdmittedRetryCountsAreRejectedBeforeWriting() throws Exception {
        withNode(PolicyFilterPlugin.request(UpdateAction.NAME, request -> {
            ((UpdateRequest) request).retryOnConflict(-1);
            return request;
        }), (node, metadata) -> {
            seed(node, "original", "original", "old");
            final var answer = send(node, "POST", "/original/_update/original", "{\"doc\":{\"value\":\"changed\"}}");
            assertEquals(answer.body(), 400, answer.statusCode());
            assertEquals("old", value(node, "original", "original"));
        });
    }

    public void testUnsupportedAdmittedAliasRequirementsAreRefused() throws Exception {
        withNode(PolicyFilterPlugin.request(UpdateAction.NAME, request -> {
            ((UpdateRequest) request).setRequireAlias(true);
            return request;
        }), (node, metadata) -> {
            seed(node, "original", "original", "old");
            final var answer = send(node, "POST", "/original/_update/original", "{\"doc\":{\"value\":\"changed\"}}");
            assertEquals(answer.body(), 501, answer.statusCode());
            assertEquals("old", value(node, "original", "original"));
        });
    }

    public void testAdmittedRetryPolicyRetriesFromAFreshReadWithOneAdmission() throws Exception {
        final var admissions = new java.util.concurrent.atomic.AtomicInteger();
        withNode(PolicyFilterPlugin.request(UpdateAction.NAME, request -> {
            admissions.incrementAndGet();
            ((UpdateRequest) request).retryOnConflict(1);
            return request;
        }), (node, metadata) -> {
            seed(node, "original", "original", "old");
            metadata.scripts().put("replace", "{\"script\":{\"lang\":\"painless\",\"source\":\"ctx._source.value=params.value\"}}");
            final var store = (HookedBlobStore) metadata.blobStore();
            store.onNextRegisterRead("scripts", "_version", () -> seed(node, "original", "original", "raced"));
            final var answer = send(
                node,
                "POST",
                "/original/_update/original",
                "{\"script\":{\"id\":\"replace\",\"params\":{\"value\":\"safe\"}}}"
            );
            assertTrue("the write must land after the update's first read", store.fired());
            assertEquals(answer.body(), 200, answer.statusCode());
            assertEquals("safe", value(node, "original", "original"));
            assertEquals("retries remain inside the one admitted operation", 1, admissions.get());
            assertTrue(answer.body(), answer.body().contains("\"_version\":3"));
        });
    }

    public void testNoOpResponsesDoNotReportSuccessfulShardWrites() throws Exception {
        withNode(PolicyFilterPlugin.request(UpdateAction.NAME, request -> request), (node, metadata) -> {
            seed(node, "original", "original", "old");
            final var answer = send(node, "POST", "/original/_update/original", "{\"doc\":{\"value\":\"old\"}}");
            assertEquals(answer.body(), 200, answer.statusCode());
            assertTrue(answer.body(), answer.body().contains("\"result\":\"noop\""));
            assertTrue(answer.body(), answer.body().contains("\"successful\":0"));
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
