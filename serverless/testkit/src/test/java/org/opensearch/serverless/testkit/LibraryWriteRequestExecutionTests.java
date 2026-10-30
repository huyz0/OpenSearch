/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

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
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shard.ShardOperations;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class LibraryWriteRequestExecutionTests extends OpenSearchTestCase {
    private void withNode(PolicyFilterPlugin plugin, org.opensearch.common.CheckedBiConsumer<ServerlessNode, MetadataPlane, Exception> work)
        throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var settings = Settings.builder()
                .put("node.name", "library-write-filter-contract")
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

    private static ShardOperations ops(ServerlessNode node, MetadataPlane metadata) {
        return new ShardOperations(node, metadata);
    }

    public void testStringIndexUsesTheAdmittedIndexIdAndSource() throws Exception {
        withNode(
            PolicyFilterPlugin.request(
                IndexAction.NAME,
                request -> new IndexRequest("selected").id("chosen").source(Map.of("value", "safe"))
            ),
            (node, metadata) -> {
                final String owner = ops(node, metadata).index("original", "original", "{\"value\":\"original\"}", true);
                assertEquals(node.localNode().getId(), owner);
                assertTrue(ops(node, metadata).get("selected", "chosen").document().source().contains("safe"));
                assertFalse(ops(node, metadata).get("original", "original").document().found());
            }
        );
    }

    public void testStringWritesExposeTheirRefreshPolicy() throws Exception {
        withNode(PolicyFilterPlugin.request(IndexAction.NAME, request -> {
            assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, ((IndexRequest) request).getRefreshPolicy());
            return request;
        }), (node, metadata) -> { ops(node, metadata).index("original", "original", "{\"value\":\"original\"}", true); });
    }

    public void testStringIndexExposesTypedNativeResults() throws Exception {
        withNode(PolicyFilterPlugin.response(IndexAction.NAME, response -> {
            final var indexed = (IndexResponse) response;
            assertEquals("original", indexed.getIndex());
            assertTrue(indexed.getSeqNo() >= 0L);
            return response;
        }), (node, metadata) -> {
            assertEquals(node.localNode().getId(), ops(node, metadata).index("original", "original", "{\"value\":\"original\"}", true));
        });
    }

    public void testStringIndexHonorsAdmittedSequenceConditions() throws Exception {
        withNode(PolicyFilterPlugin.request(IndexAction.NAME, request -> {
            ((IndexRequest) request).setIfSeqNo(12345L).setIfPrimaryTerm(1L);
            return request;
        }), (node, metadata) -> {
            expectThrows(
                VersionConflictEngineException.class,
                () -> ops(node, metadata).index("original", "original", "{\"value\":\"original\"}", true)
            );
            assertFalse(ops(node, metadata).get("original", "original").document().found());
        });
    }

    public void testStringDeleteUsesTheAdmittedIndexAndId() throws Exception {
        withNode(PolicyFilterPlugin.request(DeleteAction.NAME, request -> new DeleteRequest("selected", "victim")), (node, metadata) -> {
            ops(node, metadata).index("original", "keep", "{\"value\":\"keep\"}", true);
            ops(node, metadata).index("selected", "victim", "{\"value\":\"victim\"}", true);
            assertTrue(ops(node, metadata).delete("original", "keep", true));
            assertTrue(ops(node, metadata).get("original", "keep").document().found());
            assertFalse(ops(node, metadata).get("selected", "victim").document().found());
        });
    }

    public void testStringDeleteHonorsCachedNativeFoundnessWithoutWriting() throws Exception {
        final var cached = new DeleteResponse(new ShardId("original", "uuid-original", 0), "keep", 0L, 1L, 1L, false);
        withNode(PolicyFilterPlugin.cached(DeleteAction.NAME, cached), (node, metadata) -> {
            ops(node, metadata).index("original", "keep", "{\"value\":\"keep\"}", true);
            assertFalse(ops(node, metadata).delete("original", "keep", true));
            assertTrue(ops(node, metadata).get("original", "keep").document().found());
        });
    }

    public void testStringDeleteHonorsAdmittedSequenceConditions() throws Exception {
        withNode(PolicyFilterPlugin.request(DeleteAction.NAME, request -> {
            ((DeleteRequest) request).setIfSeqNo(12345L).setIfPrimaryTerm(1L);
            return request;
        }), (node, metadata) -> {
            ops(node, metadata).index("original", "keep", "{\"value\":\"keep\"}", true);
            expectThrows(VersionConflictEngineException.class, () -> ops(node, metadata).delete("original", "keep", true));
            assertTrue(ops(node, metadata).get("original", "keep").document().found());
        });
    }

    public void testCachedIndexResponsesDoNotInventAServingNode() throws Exception {
        final var cached = new IndexResponse(new ShardId("original", "uuid-original", 0), "original", 0L, 1L, 1L, true);
        withNode(PolicyFilterPlugin.cached(IndexAction.NAME, cached), (node, metadata) -> {
            assertNull(ops(node, metadata).index("original", "original", "{\"value\":\"original\"}", true));
            assertFalse(ops(node, metadata).get("original", "original").document().found());
        });
    }

}
