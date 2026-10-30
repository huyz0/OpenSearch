/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.SegmentPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class IndexServiceLifecycleTests extends OpenSearchTestCase {
    private static final String MAPPING = "{\"properties\":{\"value\":{\"type\":\"keyword\"}}}";

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

    public void testClosingTheLastServingShardPreservesAnOpeningSibling() throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            metadata.createIndex(new IndexDescriptor("docs", "uuid-docs", 2, MAPPING, null));
            try (var node = new ServerlessNode(settings("opening-sibling"))) {
                node.start();
                final var first = node.activateWriter(metadata, "docs", 0).orElseThrow();
                final var service = node.indicesService().indexService(first.getIndex());
                final var opening = new CountDownLatch(1);
                final var resume = new CountDownLatch(1);
                store.onNextRegisterRead("#1", SegmentPublisher.MANIFEST, () -> {
                    opening.countDown();
                    try {
                        assertTrue(resume.await(30, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                });
                try (var executor = Executors.newSingleThreadExecutor()) {
                    final var next = executor.submit(() -> node.activateWriter(metadata, "docs", 1).orElseThrow());
                    try {
                        assertTrue(opening.await(30, TimeUnit.SECONDS));
                        node.reconciler().releaseShard(first, "last serving shard closes while sibling is opening");
                        assertSame(service, node.indicesService().indexService(first.getIndex()));
                        assertEquals(0, service.numberOfShards());
                    } finally {
                        resume.countDown();
                    }
                    final var second = next.get(30, TimeUnit.SECONDS);
                    assertSame(service, node.indicesService().indexService(second.getIndex()));
                    assertNotNull(node.reconciler().shard(second));
                    node.reconciler().releaseShard(second, "last sibling closes");
                    assertNull(node.indicesService().indexService(second.getIndex()));
                }
            }
        }
    }

    public void testAFailedSiblingDoesNotPreventTheLastServiceFromClosing() throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            metadata.createIndex(new IndexDescriptor("docs", "uuid-docs", 2, MAPPING, null));
            try (var node = new ServerlessNode(settings("failed-sibling-marker"))) {
                node.start();
                final var first = node.activateWriter(metadata, "docs", 0).orElseThrow();
                store.onNextRegisterRead(
                    "#1",
                    SegmentPublisher.MANIFEST,
                    () -> { throw new IllegalStateException("injected sibling failure"); }
                );
                expectThrows(Exception.class, () -> node.activateWriter(metadata, "docs", 1));
                node.reconciler().releaseShard(first, "last serving shard after failed sibling");
                assertNull(node.indicesService().indexService(first.getIndex()));
            }
        }
    }
}
