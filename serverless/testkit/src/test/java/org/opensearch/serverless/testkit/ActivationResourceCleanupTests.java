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
import org.opensearch.core.index.Index;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.SegmentPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

public class ActivationResourceCleanupTests extends OpenSearchTestCase {
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

    public void testManifestReadFailureClosesTheEmptyServiceAndAllowsRetry() throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var descriptor = new IndexDescriptor("docs", "uuid-docs", 1, MAPPING, null);
            metadata.createIndex(descriptor);
            try (var node = new ServerlessNode(settings("manifest-open-failure"))) {
                node.start();
                final var index = new Index(descriptor.name(), descriptor.uuid());
                final var reachedManifest = new AtomicBoolean();
                store.onNextRegisterRead("segments", SegmentPublisher.MANIFEST, () -> {
                    assertNotNull("the failure must follow service creation", node.indicesService().indexService(index));
                    reachedManifest.set(true);
                    throw new IllegalStateException("injected manifest read failure");
                });
                final var failure = expectThrows(Exception.class, () -> node.activateWriter(metadata, "docs", 0));
                assertTrue(failure.toString(), reachedManifest.get());
                assertTrue(node.reconciler().openShards().isEmpty());
                assertNull("an unsuccessful activation must not retain its empty index service", node.indicesService().indexService(index));
                final var opened = node.activateWriter(metadata, "docs", 0).orElseThrow();
                assertNotNull(node.reconciler().shard(opened));
            }
        }
    }

    public void testFailedSiblingActivationPreservesTheServingShard() throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var descriptor = new IndexDescriptor("docs", "uuid-docs", 2, MAPPING, null);
            metadata.createIndex(descriptor);
            try (var node = new ServerlessNode(settings("sibling-open-failure"))) {
                node.start();
                final var first = node.activateWriter(metadata, "docs", 0).orElseThrow();
                final var service = node.indicesService().indexService(first.getIndex());
                store.onNextRegisterRead("#1", SegmentPublisher.MANIFEST, () -> {
                    assertTrue(service.hasShard(0));
                    throw new IllegalStateException("injected sibling manifest failure");
                });
                expectThrows(Exception.class, () -> node.activateWriter(metadata, "docs", 1));
                assertSame(service, node.indicesService().indexService(first.getIndex()));
                assertSame(service.getShard(0), node.reconciler().shard(first));
                assertNotNull(node.activateWriter(metadata, "docs", 1).orElseThrow());
            }
        }
    }

    public void testInvalidStoredMappingDoesNotRetainAPartiallyCreatedService() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            // Low-level storage can contain an older or corrupted definition that predates PUT validation.
            final var descriptor = new IndexDescriptor(
                "docs",
                "uuid-docs",
                1,
                "{\"properties\":{\"broken\":{\"type\":\"not_a_mapper\"}}}",
                null
            );
            metadata.createIndex(descriptor);
            try (var node = new ServerlessNode(settings("mapping-open-failure"))) {
                node.start();
                expectThrows(Exception.class, () -> node.activateWriter(metadata, "docs", 0));
                assertTrue(node.reconciler().openShards().isEmpty());
                assertNull(node.indicesService().indexService(new Index(descriptor.name(), descriptor.uuid())));
            }
        }
    }
}
