/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.testkit.CountingBlobStore;
import org.opensearch.test.OpenSearchTestCase;

public class TemplateReadCostTests extends OpenSearchTestCase {
    public void testOversizedBodiesAreNotRetainedBySingleReadsOrListings() throws Exception {
        try (var store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var container = store.blobContainer(BlobPath.cleanPath());
            final var templates = new TemplateStore(container);
            final String oversized = "x".repeat(5 * 1024 * 1024);
            templates.put("large", oversized);
            assertEquals(oversized, templates.all().get("large"));
            store.reset();
            assertEquals(oversized, templates.get("large").orElseThrow());
            assertEquals(oversized, templates.get("large").orElseThrow());
            assertEquals("oversized bodies must not stay pinned by either cache", 2L, store.blobReads());
        }
    }

    public void testSingleReadsCacheImmutableContentAndStillObserveEveryMarker() throws Exception {
        try (var store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var container = store.blobContainer(BlobPath.cleanPath());
            final var writer = new TemplateStore(container);
            writer.put("pipeline", "{\"version\":1}");
            final var shared = new TemplateStore.Cache();
            store.reset();
            for (int i = 0; i < 5; i++) {
                assertEquals("{\"version\":1}", new TemplateStore(container, shared).get("pipeline").orElseThrow());
            }
            assertEquals("immutable bytes should be fetched once without requiring all() to warm the cache", 1L, store.blobReads());
            assertEquals("each read must still observe a concurrent overwrite or deletion", 5L, store.registerReads());
            writer.put("pipeline", "{\"version\":2}");
            assertEquals("{\"version\":2}", new TemplateStore(container, shared).get("pipeline").orElseThrow());
            assertTrue(writer.delete("pipeline"));
            assertTrue(new TemplateStore(container, shared).get("pipeline").isEmpty());
        }
    }
}
