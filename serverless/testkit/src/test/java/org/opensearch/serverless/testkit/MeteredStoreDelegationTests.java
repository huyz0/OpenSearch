/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.metadata.ConditionalDeleteProbe;
import org.opensearch.serverless.store.ObjectStores;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.util.List;

/**
 * The production wrapper passes through what the backend does well, rather than letting an interface default
 * stand in for it.
 *
 * <p>It did not: the wrapper every production store is built behind overrode neither the conditional delete nor
 * the bounded listings, so the probe failed on the default's refusal and every node stayed on tombstones, and a
 * capped pattern listing listed the whole prefix.
 */
public class MeteredStoreDelegationTests extends OpenSearchTestCase {

    public void testMetadataReadCountsOnlyConsumedBytes() throws Exception {
        final var backend = org.mockito.Mockito.mock(org.opensearch.common.blobstore.BlobStore.class);
        final var raw = org.mockito.Mockito.mock(BlobContainer.class);
        final var path = BlobPath.cleanPath();
        org.mockito.Mockito.when(backend.blobContainer(path)).thenReturn(raw);
        final var stream = new ByteArrayInputStream(new byte[12]);
        org.mockito.Mockito.when(raw.readBlobWithMetadata("doc"))
            .thenReturn(new org.opensearch.common.blobstore.InputStreamWithMetadata(stream, java.util.Map.of("policy", "retained")));
        try (var metered = new ObjectStores.Metered(backend)) {
            try (var read = metered.blobContainer(path).readBlobWithMetadata("doc")) {
                assertEquals(java.util.Map.of("policy", "retained"), read.getMetadata());
                assertEquals(0L, metered.bytesRead());
                assertEquals(3, read.getInputStream().read(new byte[3]));
                assertEquals(3L, metered.bytesRead());
                assertEquals(1L, metered.blobReads());
            }
        }
    }

    public void testVersionedReadCountsMaterializedBytes() throws Exception {
        final var backend = org.mockito.Mockito.mock(org.opensearch.common.blobstore.BlobStore.class);
        final var raw = org.mockito.Mockito.mock(BlobContainer.class);
        final var path = BlobPath.cleanPath();
        org.mockito.Mockito.when(backend.blobContainer(path)).thenReturn(raw);
        org.mockito.Mockito.when(raw.readBlobWithVersion("doc"))
            .thenReturn(new org.opensearch.common.blobstore.VersionedBlob(new byte[12], "token"));
        try (var metered = new ObjectStores.Metered(backend)) {
            final var read = metered.blobContainer(path).readBlobWithVersion("doc");
            assertEquals("token", read.versionToken());
            assertEquals(12L, metered.bytesRead());
            assertEquals(1L, metered.blobReads());
        }
    }

    public void testStreamReadFailureIsCounted() throws Exception {
        final var backend = org.mockito.Mockito.mock(org.opensearch.common.blobstore.BlobStore.class);
        final var raw = org.mockito.Mockito.mock(BlobContainer.class);
        final var path = BlobPath.cleanPath();
        org.mockito.Mockito.when(backend.blobContainer(path)).thenReturn(raw);
        final var stream = org.mockito.Mockito.mock(java.io.InputStream.class);
        org.mockito.Mockito.when(stream.read()).thenThrow(new java.io.IOException("interrupted transfer"));
        org.mockito.Mockito.when(raw.readBlob("doc")).thenReturn(stream);
        try (var metered = new ObjectStores.Metered(backend); var read = metered.blobContainer(path).readBlob("doc")) {
            assertEquals("interrupted transfer", expectThrows(java.io.IOException.class, read::read).getMessage());
            assertEquals(1L, metered.errors());
            assertEquals(0L, metered.bytesRead());
        }
    }

    public void testAsyncListingCountsAttributionAndFailure() throws Exception {
        final var backend = org.mockito.Mockito.mock(org.opensearch.common.blobstore.BlobStore.class);
        final var raw = org.mockito.Mockito.mock(BlobContainer.class);
        final var path = BlobPath.cleanPath();
        org.mockito.Mockito.when(backend.blobContainer(path)).thenReturn(raw);
        final var failure = new java.io.IOException("listing unavailable");
        org.mockito.Mockito.doAnswer(invocation -> {
            final org.opensearch.core.action.ActionListener<List<BlobMetadata>> listener = invocation.getArgument(3);
            listener.onFailure(failure);
            return null;
        })
            .when(raw)
            .listBlobsByPrefixInSortedOrder(
                org.mockito.ArgumentMatchers.eq(""),
                org.mockito.ArgumentMatchers.eq(2),
                org.mockito.ArgumentMatchers.eq(BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC),
                org.mockito.ArgumentMatchers.any()
            );
        try (var metered = new ObjectStores.Metered(backend)) {
            final var attributed = new java.util.concurrent.atomic.AtomicLong();
            final var seen = new java.util.concurrent.atomic.AtomicReference<Exception>();
            ObjectStores.attributedTo(attributed, () -> {
                metered.blobContainer(path)
                    .listBlobsByPrefixInSortedOrder(
                        "",
                        2,
                        BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC,
                        org.opensearch.core.action.ActionListener.wrap(answer -> fail("listing must fail"), seen::set)
                    );
                return null;
            });
            assertSame(failure, seen.get());
            assertEquals(1L, metered.listings());
            assertEquals(1L, metered.errors());
            assertEquals(1L, attributed.get());
        }
    }

    public void testTheConditionalDeleteProbePassesThroughTheWrapper() throws Exception {
        final ObjectStores.Metered metered = new ObjectStores.Metered(new FsBlobStore(1024, createTempDir(), false));
        assertTrue(
            "a store that honours conditional deletes must be seen to through the production wrapper",
            ConditionalDeleteProbe.honoured(metered, BlobPath.cleanPath().add("probe"))
        );
    }

    public void testPagedListingsAreTheBackendsOwnAndCounted() throws Exception {
        final ObjectStores.Metered metered = new ObjectStores.Metered(new FsBlobStore(1024, createTempDir(), false));
        final BlobContainer container = metered.blobContainer(BlobPath.cleanPath().add("names"));
        for (String name : List.of("a", "b", "c", "d")) {
            container.writeBlob(name, new ByteArrayInputStream(new byte[1]), 1, false);
        }
        final long before = metered.impliedS3Requests();
        final List<BlobMetadata> page = container.listBlobsByPrefix("", "b", 1);
        assertEquals(List.of("c"), page.stream().map(BlobMetadata::name).toList());
        assertEquals("one listing, counted", before + 1, metered.impliedS3Requests());
        final BlobContainer root = metered.blobContainer(BlobPath.cleanPath());
        assertEquals(List.of("names"), root.children(null, 5).keySet().stream().filter(n -> n.startsWith("extra") == false).toList());
    }
}
