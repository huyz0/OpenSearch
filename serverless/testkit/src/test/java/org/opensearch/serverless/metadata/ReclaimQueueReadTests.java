/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReclaimQueueReadTests extends OpenSearchTestCase {
    public void testATemporaryReadFailureKeepsTheIntentForRetry() throws Exception {
        assertReadFailureIsRetried(true);
    }

    public void testAStreamFailureKeepsTheIntentForRetry() throws Exception {
        assertReadFailureIsRetried(false);
    }

    private void assertReadFailureIsRetried(boolean failOpening) throws Exception {
        final BlobStore store = mock(BlobStore.class);
        final BlobContainer root = mock(BlobContainer.class);
        final BlobContainer entries = mock(BlobContainer.class);
        final BlobPath base = BlobPath.cleanPath().add("queue");
        when(store.blobContainer(any(BlobPath.class))).thenAnswer(invocation -> {
            final BlobPath path = invocation.getArgument(0);
            return path.buildAsString().equals(base.buildAsString()) ? root : entries;
        });
        when(root.children()).thenReturn(Map.of("202601010000", entries));
        when(entries.listBlobsByPrefixInSortedOrder("", 1000, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC)).thenReturn(
            List.of(new PlainBlobMetadata("intent", 80L))
        );
        final byte[] json = "{\"name\":\"logs-a\",\"uuid\":\"old\",\"shards\":1,\"had_writer\":true}".getBytes(StandardCharsets.UTF_8);
        if (failOpening) {
            when(entries.readBlob("intent")).thenThrow(new IOException("temporary store outage"))
                .thenReturn(new ByteArrayInputStream(json));
        } else {
            when(entries.readBlob("intent")).thenReturn(new java.io.InputStream() {
                @Override
                public int read() throws IOException {
                    throw new IOException("stream interrupted");
                }
            }, new ByteArrayInputStream(json));
        }
        final ReclaimQueue queue = new ReclaimQueue(store, base);
        final long now = Instant.parse("2026-01-01T00:01:00Z").toEpochMilli();
        final AtomicInteger reclaimed = new AtomicInteger();
        final ReclaimQueue.Reclaimer reclaimer = intent -> {
            assertEquals(new ReclaimQueue.Intent("logs-a", "old", 1, true), intent);
            reclaimed.incrementAndGet();
        };
        assertEquals(0, queue.drain(now, bucket -> true, reclaimer));
        assertEquals(0, reclaimed.get());
        verify(entries, never()).deleteBlobsIgnoringIfNotExists(any());
        verify(entries, never()).delete();
        assertEquals(1, queue.drain(now, bucket -> true, reclaimer));
        assertEquals(1, reclaimed.get());
        verify(entries).deleteBlobsIgnoringIfNotExists(List.of("intent"));
    }
}
