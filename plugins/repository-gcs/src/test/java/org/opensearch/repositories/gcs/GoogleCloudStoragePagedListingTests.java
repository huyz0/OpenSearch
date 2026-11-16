/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the paged listings of {@link GoogleCloudStorageBlobContainer}.
 */
public class GoogleCloudStoragePagedListingTests extends OpenSearchTestCase {

    public void testPagedBlobsFollowProviderUnicodeOrder() throws Exception {
        paginationPreservesUnicode(false, 1);
    }

    public void testPagedChildrenFollowProviderUnicodeOrder() throws Exception {
        paginationPreservesUnicode(true, 1);
    }

    public void testPagedBlobLimitDoesNotOverflow() throws Exception {
        paginationPreservesUnicode(false, Integer.MAX_VALUE);
    }

    public void testPagedChildLimitDoesNotOverflow() throws Exception {
        paginationPreservesUnicode(true, Integer.MAX_VALUE);
    }

    @SuppressWarnings("unchecked")
    private void paginationPreservesUnicode(boolean children, int limit) throws Exception {
        final String cursor = "\uE000";
        final String following = "\uD83D\uDE00";
        final var first = mock(com.google.cloud.storage.Blob.class);
        final var second = mock(com.google.cloud.storage.Blob.class);
        when(first.getName()).thenReturn("root/" + cursor + (children ? "/" : ""));
        when(second.getName()).thenReturn("root/" + following + (children ? "/" : ""));
        when(first.isDirectory()).thenReturn(children);
        when(second.isDirectory()).thenReturn(children);
        when(first.getSize()).thenReturn(1L);
        when(second.getSize()).thenReturn(1L);
        final com.google.api.gax.paging.Page<com.google.cloud.storage.Blob> page = mock(com.google.api.gax.paging.Page.class);
        // GCS already applies its UTF-8 cursor. Its inclusive blob offset may return the cursor,
        // whereas a child offset starts past the cursor's subtree.
        when(page.iterateAll()).thenReturn(children ? List.of(second) : List.of(first, second));
        final Storage storage = mock(Storage.class);
        when(storage.get("bucket")).thenReturn(mock(Bucket.class));
        when(storage.list(eq("bucket"), any(Storage.BlobListOption[].class))).thenAnswer(invocation -> {
            final Storage.BlobListOption[] options = (Storage.BlobListOption[]) invocation.getRawArguments()[1];
            assertTrue(Arrays.asList(options).contains(Storage.BlobListOption.pageSize(limit >= 1000 ? 1000 : limit + 1)));
            return page;
        });
        final GoogleCloudStorageService service = mock(GoogleCloudStorageService.class);
        when(service.client(any(String.class), any(String.class), any(GoogleCloudStorageOperationsStats.class))).thenReturn(storage);
        try (var store = new GoogleCloudStorageBlobStore("bucket", "test", "repo", service, 1024)) {
            final var container = store.blobContainer(BlobPath.cleanPath().add("root"));
            if (children) {
                assertEquals(List.of(following), new java.util.ArrayList<>(container.children(cursor, limit).keySet()));
            } else {
                assertEquals(List.of(following), container.listBlobsByPrefix("", cursor, limit).stream().map(b -> b.name()).toList());
            }
        }
    }
}
