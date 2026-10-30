/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.repositories.gcs;

import com.google.cloud.BatchResult;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.storage.StorageException;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GoogleCloudStorageBlobStoreContainerTests extends OpenSearchTestCase {

    public void testRegisterConflictDoesNotClaimLaterAbsence() throws Exception {
        final Storage storage = mock(Storage.class);
        when(storage.get("bucket")).thenReturn(mock(Bucket.class));
        when(storage.create(any(com.google.cloud.storage.BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption[].class)))
            .thenThrow(new StorageException(412, "condition failed"));
        final GoogleCloudStorageService service = mock(GoogleCloudStorageService.class);
        when(service.client(any(String.class), any(String.class), any(GoogleCloudStorageOperationsStats.class))).thenReturn(storage);
        try (var store = new GoogleCloudStorageBlobStore("bucket", "test", "repo", service, 1024)) {
            final var result = store.compareAndSwapRegister("head", 0L, new org.opensearch.core.common.bytes.BytesArray("next"));
            assertFalse(result.applied());
            assertEquals(org.opensearch.common.blobstore.BlobRegisterCasResult.UNKNOWN_GENERATION, result.currentGeneration());
            org.mockito.Mockito.verify(storage, org.mockito.Mockito.never()).get(any(BlobId.class));
        }
    }

    @SuppressWarnings("unchecked")
    public void testDeleteBlobsIgnoringIfNotExistsThrowsIOException() throws Exception {
        final List<String> blobs = Arrays.asList("blobA", "blobB");

        final StorageBatch batch = mock(StorageBatch.class);
        if (randomBoolean()) {
            StorageBatchResult<Boolean> result = mock(StorageBatchResult.class);
            when(batch.delete(any(BlobId.class))).thenReturn(result);
            doThrow(new StorageException(new IOException("Batch submit throws a storage exception"))).when(batch).submit();
        } else {
            StorageBatchResult<Boolean> resultA = mock(StorageBatchResult.class);
            doReturn(resultA).when(batch).delete(eq(BlobId.of("bucket", "blobA")));
            doAnswer(invocation -> {
                StorageException storageException = new StorageException(new IOException("Batched delete throws a storage exception"));
                ((BatchResult.Callback) invocation.getArguments()[0]).error(storageException);
                return null;
            }).when(resultA).notify(any(StorageBatchResult.Callback.class));

            StorageBatchResult<Boolean> resultB = mock(StorageBatchResult.class);
            doReturn(resultB).when(batch).delete(eq(BlobId.of("bucket", "blobB")));
            doAnswer(invocation -> {
                if (randomBoolean()) {
                    StorageException storageException = new StorageException(new IOException("Batched delete throws a storage exception"));
                    ((BatchResult.Callback) invocation.getArguments()[0]).error(storageException);
                } else {
                    ((BatchResult.Callback) invocation.getArguments()[0]).success(randomBoolean());
                }
                return null;
            }).when(resultB).notify(any(StorageBatchResult.Callback.class));

            doNothing().when(batch).submit();
        }

        final Storage storage = mock(Storage.class);
        when(storage.get("bucket")).thenReturn(mock(Bucket.class));
        when(storage.batch()).thenReturn(batch);

        final GoogleCloudStorageService storageService = mock(GoogleCloudStorageService.class);
        when(storageService.client(any(String.class), any(String.class), any(GoogleCloudStorageOperationsStats.class))).thenReturn(storage);

        try (BlobStore store = new GoogleCloudStorageBlobStore("bucket", "test", "repo", storageService, randomIntBetween(1, 8) * 1024)) {
            final BlobContainer container = store.blobContainer(new BlobPath());

            IOException e = expectThrows(IOException.class, () -> container.deleteBlobsIgnoringIfNotExists(blobs));
            assertThat(e.getCause(), instanceOf(StorageException.class));
        }
    }

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
