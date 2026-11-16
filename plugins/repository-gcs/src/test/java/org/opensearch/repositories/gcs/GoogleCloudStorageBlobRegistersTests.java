/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link GoogleCloudStorageBlobRegisters}.
 */
public class GoogleCloudStorageBlobRegistersTests extends OpenSearchTestCase {

    public void testRegisterConflictDoesNotClaimLaterAbsence() throws Exception {
        final Storage storage = mock(Storage.class);
        when(storage.get("bucket")).thenReturn(mock(Bucket.class));
        when(storage.create(any(com.google.cloud.storage.BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption[].class)))
            .thenThrow(new StorageException(412, "condition failed"));
        final GoogleCloudStorageService service = mock(GoogleCloudStorageService.class);
        when(service.client(any(String.class), any(String.class), any(GoogleCloudStorageOperationsStats.class))).thenReturn(storage);
        try (var store = new GoogleCloudStorageBlobStore("bucket", "test", "repo", service, 1024)) {
            final var result = store.registers.compareAndSwap("head", 0L, new org.opensearch.core.common.bytes.BytesArray("next"));
            assertFalse(result.applied());
            assertEquals(org.opensearch.common.blobstore.BlobRegisterCasResult.UNKNOWN_GENERATION, result.currentGeneration());
            org.mockito.Mockito.verify(storage, org.mockito.Mockito.never()).get(any(BlobId.class));
        }
    }

    public void testRegisterReadFollowsAGenerationReplacedBetweenMetadataAndContent() throws Exception {
        final Storage storage = mock(Storage.class);
        when(storage.get("bucket")).thenReturn(mock(Bucket.class));
        final var replaced = mock(com.google.cloud.storage.Blob.class);
        when(replaced.getGeneration()).thenReturn(7L);
        when(replaced.getContent()).thenThrow(new StorageException(404, "No such object"));
        final var current = mock(com.google.cloud.storage.Blob.class);
        when(current.getGeneration()).thenReturn(8L);
        when(current.getContent()).thenReturn("next".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        when(storage.get(any(BlobId.class))).thenReturn(replaced, current);
        final GoogleCloudStorageService service = mock(GoogleCloudStorageService.class);
        when(service.client(any(String.class), any(String.class), any(GoogleCloudStorageOperationsStats.class))).thenReturn(storage);
        try (var store = new GoogleCloudStorageBlobStore("bucket", "test", "repo", service, 1024)) {
            final var register = store.registers.read("head").orElseThrow();
            assertEquals(8L, register.generation());
            assertEquals("next", register.value().utf8ToString());
        }
    }

    public void testRegisterReadReportsAbsenceWhenTheRegisterIsDeletedMidRead() throws Exception {
        final Storage storage = mock(Storage.class);
        when(storage.get("bucket")).thenReturn(mock(Bucket.class));
        final var deleted = mock(com.google.cloud.storage.Blob.class);
        when(deleted.getContent()).thenThrow(new StorageException(404, "No such object"));
        when(storage.get(any(BlobId.class))).thenReturn(deleted, (com.google.cloud.storage.Blob) null);
        final GoogleCloudStorageService service = mock(GoogleCloudStorageService.class);
        when(service.client(any(String.class), any(String.class), any(GoogleCloudStorageOperationsStats.class))).thenReturn(storage);
        try (var store = new GoogleCloudStorageBlobStore("bucket", "test", "repo", service, 1024)) {
            assertTrue(store.registers.read("head").isEmpty());
        }
    }

    public void testRegisterReadGivesUpUnderEndlessReplacement() throws Exception {
        final Storage storage = mock(Storage.class);
        when(storage.get("bucket")).thenReturn(mock(Bucket.class));
        final var replaced = mock(com.google.cloud.storage.Blob.class);
        when(replaced.getContent()).thenThrow(new StorageException(404, "No such object"));
        when(storage.get(any(BlobId.class))).thenReturn(replaced);
        final GoogleCloudStorageService service = mock(GoogleCloudStorageService.class);
        when(service.client(any(String.class), any(String.class), any(GoogleCloudStorageOperationsStats.class))).thenReturn(storage);
        try (var store = new GoogleCloudStorageBlobStore("bucket", "test", "repo", service, 1024)) {
            final StorageException e = expectThrows(StorageException.class, () -> store.registers.read("head"));
            assertEquals(404, e.getCode());
            org.mockito.Mockito.verify(storage, org.mockito.Mockito.times(GoogleCloudStorageBlobRegisters.MAX_REGISTER_READ_ATTEMPTS))
                .get(any(BlobId.class));
        }
    }
}
