/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.repositories.s3;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ThreadLeakFilters(filters = ResponseInputStreamTimeoutThreadFilter.class)
public class S3RegisterConflictTests extends OpenSearchTestCase {
    private S3BlobContainer container(S3Client client) {
        final var store = mock(S3BlobStore.class);
        when(store.bucket()).thenReturn("bucket");
        when(store.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(store.clientReference()).thenAnswer(invocation -> new AmazonS3Reference(client));
        return new S3BlobContainer(BlobPath.cleanPath(), store);
    }

    public void testFailedConditionalCreateCannotClaimLaterAbsence() throws Exception {
        final var client = mock(S3Client.class);
        when(client.getObject(any(GetObjectRequest.class))).thenThrow(NoSuchKeyException.builder().statusCode(404).build());
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(
            S3Exception.builder().statusCode(412).build()
        );
        final var result = container(client).compareAndSwapRegister("head", 0L, new BytesArray("next"));
        assertFalse(result.applied());
        // The request lost to a concurrent create, then that register was deleted before a later GET.
        // A refused create cannot linearize at absence: the generation at its failure is unknown.
        assertEquals(org.opensearch.common.blobstore.BlobRegisterCasResult.UNKNOWN_GENERATION, result.currentGeneration());
        verify(client, times(1)).getObject(any(GetObjectRequest.class));
    }

    public void testFailedKnownConditionalUpdateDoesNotReadALaterGeneration() throws Exception {
        final var client = mock(S3Client.class);
        final byte[] content = ByteBuffer.allocate(Long.BYTES + 1).putLong(7L).put((byte) 'x').array();
        when(client.getObject(any(GetObjectRequest.class))).thenReturn(
            new ResponseInputStream<>(GetObjectResponse.builder().eTag("observed").build(), new ByteArrayInputStream(content))
        ).thenThrow(NoSuchKeyException.builder().statusCode(404).build());
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(
            S3Exception.builder().statusCode(412).build()
        );
        final var container = container(client);
        assertEquals(7L, container.readRegister("head").orElseThrow().generation());
        final var result = container.compareAndSwapRegister("head", 7L, new BytesArray("next"));
        assertFalse(result.applied());
        assertEquals(org.opensearch.common.blobstore.BlobRegisterCasResult.UNKNOWN_GENERATION, result.currentGeneration());
        verify(client, times(1)).getObject(any(GetObjectRequest.class));
    }

    public void testFailedDirectCreateReportsUnknownWithoutARead() throws Exception {
        final var client = mock(S3Client.class);
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(
            S3Exception.builder().statusCode(412).build()
        );
        final var result = container(client).createRegisterIfAbsent("head", new BytesArray("next"));
        assertFalse(result.applied());
        assertEquals(org.opensearch.common.blobstore.BlobRegisterCasResult.UNKNOWN_GENERATION, result.currentGeneration());
        verify(client, times(0)).getObject(any(GetObjectRequest.class));
    }
}
