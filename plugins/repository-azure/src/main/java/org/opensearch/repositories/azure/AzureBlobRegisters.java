/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.azure;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobDownloadContentResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.common.implementation.Constants;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.secure_sm.AccessController;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The registers of one Azure container: small blobs updated by compare-and-swap on Azure's ETag preconditions, as
 * {@link org.opensearch.common.blobstore.ConditionalBlobContainer} describes them. Names are full blob names.
 */
final class AzureBlobRegisters {

    private final Supplier<Tuple<BlobServiceClient, Supplier<Context>>> clients;
    private final String container;
    private final Supplier<Duration> timeout;

    AzureBlobRegisters(Supplier<Tuple<BlobServiceClient, Supplier<Context>>> client, String container, Supplier<Duration> timeout) {
        this.clients = client;
        this.container = container;
        this.timeout = timeout;
    }

    /**
     * Reads the register at {@code blobName}: an ordinary blob whose body is {@code <8-byte
     * big-endian generation><value bytes>}, the same wire format {@code FsBlobContainer}/
     * {@code S3BlobContainer} use -- unlike GCS, Azure blob ETags are opaque (like S3's), so a
     * self-managed generation counter has to be embedded in the blob's own bytes rather than read
     * back from blob metadata.
     */
    Optional<BlobRegister> read(String blobName) throws URISyntaxException, IOException {
        final Tuple<BlobServiceClient, Supplier<Context>> client = clients.get();
        final BlobContainerClient blobContainer = client.v1().getBlobContainerClient(container);
        final BlobClient blob = blobContainer.getBlobClient(blobName);
        try {
            final BlobDownloadContentResponse response = AccessController.doPrivileged(
                () -> blob.downloadContentWithResponse(null, null, timeout.get(), client.v2().get())
            );
            return Optional.of(BlobRegister.fromBytes(response.getValue().toBytes()));
        } catch (final BlobStorageException e) {
            if (e.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                return Optional.empty();
            }
            throw e;
        }
    }

    /**
     * CASes the register at {@code blobName} using Azure's real conditional-write primitives
     * ({@code If-Match}/{@code If-None-Match} via {@link BlobRequestConditions}) as the actual
     * concurrency guard: a freshly-read generation is checked client-side purely as a fast-fail,
     * but the authoritative check is Azure itself evaluating the conditional upload atomically
     * against the blob's live ETag, so a racing writer between our read and our write is caught
     * there (as an {@code HTTP_PRECON_FAILED} response), not missed.
     */
    BlobRegisterCasResult compareAndSwap(String blobName, long expectedGeneration, BytesReference newValue) throws URISyntaxException,
        IOException {
        final Tuple<BlobServiceClient, Supplier<Context>> client = clients.get();
        final BlobContainerClient blobContainer = client.v1().getBlobContainerClient(container);
        final BlobClient blob = blobContainer.getBlobClient(blobName);

        String currentETag;
        long currentGeneration;
        try {
            final BlobDownloadContentResponse response = AccessController.doPrivileged(
                () -> blob.downloadContentWithResponse(null, null, timeout.get(), client.v2().get())
            );
            currentETag = response.getDeserializedHeaders().getETag();
            currentGeneration = BlobRegister.fromBytes(response.getValue().toBytes()).generation();
        } catch (final BlobStorageException e) {
            if (e.getStatusCode() != HttpURLConnection.HTTP_NOT_FOUND) {
                throw e;
            }
            currentETag = null;
            currentGeneration = BlobRegister.ABSENT_GENERATION;
        }

        if (currentGeneration != expectedGeneration) {
            return BlobRegisterCasResult.conflict(currentGeneration);
        }

        // A random start rather than 1, so generations never repeat across a register's incarnations; see
        // BlobRegister#initialGeneration.
        final long newGeneration = expectedGeneration == BlobRegister.ABSENT_GENERATION
            ? BlobRegister.initialGeneration()
            : expectedGeneration + 1;
        final byte[] bytesToWrite = BlobRegister.toBytes(newGeneration, newValue);
        final BlobRequestConditions conditions = new BlobRequestConditions();
        if (currentETag == null) {
            conditions.setIfNoneMatch(Constants.HeaderConstants.ETAG_WILDCARD);
        } else {
            conditions.setIfMatch(currentETag);
        }

        try {
            AccessController.doPrivileged(
                () -> blob.uploadWithResponse(
                    new BlobParallelUploadOptions(new ByteArrayInputStream(bytesToWrite), bytesToWrite.length).setRequestConditions(
                        conditions
                    ),
                    timeout.get(),
                    client.v2().get()
                )
            );
            return BlobRegisterCasResult.applied(newGeneration);
        } catch (final BlobStorageException e) {
            // Azure returns 412 for an If-Match mismatch (updating an existing register) but 409
            // BlobAlreadyExists for an If-None-Match:* mismatch (racing put-if-absent) -- both mean
            // the same thing here: someone raced us between our read and this write.
            final boolean ifMatchConflict = e.getStatusCode() == HttpURLConnection.HTTP_PRECON_FAILED;
            final boolean ifNoneMatchConflict = e.getStatusCode() == HttpURLConnection.HTTP_CONFLICT
                && BlobErrorCode.BLOB_ALREADY_EXISTS.equals(e.getErrorCode());
            if (ifMatchConflict || ifNoneMatchConflict) {
                // A subsequent read may describe a different state from the conditional failure.
                return BlobRegisterCasResult.conflict(BlobRegisterCasResult.UNKNOWN_GENERATION);
            }
            throw e;
        }
    }

    /**
     * Deletes the register only if it still holds {@code expectedGeneration}: its ETag is read with its
     * generation, and the delete carries {@code If-Match} on that ETag, which Azure evaluates atomically
     * against the live blob. A write landing after the read is refused, not deleted.
     */
    boolean deleteIfUnchanged(String blobName, long expectedGeneration) throws URISyntaxException, IOException {
        final Tuple<BlobServiceClient, Supplier<Context>> client = clients.get();
        final BlobContainerClient blobContainer = client.v1().getBlobContainerClient(container);
        final BlobClient blob = blobContainer.getBlobClient(blobName);
        final String eTag;
        try {
            final BlobDownloadContentResponse response = AccessController.doPrivileged(
                () -> blob.downloadContentWithResponse(null, null, timeout.get(), client.v2().get())
            );
            if (BlobRegister.fromBytes(response.getValue().toBytes()).generation() != expectedGeneration) {
                return false;
            }
            eTag = response.getDeserializedHeaders().getETag();
        } catch (final BlobStorageException e) {
            if (e.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                return false;
            }
            throw e;
        }
        final BlobRequestConditions conditions = new BlobRequestConditions().setIfMatch(eTag);
        try {
            AccessController.doPrivileged(() -> blob.deleteWithResponse(null, conditions, timeout.get(), client.v2().get()));
            return true;
        } catch (final BlobStorageException e) {
            if (e.getStatusCode() == HttpURLConnection.HTTP_PRECON_FAILED || e.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                return false;
            }
            throw e;
        }
    }

}
