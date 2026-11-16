/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.secure_sm.AccessController;

import java.io.IOException;
import java.util.Optional;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;

/**
 * The registers of one bucket: small objects updated by compare-and-swap on GCS generation preconditions, as
 * {@link org.opensearch.common.blobstore.ConditionalBlobContainer} describes them. Names are full object names.
 */
final class GoogleCloudStorageBlobRegisters {

    private final CheckedSupplier<Storage, IOException> client;
    private final String bucketName;

    GoogleCloudStorageBlobRegisters(CheckedSupplier<Storage, IOException> client, String bucketName) {
        this.client = client;
        this.bucketName = bucketName;
    }

    /**
     * Reads the register at {@code blobName}. Unlike {@code FsBlobContainer}/{@code
     * S3BlobContainer}, no self-managed generation counter needs to be embedded in the blob's own
     * bytes: GCS objects already carry a real, monotonically increasing generation number as
     * first-class metadata, which maps directly onto {@link BlobRegister#generation()}.
     */
    Optional<BlobRegister> read(String blobName) throws IOException {
        final BlobId blobId = BlobId.of(bucketName, blobName);
        // The download is pinned to the generation the metadata named, so the value and generation always match.
        // A swap landing between the two requests removes that generation (buckets without object versioning keep
        // only the live one), and the download answers 404. The metadata is then read again: the register now holds
        // a later generation, or is gone.
        for (int attempt = 1;; attempt++) {
            final Blob blob = AccessController.doPrivilegedChecked(() -> client.get().get(blobId));
            if (blob == null) {
                return Optional.empty();
            }
            try {
                final byte[] content = AccessController.doPrivilegedChecked(() -> blob.getContent());
                return Optional.of(new BlobRegister(blob.getGeneration(), new BytesArray(content)));
            } catch (StorageException e) {
                if (e.getCode() != HTTP_NOT_FOUND || attempt >= MAX_REGISTER_READ_ATTEMPTS) {
                    throw e;
                }
            }
        }
    }

    /** How many times a register read follows a generation replaced under it before giving up. */
    static final int MAX_REGISTER_READ_ATTEMPTS = 16;

    /**
     * CASes the register at {@code blobName} using GCS's real conditional-write primitives
     * ({@code BlobTargetOption.doesNotExist()} / {@code generationMatch(long)}) as the actual
     * concurrency guard: GCS evaluates the generation match atomically server-side against the
     * object's live generation, so a concurrent writer racing between our check and this call is
     * caught there (as an {@code HTTP_PRECON_FAILED} response), not missed.
     */
    BlobRegisterCasResult compareAndSwap(String blobName, long expectedGeneration, BytesReference newValue) throws IOException {
        final BlobId blobId = BlobId.of(bucketName, blobName);
        final BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        final byte[] bytes = BytesReference.toBytes(newValue);
        final Storage.BlobTargetOption[] targetOptions = expectedGeneration == BlobRegister.ABSENT_GENERATION
            ? new Storage.BlobTargetOption[] { Storage.BlobTargetOption.doesNotExist() }
            : new Storage.BlobTargetOption[] { Storage.BlobTargetOption.generationMatch(expectedGeneration) };
        try {
            final Blob written = AccessController.doPrivilegedChecked(() -> client.get().create(blobInfo, bytes, targetOptions));
            return BlobRegisterCasResult.applied(written.getGeneration());
        } catch (final StorageException e) {
            if (e.getCode() == HTTP_PRECON_FAILED) {
                // Someone raced us. The backend did not return the conflicting generation.
                // A subsequent read may describe a different state from the conditional failure.
                return BlobRegisterCasResult.conflict(BlobRegisterCasResult.UNKNOWN_GENERATION);
            }
            throw e;
        }
    }

    /**
     * Deletes the register only if its live generation is still {@code expectedGeneration}, with GCS's own
     * {@code generationMatch} precondition on the delete. A GCS generation is already unique across an
     * object's incarnations, so nothing more is needed to keep a delete from removing a later write.
     */
    boolean deleteIfUnchanged(String blobName, long expectedGeneration) throws IOException {
        final BlobId blobId = BlobId.of(bucketName, blobName);
        try {
            return AccessController.doPrivilegedChecked(
                () -> client.get().delete(blobId, Storage.BlobSourceOption.generationMatch(expectedGeneration))
            );
        } catch (final StorageException e) {
            if (e.getCode() == HTTP_PRECON_FAILED) {
                return false;
            }
            throw e;
        }
    }
}
