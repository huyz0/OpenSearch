/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.secure_sm.AccessController;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The registers of one {@link S3BlobContainer}: small objects updated by compare-and-swap on S3's conditional
 * writes, as {@link org.opensearch.common.blobstore.ConditionalBlobContainer} describes them.
 */
final class S3BlobRegisters {

    private final S3BlobStore blobStore;
    private final String keyPath;

    S3BlobRegisters(S3BlobStore blobStore, String keyPath) {
        this.blobStore = blobStore;
        this.keyPath = keyPath;
    }

    private String buildKey(String blobName) {
        return keyPath + blobName;
    }

    /**
     * Reads the register at {@code blobName}: an ordinary object whose body is {@code <8-byte
     * big-endian generation><value bytes>}, per the same wire format {@code FsBlobContainer} uses.
     */
    Optional<BlobRegister> read(String blobName) throws IOException {
        String key = buildKey(blobName);
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(blobStore.bucket())
                .key(key)
                .expectedBucketOwner(blobStore.expectedBucketOwner())
                .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().getObjectMetricPublisher))
                .build();
            try (
                ResponseInputStream<GetObjectResponse> response = AccessController.doPrivileged(
                    () -> clientReference.get().getObject(getObjectRequest)
                )
            ) {
                final BlobRegister register = BlobRegister.fromBytes(response.readAllBytes());
                remember(blobName, register.generation(), response.response().eTag());
                return Optional.of(register);
            }
        } catch (NoSuchKeyException e) {
            forget(blobName);
            return Optional.empty();
        } catch (S3Exception e) {
            // Real S3 responds to a missing key with a NoSuchKeyException-typed 404, but not every
            // S3-compatible endpoint (including the fixture this is verified against) returns the
            // error body needed for the SDK to unmarshal that specific type, so a bare 404 must be
            // treated as absence too rather than surfacing as a generic failure.
            if (isMissingRegister(e)) {
                forget(blobName);
                return Optional.empty();
            }
            throw new IOException("Unable to read register [" + blobName + "]", e);
        } catch (SdkException e) {
            throw new IOException("Unable to read register [" + blobName + "]", e);
        }
    }

    /** Accepts missing-key and legacy unlabelled 404 responses, while preserving bucket and service errors. */
    private static boolean isMissingRegister(S3Exception failure) {
        if (failure.statusCode() != 404) {
            return false;
        }
        final String code = failure.awsErrorDetails() == null ? null : failure.awsErrorDetails().errorCode();
        return code == null || code.isEmpty() || "NoSuchKey".equals(code);
    }

    /**
     * CASes the register at {@code blobName} using S3's real conditional-write primitives
     * ({@code If-Match}/{@code If-None-Match}) as the actual concurrency guard, rather than
     * anything client-side: {@code expectedGeneration} is checked against a freshly-read current
     * generation purely as a fast-fail (no point attempting a write we already know is stale), but
     * the authoritative check is the conditional {@code PutObject} itself, which S3 evaluates
     * atomically against the object's live ETag. A concurrent writer racing between our read and
     * our put is caught there (as a 412 response), not missed.
     */
    BlobRegisterCasResult compareAndSwap(String blobName, long expectedGeneration, BytesReference newValue) throws IOException {
        String key = buildKey(blobName);
        final KnownVersion known = knownVersion(blobName);
        if (expectedGeneration != BlobRegister.ABSENT_GENERATION && known != null && known.generation() == expectedGeneration) {
            return putIfMatch(blobName, known.eTag(), expectedGeneration + 1, newValue);
        }
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            S3Client client = clientReference.get();

            String currentETag;
            long currentGeneration;
            try {
                GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(blobStore.bucket())
                    .key(key)
                    .expectedBucketOwner(blobStore.expectedBucketOwner())
                    .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().getObjectMetricPublisher))
                    .build();
                try (
                    ResponseInputStream<GetObjectResponse> response = AccessController.doPrivileged(
                        () -> client.getObject(getObjectRequest)
                    )
                ) {
                    currentETag = response.response().eTag();
                    currentGeneration = BlobRegister.fromBytes(response.readAllBytes()).generation();
                }
            } catch (NoSuchKeyException e) {
                currentETag = null;
                currentGeneration = BlobRegister.ABSENT_GENERATION;
            } catch (S3Exception e) {
                // See read: a bare 404 (not a properly-typed NoSuchKeyException) also means absent.
                if (isMissingRegister(e) == false) {
                    throw e;
                }
                currentETag = null;
                currentGeneration = BlobRegister.ABSENT_GENERATION;
            }

            if (currentGeneration != expectedGeneration) {
                return BlobRegisterCasResult.conflict(currentGeneration);
            }

            long newGeneration = expectedGeneration == BlobRegister.ABSENT_GENERATION
                ? BlobRegister.initialGeneration()
                : expectedGeneration + 1;
            byte[] bytesToWrite = BlobRegister.toBytes(newGeneration, newValue);
            PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
                .bucket(blobStore.bucket())
                .key(key)
                .contentLength((long) bytesToWrite.length)
                .expectedBucketOwner(blobStore.expectedBucketOwner())
                .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().putObjectMetricPublisher));
            if (currentETag == null) {
                putObjectRequestBuilder.ifNoneMatch("*");
            } else {
                putObjectRequestBuilder.ifMatch(currentETag);
            }
            PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();

            try {
                final PutObjectResponse written = AccessController.doPrivileged(
                    () -> client.putObject(putObjectRequest, RequestBody.fromBytes(bytesToWrite))
                );
                remember(blobName, newGeneration, written.eTag());
                return BlobRegisterCasResult.applied(newGeneration);
            } catch (S3Exception e) {
                forget(blobName);
                // A transient conflict does not prove that the precondition was unmet. Use the same
                // distinction as versioned writes, preserving service errors as IO failures.
                if (S3BlobContainer.isConditionFailure(e, currentETag != null)) {
                    return BlobRegisterCasResult.conflict(BlobRegisterCasResult.UNKNOWN_GENERATION);
                }
                throw e;
            }
        } catch (SdkException e) {
            throw new IOException("Unable to CAS register [" + blobName + "]", e);
        }
    }

    /**
     * One conditional PUT, where {@link #compareAndSwap} needs a GET first.
     *
     * <p>That GET is not wasted work in the general case: a CAS against an arbitrary generation has to
     * learn the current one. It is wasted here. The only thing the read tells the create path is that
     * {@code currentETag} is null, which selects {@code ifNoneMatch("*")}, and that header is itself the
     * atomic "must not exist" check evaluated by S3 against the live object. The class comment on
     * {@code compareAndSwap} already says the conditional write is the authoritative guard and
     * the read is "purely as a fast-fail", so dropping it costs no safety.
     *
     * <p>Conflict reports {@link BlobRegisterCasResult#UNKNOWN_GENERATION} rather than reading to find the real
     * generation, because a caller that lost a create race wants to know it lost, and paying a round trip
     * to decorate that would reintroduce the cost this exists to remove. A caller that needs the winning
     * value reads it itself, on a path that by definition is not the common one.
     */
    BlobRegisterCasResult createIfAbsent(String blobName, BytesReference value) throws IOException {
        String key = buildKey(blobName);
        long newGeneration = BlobRegister.initialGeneration();
        byte[] bytesToWrite = BlobRegister.toBytes(newGeneration, value);

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
            .bucket(blobStore.bucket())
            .key(key)
            .contentLength((long) bytesToWrite.length)
            .expectedBucketOwner(blobStore.expectedBucketOwner())
            .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().putObjectMetricPublisher))
            .ifNoneMatch("*")
            .build();

        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            S3Client client = clientReference.get();
            try {
                final PutObjectResponse written = AccessController.doPrivileged(
                    () -> client.putObject(putObjectRequest, RequestBody.fromBytes(bytesToWrite))
                );
                remember(blobName, newGeneration, written.eTag());
                return BlobRegisterCasResult.applied(newGeneration);
            } catch (S3Exception e) {
                // Only a definitively unmet precondition means somebody already owns the name.
                if (S3BlobContainer.isConditionFailure(e, false)) {
                    return BlobRegisterCasResult.conflict(BlobRegisterCasResult.UNKNOWN_GENERATION);
                }
                throw e;
            }
        } catch (SdkException e) {
            throw new IOException("Unable to create register [" + blobName + "]", e);
        }
    }

    /**
     * Deletes the register only if it still holds {@code expectedGeneration}, with S3's conditional
     * {@code DeleteObject} ({@code If-Match} on the object's ETag).
     *
     * <p>The ETag of the expected version is the one this container last saw for that generation, or is
     * read first. S3 evaluates {@code If-Match} on a delete atomically against the live object, so a write
     * that landed after the read is not deleted: the delete is refused with a 412. An S3-compatible store
     * that ignores the header would delete it anyway, which is why a caller relying on this probes first.
     */
    boolean deleteIfUnchanged(String blobName, long expectedGeneration) throws IOException {
        KnownVersion known = knownVersion(blobName);
        if (known == null || known.generation() != expectedGeneration) {
            final Optional<BlobRegister> current = read(blobName);
            if (current.isEmpty() || current.get().generation() != expectedGeneration) {
                return false;
            }
            known = knownVersion(blobName);
            if (known == null || known.generation() != expectedGeneration) {
                return false;
            }
        }
        final DeleteObjectRequest request = DeleteObjectRequest.builder()
            .bucket(blobStore.bucket())
            .key(buildKey(blobName))
            .expectedBucketOwner(blobStore.expectedBucketOwner())
            .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().deleteObjectsMetricPublisher))
            .ifMatch(known.eTag())
            .build();
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            AccessController.doPrivileged(() -> clientReference.get().deleteObject(request));
            forget(blobName);
            return true;
        } catch (S3Exception e) {
            forget(blobName);
            if (e.statusCode() == 412 || e.statusCode() == 409 || e.statusCode() == 404) {
                return false;
            }
            throw new IOException("Unable to conditionally delete register [" + blobName + "]", e);
        } catch (SdkException e) {
            throw new IOException("Unable to conditionally delete register [" + blobName + "]", e);
        }
    }

    /** The generation and ETag this container last saw for a register. */
    private record KnownVersion(long generation, String eTag) {
    }

    /**
     * What this container last read or wrote for each register, so a compare-and-swap against that very
     * version can be one conditional PUT rather than a GET and a PUT.
     *
     * <p><b>Why the ETag is a safe token for a generation.</b> A register's body begins with its generation,
     * a generation only increases within an incarnation, and a new incarnation starts at a random
     * {@link BlobRegister#initialGeneration()} -- so no two versions of one register ever have the same
     * body, and none the same ETag. {@code If-Match} on the remembered ETag therefore succeeds exactly when
     * the object is still the version whose generation was remembered, which is the condition the caller
     * asked for. A stale entry costs a refused write and a read, never a wrong one.
     *
     * <p>Bounded: an entry that fell out costs the GET it would have saved, nothing more.
     */
    private final Map<String, KnownVersion> knownVersions = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
        protected boolean removeEldestEntry(Map.Entry<String, KnownVersion> eldest) {
            return size() > 10_000;
        }
    });

    private KnownVersion knownVersion(String blobName) {
        return knownVersions.get(blobName);
    }

    private void remember(String blobName, long generation, String eTag) {
        if (eTag != null) {
            knownVersions.put(blobName, new KnownVersion(generation, eTag));
        }
    }

    private void forget(String blobName) {
        knownVersions.remove(blobName);
    }

    /** The compare-and-swap against a version this container already knows: one conditional PUT, no GET. */
    private BlobRegisterCasResult putIfMatch(String blobName, String eTag, long newGeneration, BytesReference newValue) throws IOException {
        final byte[] bytesToWrite = BlobRegister.toBytes(newGeneration, newValue);
        final PutObjectRequest request = PutObjectRequest.builder()
            .bucket(blobStore.bucket())
            .key(buildKey(blobName))
            .contentLength((long) bytesToWrite.length)
            .expectedBucketOwner(blobStore.expectedBucketOwner())
            .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().putObjectMetricPublisher))
            .ifMatch(eTag)
            .build();
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            final PutObjectResponse written = AccessController.doPrivileged(
                () -> clientReference.get().putObject(request, RequestBody.fromBytes(bytesToWrite))
            );
            remember(blobName, newGeneration, written.eTag());
            return BlobRegisterCasResult.applied(newGeneration);
        } catch (S3Exception e) {
            forget(blobName);
            // The same definitive precondition failures as a versioned If-Match write.
            if (S3BlobContainer.isConditionFailure(e, true)) {
                return BlobRegisterCasResult.conflict(BlobRegisterCasResult.UNKNOWN_GENERATION);
            }
            throw new IOException("Unable to CAS register [" + blobName + "]", e);
        } catch (SdkException e) {
            throw new IOException("Unable to CAS register [" + blobName + "]", e);
        }
    }
}
