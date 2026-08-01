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

package org.opensearch.repositories.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectAttributes;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;
import software.amazon.awssdk.utils.CollectionUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.StreamContext;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.InputStreamWithMetadata;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.support.AbstractBlobContainer;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.repositories.s3.async.S3AsyncDeleteHelper;
import org.opensearch.repositories.s3.async.SizeBasedBlockingQ;
import org.opensearch.repositories.s3.async.UploadRequest;
import org.opensearch.repositories.s3.utils.HttpRangeUtils;
import org.opensearch.repositories.s3.utils.SseKmsUtil;
import org.opensearch.secure_sm.AccessController;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import static org.opensearch.repositories.s3.S3Repository.MAX_FILE_SIZE;
import static org.opensearch.repositories.s3.S3Repository.MAX_FILE_SIZE_USING_MULTIPART;
import static org.opensearch.repositories.s3.S3Repository.MIN_PART_SIZE_USING_MULTIPART;
import static org.opensearch.repositories.s3.utils.SseKmsUtil.configureEncryptionSettings;

class S3BlobContainer extends AbstractBlobContainer implements AsyncMultiStreamBlobContainer {

    private static final Logger logger = LogManager.getLogger(S3BlobContainer.class);
    private static final long DEFAULT_OPERATION_TIMEOUT = TimeUnit.SECONDS.toSeconds(30);

    private final S3BlobStore blobStore;
    private final String keyPath;

    S3BlobContainer(BlobPath path, S3BlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        this.keyPath = path.buildAsString();
    }

    @Override
    public boolean blobExists(String blobName) {
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            AccessController.doPrivileged(
                () -> clientReference.get()
                    .headObject(
                        HeadObjectRequest.builder()
                            .bucket(blobStore.bucket())
                            .key(buildKey(blobName))
                            .expectedBucketOwner(blobStore.expectedBucketOwner())
                            .build()
                    )
            );
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (final Exception e) {
            throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
        }
    }

    @ExperimentalApi
    @Override
    public InputStreamWithMetadata readBlobWithMetadata(String blobName) throws IOException {
        S3RetryingInputStream s3RetryingInputStream = new S3RetryingInputStream(blobStore, buildKey(blobName));
        return new InputStreamWithMetadata(s3RetryingInputStream, s3RetryingInputStream.getMetadata());
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        return new S3RetryingInputStream(blobStore, buildKey(blobName));
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        if (position < 0L) {
            throw new IllegalArgumentException("position must be non-negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        if (length == 0) {
            return new ByteArrayInputStream(new byte[0]);
        } else {
            return new S3RetryingInputStream(blobStore, buildKey(blobName), position, Math.addExact(position, length - 1));
        }
    }

    @Override
    public long readBlobPreferredLength() {
        // This container returns streams that must be fully consumed, so we tell consumers to make bounded requests.
        return new ByteSizeValue(32, ByteSizeUnit.MB).getBytes();
    }

    /**
     * Honours {@code failIfAlreadyExists} by asking S3 to enforce it, throwing
     * {@link FileAlreadyExistsException} when the key is already present, which is what
     * {@code FsBlobContainer} does and therefore what every caller was already written against.
     *
     * <p>This used to be documented as unenforceable, "as the S3 API has no way to enforce this due to
     * its weak consistency model". Both halves stopped being true: S3 became strongly consistent in
     * December 2020 and gained conditional writes in August 2024. {@link #compareAndSwapRegister} in this
     * same class has used {@code If-None-Match} since it was written.
     *
     * <p>The flag being silently dropped was not only a trap for future code. Six callers in
     * {@code serverless-storage} already pass true for write-once state (commit manifests, bundles, clone
     * lineage, resharding ranges), and every one of them was protected on a filesystem repository and
     * unprotected on S3, so the divergence was invisible to tests. A losing manifest write is a lost
     * commit.
     *
     * <p>Requires an endpoint that implements conditional writes. Older S3-compatible stores do not, and
     * against those the precondition is ignored by the server and this reverts to the previous
     * last-writer-wins behaviour rather than failing.
     */
    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        writeBlobWithMetadata(blobName, inputStream, blobSize, failIfAlreadyExists, null);
    }

    /**
     * Write blob with its object metadata and optional encryption settings.
     */
    @ExperimentalApi
    @Override
    public void writeBlobWithMetadata(
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists,
        @Nullable Map<String, String> metadata,
        @Nullable CryptoMetadata cryptoMetadata
    ) throws IOException {
        assert inputStream.markSupported() : "No mark support on inputStream breaks the S3 SDK's ability to retry requests";
        AccessController.doPrivilegedChecked(() -> {
            if (blobSize <= getLargeBlobThresholdInBytes()) {
                executeSingleUpload(blobStore, buildKey(blobName), inputStream, blobSize, metadata, cryptoMetadata, failIfAlreadyExists);
            } else {
                executeMultipartUpload(blobStore, buildKey(blobName), inputStream, blobSize, metadata, cryptoMetadata, failIfAlreadyExists);
            }
        });
    }

    /**
     * Write blob with its object metadata.
     */
    @ExperimentalApi
    @Override
    public void writeBlobWithMetadata(
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists,
        @Nullable Map<String, String> metadata
    ) throws IOException {
        // Delegate to crypto-aware version with null CryptoMetadata
        writeBlobWithMetadata(blobName, inputStream, blobSize, failIfAlreadyExists, metadata, null);
    }

    @Override
    public void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException {
        CryptoMetadata crypto = writeContext.getCryptoMetadata();

        String indexKmsKey = null;
        String indexEncContext = null;

        if (crypto != null) {
            indexKmsKey = crypto.getKeyArn().orElse(null);
            indexEncContext = crypto.getEncryptionContext().orElse(null);
        }
        String mergeEncContext = SseKmsUtil.mergeAndEncodeEncryptionContexts(
            indexEncContext,
            blobStore.serverSideEncryptionEncryptionContext()
        );

        UploadRequest uploadRequest = new UploadRequest(
            blobStore.bucket(),
            buildKey(writeContext.getFileName()),
            writeContext.getFileSize(),
            writeContext.getWritePriority(),
            writeContext.getUploadFinalizer(),
            writeContext.doRemoteDataIntegrityCheck(),
            writeContext.getExpectedChecksum(),
            blobStore.isUploadRetryEnabled(),
            writeContext.getMetadata(),
            blobStore.serverSideEncryptionType(),
            indexKmsKey != null ? indexKmsKey : blobStore.serverSideEncryptionKmsKey(),
            blobStore.serverSideEncryptionBucketKey(),
            mergeEncContext,
            blobStore.expectedBucketOwner()
        );
        try {
            // If file size is greater than the queue capacity than SizeBasedBlockingQ will always reject the upload.
            // Therefore, redirecting it to slow client.
            if ((uploadRequest.getWritePriority() == WritePriority.LOW
                && blobStore.getLowPrioritySizeBasedBlockingQ().isMaxCapacityBelowContentLength(uploadRequest.getContentLength()) == false)
                || (uploadRequest.getWritePriority() != WritePriority.HIGH
                    && uploadRequest.getWritePriority() != WritePriority.URGENT
                    && blobStore.getNormalPrioritySizeBasedBlockingQ()
                        .isMaxCapacityBelowContentLength(uploadRequest.getContentLength()) == false)) {
                StreamContext streamContext = AccessController.doPrivileged(
                    () -> writeContext.getStreamProvider(uploadRequest.getContentLength())
                );
                InputStreamContainer inputStream = streamContext.provideStream(0);
                try {
                    executeMultipartUpload(
                        blobStore,
                        uploadRequest.getKey(),
                        inputStream.getInputStream(),
                        uploadRequest.getContentLength(),
                        uploadRequest.getMetadata(),
                        crypto,
                        // The async upload path carries no such flag, and its callers write
                        // content-addressed segment data where a rewrite is the same bytes. Passing false
                        // keeps this path exactly as it was rather than inventing a guarantee for it.
                        false
                    );
                    completionListener.onResponse(null);
                } catch (Exception ex) {
                    logger.error(
                        () -> new ParameterizedMessage(
                            "Failed to upload large file {} of size {} ",
                            uploadRequest.getKey(),
                            uploadRequest.getContentLength()
                        ),
                        ex
                    );
                    completionListener.onFailure(ex);
                }
                return;
            }
            long partSize = blobStore.getAsyncTransferManager()
                .calculateOptimalPartSize(writeContext.getFileSize(), writeContext.getWritePriority(), blobStore.isUploadRetryEnabled());
            StreamContext streamContext = AccessController.doPrivileged(() -> writeContext.getStreamProvider(partSize));
            try (AmazonAsyncS3Reference amazonS3Reference = AccessController.doPrivileged(blobStore::asyncClientReference)) {

                S3AsyncClient s3AsyncClient;
                if (writeContext.getWritePriority() == WritePriority.URGENT) {
                    s3AsyncClient = amazonS3Reference.get().urgentClient();
                } else if (writeContext.getWritePriority() == WritePriority.HIGH) {
                    s3AsyncClient = amazonS3Reference.get().priorityClient();
                } else {
                    s3AsyncClient = amazonS3Reference.get().client();
                }

                if (writeContext.getWritePriority() == WritePriority.URGENT
                    || writeContext.getWritePriority() == WritePriority.HIGH
                    || blobStore.isPermitBackedTransferEnabled() == false) {
                    createFileCompletableFuture(s3AsyncClient, uploadRequest, streamContext, completionListener);
                } else if (writeContext.getWritePriority() == WritePriority.LOW) {
                    blobStore.getLowPrioritySizeBasedBlockingQ()
                        .produce(
                            new SizeBasedBlockingQ.Item(
                                writeContext.getFileSize(),
                                () -> createFileCompletableFuture(s3AsyncClient, uploadRequest, streamContext, completionListener)
                            )
                        );
                } else if (writeContext.getWritePriority() == WritePriority.NORMAL) {
                    blobStore.getNormalPrioritySizeBasedBlockingQ()
                        .produce(
                            new SizeBasedBlockingQ.Item(
                                writeContext.getFileSize(),
                                () -> createFileCompletableFuture(s3AsyncClient, uploadRequest, streamContext, completionListener)
                            )
                        );
                } else {
                    throw new IllegalStateException("Cannot perform upload for other priority types.");
                }
            }
        } catch (Exception e) {
            logger.info("exception error from blob container for file {}", writeContext.getFileName());
            throw new IOException(e);
        }
    }

    private CompletableFuture<Void> createFileCompletableFuture(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        StreamContext streamContext,
        ActionListener<Void> completionListener
    ) {
        CompletableFuture<Void> completableFuture = blobStore.getAsyncTransferManager()
            .uploadObject(s3AsyncClient, uploadRequest, streamContext, blobStore.getStatsMetricPublisher());
        return completableFuture.whenComplete((response, throwable) -> {
            if (throwable == null) {
                completionListener.onResponse(response);
            } else {
                Exception ex = throwable instanceof Error ? new Exception(throwable) : (Exception) throwable;
                completionListener.onFailure(ex);
            }
        });
    }

    @ExperimentalApi
    @Override
    public void readBlobAsync(String blobName, ActionListener<ReadContext> listener) {
        try (AmazonAsyncS3Reference amazonS3Reference = AccessController.doPrivileged(blobStore::asyncClientReference)) {
            final S3AsyncClient s3AsyncClient = amazonS3Reference.get().client();
            final String bucketName = blobStore.bucket();
            final String blobKey = buildKey(blobName);

            final CompletableFuture<GetObjectAttributesResponse> blobMetadataFuture = getBlobMetadata(s3AsyncClient, bucketName, blobKey);

            blobMetadataFuture.whenComplete((blobMetadata, throwable) -> {
                if (throwable != null) {
                    Exception ex = throwable.getCause() instanceof Exception
                        ? (Exception) throwable.getCause()
                        : new Exception(throwable.getCause());
                    listener.onFailure(ex);
                    return;
                }

                try {
                    final List<ReadContext.StreamPartCreator> blobPartInputStreamFutures = new ArrayList<>();
                    final long blobSize = blobMetadata.objectSize();
                    final Integer numberOfParts = blobMetadata.objectParts() == null ? null : blobMetadata.objectParts().totalPartsCount();
                    final String blobChecksum = blobMetadata.checksum() == null ? null : blobMetadata.checksum().checksumCRC32();

                    if (numberOfParts == null) {
                        blobPartInputStreamFutures.add(() -> getBlobPartInputStreamContainer(s3AsyncClient, bucketName, blobKey, null));
                    } else {
                        // S3 multipart files use 1 to n indexing
                        for (int partNumber = 1; partNumber <= numberOfParts; partNumber++) {
                            final int innerPartNumber = partNumber;
                            blobPartInputStreamFutures.add(
                                () -> getBlobPartInputStreamContainer(s3AsyncClient, bucketName, blobKey, innerPartNumber)
                            );
                        }
                    }
                    listener.onResponse(new ReadContext.Builder(blobSize, blobPartInputStreamFutures).blobChecksum(blobChecksum).build());
                } catch (Exception ex) {
                    listener.onFailure(ex);
                }
            });
        } catch (Exception ex) {
            listener.onFailure(SdkException.create("Error occurred while fetching blob parts from the repository", ex));
        }
    }

    public boolean remoteIntegrityCheckSupported() {
        return true;
    }

    // package private for testing
    long getLargeBlobThresholdInBytes() {
        return blobStore.bufferSizeInBytes();
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete() throws IOException {
        PlainActionFuture<DeleteResult> future = new PlainActionFuture<>();
        deleteAsync(future);
        return getFutureValue(future);
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        deleteBlobsAsyncIgnoringIfNotExists(blobNames, future);
        getFutureValue(future);
    }

    private <T> T getFutureValue(PlainActionFuture<T> future) throws IOException {
        try {
            return future.get(DEFAULT_OPERATION_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Future got interrupted", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new RuntimeException(e.getCause());
        } catch (TimeoutException e) {
            FutureUtils.cancel(future);
            throw new IOException("Delete operation timed out after 30 seconds", e);
        }
    }

    @Override
    public List<BlobMetadata> listBlobsByPrefixInSortedOrder(String blobNamePrefix, int limit, BlobNameSortOrder blobNameSortOrder)
        throws IOException {
        // As AWS S3 returns list of keys in Lexicographic order, we don't have to fetch all the keys in order to sort them
        // We fetch only keys as per the given limit to optimize the fetch. If provided sort order is not Lexicographic,
        // we fall-back to default implementation of fetching all the keys and sorting them.
        if (blobNameSortOrder != BlobNameSortOrder.LEXICOGRAPHIC) {
            return super.listBlobsByPrefixInSortedOrder(blobNamePrefix, limit, blobNameSortOrder);
        } else {
            if (limit < 0) {
                throw new IllegalArgumentException("limit should not be a negative value");
            }
            String prefix = blobNamePrefix == null ? keyPath : buildKey(blobNamePrefix);
            try (AmazonS3Reference clientReference = blobStore.clientReference()) {
                List<BlobMetadata> blobs = executeListing(clientReference, listObjectsRequest(prefix, limit), limit).stream()
                    .flatMap(listing -> listing.contents().stream())
                    .map(s3Object -> new PlainBlobMetadata(s3Object.key().substring(keyPath.length()), s3Object.size()))
                    .collect(Collectors.toList());
                return blobs.subList(0, Math.min(limit, blobs.size()));
            } catch (final Exception e) {
                throw new IOException("Exception when listing blobs by prefix [" + prefix + "]", e);
            }
        }
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(@Nullable String blobNamePrefix) throws IOException {
        String prefix = blobNamePrefix == null ? keyPath : buildKey(blobNamePrefix);
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            return executeListing(clientReference, listObjectsRequest(prefix)).stream()
                .flatMap(listing -> listing.contents().stream())
                .map(s3Object -> new PlainBlobMetadata(s3Object.key().substring(keyPath.length()), s3Object.size()))
                .collect(Collectors.toMap(PlainBlobMetadata::name, Function.identity()));
        } catch (final SdkException e) {
            throw new IOException("Exception when listing blobs by prefix [" + prefix + "]", e);
        }
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            return executeListing(clientReference, listObjectsRequest(keyPath)).stream().flatMap(listObjectsResponse -> {
                assert listObjectsResponse.contents().stream().noneMatch(s -> {
                    for (CommonPrefix commonPrefix : listObjectsResponse.commonPrefixes()) {
                        if (s.key().substring(keyPath.length()).startsWith(commonPrefix.prefix())) {
                            return true;
                        }
                    }
                    return false;
                }) : "Response contained children for listed common prefixes.";
                return listObjectsResponse.commonPrefixes().stream();
            })
                .map(commonPrefix -> commonPrefix.prefix().substring(keyPath.length()))
                .filter(name -> name.isEmpty() == false)
                // Stripping the trailing slash off of the common prefix
                .map(name -> name.substring(0, name.length() - 1))
                .collect(Collectors.toMap(Function.identity(), name -> blobStore.blobContainer(path().add(name))));
        } catch (final SdkException e) {
            throw new IOException("Exception when listing children of [" + path().buildAsString() + ']', e);
        }
    }

    private static List<ListObjectsV2Response> executeListing(AmazonS3Reference clientReference, ListObjectsV2Request listObjectsRequest) {
        return executeListing(clientReference, listObjectsRequest, -1);
    }

    private static List<ListObjectsV2Response> executeListing(
        AmazonS3Reference clientReference,
        ListObjectsV2Request listObjectsRequest,
        int limit
    ) {
        return AccessController.doPrivileged(() -> {
            final List<ListObjectsV2Response> results = new ArrayList<>();
            int totalObjects = 0;
            ListObjectsV2Iterable listObjectsIterable = clientReference.get().listObjectsV2Paginator(listObjectsRequest);
            for (ListObjectsV2Response listObjectsV2Response : listObjectsIterable) {
                results.add(listObjectsV2Response);
                totalObjects += listObjectsV2Response.contents().size();
                if (limit != -1 && totalObjects >= limit) {
                    break;
                }
            }
            return results;
        });
    }

    private ListObjectsV2Request listObjectsRequest(String keyPath) {
        return ListObjectsV2Request.builder()
            .bucket(blobStore.bucket())
            .prefix(keyPath)
            .delimiter("/")
            .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().listObjectsMetricPublisher))
            .expectedBucketOwner(blobStore.expectedBucketOwner())
            .build();
    }

    private ListObjectsV2Request listObjectsRequest(String keyPath, int limit) {
        return listObjectsRequest(keyPath).toBuilder().maxKeys(Math.min(limit, 1000)).build();
    }

    private String buildKey(String blobName) {
        return keyPath + blobName;
    }

    /**
     * Uploads a blob using a single upload request
     */
    void executeSingleUpload(
        final S3BlobStore blobStore,
        final String blobName,
        final InputStream input,
        final long blobSize,
        final Map<String, String> metadata,
        @Nullable CryptoMetadata cryptoMetadata,
        final boolean failIfAlreadyExists
    ) throws IOException {

        // Extra safety checks
        if (blobSize > MAX_FILE_SIZE.getBytes()) {
            throw new IllegalArgumentException("Upload request size [" + blobSize + "] can't be larger than " + MAX_FILE_SIZE);
        }
        if (blobSize > blobStore.bufferSizeInBytes()) {
            throw new IllegalArgumentException("Upload request size [" + blobSize + "] can't be larger than buffer size");
        }

        PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
            .bucket(blobStore.bucket())
            .key(blobName)
            .contentLength(blobSize)
            .storageClass(blobStore.getStorageClass())
            .acl(blobStore.getCannedACL())
            .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().putObjectMetricPublisher))
            .expectedBucketOwner(blobStore.expectedBucketOwner());

        if (CollectionUtils.isNotEmpty(metadata)) {
            putObjectRequestBuilder = putObjectRequestBuilder.metadata(metadata);
        }

        if (failIfAlreadyExists) {
            // The precondition is the enforcement, evaluated atomically by S3 against the live object.
            // Reading first and then writing would leave a window a concurrent creator fits through,
            // which is the race compareAndSwapRegister already documents avoiding the same way.
            putObjectRequestBuilder.ifNoneMatch("*");
        }

        configureEncryptionSettings(putObjectRequestBuilder, blobStore, cryptoMetadata);

        PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            final InputStream requestInputStream;
            if (blobStore.isUploadRetryEnabled()) {
                requestInputStream = new BufferedInputStream(input, (int) (blobSize + 1));
            } else {
                requestInputStream = input;
            }
            AccessController.doPrivileged(
                () -> clientReference.get().putObject(putObjectRequest, RequestBody.fromInputStream(requestInputStream, blobSize))
            );
        } catch (final S3Exception e) {
            // 412 is the precondition failing, which means the key was already there. Reported as the
            // same exception FsBlobContainer throws so callers need one catch rather than two.
            if (failIfAlreadyExists && e.statusCode() == 412) {
                throw new FileAlreadyExistsException("Blob [" + blobName + "] already exists, cannot overwrite");
            }
            throw new IOException("Unable to upload object [" + blobName + "] using a single upload", e);
        } catch (final SdkException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using a single upload", e);
        }
    }

    /**
     * Uploads a blob using multipart upload requests.
     */
    void executeMultipartUpload(
        final S3BlobStore blobStore,
        final String blobName,
        final InputStream input,
        final long blobSize,
        final Map<String, String> metadata,
        @Nullable CryptoMetadata cryptoMetadata,
        final boolean failIfAlreadyExists
    ) throws IOException {

        ensureMultiPartUploadSize(blobSize);
        final long partSize = blobStore.bufferSizeInBytes();
        final Tuple<Long, Long> multiparts = numberOfMultiparts(blobSize, partSize);

        if (multiparts.v1() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Too many multipart upload requests, maybe try a larger buffer size?");
        }

        final int nbParts = multiparts.v1().intValue();
        final long lastPartSize = multiparts.v2();
        assert blobSize == (((nbParts - 1) * partSize) + lastPartSize) : "blobSize does not match multipart sizes";

        final SetOnce<String> uploadId = new SetOnce<>();
        final String bucketName = blobStore.bucket();
        boolean success = false;

        CreateMultipartUploadRequest.Builder createMultipartUploadRequestBuilder = CreateMultipartUploadRequest.builder()
            .bucket(bucketName)
            .key(blobName)
            .storageClass(blobStore.getStorageClass())
            .acl(blobStore.getCannedACL())
            .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().multipartUploadMetricCollector))
            .expectedBucketOwner(blobStore.expectedBucketOwner());

        if (CollectionUtils.isNotEmpty(metadata)) {
            createMultipartUploadRequestBuilder.metadata(metadata);
        }

        configureEncryptionSettings(createMultipartUploadRequestBuilder, blobStore, cryptoMetadata);

        final InputStream requestInputStream;
        if (blobStore.isUploadRetryEnabled()) {
            requestInputStream = new BufferedInputStream(input, (int) (partSize + 1));
        } else {
            requestInputStream = input;
        }

        CreateMultipartUploadRequest createMultipartUploadRequest = createMultipartUploadRequestBuilder.build();
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            uploadId.set(
                AccessController.doPrivileged(() -> clientReference.get().createMultipartUpload(createMultipartUploadRequest).uploadId())
            );
            if (Strings.isEmpty(uploadId.get())) {
                throw new IOException("Failed to initialize multipart upload " + blobName);
            }

            final List<CompletedPart> parts = new ArrayList<>();

            long bytesCount = 0;
            for (int i = 1; i <= nbParts; i++) {
                final UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                    .bucket(bucketName)
                    .key(blobName)
                    .uploadId(uploadId.get())
                    .partNumber(i)
                    .contentLength((i < nbParts) ? partSize : lastPartSize)
                    .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().multipartUploadMetricCollector))
                    .expectedBucketOwner(blobStore.expectedBucketOwner())
                    .build();

                bytesCount += uploadPartRequest.contentLength();
                final UploadPartResponse uploadResponse = AccessController.doPrivileged(
                    () -> clientReference.get()
                        .uploadPart(uploadPartRequest, RequestBody.fromInputStream(requestInputStream, uploadPartRequest.contentLength()))
                );
                parts.add(CompletedPart.builder().partNumber(uploadPartRequest.partNumber()).eTag(uploadResponse.eTag()).build());
            }

            if (bytesCount != blobSize) {
                throw new IOException(
                    "Failed to execute multipart upload for [" + blobName + "], expected " + blobSize + "bytes sent but got " + bytesCount
                );
            }

            CompleteMultipartUploadRequest.Builder completeMultipartUploadRequestBuilder = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(blobName)
                .uploadId(uploadId.get())
                .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().multipartUploadMetricCollector))
                .expectedBucketOwner(blobStore.expectedBucketOwner());

            if (failIfAlreadyExists) {
                // The precondition goes on the completion rather than on the creation, because that is the
                // request that publishes the key. Parts uploaded against a losing upload are discarded by
                // the abort in the finally block below.
                completeMultipartUploadRequestBuilder.ifNoneMatch("*");
            }

            CompleteMultipartUploadRequest completeMultipartUploadRequest = completeMultipartUploadRequestBuilder.build();
            AccessController.doPrivileged(() -> clientReference.get().completeMultipartUpload(completeMultipartUploadRequest));
            success = true;

        } catch (final S3Exception e) {
            if (failIfAlreadyExists && e.statusCode() == 412) {
                throw new FileAlreadyExistsException("Blob [" + blobName + "] already exists, cannot overwrite");
            }
            throw new IOException("Unable to upload object [" + blobName + "] using multipart upload", e);
        } catch (final SdkException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using multipart upload", e);
        } finally {
            if ((success == false) && Strings.hasLength(uploadId.get())) {
                AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(blobName)
                    .uploadId(uploadId.get())
                    .expectedBucketOwner(blobStore.expectedBucketOwner())
                    .build();
                try (AmazonS3Reference clientReference = blobStore.clientReference()) {
                    AccessController.doPrivileged(() -> clientReference.get().abortMultipartUpload(abortRequest));
                }
            }
        }
    }

    // non-static, package private for testing
    void ensureMultiPartUploadSize(final long blobSize) {
        if (blobSize > MAX_FILE_SIZE_USING_MULTIPART.getBytes()) {
            throw new IllegalArgumentException(
                "Multipart upload request size [" + blobSize + "] can't be larger than " + MAX_FILE_SIZE_USING_MULTIPART
            );
        }
        if (blobSize < MIN_PART_SIZE_USING_MULTIPART.getBytes()) {
            throw new IllegalArgumentException(
                "Multipart upload request size [" + blobSize + "] can't be smaller than " + MIN_PART_SIZE_USING_MULTIPART
            );
        }
    }

    /**
     * Returns the number parts of size of {@code partSize} needed to reach {@code totalSize},
     * along with the size of the last (or unique) part.
     *
     * @param totalSize the total size
     * @param partSize  the part size
     * @return a {@link Tuple} containing the number of parts to fill {@code totalSize} and
     * the size of the last part
     */
    static Tuple<Long, Long> numberOfMultiparts(final long totalSize, final long partSize) {
        if (partSize <= 0) {
            throw new IllegalArgumentException("Part size must be greater than zero");
        }

        if ((totalSize == 0L) || (totalSize <= partSize)) {
            return Tuple.tuple(1L, totalSize);
        }

        final long parts = totalSize / partSize;
        final long remaining = totalSize % partSize;

        if (remaining == 0) {
            return Tuple.tuple(parts, partSize);
        } else {
            return Tuple.tuple(parts + 1, remaining);
        }
    }

    /**
     * Fetches a part of the blob from the S3 bucket and transforms it to an {@link InputStreamContainer}, which holds
     * the stream and its related metadata.
     * @param s3AsyncClient Async client to be utilized to fetch the object part
     * @param bucketName Name of the S3 bucket
     * @param blobKey Identifier of the blob for which the parts will be fetched
     * @param partNumber Optional part number for the blob to be retrieved
     * @return A future of {@link InputStreamContainer} containing the stream and stream metadata.
     */
    CompletableFuture<InputStreamContainer> getBlobPartInputStreamContainer(
        S3AsyncClient s3AsyncClient,
        String bucketName,
        String blobKey,
        @Nullable Integer partNumber
    ) {
        final boolean isMultipartObject = partNumber != null;
        final GetObjectRequest.Builder getObjectRequestBuilder = GetObjectRequest.builder()
            .bucket(bucketName)
            .key(blobKey)
            .expectedBucketOwner(blobStore.expectedBucketOwner());

        if (isMultipartObject) {
            getObjectRequestBuilder.partNumber(partNumber);
        }
        return AccessController.doPrivileged(
            () -> s3AsyncClient.getObject(getObjectRequestBuilder.build(), AsyncResponseTransformer.toBlockingInputStream())
                .thenApply(response -> transformResponseToInputStreamContainer(response, isMultipartObject))
        );
    }

    /**
     * Transforms the stream response object from S3 into an {@link InputStreamContainer}
     * @param streamResponse Response stream object from S3
     * @param isMultipartObject Flag to denote a multipart object response
     * @return {@link InputStreamContainer} containing the stream and stream metadata
     */
    // Package-Private for testing.
    static InputStreamContainer transformResponseToInputStreamContainer(
        ResponseInputStream<GetObjectResponse> streamResponse,
        boolean isMultipartObject
    ) {
        final GetObjectResponse getObjectResponse = streamResponse.response();
        final String contentRange = getObjectResponse.contentRange();
        final Long contentLength = getObjectResponse.contentLength();
        if ((isMultipartObject && contentRange == null) || contentLength == null) {
            throw SdkException.builder().message("Failed to fetch required metadata for blob part").build();
        }
        final long offset = isMultipartObject ? HttpRangeUtils.getStartOffsetFromRangeHeader(getObjectResponse.contentRange()) : 0L;
        return new InputStreamContainer(streamResponse, getObjectResponse.contentLength(), offset);
    }

    /**
     * Retrieves the metadata like checksum, object size and parts for the provided blob within the S3 bucket.
     * @param s3AsyncClient Async client to be utilized to fetch the metadata
     * @param bucketName Name of the S3 bucket
     * @param blobName Identifier of the blob for which the metadata will be fetched
     * @return A future containing the metadata within {@link GetObjectAttributesResponse}
     */
    CompletableFuture<GetObjectAttributesResponse> getBlobMetadata(S3AsyncClient s3AsyncClient, String bucketName, String blobName) {
        // Fetch blob metadata - part info, size, checksum
        final GetObjectAttributesRequest getObjectAttributesRequest = GetObjectAttributesRequest.builder()
            .bucket(bucketName)
            .key(blobName)
            .objectAttributes(ObjectAttributes.CHECKSUM, ObjectAttributes.OBJECT_SIZE, ObjectAttributes.OBJECT_PARTS)
            .expectedBucketOwner(blobStore.expectedBucketOwner())
            .build();

        return AccessController.doPrivileged(() -> s3AsyncClient.getObjectAttributes(getObjectAttributesRequest));
    }

    @Override
    public void deleteAsync(ActionListener<DeleteResult> completionListener) {
        logger.debug("Starting async deletion for path [{}]", keyPath);
        try (AmazonAsyncS3Reference asyncClientReference = blobStore.asyncClientReference()) {
            S3AsyncClient s3AsyncClient = asyncClientReference.get().client();

            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(blobStore.bucket())
                .prefix(keyPath)
                .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().listObjectsMetricPublisher))
                .build();
            ListObjectsV2Publisher listPublisher = s3AsyncClient.listObjectsV2Paginator(listRequest);

            AtomicLong deletedBlobs = new AtomicLong();
            AtomicLong deletedBytes = new AtomicLong();

            CompletableFuture<Void> listingFuture = new CompletableFuture<>();

            listPublisher.subscribe(new Subscriber<>() {
                private Subscription subscription;
                private final List<String> objectsToDelete = new ArrayList<>();
                private CompletableFuture<Void> deletionChain = CompletableFuture.completedFuture(null);

                @Override
                public void onSubscribe(Subscription s) {
                    this.subscription = s;
                    logger.debug("Subscribed to list objects publisher for path [{}]", keyPath);
                    subscription.request(1);
                }

                @Override
                public void onNext(ListObjectsV2Response response) {
                    response.contents().forEach(s3Object -> {
                        deletedBlobs.incrementAndGet();
                        deletedBytes.addAndGet(s3Object.size());
                        objectsToDelete.add(s3Object.key());
                    });

                    logger.debug("Found {} objects to delete in current batch for path [{}]", response.contents().size(), keyPath);

                    int bulkDeleteSize = blobStore.getBulkDeletesSize();
                    if (objectsToDelete.size() >= bulkDeleteSize) {
                        int fullBatchesCount = objectsToDelete.size() / bulkDeleteSize;
                        int itemsToDelete = fullBatchesCount * bulkDeleteSize;

                        List<String> batchToDelete = new ArrayList<>(objectsToDelete.subList(0, itemsToDelete));
                        objectsToDelete.subList(0, itemsToDelete).clear();

                        logger.debug("Executing bulk delete of {} objects for path [{}]", batchToDelete.size(), keyPath);
                        deletionChain = S3AsyncDeleteHelper.executeDeleteChain(
                            s3AsyncClient,
                            blobStore,
                            batchToDelete,
                            deletionChain,
                            () -> subscription.request(1)
                        );
                    } else {
                        subscription.request(1);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    logger.error(() -> new ParameterizedMessage("Failed to list objects for deletion in path [{}]", keyPath), t);
                    listingFuture.completeExceptionally(new IOException("Failed to list objects for deletion", t));
                }

                @Override
                public void onComplete() {
                    logger.debug(
                        "Completed listing objects for path [{}], remaining objects to delete: {}",
                        keyPath,
                        objectsToDelete.size()
                    );
                    if (!objectsToDelete.isEmpty()) {
                        logger.debug("Executing final bulk delete of {} objects for path [{}]", objectsToDelete.size(), keyPath);
                        deletionChain = S3AsyncDeleteHelper.executeDeleteChain(
                            s3AsyncClient,
                            blobStore,
                            objectsToDelete,
                            deletionChain,
                            null
                        );
                    }
                    deletionChain.whenComplete((v, throwable) -> {
                        if (throwable != null) {
                            logger.error(
                                () -> new ParameterizedMessage("Failed to complete deletion chain for path [{}]", keyPath),
                                throwable
                            );
                            listingFuture.completeExceptionally(throwable);
                        } else {
                            logger.debug("Successfully completed deletion chain for path [{}]", keyPath);
                            listingFuture.complete(null);
                        }
                    });
                }
            });

            listingFuture.whenComplete((v, throwable) -> {
                if (throwable != null) {
                    logger.error(() -> new ParameterizedMessage("Failed to complete async deletion for path [{}]", keyPath), throwable);
                    completionListener.onFailure(
                        throwable instanceof Exception e ? e : new IOException("Unexpected error during async deletion", throwable)
                    );
                } else {
                    logger.debug(
                        "Successfully completed async deletion for path [{}]. Deleted {} blobs totaling {} bytes",
                        keyPath,
                        deletedBlobs.get(),
                        deletedBytes.get()
                    );
                    completionListener.onResponse(new DeleteResult(deletedBlobs.get(), deletedBytes.get()));
                }
            });
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Failed to initiate async deletion for path [{}]", keyPath), e);
            completionListener.onFailure(new IOException("Failed to initiate async deletion", e));
        }
    }

    @Override
    public void deleteBlobsAsyncIgnoringIfNotExists(List<String> blobNames, ActionListener<Void> completionListener) {
        if (blobNames.isEmpty()) {
            completionListener.onResponse(null);
            return;
        }

        try (AmazonAsyncS3Reference asyncClientReference = blobStore.asyncClientReference()) {
            S3AsyncClient s3AsyncClient = asyncClientReference.get().client();

            List<String> keysToDelete = blobNames.stream().map(this::buildKey).collect(Collectors.toList());

            S3AsyncDeleteHelper.executeDeleteChain(s3AsyncClient, blobStore, keysToDelete, CompletableFuture.completedFuture(null), null)
                .whenComplete((v, throwable) -> {
                    if (throwable != null) {
                        completionListener.onFailure(new IOException("Failed to delete blobs " + blobNames, throwable));
                    } else {
                        completionListener.onResponse(null);
                    }
                });
        } catch (Exception e) {
            completionListener.onFailure(new IOException("Failed to initiate async blob deletion", e));
        }
    }

    /**
     * Reads the register at {@code blobName}: an ordinary object whose body is {@code <8-byte
     * big-endian generation><value bytes>}, per the same wire format {@code FsBlobContainer} uses.
     */
    @Override
    public Optional<BlobRegister> readRegister(String blobName) throws IOException {
        String key = buildKey(blobName);
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(blobStore.bucket())
                .key(key)
                .expectedBucketOwner(blobStore.expectedBucketOwner())
                .build();
            try (
                ResponseInputStream<GetObjectResponse> response = AccessController.doPrivileged(
                    () -> clientReference.get().getObject(getObjectRequest)
                )
            ) {
                return Optional.of(deserializeRegister(response.readAllBytes()));
            }
        } catch (NoSuchKeyException e) {
            return Optional.empty();
        } catch (S3Exception e) {
            // Real S3 responds to a missing key with a NoSuchKeyException-typed 404, but not every
            // S3-compatible endpoint (including the fixture this is verified against) returns the
            // error body needed for the SDK to unmarshal that specific type, so a bare 404 must be
            // treated as absence too rather than surfacing as a generic failure.
            if (e.statusCode() == 404) {
                return Optional.empty();
            }
            throw new IOException("Unable to read register [" + blobName + "]", e);
        } catch (SdkException e) {
            throw new IOException("Unable to read register [" + blobName + "]", e);
        }
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
    @Override
    public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
        throws IOException {
        String key = buildKey(blobName);
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            S3Client client = clientReference.get();

            String currentETag;
            long currentGeneration;
            try {
                GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(blobStore.bucket())
                    .key(key)
                    .expectedBucketOwner(blobStore.expectedBucketOwner())
                    .build();
                try (
                    ResponseInputStream<GetObjectResponse> response = AccessController.doPrivileged(
                        () -> client.getObject(getObjectRequest)
                    )
                ) {
                    currentETag = response.response().eTag();
                    currentGeneration = deserializeRegister(response.readAllBytes()).generation();
                }
            } catch (NoSuchKeyException e) {
                currentETag = null;
                currentGeneration = BlobRegister.ABSENT_GENERATION;
            } catch (S3Exception e) {
                // See readRegister: a bare 404 (not a properly-typed NoSuchKeyException) also means absent.
                if (e.statusCode() != 404) {
                    throw e;
                }
                currentETag = null;
                currentGeneration = BlobRegister.ABSENT_GENERATION;
            }

            if (currentGeneration != expectedGeneration) {
                return BlobRegisterCasResult.conflict(currentGeneration);
            }

            long newGeneration = expectedGeneration + 1;
            byte[] bytesToWrite = serializeRegister(newGeneration, newValue);
            PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
                .bucket(blobStore.bucket())
                .key(key)
                .contentLength((long) bytesToWrite.length)
                .expectedBucketOwner(blobStore.expectedBucketOwner());
            if (currentETag == null) {
                putObjectRequestBuilder.ifNoneMatch("*");
            } else {
                putObjectRequestBuilder.ifMatch(currentETag);
            }
            PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();

            try {
                AccessController.doPrivileged(() -> client.putObject(putObjectRequest, RequestBody.fromBytes(bytesToWrite)));
                return BlobRegisterCasResult.applied(newGeneration);
            } catch (S3Exception e) {
                if (e.statusCode() == 412) {
                    // Someone raced us between our read and this put; report a conflict rather
                    // than a wrapped exception so the caller re-reads and retries as normal.
                    return BlobRegisterCasResult.conflict(
                        readRegister(blobName).map(BlobRegister::generation).orElse(BlobRegister.ABSENT_GENERATION)
                    );
                }
                throw e;
            }
        } catch (SdkException e) {
            throw new IOException("Unable to CAS register [" + blobName + "]", e);
        }
    }

    /**
     * One conditional PUT, where {@link #compareAndSwapRegister} needs a GET first.
     *
     * <p>That GET is not wasted work in the general case: a CAS against an arbitrary generation has to
     * learn the current one. It is wasted here. The only thing the read tells the create path is that
     * {@code currentETag} is null, which selects {@code ifNoneMatch("*")}, and that header is itself the
     * atomic "must not exist" check evaluated by S3 against the live object. The class comment on
     * {@code compareAndSwapRegister} already says the conditional write is the authoritative guard and
     * the read is "purely as a fast-fail", so dropping it costs no safety.
     *
     * <p>Conflict reports {@link BlobRegister#ABSENT_GENERATION} rather than reading to find the real
     * generation, because a caller that lost a create race wants to know it lost, and paying a round trip
     * to decorate that would reintroduce the cost this exists to remove. A caller that needs the winning
     * value reads it itself, on a path that by definition is not the common one.
     */
    @Override
    public BlobRegisterCasResult createRegisterIfAbsent(String blobName, BytesReference value) throws IOException {
        String key = buildKey(blobName);
        long newGeneration = BlobRegister.ABSENT_GENERATION + 1;
        byte[] bytesToWrite = serializeRegister(newGeneration, value);

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
            .bucket(blobStore.bucket())
            .key(key)
            .contentLength((long) bytesToWrite.length)
            .expectedBucketOwner(blobStore.expectedBucketOwner())
            .ifNoneMatch("*")
            .build();

        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            S3Client client = clientReference.get();
            try {
                AccessController.doPrivileged(() -> client.putObject(putObjectRequest, RequestBody.fromBytes(bytesToWrite)));
                return BlobRegisterCasResult.applied(newGeneration);
            } catch (S3Exception e) {
                if (e.statusCode() == 412) {
                    return BlobRegisterCasResult.conflict(BlobRegister.ABSENT_GENERATION);
                }
                throw e;
            }
        } catch (SdkException e) {
            throw new IOException("Unable to create register [" + blobName + "]", e);
        }
    }

    private static BlobRegister deserializeRegister(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long generation = buffer.getLong();
        byte[] value = new byte[buffer.remaining()];
        buffer.get(value);
        return new BlobRegister(generation, new BytesArray(value));
    }

    private static byte[] serializeRegister(long generation, BytesReference value) {
        byte[] valueBytes = BytesReference.toBytes(value);
        ByteBuffer buffer = ByteBuffer.allocate(8 + valueBytes.length);
        buffer.putLong(generation);
        buffer.put(valueBytes);
        return buffer.array();
    }
}
