/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.repositories.s3.async.AsyncExecutorContainer;
import org.opensearch.repositories.s3.async.AsyncTransferEventLoopGroup;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Builds an {@link S3BlobStore} pointed at an S3-compatible endpoint.
 *
 * <p><b>This class lives in {@code org.opensearch.repositories.s3} on purpose.</b> {@code S3BlobStore}'s
 * constructor and {@code S3Service}'s are package-private, and that was recorded as the blocker for
 * M12/R11 — "blocked on a 17-argument package-private constructor rather than on credentials". It was
 * never really a blocker: a test helper in the same package reaches them, and the alternative (writing a
 * second S3 client just for tests) would have measured the wrong thing. What R11 needs to exercise is
 * the container the shell would actually run on, not a lookalike.
 *
 * <p>The async client is built as well, and it has to be: {@code deleteBlobsIgnoringIfNotExists} looks
 * synchronous and is not — it delegates to the async delete chain, so a store with a null
 * {@code S3AsyncService} passes every register test and then fails the moment anything is deleted.
 * Deletion is not incidental here; it is how tombstones, garbage collection and lease release work.
 *
 * <p>Multipart upload stays off. The transfer manager it needs is a much larger construction and nothing
 * R11 asserts goes near it, so it is left null deliberately — a test that strayed into that path would
 * get a NullPointerException rather than a wrong answer, which is the failure mode to prefer.
 */
public final class MinioBlobStores {

    private MinioBlobStores() {}

    /**
     * Creates the bucket if it does not exist and returns a blob store over it.
     *
     * @param endpoint the S3-compatible endpoint, e.g. {@code http://127.0.0.1:9000}
     * @param accessKey the access key
     * @param secretKey the secret key
     * @param bucket the bucket to use
     * @param configPath a directory standing in for the node's config path
     * @return a blob store, ready to use
     */
    /**
     * Removes a bucket the suite created, and everything left in it.
     *
     * <p><b>Why this exists.</b> The suite makes a bucket per container so that one run's leftovers cannot
     * decide another run's listing assertions. Nothing removed them. On MinIO an abandoned empty bucket costs
     * nothing, which is why it went unnoticed; on SeaweedFS a bucket is a <em>collection</em>, every
     * collection claims volumes, and the volume server has a finite number of them. Enough runs and it stops
     * being able to allocate, at which point every conformance test fails against a store that is not
     * misbehaving — which is the worst kind of failure this suite can produce, because the suite exists to
     * tell a real deviation from a fake one.
     *
     * <p>Failures here are swallowed deliberately: cleanup that fails a passing test would trade a real
     * signal for a housekeeping one.
     *
     * @param endpoint the S3 endpoint
     * @param accessKey the access key
     * @param secretKey the secret key
     * @param bucket the bucket to remove
     * @param configPath a scratch path for client settings
     */
    public static void deleteBucket(String endpoint, String accessKey, String secretKey, String bucket, Path configPath) {
        final MockSecureSettings secure = new MockSecureSettings();
        secure.setString("s3.client.default.access_key", accessKey);
        secure.setString("s3.client.default.secret_key", secretKey);
        final Settings settings = Settings.builder()
            .put("s3.client.default.endpoint", endpoint)
            .put("s3.client.default.protocol", "http")
            .put("s3.client.default.path_style_access", true)
            .put("s3.client.default.region", "us-east-1")
            .setSecureSettings(secure)
            .build();

        final S3Service service = new S3Service(configPath);
        service.refreshAndClearCache(S3ClientSettings.load(settings, configPath));
        final RepositoryMetadata metadata = new RepositoryMetadata("serverless-r11-cleanup", "s3", settings);
        try (AmazonS3Reference reference = service.client(metadata)) {
            String token = null;
            do {
                final var listing = reference.get()
                    .listObjectsV2(
                        software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder()
                            .bucket(bucket)
                            .continuationToken(token)
                            .build()
                    );
                for (var object : listing.contents()) {
                    reference.get()
                        .deleteObject(
                            software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder().bucket(bucket).key(object.key()).build()
                        );
                }
                token = Boolean.TRUE.equals(listing.isTruncated()) ? listing.nextContinuationToken() : null;
            } while (token != null);
            reference.get().deleteBucket(software.amazon.awssdk.services.s3.model.DeleteBucketRequest.builder().bucket(bucket).build());
        } catch (Exception e) {
            // See above: a cleanup failure must not fail a test that passed.
        } finally {
            try {
                service.close();
            } catch (Exception ignored) {
                // Nothing left to do about it.
            }
        }
    }

    public static S3BlobStore create(String endpoint, String accessKey, String secretKey, String bucket, Path configPath) {
        final MockSecureSettings secure = new MockSecureSettings();
        secure.setString("s3.client.default.access_key", accessKey);
        secure.setString("s3.client.default.secret_key", secretKey);
        final Settings settings = Settings.builder()
            .put("s3.client.default.endpoint", endpoint)
            // MinIO is reached by path, not by a bucket-shaped hostname. Without this the SDK addresses
            // http://bucket.127.0.0.1:9000 and nothing resolves.
            .put("s3.client.default.path_style_access", true)
            .put("s3.client.default.region", "us-east-1")
            .setSecureSettings(secure)
            .build();

        final S3Service service = new S3Service(configPath);
        service.refreshAndClearCache(S3ClientSettings.load(settings, configPath));

        final S3AsyncService asyncService = new S3AsyncService(configPath);
        asyncService.refreshAndClearCache(S3ClientSettings.load(settings, configPath));
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final AsyncExecutorContainer executors = new AsyncExecutorContainer(executor, executor, new AsyncTransferEventLoopGroup(1));

        final RepositoryMetadata metadata = new RepositoryMetadata("serverless-r11", "s3", settings);
        try (AmazonS3Reference reference = service.client(metadata)) {
            try {
                reference.get().createBucket(CreateBucketRequest.builder().bucket(bucket).build());
            } catch (BucketAlreadyOwnedByYouException e) {
                // Re-running the suite against a live endpoint is normal, not an error.
            }
        }

        return new S3BlobStore(
            service,
            asyncService,
            false,
            bucket,
            new ByteSizeValue(5, ByteSizeUnit.MB),
            "private",
            "STANDARD",
            1000,
            metadata,
            null,              // no transfer manager: multipart upload is off
            executors,         // urgent
            executors,         // priority
            executors,         // normal
            null,
            null,
            null,
            // Empty rather than null. S3BlobStore compares these with String.equals on every read and
            // every listing, so a null is an NPE on the first ranged read -- which is how this was found.
            "",
            "",
            false,
            "",
            null
        );
    }
}
