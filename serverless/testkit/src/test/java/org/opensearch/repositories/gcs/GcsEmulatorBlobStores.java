/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import org.opensearch.common.settings.Settings;

import java.io.IOException;

/**
 * Builds a {@link GoogleCloudStorageBlobStore} pointed at a GCS emulator such as fake-gcs-server.
 *
 * <p>Lives in the plugin's package because the blob store's constructor and the service's
 * {@code createStorageOptions} are package-private. Going through them, rather than writing a second GCS
 * client for tests, means the conformance suite exercises the same register code a node runs: the
 * {@code doesNotExist} and {@code generationMatch} preconditions and the {@code startOffset} listing.
 *
 * <p>The emulator takes no credentials, so the client is built with {@link NoCredentials} instead of looking
 * up Application Default Credentials, which would fail (and log) on a developer machine.
 */
public final class GcsEmulatorBlobStores {

    private static final String CLIENT = "default";

    private GcsEmulatorBlobStores() {}

    /** A storage service that talks to an emulator without credentials. */
    private static final class EmulatorStorageService extends GoogleCloudStorageService {
        EmulatorStorageService() {
            super(new GoogleApplicationDefaultCredentials() {
                @Override
                public GoogleCredentials get() {
                    return null;
                }
            });
        }

        @Override
        StorageOptions createStorageOptions(GoogleCloudStorageClientSettings clientSettings, HttpTransportOptions httpTransportOptions) {
            return super.createStorageOptions(clientSettings, httpTransportOptions).toBuilder()
                .setCredentials(NoCredentials.getInstance())
                .build();
        }
    }

    private static GoogleCloudStorageService service(String endpoint) {
        final Settings settings = Settings.builder()
            .put("gcs.client." + CLIENT + ".endpoint", endpoint)
            .put("gcs.client." + CLIENT + ".project_id", "serverless-test")
            .build();
        final GoogleCloudStorageService service = new EmulatorStorageService();
        service.refreshAndClearCache(GoogleCloudStorageClientSettings.load(settings));
        return service;
    }

    /**
     * Creates the bucket if it does not exist and returns a blob store over it.
     *
     * @param endpoint the emulator endpoint, e.g. {@code http://127.0.0.1:4443}
     * @param bucket the bucket to use
     * @return a blob store, ready to use
     * @throws IOException if the emulator cannot be reached
     */
    public static GoogleCloudStorageBlobStore create(String endpoint, String bucket) throws IOException {
        final GoogleCloudStorageService service = service(endpoint);
        final String repository = "serverless-gcs-" + bucket;
        final GoogleCloudStorageBlobStore store = new GoogleCloudStorageBlobStore(bucket, CLIENT, repository, service, 1024);
        final Storage client = service.client(CLIENT, repository + "-admin", new GoogleCloudStorageOperationsStats(bucket));
        try {
            client.create(BucketInfo.of(bucket));
        } catch (StorageException e) {
            if (e.getCode() != 409) {
                throw e;
            }
            // The bucket is already there: re-running against a live emulator is normal.
        }
        return store;
    }

    /**
     * Removes a bucket and everything left in it. Failures are swallowed so that housekeeping cannot fail a
     * test that passed.
     *
     * @param endpoint the emulator endpoint
     * @param bucket the bucket to remove
     */
    public static void deleteBucket(String endpoint, String bucket) {
        try {
            final GoogleCloudStorageService service = service(endpoint);
            final Storage client = service.client(CLIENT, "serverless-gcs-cleanup-" + bucket, new GoogleCloudStorageOperationsStats(bucket));
            for (Blob blob : client.list(bucket, Storage.BlobListOption.versions(true)).iterateAll()) {
                client.delete(blob.getBlobId());
            }
            client.delete(bucket);
        } catch (Exception e) {
            // See above: a cleanup failure must not fail a test that passed.
        }
    }
}
