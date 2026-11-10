/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.azure;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

/**
 * Builds an {@link AzureBlobStore} pointed at Azurite, the Azure Storage emulator.
 *
 * <p>The store is the production {@code AzureBlobStore}, so the conformance suite exercises the same
 * {@code If-Match} / {@code If-None-Match} register code a node runs. The client reaches the emulator through
 * the same {@code endpoint_suffix} connection-string override the plugin's own HTTP fixture tests use.
 */
public final class AzuriteBlobStores {

    /** Azurite's fixed development account. */
    public static final String ACCOUNT = "devstoreaccount1";

    /** Azurite's fixed, publicly documented development key. Not a secret. */
    public static final String KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private static final String CLIENT = "azurite";

    private AzuriteBlobStores() {}

    /**
     * The executor the Azure plugin registers, which a thread pool for {@link #create} must carry.
     *
     * @return the plugin's executor builder
     */
    public static ExecutorBuilder<?> executorBuilder() {
        return AzureRepositoryPlugin.executorBuilder();
    }

    private static AzureStorageService service(String endpoint) {
        final MockSecureSettings secure = new MockSecureSettings();
        secure.setString(AzureStorageSettings.ACCOUNT_SETTING.getConcreteSettingForNamespace(CLIENT).getKey(), ACCOUNT);
        secure.setString(AzureStorageSettings.KEY_SETTING.getConcreteSettingForNamespace(CLIENT).getKey(), KEY);
        // The plugin builds "...;EndpointSuffix=<suffix>"; a suffix that carries its own BlobEndpoint overrides
        // the account-derived host, which is how a non-Azure endpoint is reached.
        final String suffix = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + endpoint + "/" + ACCOUNT;
        final Settings settings = Settings.builder()
            .put(AzureStorageSettings.ENDPOINT_SUFFIX_SETTING.getConcreteSettingForNamespace(CLIENT).getKey(), suffix)
            .setSecureSettings(secure)
            .build();
        return new AzureStorageService(settings);
    }

    /**
     * Creates the container if it does not exist and returns a blob store over it.
     *
     * @param endpoint the Azurite blob endpoint, e.g. {@code http://127.0.0.1:10000}
     * @param container the container to use
     * @param threadPool a thread pool carrying {@link #executorBuilder()}
     * @return a blob store, ready to use
     */
    public static AzureBlobStore create(String endpoint, String container, ThreadPool threadPool) {
        final AzureStorageService service = service(endpoint);
        service.client(CLIENT).v1().getBlobContainerClient(container).createIfNotExists();
        final RepositoryMetadata metadata = new RepositoryMetadata(
            "serverless-azure-" + container,
            AzureRepository.TYPE,
            Settings.builder()
                .put(AzureRepository.Repository.CONTAINER_SETTING.getKey(), container)
                .put(AzureRepository.Repository.CLIENT_NAME.getKey(), CLIENT)
                .build()
        );
        return new AzureBlobStore(metadata, service, threadPool);
    }

    /**
     * Removes a container and everything in it. Failures are swallowed so that housekeeping cannot fail a test
     * that passed.
     *
     * @param endpoint the Azurite blob endpoint
     * @param container the container to remove
     */
    public static void deleteContainer(String endpoint, String container) {
        AzureStorageService service = null;
        try {
            service = service(endpoint);
            service.client(CLIENT).v1().getBlobContainerClient(container).deleteIfExists();
        } catch (Exception e) {
            // See above.
        } finally {
            if (service != null) {
                try {
                    service.close();
                } catch (Exception ignored) {
                    // Nothing left to do about it.
                }
            }
        }
    }
}
