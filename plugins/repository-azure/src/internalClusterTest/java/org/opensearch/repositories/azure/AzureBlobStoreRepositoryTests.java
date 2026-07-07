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

package org.opensearch.repositories.azure;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.common.implementation.Constants;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.blobstore.OpenSearchMockAPIBasedRepositoryIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.AfterClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import fixture.azure.AzureHttpHandler;
import reactor.core.scheduler.Schedulers;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class AzureBlobStoreRepositoryTests extends OpenSearchMockAPIBasedRepositoryIntegTestCase {
    @AfterClass
    public static void shutdownSchedulers() {
        Schedulers.shutdownNow();
    }

    @Override
    protected String repositoryType() {
        return AzureRepository.TYPE;
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder()
            .put(super.repositorySettings())
            .put(AzureRepository.Repository.CONTAINER_SETTING.getKey(), "container")
            .put(AzureStorageSettings.ACCOUNT_SETTING.getKey(), "test")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestAzureRepositoryPlugin.class);
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Collections.singletonMap("/container", new AzureHTTPStatsCollectorHandler(new AzureBlobStoreHttpHandler("container")));
    }

    @Override
    protected HttpHandler createErroneousHttpHandler(final HttpHandler delegate) {
        return new AzureErroneousHttpHandler(delegate, randomDoubleBetween(0, 0.25, false));
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final String key = Base64.getEncoder().encodeToString(randomAlphaOfLength(10).getBytes(StandardCharsets.UTF_8));
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(AzureStorageSettings.ACCOUNT_SETTING.getConcreteSettingForNamespace("test").getKey(), "account");
        secureSettings.setString(AzureStorageSettings.KEY_SETTING.getConcreteSettingForNamespace("test").getKey(), key);

        final String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + httpServerUrl() + "/";
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(AzureStorageSettings.ENDPOINT_SUFFIX_SETTING.getConcreteSettingForNamespace("test").getKey(), endpoint)
            .setSecureSettings(secureSettings)
            .build();
    }

    /**
     * Exercises {@link AzureBlobContainer#compareAndSwapRegister}/{@code readRegister} against the
     * real {@link AzureHttpHandler} fixture (extended to enforce real Azure If-Match/If-None-Match
     * conditional-write semantics for exactly this purpose): proof that the conditional-write guard
     * is actually enforced server-side, not merely that our request-construction code compiles.
     */
    public void testCompareAndSwapRegisterUsesRealConditionalWrites() throws Exception {
        final String repoName = createRepository(randomName());
        final BlobContainer container = getBlobContainer(repoName);
        final String registerName = "test-register";
        try {
            BlobRegisterCasResult first = container.compareAndSwapRegister(
                registerName,
                BlobRegister.ABSENT_GENERATION,
                new BytesArray("v1".getBytes(StandardCharsets.UTF_8))
            );
            assertTrue(first.applied());
            assertEquals(1L, first.currentGeneration());

            BlobRegisterCasResult racerConflict = container.compareAndSwapRegister(
                registerName,
                BlobRegister.ABSENT_GENERATION,
                new BytesArray("racer".getBytes(StandardCharsets.UTF_8))
            );
            assertFalse("a second put-if-absent must lose to the one that already succeeded", racerConflict.applied());
            assertEquals(1L, racerConflict.currentGeneration());

            Optional<BlobRegister> read = container.readRegister(registerName);
            assertTrue(read.isPresent());
            assertEquals(1L, read.get().generation());
            assertEquals("v1", read.get().value().utf8ToString());

            BlobRegisterCasResult staleUpdate = container.compareAndSwapRegister(
                registerName,
                0L,
                new BytesArray("stale".getBytes(StandardCharsets.UTF_8))
            );
            assertFalse(staleUpdate.applied());
            assertEquals(1L, staleUpdate.currentGeneration());

            BlobRegisterCasResult goodUpdate = container.compareAndSwapRegister(
                registerName,
                1L,
                new BytesArray("v2".getBytes(StandardCharsets.UTF_8))
            );
            assertTrue(goodUpdate.applied());
            assertEquals(2L, goodUpdate.currentGeneration());
            assertEquals("v2", container.readRegister(registerName).get().value().utf8ToString());
        } finally {
            container.deleteBlobsIgnoringIfNotExists(Collections.singletonList(registerName));
        }
    }

    /**
     * Concurrent CAS-retry-loop updates against the same register must never lose an update: only
     * meaningful against a fixture that actually enforces conditional writes server-side.
     */
    public void testConcurrentCompareAndSwapRegisterRetryLoopLosesNoUpdates() throws Exception {
        final String repoName = createRepository(randomName());
        final BlobContainer container = getBlobContainer(repoName);
        final String registerName = "counter-register";

        int incrementerCount = 8;
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(incrementerCount);
        java.util.concurrent.CountDownLatch startLatch = new java.util.concurrent.CountDownLatch(1);
        try {
            java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < incrementerCount; i++) {
                futures.add(executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int attempt = 0; attempt < 50; attempt++) {
                            Optional<BlobRegister> current = container.readRegister(registerName);
                            long currentGeneration = current.map(BlobRegister::generation).orElse(BlobRegister.ABSENT_GENERATION);
                            int currentValue = current.map(r -> Integer.parseInt(r.value().utf8ToString())).orElse(0);
                            BlobRegisterCasResult result = container.compareAndSwapRegister(
                                registerName,
                                currentGeneration,
                                new BytesArray(Integer.toString(currentValue + 1).getBytes(StandardCharsets.UTF_8))
                            );
                            if (result.applied()) {
                                return;
                            }
                        }
                        throw new AssertionError("failed to apply an increment after 50 attempts");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            startLatch.countDown();
            for (java.util.concurrent.Future<?> future : futures) {
                future.get(60, java.util.concurrent.TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }

        try {
            int finalValue = Integer.parseInt(container.readRegister(registerName).get().value().utf8ToString());
            assertEquals(
                "every concurrent incrementer's update must be reflected, none lost to a missed conflict",
                incrementerCount,
                finalValue
            );
        } finally {
            container.deleteBlobsIgnoringIfNotExists(Collections.singletonList(registerName));
        }
    }

    private BlobContainer getBlobContainer(String repoName) {
        final org.opensearch.repositories.RepositoriesService repositoriesService = internalCluster().getClusterManagerNodeInstance(
            org.opensearch.repositories.RepositoriesService.class
        );
        final org.opensearch.repositories.blobstore.BlobStoreRepository repository =
            (org.opensearch.repositories.blobstore.BlobStoreRepository) repositoriesService.repository(repoName);
        return repository.blobStore().blobContainer(repository.basePath());
    }

    /**
     * AzureRepositoryPlugin that allows to set low values for the Azure's client retry policy
     * and for BlobRequestOptions#getSingleBlobPutThresholdInBytes().
     */
    public static class TestAzureRepositoryPlugin extends AzureRepositoryPlugin {

        public TestAzureRepositoryPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            // No-op in tests — avoids interference with Reactor/Netty event loop initialization
        }

        @Override
        AzureStorageService createAzureStoreService(final Settings settings) {
            return new AzureStorageService(settings) {
                @Override
                RequestRetryOptions createRetryPolicy(final AzureStorageSettings azureStorageSettings, String secondaryHost) {
                    return new RequestRetryOptions(
                        RetryPolicyType.EXPONENTIAL,
                        azureStorageSettings.getMaxRetries(),
                        1,
                        100L,
                        500L,
                        secondaryHost
                    );
                }

                @Override
                ParallelTransferOptions getBlobRequestOptionsForWriteBlob() {
                    return new ParallelTransferOptions().setMaxSingleUploadSizeLong(ByteSizeUnit.MB.toBytes(1));
                }
            };
        }
    }

    @SuppressForbidden(reason = "this test uses a HttpHandler to emulate an Azure endpoint")
    private static class AzureBlobStoreHttpHandler extends AzureHttpHandler implements BlobStoreHttpHandler {

        AzureBlobStoreHttpHandler(final String container) {
            super(container);
        }
    }

    /**
     * HTTP handler that injects random Azure service errors
     * <p>
     * Note: it is not a good idea to allow this handler to simulate too many errors as it would
     * slow down the test suite.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
    private static class AzureErroneousHttpHandler extends ErroneousHttpHandler {

        AzureErroneousHttpHandler(final HttpHandler delegate, final double maxErrorsPercentage) {
            super(delegate, maxErrorsPercentage);
        }

        @Override
        protected void handleAsError(final HttpExchange exchange) throws IOException {
            try {
                drainInputStream(exchange.getRequestBody());
                AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
            } finally {
                exchange.close();
            }
        }

        @Override
        protected String requestUniqueId(final HttpExchange exchange) {
            final String requestId = exchange.getRequestHeaders().getFirst(Constants.HeaderConstants.CLIENT_REQUEST_ID);
            return exchange.getRequestMethod() + " " + requestId;
        }
    }

    /**
     * HTTP handler that keeps track of requests performed against Azure Storage.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
    private static class AzureHTTPStatsCollectorHandler extends HttpStatsCollectorHandler {

        private static final Logger testLogger = LogManager.getLogger(AzureHTTPStatsCollectorHandler.class);
        private static final Pattern listPattern = Pattern.compile("GET /[a-zA-Z0-9]+\\??.+");
        private static final Pattern getPattern = Pattern.compile("GET /[^?/]+/[^?/]+\\??.*");

        private AzureHTTPStatsCollectorHandler(HttpHandler delegate) {
            super(delegate);
        }

        @Override
        protected void maybeTrack(String request, Headers headers) {
            testLogger.info(request, headers);
            if (getPattern.matcher(request).matches()) {
                trackRequest("GetBlob");
            } else if (Regex.simpleMatch("HEAD /*/*", request)) {
                trackRequest("GetBlobProperties");
            } else if (listPattern.matcher(request).matches()) {
                trackRequest("ListBlobs");
            } else if (isPutBlock(request)) {
                trackRequest("PutBlock");
            } else if (isPutBlockList(request)) {
                trackRequest("PutBlockList");
            } else if (Regex.simpleMatch("PUT /*/*", request)) {
                trackRequest("PutBlob");
            }
        }

        // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block
        private boolean isPutBlock(String request) {
            return Regex.simpleMatch("PUT /*/*?*comp=block*", request) && request.contains("blockid=");
        }

        // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list
        private boolean isPutBlockList(String request) {
            return Regex.simpleMatch("PUT /*/*?*comp=blocklist*", request);
        }
    }
}
