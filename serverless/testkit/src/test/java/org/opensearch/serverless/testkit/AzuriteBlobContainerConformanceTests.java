/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.repositories.azure.AzuriteBlobStores;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * The conformance suite against Azurite, through the production {@code AzureBlobStore}.
 *
 * <p>What it covers: the register's {@code If-None-Match: *} create and {@code If-Match} ETag swap and delete,
 * the generation the store embeds in the blob, and the prefix walk that stands in for paged listing.
 *
 * <p>What it does not: Azurite's ETag rules are close to the service's but not identical, and it is a single
 * process. Agreement here shows the client maps Azure semantics correctly; it says nothing about the real
 * service under contention.
 */
// The Azure SDK's reactor threads outlive the test; they belong to the client.
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class AzuriteBlobContainerConformanceTests extends EmulatorConformanceTestCase {

    /** System property naming the Azurite blob endpoint. */
    public static final String ENDPOINT = "tests.serverless.azure.endpoint";

    private final List<String> created = new ArrayList<>();
    private ThreadPool threadPool;

    static String azureEndpoint() {
        return System.getProperty(ENDPOINT, "http://127.0.0.1:10000");
    }

    static String azureHowToStart() {
        return "docker run -d --name serverless-azurite -p 10000:10000 mcr.microsoft.com/azure-storage/azurite:3.35.0 "
            + "azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck --loose";
    }

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool(getTestName(), AzuriteBlobStores.executorBuilder());
    }

    @Override
    protected String endpoint() {
        return azureEndpoint();
    }

    @Override
    protected String probeUrl() {
        return azureEndpoint() + "/" + AzuriteBlobStores.ACCOUNT + "?comp=list";
    }

    @Override
    protected String howToStart() {
        return azureHowToStart();
    }

    @Override
    protected String provider() {
        return "azure";
    }

    @Override
    protected BlobStore newStore() throws Exception {
        final String container = "conformance-" + randomAlphaOfLength(12).toLowerCase(Locale.ROOT);
        created.add(container);
        return AzuriteBlobStores.create(azureEndpoint(), container, threadPool);
    }

    /** Removes the containers this test made, then stops the thread pool. */
    @After
    public void removeContainersThisTestMade() {
        for (String container : created) {
            AzuriteBlobStores.deleteContainer(azureEndpoint(), container);
        }
        created.clear();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }
}
