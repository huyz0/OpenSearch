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

/** The write-ahead log crash points on Azurite, through the production Azure blob store. */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class AzuriteWalCrashPointTests extends WalCrashPointTestCase {

    private final List<String> created = new ArrayList<>();
    private final List<BlobStore> stores = new ArrayList<>();
    private ThreadPool threadPool;

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool(getTestName(), AzuriteBlobStores.executorBuilder());
    }

    @Override
    protected BlobStore newStore() throws Exception {
        final String endpoint = AzuriteBlobContainerConformanceTests.azureEndpoint();
        assumeTrue(
            "no azure emulator at " + endpoint + "; start one with: " + AzuriteBlobContainerConformanceTests.azureHowToStart(),
            EmulatorConformanceTestCase.reachable(endpoint + "/" + AzuriteBlobStores.ACCOUNT + "?comp=list")
        );
        final String container = "crash-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        created.add(container);
        final BlobStore store = AzuriteBlobStores.create(endpoint, container, threadPool);
        stores.add(store);
        return store;
    }

    @After
    public void removeContainersThisTestMade() throws Exception {
        org.opensearch.common.util.io.IOUtils.close(stores);
        stores.clear();
        for (String container : created) {
            AzuriteBlobStores.deleteContainer(AzuriteBlobContainerConformanceTests.azureEndpoint(), container);
        }
        created.clear();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }
}
