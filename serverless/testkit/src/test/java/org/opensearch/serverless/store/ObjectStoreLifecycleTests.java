/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.store;

import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class ObjectStoreLifecycleTests extends OpenSearchTestCase {
    public void testTheClientPoolStopsEvenWhenStoreCloseFails() throws Exception {
        final BlobStore store = mock(BlobStore.class);
        doThrow(new IOException("store close failed")).when(store).close();
        final ThreadPool pool = new TestThreadPool("store-close-failure");
        try {
            final var handle = new ObjectStores.Handle(store, pool);
            assertEquals("store close failed", expectThrows(IOException.class, handle::close).getMessage());
            assertTrue("closing the store must terminate its pool even after a failure", pool.awaitTermination(1, TimeUnit.MILLISECONDS));
        } finally {
            ThreadPool.terminate(pool, 10, TimeUnit.SECONDS);
        }
    }

    public void testAllOwnersCloseOnceEvenWhenOneFails() throws Exception {
        final BlobStore store = mock(BlobStore.class);
        final var closed = new java.util.ArrayList<String>();
        final java.io.Closeable repository = () -> {
            closed.add("repository");
            throw new IOException("repository close failed");
        };
        final java.io.Closeable plugin = () -> {
            closed.add("plugin");
            throw new IOException("plugin close failed");
        };
        final var handle = new ObjectStores.Handle(store, null, repository, plugin);
        final var failure = expectThrows(IOException.class, handle::close);
        assertEquals("repository close failed", failure.getMessage());
        assertEquals("plugin close failed", failure.getSuppressed()[0].getMessage());
        assertEquals(java.util.List.of("repository", "plugin"), closed);
        handle.close();
        assertEquals(java.util.List.of("repository", "plugin"), closed);
        org.mockito.Mockito.verify(store, org.mockito.Mockito.never()).close();
    }

    public void testS3ClientResourcesCanBeOpenedAndClosedWithoutABucketRequest() throws Exception {
        final var secure = new org.opensearch.common.settings.MockSecureSettings();
        secure.setString("s3.client.default.access_key", "test-access");
        secure.setString("s3.client.default.secret_key", "test-secret");
        final var home = createTempDir();
        final var settings = org.opensearch.common.settings.Settings.builder()
            .put("node.name", "store-client-lifecycle")
            .put("path.home", home)
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(ObjectStores.TYPE, "s3")
            .put(ObjectStores.BUCKET, "lifecycle-test")
            .setSecureSettings(secure)
            .build();
        try (var node = new org.opensearch.serverless.shell.ServerlessNode(settings)) {
            node.start();
            try (var handle = ObjectStores.create(settings, node.clusterService(), home.resolve("config"))) {
                assertTrue(handle.blobStore() instanceof ObjectStores.Metered);
            }
        }
    }

}
