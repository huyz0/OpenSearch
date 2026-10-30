/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** An exit must not erase another request's active shard admission. */
public class ShardAdmissionTests extends OpenSearchTestCase {
    public void testAClosedShardCannotAdmitAnotherQuery() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30000L);
            plane.createIndex(new IndexDescriptor("index", "uuid-index-closed-admission", 1, "{}", null));
            final var settings = Settings.builder()
                .put("node.name", "closed-admission")
                .put("cluster.name", "shard-admission")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings)) {
                node.start();
                node.setMetadataPlane(plane);
                final var id = node.activateWriter(plane, "index", 0).orElseThrow();
                node.releaseShard(id, "closed before query admission");
                expectThrows(org.opensearch.index.shard.IndexShardClosedException.class, () -> node.reconciler().enter(id));
                assertEquals(0, node.reconciler().inFlight(id));
            }
        }
    }

    public void testConcurrentExitsKeepEveryHeldAdmission() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30000L);
            plane.createIndex(new IndexDescriptor("index", "uuid-index-admission", 1, "{}", null));
            final var settings = Settings.builder()
                .put("node.name", "shard-admission")
                .put("cluster.name", "shard-admission")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings)) {
                node.start();
                node.setMetadataPlane(plane);
                final var id = node.activateWriter(plane, "index", 0).orElseThrow();
                final var reconciler = node.reconciler();
                final int workers = 8;
                final var executor = Executors.newFixedThreadPool(workers);
                try {
                    for (int round = 0; round < 20; round++) {
                        final var start = new CountDownLatch(1);
                        final var held = new CountDownLatch(workers);
                        final var release = new CountDownLatch(1);
                        final var completed = new ArrayList<java.util.concurrent.Future<?>>();
                        for (int i = 0; i < workers; i++) {
                            completed.add(executor.submit(() -> {
                                start.await();
                                for (int attempt = 0; attempt < 10_000; attempt++) {
                                    reconciler.enter(id);
                                    reconciler.exit(id);
                                }
                                reconciler.enter(id);
                                try {
                                    held.countDown();
                                    assertTrue(release.await(30, TimeUnit.SECONDS));
                                } finally {
                                    reconciler.exit(id);
                                }
                                return null;
                            }));
                        }
                        start.countDown();
                        try {
                            assertTrue(held.await(30, TimeUnit.SECONDS));
                            assertEquals(
                                "every admitted request still holding the shard must be counted",
                                workers,
                                reconciler.inFlight(id)
                            );
                        } finally {
                            release.countDown();
                        }
                        for (var future : completed) {
                            future.get(30, TimeUnit.SECONDS);
                        }
                        assertEquals(0, reconciler.inFlight(id));
                    }
                } finally {
                    executor.shutdownNow();
                    assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
                }
            }
        }
    }
}
