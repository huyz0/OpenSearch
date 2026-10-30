/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AliasReverseHintRaceTests extends OpenSearchTestCase {
    protected BlobStore newStore() throws Exception {
        return new FsBlobStore(1024, createTempDir(), false);
    }

    public void testCompletedReaddKeepsItsReverseHint() throws Exception {
        removalCannotStripReadd(false);
    }

    public void testReaddPublicationCrashStillKeepsItsReverseHint() throws Exception {
        removalCannotStripReadd(true);
    }

    private void removalCannotStripReadd(boolean failAfterPublication) throws Exception {
        try (var store = new HookedBlobStore(newStore()); var worker = Executors.newSingleThreadExecutor()) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            metadata.createIndex(new IndexDescriptor("docs", "uuid-docs", 1, null, null));
            metadata.createIndex(new IndexDescriptor("other", "uuid-other", 1, null, null));
            metadata.createAlias(new AliasRecord("view", List.of("docs", "other")));
            final var before = metadata.aliasWithGeneration("view").orElseThrow();
            final var removing = new CountDownLatch(1);
            final var resume = new CountDownLatch(1);
            store.onNextRegisterSwap("indices", "docs", () -> {
                removing.countDown();
                try {
                    assertTrue("removal was never resumed", resume.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });
            final var removed = worker.submit(
                () -> metadata.updateAlias(before.alias(), new AliasRecord("view", List.of("other")), before.generation())
            );
            try {
                assertTrue("removal never reached its descriptor CAS", removing.await(10, TimeUnit.SECONDS));
                final var current = metadata.aliasWithGeneration("view").orElseThrow();
                final var restored = new AliasRecord("view", List.of("docs", "other"));
                if (failAfterPublication) {
                    final Runnable[] afterSwap = new Runnable[1];
                    afterSwap[0] = () -> {
                        try {
                            final var published = metadata.resolve("view").alias();
                            if (published.mutationPending()) {
                                store.onNextRegisterSwapAfter("indices", "view", afterSwap[0]);
                            } else {
                                throw new IllegalStateException("stopped after publication");
                            }
                        } catch (java.io.IOException e) {
                            throw new AssertionError(e);
                        }
                    };
                    store.onNextRegisterSwapAfter("indices", "view", afterSwap[0]);
                    assertEquals(
                        "stopped after publication",
                        expectThrows(
                            IllegalStateException.class,
                            () -> metadata.updateAlias(current.alias(), restored, current.generation())
                        ).getMessage()
                    );
                } else {
                    assertTrue(metadata.updateAlias(current.alias(), restored, current.generation()).isPresent());
                }
            } finally {
                resume.countDown();
            }
            assertTrue(removed.get(10, TimeUnit.SECONDS).isPresent());
            assertEquals(List.of("view"), metadata.aliasesOf(metadata.describe("docs").orElseThrow()));
        }
    }
}
