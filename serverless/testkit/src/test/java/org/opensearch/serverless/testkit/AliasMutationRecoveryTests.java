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
import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.IndexAlreadyExistsException;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class AliasMutationRecoveryTests extends OpenSearchTestCase {
    private static IndexDescriptor index(String name) {
        return new IndexDescriptor(name, "uuid-" + name, 1, null, null);
    }

    public void testUnpublishedCreationIsHiddenButReservesItsName() throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var clock = new AtomicLong(1000L);
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, 30_000L);
            metadata.createIndex(index("docs"));
            store.onNextRegisterSwapAfter("indices", "view", () -> { throw new AssertionError("crash"); });
            expectThrows(AssertionError.class, () -> metadata.createAlias(new AliasRecord("view", List.of("docs"))));
            assertTrue(metadata.resolve("view").absent());
            assertTrue(metadata.resolveForRouting("view").absent());
            assertTrue(metadata.descriptors().resolve("view").absent());
            expectThrows(IndexAlreadyExistsException.class, () -> metadata.createIndex(index("view")));
            clock.addAndGet(30_001L);
            assertTrue(metadata.resolve("view").absent());
            metadata.createAlias(new AliasRecord("view", List.of("docs")));
            assertEquals(List.of("view"), metadata.aliasesOf(metadata.describe("docs").orElseThrow()));
        }
    }

    public void testMissingMemberDoesNotPublishOrLeaveAHint() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            metadata.createIndex(index("docs"));
            expectThrows(IOException.class, () -> metadata.createAlias(new AliasRecord("view", List.of("docs", "missing"))));
            assertTrue(metadata.resolve("view").absent());
            assertTrue(metadata.describe("docs").orElseThrow().aliasedBy().isEmpty());
        }
    }

    public void testExpiredUpdateKeepsOldMembershipAndCleansPreparedHint() throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var clock = new AtomicLong(1000L);
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, 30_000L);
            metadata.createIndex(index("docs"));
            metadata.createIndex(index("other"));
            metadata.createAlias(new AliasRecord("view", List.of("docs")));
            final var before = metadata.aliasWithGeneration("view").orElseThrow();
            store.onNextRegisterSwapAfter("indices", "other", () -> {
                store.onNextRegisterSwap("indices", "view", () -> { throw new AssertionError("crash"); });
            });
            expectThrows(
                AssertionError.class,
                () -> metadata.updateAlias(before.alias(), new AliasRecord("view", List.of("docs", "other")), before.generation())
            );
            assertEquals(List.of("docs"), metadata.resolve("view").alias().indices());
            assertTrue(metadata.describe("other").orElseThrow().aliasedBy().contains("view"));
            clock.addAndGet(30_001L);
            assertFalse(metadata.resolve("view").alias().mutationPending());
            assertEquals(List.of("docs"), metadata.resolve("view").alias().indices());
            assertTrue(metadata.describe("other").orElseThrow().aliasedBy().isEmpty());
        }
    }

    public void testRecoveredWriterCannotPublishAfterItsDelayedHintCas() throws Exception {
        try (
            var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false));
            var worker = Executors.newSingleThreadExecutor()
        ) {
            final var clock = new AtomicLong(1000L);
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, 30_000L);
            metadata.createIndex(index("docs"));
            metadata.createIndex(index("other"));
            metadata.createAlias(new AliasRecord("view", List.of("docs")));
            final var before = metadata.aliasWithGeneration("view").orElseThrow();
            final var preparing = new CountDownLatch(1);
            final var resume = new CountDownLatch(1);
            store.onNextRegisterSwap("indices", "other", () -> {
                preparing.countDown();
                try {
                    assertTrue(resume.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });
            final var updating = worker.submit(
                () -> metadata.updateAlias(before.alias(), new AliasRecord("view", List.of("docs", "other")), before.generation())
            );
            try {
                assertTrue(preparing.await(10, TimeUnit.SECONDS));
                clock.addAndGet(30_001L);
                assertFalse(metadata.resolve("view").alias().mutationPending());
            } finally {
                resume.countDown();
            }
            assertTrue(expectThrows(ExecutionException.class, () -> updating.get(10, TimeUnit.SECONDS)).getCause() instanceof IOException);
            assertEquals(List.of("docs"), metadata.resolve("view").alias().indices());
            assertTrue(metadata.describe("other").orElseThrow().aliasedBy().isEmpty());
        }
    }

    public void testDeletingPreparedMemberCancelsItsUnpublishedAddition() throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            metadata.createIndex(index("docs"));
            metadata.createIndex(index("other"));
            metadata.createAlias(new AliasRecord("view", List.of("docs")));
            final var before = metadata.aliasWithGeneration("view").orElseThrow();
            store.onNextRegisterSwapAfter("indices", "other", () -> {
                store.onNextRegisterSwap("indices", "view", () -> { throw new AssertionError("crash"); });
            });
            expectThrows(
                AssertionError.class,
                () -> metadata.updateAlias(before.alias(), new AliasRecord("view", List.of("docs", "other")), before.generation())
            );
            assertTrue(metadata.deleteIndex("other", "uuid-other"));
            assertFalse(metadata.resolve("view").alias().mutationPending());
            assertEquals(List.of("docs"), metadata.resolve("view").alias().indices());
            assertEquals(List.of("view"), metadata.aliasesOf(metadata.describe("docs").orElseThrow()));
        }
    }

    public void testPreparedRecordRoundTripPreservesStreamFields() throws Exception {
        final var pending = new AliasRecord("stream", List.of("old"), true, 7L, "@timestamp").preparing(List.of("new"), 31_000L, false);
        final var read = AliasRecord.fromStream(pending.toBytes().streamInput());
        assertTrue(read.mutationPending());
        assertFalse(read.creationPending());
        assertEquals(31_000L, read.mutationUntil());
        assertEquals(List.of("new"), read.pendingAdditions());
        assertTrue(read.protectsHint("old"));
        assertTrue(read.protectsHint("new"));
        assertEquals(7L, read.published().generation());
        assertEquals("@timestamp", read.published().timestampField());
        assertFalse(read.published().mutationPending());
    }
}
