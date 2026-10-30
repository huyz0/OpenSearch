/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RepositoryDescriptor;
import org.opensearch.serverless.metadata.SnapshotRecord;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/** A delayed snapshot operation must not revive a deleted snapshot or destroy its replacement. */
public class SnapshotLifecycleTests extends OpenSearchTestCase {
    private MetadataPlane plane(BlobStore store) throws Exception {
        final var plane = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1_000L, 30_000L);
        plane.putRepository(new RepositoryDescriptor("repo", "fs", Map.of(), 1_000L));
        plane.createIndex(new IndexDescriptor("index", "uuid-index", 1, "{}", null));
        return plane;
    }

    private static SnapshotRecord snapshot(String uuid, boolean capturing) {
        return new SnapshotRecord(
            "repo",
            "view",
            uuid,
            true,
            1_000L,
            capturing ? 1_000L : 2_000L,
            Map.of(
                "index",
                new SnapshotRecord.SnapshottedIndex(
                    "uuid-index",
                    1,
                    null,
                    capturing ? Map.of() : Map.of(0, new CommitManifest(1L, Map.of("file", "t=1")))
                )
            )
        );
    }

    public void testDeletedCaptureCannotOverwriteAReplacement() throws Exception {
        try (var store = new RacingStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = plane(store);
            final String first = org.opensearch.common.UUIDs.randomBase64UUID();
            final String replacement = org.opensearch.common.UUIDs.randomBase64UUID();
            final var reserved = plane.createSnapshot(snapshot(first, true));
            assertTrue(plane.deleteSnapshot("repo", "view"));
            plane.createSnapshot(snapshot(replacement, true));
            expectThrows(
                IOException.class,
                () -> plane.finishSnapshot(snapshot(first, false).withStorageGeneration(reserved.storageGeneration()))
            );
            assertEquals(replacement, plane.snapshot("repo", "view").orElseThrow().uuid());
        }
    }

    public void testDeletionWinsAgainstAnInflightCaptureFinish() throws Exception {
        try (var store = new RacingStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = plane(store);
            final String uuid = org.opensearch.common.UUIDs.randomBase64UUID();
            final var reserved = plane.createSnapshot(snapshot(uuid, true));
            store.mutate.set(() -> assertTrue(plane.deleteSnapshot("repo", "view")));
            expectThrows(
                IOException.class,
                () -> plane.finishSnapshot(snapshot(uuid, false).withStorageGeneration(reserved.storageGeneration()))
            );
            assertTrue(plane.snapshot("repo", "view").isEmpty());
        }
    }

    public void testStaleDeleteCannotRemoveAReplacement() throws Exception {
        try (var store = new RacingStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = plane(store);
            final String first = org.opensearch.common.UUIDs.randomBase64UUID();
            final String replacement = org.opensearch.common.UUIDs.randomBase64UUID();
            plane.createSnapshot(snapshot(first, false));
            store.mutate.set(() -> {
                assertTrue(plane.deleteSnapshot("repo", "view"));
                plane.createSnapshot(snapshot(replacement, false));
            });
            assertFalse("the original lifetime was already deleted", plane.deleteSnapshot("repo", "view"));
            assertEquals(replacement, plane.snapshot("repo", "view").orElseThrow().uuid());
        }
    }

    public void testAnInflightReservationCannotReturnAfterDeletion() throws Exception {
        try (var store = new RacingStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = plane(store);
            store.mutate.set(() -> assertTrue(plane.deleteSnapshot("repo", "view")));
            expectThrows(IOException.class, () -> plane.createSnapshot(snapshot(org.opensearch.common.UUIDs.randomBase64UUID(), true)));
            assertTrue(plane.snapshot("repo", "view").isEmpty());
            assertTrue(plane.liveSnapshots().isEmpty());
            assertTrue(
                store.blobContainer(org.opensearch.serverless.metadata.RegisterMap.snapshots(BlobPath.cleanPath()))
                    .listBlobs()
                    .keySet()
                    .stream()
                    .noneMatch(name -> name.contains("#"))
            );
        }
    }

    public void testCollectorReadsOneRegisterPerActiveSnapshot() throws Exception {
        try (var store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = plane(store);
            final var reserved = plane.createSnapshot(snapshot(org.opensearch.common.UUIDs.randomBase64UUID(), false));
            store.reset();
            assertEquals(reserved.storageGeneration(), plane.liveSnapshots().get(0).storageGeneration());
            assertEquals(1L, store.registerReads());
            assertEquals(0L, store.blobReads());
        }
    }

    public void testFailedCaptureCleanupLeavesAReplacementAlone() throws Exception {
        try (var store = new RacingStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = plane(store);
            final var old = plane.createSnapshot(snapshot(org.opensearch.common.UUIDs.randomBase64UUID(), true));
            assertTrue(plane.deleteSnapshot(old));
            final var replacement = plane.createSnapshot(snapshot(org.opensearch.common.UUIDs.randomBase64UUID(), true));
            assertFalse(plane.deleteSnapshot(old));
            assertEquals(replacement.storageGeneration(), plane.snapshot("repo", "view").orElseThrow().storageGeneration());
        }
    }

    public void testLongSnapshotNamesAndCopiedIndexPathsFitFilesystemComponents() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = plane(store);
            final String repo = "r".repeat(255);
            final String name = "s".repeat(255);
            final String index = "i".repeat(255);
            plane.putRepository(new RepositoryDescriptor(repo, "fs", Map.of(), 1000L));
            final var reserved = plane.createSnapshot(
                new SnapshotRecord(
                    repo,
                    name,
                    org.opensearch.common.UUIDs.randomBase64UUID(),
                    false,
                    1000L,
                    1000L,
                    Map.of(index, new SnapshotRecord.SnapshottedIndex("uuid-index", 1, null, Map.of()))
                )
            );
            final var path = org.opensearch.serverless.metadata.RegisterMap.snapshotShardData(BlobPath.cleanPath(), reserved, index, 0);
            final var data = store.blobContainer(path);
            data.writeBlobAtomic("copied", new java.io.ByteArrayInputStream(new byte[] { 1 }), 1L, true);
            assertTrue(data.blobExists("copied"));
            assertTrue(plane.deleteSnapshot(reserved));
            assertTrue(plane.snapshot(repo, name).isEmpty());
        }
    }

    public void testLegacyCopiedStorageRemainsReadableAndIsNeverReused() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = plane(store);
            final var legacy = snapshot(org.opensearch.common.UUIDs.randomBase64UUID(), false);
            final var bytes = legacy.toBytes();
            try (var input = bytes.streamInput()) {
                store.blobContainer(org.opensearch.serverless.metadata.RegisterMap.snapshots(BlobPath.cleanPath()))
                    .writeBlobAtomic(legacy.key(), input, bytes.length(), true);
            }
            final var read = plane.snapshot("repo", "view").orElseThrow();
            assertNull(read.storageGeneration());
            assertEquals(
                org.opensearch.serverless.metadata.RegisterMap.snapshotShardData(BlobPath.cleanPath(), "repo", "view", "index", 0),
                org.opensearch.serverless.metadata.RegisterMap.snapshotShardData(BlobPath.cleanPath(), read, "index", 0)
            );
            assertTrue(plane.deleteSnapshot("repo", "view"));
            final var replacement = plane.createSnapshot(snapshot(org.opensearch.common.UUIDs.randomBase64UUID(), false));
            assertNotNull(replacement.storageGeneration());
            assertNotEquals(
                org.opensearch.serverless.metadata.RegisterMap.snapshotShardData(BlobPath.cleanPath(), read, "index", 0),
                org.opensearch.serverless.metadata.RegisterMap.snapshotShardData(BlobPath.cleanPath(), replacement, "index", 0)
            );
        }
    }

    public void testStaleDeletionDoesNotRemoveReplacementCopiedFiles() throws Exception {
        try (var store = new RacingStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = plane(store);
            final String firstId = org.opensearch.common.UUIDs.randomBase64UUID();
            final var first = plane.createSnapshot(
                new SnapshotRecord("repo", "view", firstId, false, 1000L, 2000L, snapshot(firstId, false).indices())
            );
            final var firstFiles = store.blobContainer(
                org.opensearch.serverless.metadata.RegisterMap.snapshotShardData(BlobPath.cleanPath(), first, "index", 0).add("t=1")
            );
            firstFiles.writeBlobAtomic("file", new java.io.ByteArrayInputStream(new byte[] { 1 }), 1L, true);
            final AtomicReference<SnapshotRecord> replacement = new AtomicReference<>();
            store.mutate.set(() -> {
                assertTrue(plane.deleteSnapshot(first));
                final String nextId = org.opensearch.common.UUIDs.randomBase64UUID();
                final var next = plane.createSnapshot(
                    new SnapshotRecord("repo", "view", nextId, false, 1000L, 2000L, snapshot(nextId, false).indices())
                );
                replacement.set(next);
                store.blobContainer(
                    org.opensearch.serverless.metadata.RegisterMap.snapshotShardData(BlobPath.cleanPath(), next, "index", 0).add("t=1")
                ).writeBlobAtomic("file", new java.io.ByteArrayInputStream(new byte[] { 2 }), 1L, true);
            });
            assertFalse(plane.deleteSnapshot("repo", "view"));
            assertEquals(replacement.get().storageGeneration(), plane.snapshot("repo", "view").orElseThrow().storageGeneration());
            try (
                var input = store.blobContainer(
                    org.opensearch.serverless.metadata.RegisterMap.snapshotShardData(BlobPath.cleanPath(), replacement.get(), "index", 0)
                        .add("t=1")
                ).readBlob("file")
            ) {
                assertEquals(2, input.read());
                assertEquals(-1, input.read());
            }
        }
    }

    public void testUnreadableAuthoritativeStateStopsCollection() throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = plane(store);
            plane.createSnapshot(snapshot(org.opensearch.common.UUIDs.randomBase64UUID(), false));
            store.failRegisterReadsUnder("snapshot-states");
            expectThrows(IOException.class, plane::liveSnapshots);
            store.failRegisterReadsUnder(null);
            assertEquals(1, plane.liveSnapshots().size());
        }
    }

    public void testFailedReservationDoesNotUnpinAnAcknowledgedCompletion() throws Exception {
        try (var store = new RacingStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = plane(store);
            final String uuid = org.opensearch.common.UUIDs.randomBase64UUID();
            store.mutate.set(() -> store.read.set(() -> {
                final var reserved = plane.snapshot("repo", "view").orElseThrow();
                plane.finishSnapshot(snapshot(uuid, false).withStorageGeneration(reserved.storageGeneration()));
                throw new IOException("reservation verification failed after completion succeeded");
            }));
            expectThrows(IOException.class, () -> plane.createSnapshot(snapshot(uuid, true)));
            assertEquals(uuid, plane.snapshot("repo", "view").orElseThrow().uuid());
            assertEquals("the completed snapshot must remain discoverable by collectors", 1, plane.liveSnapshots().size());
        }
    }

    @FunctionalInterface
    private interface IoAction {
        void run() throws IOException;
    }

    private static final class RacingStore implements BlobStore {
        private final BlobStore delegate;
        final AtomicReference<IoAction> mutate = new AtomicReference<>();
        final AtomicReference<IoAction> read = new AtomicReference<>();

        RacingStore(BlobStore delegate) {
            this.delegate = delegate;
        }

        private static void run(AtomicReference<IoAction> hook) throws IOException {
            final var action = hook.getAndSet(null);
            if (action != null) {
                action.run();
            }
        }

        @Override
        public void validateKey(BlobPath path, String blobName) {
            delegate.validateKey(path, blobName);
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            final var inner = delegate.blobContainer(path);
            if (path.buildAsString().contains("snapshot") == false) {
                return inner;
            }
            return new DelegatingBlobContainer(inner) {
                @Override
                public void writeBlob(String name, InputStream input, long size, boolean fail) throws IOException {
                    run(mutate);
                    super.writeBlob(name, input, size, fail);
                }

                @Override
                public void writeBlobAtomic(String name, InputStream input, long size, boolean fail) throws IOException {
                    run(mutate);
                    super.writeBlobAtomic(name, input, size, fail);
                }

                @Override
                public BlobRegisterCasResult compareAndSwapRegister(String name, long generation, BytesReference bytes) throws IOException {
                    run(mutate);
                    return super.compareAndSwapRegister(name, generation, bytes);
                }

                @Override
                public void deleteBlobsIgnoringIfNotExists(List<String> names) throws IOException {
                    run(mutate);
                    super.deleteBlobsIgnoringIfNotExists(names);
                }

                @Override
                public InputStream readBlob(String name) throws IOException {
                    final var input = super.readBlob(name);
                    try {
                        run(read);
                        return input;
                    } catch (Throwable failure) {
                        input.close();
                        throw failure;
                    }
                }

                @Override
                public Optional<BlobRegister> readRegister(String name) throws IOException {
                    final var value = super.readRegister(name);
                    run(read);
                    return value;
                }
            };
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
