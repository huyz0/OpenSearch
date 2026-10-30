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
import org.opensearch.serverless.metadata.PointInTime;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/** A successful keep-alive cannot be undone by stale writes, expiry, or release. */
public class PointInTimeLifecycleTests extends OpenSearchTestCase {
    private static PointInTime pin(String id, long expiry) {
        return new PointInTime(id, "index", "uuid-index", expiry, Map.of(0, new CommitManifest(1L, Map.of("file", "t=1"))));
    }

    private MetadataPlane plane(BlobStore store) throws Exception {
        final var plane = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1_000L, 30_000L);
        plane.createIndex(new IndexDescriptor("index", "uuid-index", 1, "{}", null));
        return plane;
    }

    public void testReleaseWinsAgainstAnInflightExtension() throws Exception {
        try (var store = new RacingStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = plane(store);
            final var pin = pin(org.opensearch.common.UUIDs.randomBase64UUID(), 10_000L);
            plane.createPointInTime(pin);
            store.mutate.set(() -> assertTrue(plane.releasePointInTime(pin.id())));
            assertFalse("a released view must not be recreated", plane.extendPointInTime(pin.withExpiry(20_000L), 10_000L));
            assertTrue(plane.pointInTime(pin.id()).isEmpty());
        }
    }

    public void testFinishCannotResurrectAReleasedCapture() throws Exception {
        try (var store = new RacingStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = plane(store);
            final var pin = pin(org.opensearch.common.UUIDs.randomBase64UUID(), 10_000L);
            plane.beginPointInTime(new PointInTime(pin.id(), "index", "uuid-index", 10_000L, Map.of(), true));
            store.mutate.set(() -> assertTrue(plane.releasePointInTime(pin.id())));
            expectThrows(IOException.class, () -> plane.finishPointInTime(pin));
            assertTrue(plane.pointInTime(pin.id()).isEmpty());
        }
    }

    public void testAConcurrentLongerExtensionIsNotShortened() throws Exception {
        try (var store = new RacingStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = plane(store);
            final var pin = pin(org.opensearch.common.UUIDs.randomBase64UUID(), 10_000L);
            plane.createPointInTime(pin);
            store.mutate.set(() -> assertTrue(plane.extendPointInTime(pin.withExpiry(30_000L), 10_000L)));
            assertFalse(plane.extendPointInTime(pin.withExpiry(20_000L), 10_000L));
            assertEquals(30_000L, plane.pointInTime(pin.id()).orElseThrow().expiresAtMillis());
        }
    }

    public void testAnExpiryPassDoesNotDeleteASuccessfulExtension() throws Exception {
        try (var store = new RacingStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = plane(store);
            final var pin = pin(org.opensearch.common.UUIDs.randomBase64UUID(), 10_000L);
            plane.createPointInTime(pin);
            // The extension was admitted before expiry; the expiry pass uses its later cutoff.
            store.mutate.set(() -> assertTrue(plane.extendPointInTime(pin.withExpiry(30_000L), 10_000L)));
            assertEquals(0, plane.reapPointsInTime(10_001L));
            assertEquals(30_000L, plane.pointInTime(pin.id()).orElseThrow().expiresAtMillis());
        }
    }

    public void testCollectionDoesNotDropAnExtendedPinAfterAStaleRead() throws Exception {
        try (var store = new RacingStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = plane(store);
            final var pin = pin(org.opensearch.common.UUIDs.randomBase64UUID(), 10_000L);
            plane.createPointInTime(pin);
            store.read.set(() -> assertTrue(plane.extendPointInTime(pin.withExpiry(30_000L), 10_000L)));
            final var live = plane.livePointsInTime(10_001L);
            assertEquals("expiry must be claimed atomically before a pin stops protecting files", 1, live.size());
            assertEquals(30_000L, live.get(0).expiresAtMillis());
        }
    }

    public void testLegacyRecordsAreAdoptedAndExtendedSafely() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = plane(store);
            final var pin = pin(org.opensearch.common.UUIDs.randomBase64UUID(), 10_000L);
            final var inventory = store.blobContainer(org.opensearch.serverless.metadata.RegisterMap.pointsInTime(BlobPath.cleanPath()));
            final var bytes = pin.toBytes();
            try (var input = bytes.streamInput()) {
                inventory.writeBlobAtomic(pin.id(), input, bytes.length(), true);
            }
            assertEquals(10_000L, plane.pointInTime(pin.id()).orElseThrow().expiresAtMillis());
            assertTrue(plane.extendPointInTime(pin.withExpiry(20_000L), 10_000L));
            try (var input = inventory.readBlob(pin.id())) {
                final var marker = PointInTime.fromStream(input);
                assertTrue("older collectors must retain an extended legacy view", marker.isCapturing());
                assertEquals(Long.MAX_VALUE, marker.expiresAtMillis());
            }
            assertEquals(20_000L, plane.livePointsInTime(15_000L).get(0).expiresAtMillis());
            assertTrue(plane.releasePointInTime(pin.id()));
            assertTrue(plane.pointInTime(pin.id()).isEmpty());
        }
    }

    public void testReleasedIdsCannotBeReused() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = plane(store);
            final var pin = pin(org.opensearch.common.UUIDs.randomBase64UUID(), 10_000L);
            plane.createPointInTime(pin);
            assertTrue(plane.releasePointInTime(pin.id()));
            expectThrows(java.nio.file.FileAlreadyExistsException.class, () -> plane.createPointInTime(pin));
            assertFalse(plane.extendPointInTime(pin.withExpiry(20_000L), 10_000L));
        }
    }

    public void testReleasedHistoryDoesNotIncreaseInventoryReadCost() throws Exception {
        try (var store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = plane(store);
            for (int i = 0; i < 20; i++) {
                final var pin = pin(org.opensearch.common.UUIDs.randomBase64UUID(), 10_000L);
                plane.createPointInTime(pin);
                assertTrue(plane.releasePointInTime(pin.id()));
            }
            store.reset();
            assertTrue(plane.livePointsInTime(1_000L).isEmpty());
            assertEquals(0L, store.registerReads());
            assertEquals(0L, store.blobReads());
        }
    }

    public void testExtensionCannotReplaceCapturedFilesOrIdentity() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = plane(store);
            final var pin = pin(org.opensearch.common.UUIDs.randomBase64UUID(), 10_000L);
            plane.createPointInTime(pin);
            final var changed = new PointInTime(
                pin.id(),
                "other",
                "other-uuid",
                20_000L,
                Map.of(0, new CommitManifest(2L, Map.of("other-file", "t=2")))
            );
            assertTrue(plane.extendPointInTime(changed, 10_000L));
            final var stored = plane.pointInTime(pin.id()).orElseThrow();
            assertEquals(pin.index(), stored.index());
            assertEquals(pin.indexUuid(), stored.indexUuid());
            assertEquals(pin.shards().get(0).files(), stored.shards().get(0).files());
        }
    }

    public void testAmbiguousCreationFailureDoesNotUnpinAnAcknowledgedExtension() throws Exception {
        try (var store = new RacingStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = plane(store);
            final var pin = pin(org.opensearch.common.UUIDs.randomBase64UUID(), 10_000L);
            store.mutate.set(() -> {
                // The provider committed the marker but failed to return its acknowledgement.
                final var marker = new PointInTime(pin.id(), pin.index(), pin.indexUuid(), Long.MAX_VALUE, Map.of(), true).toBytes();
                try (var input = marker.streamInput()) {
                    store.delegate.blobContainer(org.opensearch.serverless.metadata.RegisterMap.pointsInTime(BlobPath.cleanPath()))
                        .writeBlobAtomic(pin.id(), input, marker.length(), true);
                }
                assertTrue(plane.extendPointInTime(pin.withExpiry(20_000L), 10_000L));
                throw new IOException("lost marker acknowledgement after keep-alive succeeded");
            });
            expectThrows(IOException.class, () -> plane.createPointInTime(pin));
            assertEquals(20_000L, plane.pointInTime(pin.id()).orElseThrow().expiresAtMillis());
            assertEquals("the acknowledged view must remain discoverable by collectors", 1, plane.livePointsInTime(1_000L).size());
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
            if (path.buildAsString().contains("pit") == false) {
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
