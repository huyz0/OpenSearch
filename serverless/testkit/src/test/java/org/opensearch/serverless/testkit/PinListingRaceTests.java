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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.PointInTime;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.serverless.metadata.SnapshotRecord;
import org.opensearch.serverless.reconcile.GarbageCollector;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.SegmentPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.io.UncheckedIOException;
import java.util.Map;

/** A released pin disappearing after discovery must not disable collection for unrelated data. */
public class PinListingRaceTests extends OpenSearchTestCase {
    public void testReleasedPitDoesNotPreventCollection() throws Exception {
        collectAfterPinDeleted(true);
    }

    public void testDeletedSnapshotDoesNotPreventCollection() throws Exception {
        collectAfterPinDeleted(false);
    }

    private void collectAfterPinDeleted(boolean pit) throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1_000L, 30_000L);
            metadata.createIndex(new IndexDescriptor("protected", "uuid-protected", 1, "{}", null));
            final var data = metadata.shardData("protected", 0);
            final var files = store.blobContainer(data.add("t=1"));
            files.writeBlobAtomic("old-file", new java.io.ByteArrayInputStream(new byte[] { 1 }), 1L, true);
            store.blobContainer(data).createRegisterIfAbsent(SegmentPublisher.MANIFEST, new CommitManifest(2L, Map.of()).toBytes());
            final var captured = new CommitManifest(1L, Map.of("old-file", "t=1"));
            final String id = org.opensearch.common.UUIDs.randomBase64UUID();
            if (pit) {
                metadata.createPointInTime(new PointInTime(id, "protected", "uuid-protected", 100_000L, Map.of(0, captured)));
            } else {
                final var record = new SnapshotRecord(
                    "repo",
                    "old",
                    "snapshot-uuid",
                    true,
                    1_000L,
                    1_000L,
                    Map.of("protected", new SnapshotRecord.SnapshottedIndex("uuid-protected", 1, null, Map.of(0, captured)))
                );
                final var bytes = record.toBytes();
                store.blobContainer(RegisterMap.snapshots(BlobPath.cleanPath()))
                    .writeBlob(record.key(), bytes.streamInput(), bytes.length(), true);
            }
            store.onNextListing(pit ? "pits" : "snapshots", () -> {
                try {
                    assertTrue(pit ? metadata.releasePointInTime(id) : metadata.deleteSnapshot("repo", "old"));
                } catch (java.io.IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            final var deleted = new GarbageCollector(store, BlobPath.cleanPath()).collectShard(metadata, "protected", 0);
            assertTrue("the pin must disappear after the listing was captured", store.fired());
            assertTrue("a released pin cannot stop collection: " + deleted, deleted.contains("t=1/old-file"));
            assertFalse(files.blobExists("old-file"));
        }
    }
}
