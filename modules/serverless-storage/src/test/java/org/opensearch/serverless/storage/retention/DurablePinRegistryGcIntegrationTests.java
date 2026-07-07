/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.gc.ManifestRetentionPolicy;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Confirms a snapshot pin created through the real {@link BlobContainerDurablePinRegistry} (not a
 * hand-built {@code Set}) actually protects its manifest across a {@link ManifestRetentionPolicy}
 * sweep, and that removing the pin (the snapshot being deleted) makes the generation eligible for
 * deletion again -- the full lifecycle from rfc-serverless-opensearch.md &sect;14.
 */
public class DurablePinRegistryGcIntegrationTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;

    private static CommitManifest manifest(long term, long generation, long createdAtMillis) {
        return new CommitManifest(
            INDEX_UUID,
            SHARD_ID,
            term,
            generation,
            "segments_" + generation,
            Map.of("segments_" + generation, new FileReference("bundle-" + term + "-" + generation, 0, 10, 1L)),
            0,
            0,
            null,
            0,
            PruningStats.empty(),
            createdAtMillis
        );
    }

    public void testSnapshotPinProtectsGenerationAcrossGcSweepUntilRemoved() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        DurablePinRegistry registry = new BlobContainerDurablePinRegistry(blobContainer);

        CommitManifest snapshotted = manifest(1, 0, 0L);
        CommitManifest latest = manifest(1, 5, 5000L);

        // No pin yet: the superseded generation is deletable.
        List<CommitManifest> deletableBefore = ManifestRetentionPolicy.computeDeletableManifests(
            List.of(snapshotted, latest),
            Long.MAX_VALUE,
            Set.of(),
            Set.of()
        );
        assertEquals(List.of(snapshotted), deletableBefore);

        // A snapshot is taken, pinning generation 0.
        registry.addPin(INDEX_UUID, SHARD_ID, new PinRecord("nightly-snapshot", 1, 0));

        List<CommitManifest> deletableWithPin = ManifestRetentionPolicy.computeDeletableManifests(
            List.of(snapshotted, latest),
            Long.MAX_VALUE,
            Set.of(),
            registry.getPinnedManifestIds(INDEX_UUID, SHARD_ID)
        );
        assertEquals("a durable pin from a real snapshot must protect its manifest", List.of(), deletableWithPin);

        // The snapshot is deleted: the pin is removed, and the generation becomes deletable again.
        registry.removePin(INDEX_UUID, SHARD_ID, "nightly-snapshot");

        List<CommitManifest> deletableAfterUnpin = ManifestRetentionPolicy.computeDeletableManifests(
            List.of(snapshotted, latest),
            Long.MAX_VALUE,
            Set.of(),
            registry.getPinnedManifestIds(INDEX_UUID, SHARD_ID)
        );
        assertEquals(List.of(snapshotted), deletableAfterUnpin);
    }
}
