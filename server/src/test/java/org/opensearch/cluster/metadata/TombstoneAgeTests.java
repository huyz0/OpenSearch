/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * A tombstone can say how old it is, and an unknown age is never old.
 *
 * <h2>Why a tombstone needs this and a live descriptor does not</h2>
 *
 * A live descriptor is rewritten whenever anything about its index changes, and can always be re-derived
 * from the index it describes. A tombstone is written once, has nothing behind it to re-derive from, and has
 * to survive on its own for a retention window. It is therefore the one descriptor that has to be able to
 * answer how old it is, because reclaiming it early is a resurrection and never reclaiming it is a leak.
 *
 * <h2>The direction the default has to fail in</h2>
 *
 * Zero means two different things, "not a tombstone" and "written before this field existed", and they are
 * deliberately not distinguished. Both have to be read as an unknown age, and an unknown age must never be
 * treated as old enough to reclaim. Any reclamation that treated zero as "epoch, therefore ancient" would
 * delete every pre-existing tombstone on its first pass, which is the worst available outcome and is exactly
 * what a naive {@code deletedAt < cutoff} would do.
 */
public class TombstoneAgeTests extends OpenSearchTestCase {

    private static IndexDescriptor descriptor() {
        return IndexDescriptor.from(
            IndexMetadata.builder("tenant-a")
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, "tenant-a-uuid")
                        .build()
                )
                .numberOfShards(3)
                .numberOfReplicas(0)
                .build()
        );
    }

    public void testALiveDescriptorHasNoDeletionTime() {
        assertEquals(0L, descriptor().deletedAtMillis());
    }

    public void testATombstoneCarriesWhenItWasDeleted() {
        IndexDescriptor tombstone = descriptor().tombstoned(1_700_000_000_000L);

        assertFalse(tombstone.exists());
        assertEquals(1_700_000_000_000L, tombstone.deletedAtMillis());
    }

    /** The no-argument form still exists and still means unknown age, not epoch. */
    public void testAnUnstampedTombstoneReportsUnknownRatherThanAncient() {
        IndexDescriptor tombstone = descriptor().tombstoned();

        assertFalse(tombstone.exists());
        assertEquals("zero is the unknown-age sentinel, and a reclaimer must refuse to age it", 0L, tombstone.deletedAtMillis());
    }

    public void testTheDeletionTimeSurvivesTheWire() throws IOException {
        IndexDescriptor tombstone = descriptor().tombstoned(1_700_000_000_000L);

        IndexDescriptor restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            tombstone.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new IndexDescriptor(in);
            }
        }

        assertEquals(tombstone.deletedAtMillis(), restored.deletedAtMillis());
        assertFalse(restored.exists());
    }

    /**
     * Tombstoning preserves the routing geometry, which it did not before.
     *
     * <p>The old {@code tombstoned()} built its copy through the fourteen-argument constructor, and that one
     * defaults {@code routingNumShards} and {@code routingPartitionSize} to zero. So every tombstone silently
     * lost the geometry of the index it stood for. Nothing is known to have read it back, which is why this
     * went unnoticed; a descriptor that describes a three-shard index as having zero routing shards is wrong
     * whether or not anyone currently looks.
     */
    public void testTombstoningKeepsTheRoutingGeometry() {
        IndexDescriptor live = descriptor();
        IndexDescriptor tombstone = live.tombstoned(1L);

        assertEquals(live.routingNumShards(), tombstone.routingNumShards());
        assertEquals(live.routingPartitionSize(), tombstone.routingPartitionSize());
        assertTrue("the fixture must have a geometry worth losing", live.routingNumShards() > 0);
    }
}
