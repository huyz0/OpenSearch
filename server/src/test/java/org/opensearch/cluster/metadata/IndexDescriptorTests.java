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
import java.util.List;

/**
 * H2a. Whether the descriptor carries everything that needs it, which is the premise of Area H.
 *
 * <p>If a coordinator cannot route from a descriptor alone, the cluster state entry cannot be removed and
 * the whole area collapses. So the load-bearing test here is not serialization: it is that the fields
 * placement reads from {@link IndexMetadata} are all present and identical.
 *
 * <p>That list was audited rather than assumed. {@code ComputedRoutingTable} reads the index, the uuid,
 * the shard count and the search-only replica count; {@code ComputedPlacementGate} reads whether the
 * index is serverless. Those five are asserted individually below, so a field quietly dropped from the
 * descriptor fails here rather than in a routing decision.
 */
public class IndexDescriptorTests extends OpenSearchTestCase {

    /**
     * The load-bearing one. Every input placement takes from metadata must survive the reduction to a
     * descriptor unchanged.
     */
    public void testDescriptorCarriesEverythingPlacementReads() {
        IndexMetadata metadata = indexMetadata("logs-2024", 7, 2, true);

        IndexDescriptor descriptor = IndexDescriptor.from(metadata);

        assertEquals("placement hashes the index, so the name must survive", metadata.getIndex().getName(), descriptor.index().getName());
        assertEquals("placement hashes the uuid, so it must survive exactly", metadata.getIndexUUID(), descriptor.index().getUUID());
        assertEquals("the shard count decides how many placements are computed", metadata.getNumberOfShards(), descriptor.shardCount());
        assertEquals(
            "search-only replicas decide how many candidates become search copies",
            metadata.getNumberOfSearchOnlyReplicas(),
            descriptor.searchOnlyReplicaCount()
        );
        assertTrue("the gate reads the serverless flag to decide whether placement applies at all", descriptor.serverless());
    }

    /** An ordinary index must be distinguishable from a serverless one, or the gate cannot gate. */
    public void testAnOrdinaryIndexIsNotServerless() {
        IndexDescriptor descriptor = IndexDescriptor.from(indexMetadata("ordinary", 1, 0, false));

        assertFalse("an index without the setting must not be treated as serverless", descriptor.serverless());
    }

    /** Resolution needs closed state without materializing metadata, or the descriptor saves nothing. */
    public void testClosedStateSurvives() {
        IndexMetadata closed = IndexMetadata.builder(indexMetadata("closed-idx", 1, 0, true)).state(IndexMetadata.State.CLOSE).build();

        assertEquals(IndexDescriptor.State.CLOSE, IndexDescriptor.from(closed).state());
        assertTrue("a closed index still exists, which is what distinguishes it from a deleted one", IndexDescriptor.from(closed).exists());
    }

    /**
     * The tombstone, which is what replaces {@code IndexGraveyard}. Deletion has to be recorded rather
     * than represented by absence, since absence cannot be distinguished from not having looked.
     */
    public void testTombstoneRecordsDeletionRatherThanRemoving() {
        IndexDescriptor live = IndexDescriptor.from(indexMetadata("to-delete", 3, 0, true));

        IndexDescriptor dead = live.tombstoned();

        assertFalse("a tombstoned descriptor must not report the index as existing", dead.exists());
        assertEquals("the uuid must survive deletion, since dangling data is identified by it", live.uuid(), dead.uuid());
        assertEquals("and the shard count, so the data that must be reclaimed can be enumerated", live.shardCount(), dead.shardCount());
    }

    /** Aliases are carried so resolution can answer without loading full metadata. */
    public void testAliasesAreCarried() {
        IndexMetadata withAlias = IndexMetadata.builder(indexMetadata("aliased", 1, 0, true))
            .putAlias(AliasMetadata.builder("logs").build())
            .build();

        assertEquals(List.of("logs"), IndexDescriptor.from(withAlias).aliases());
    }

    public void testRoundTrip() throws IOException {
        IndexDescriptor descriptor = IndexDescriptor.from(indexMetadata("round-trip", 5, 1, true));

        IndexDescriptor read;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            descriptor.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                read = new IndexDescriptor(in);
            }
        }

        assertEquals("a descriptor that does not survive the wire cannot be stored in an index", descriptor, read);
    }

    /**
     * The size claim Area H rests on, checked rather than asserted in prose. A hundred million
     * descriptors at this size is a twenty gigabyte index; at IndexMetadata's measured 2,944 B it would
     * be nearly three hundred.
     */
    public void testDescriptorIsSmall() throws IOException {
        IndexDescriptor descriptor = IndexDescriptor.from(indexMetadata("a-fairly-typical-index-name-2024.01.01", 3, 1, true));

        int bytes;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            descriptor.writeTo(out);
            bytes = out.bytes().length();
        }

        logger.warn("H2a descriptor serialized size: {} bytes", bytes);
        assertTrue("a descriptor must stay small enough that a hundred million fit in one index: " + bytes + " B", bytes < 400);
    }

    private static IndexMetadata indexMetadata(String name, int shards, int searchReplicas, boolean serverless) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid-000000")
                    .put("index.serverless_storage.enabled", serverless)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, searchReplicas)
                    .build()
            )
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .build();
    }
}
