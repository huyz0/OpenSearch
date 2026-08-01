/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * C2. The two pieces of routing geometry a descriptor has to carry, and the two silent failures that
 * follow from omitting them.
 *
 * <p>Both are silent in the same way and that is why they get tests rather than a comment. Routing a
 * document is {@code murmur3(routing ?: id) % routingNumShards / routingFactor} plus a partition offset,
 * and every input to that has a plausible default. A descriptor with no routing divisor routes against the
 * shard count, which is correct until the index is resharded. A descriptor with no partition size routes
 * with an offset of zero, which is correct until somebody sets the setting. Neither throws, neither logs,
 * and both put documents on a shard that will not be searched for them.
 */
public class IndexDescriptorRoutingGeometryTests extends OpenSearchTestCase {

    private static IndexDescriptor descriptor(int shardCount, int routingNumShards, int routingPartitionSize) {
        return new IndexDescriptor(
            "tenant",
            "tenant-uuid",
            shardCount,
            0,
            true,
            IndexDescriptor.State.OPEN,
            List.of(),
            Version.CURRENT.id,
            false,
            false,
            false,
            false,
            0L,
            0L,
            routingNumShards,
            routingPartitionSize
        );
    }

    public void testUnsetGeometryResolvesToTheOldBehaviour() {
        IndexDescriptor unset = descriptor(5, 0, 0);

        assertEquals("an index that was never resharded routes against its shard count", 5, unset.effectiveRoutingNumShards());
        assertEquals("and one with no partition size is not partitioned", 1, unset.effectiveRoutingPartitionSize());
        assertFalse(unset.toIndexMetadata().isRoutingPartitionedIndex());
    }

    /**
     * The reshard case, with the direction the right way round.
     *
     * <p>{@code number_of_routing_shards} is the pre-split shard space, so it is at or above the shard
     * count and a whole multiple of it. Splitting grows the shard count toward it and leaves it alone,
     * which is exactly what keeps a document routing to the same place across a split. A descriptor that
     * cannot carry it makes the divisor collapse to the current shard count and every document move.
     */
    public void testASplittableIndexKeepsItsPreSplitRoutingSpace() {
        IndexDescriptor splittable = descriptor(5, 20, 0);

        assertEquals(20, splittable.effectiveRoutingNumShards());
        IndexMetadata metadata = splittable.toIndexMetadata();
        assertEquals(20, metadata.getRoutingNumShards());
        assertEquals("the shard count itself is unchanged", 5, metadata.getNumberOfShards());
        assertEquals("and the factor follows from the two", 4, metadata.getRoutingFactor());
    }

    /** A value below the shard count is not a routing space, so it resolves to the shard count. */
    public void testAnImpossibleDivisorFallsBackRatherThanBreakingConstruction() {
        assertEquals(10, descriptor(10, 4, 0).effectiveRoutingNumShards());
    }

    /** The setting that used to be accepted and ignored. */
    public void testAPartitionedIndexIsActuallyPartitioned() {
        IndexMetadata metadata = descriptor(8, 0, 4).toIndexMetadata();

        assertTrue("routing_partition_size must survive onto the synthesised metadata", metadata.isRoutingPartitionedIndex());
        assertEquals(4, metadata.getRoutingPartitionSize());
    }

    /**
     * The failure this closes, shown rather than described: with a partition size, two documents sharing a
     * routing value must be able to land on different shards. Without it they cannot, because the offset is
     * always zero.
     */
    public void testPartitioningActuallySpreadsOneRoutingValue() {
        IndexMetadata partitioned = descriptor(8, 0, 4).toIndexMetadata();
        IndexMetadata notPartitioned = descriptor(8, 0, 0).toIndexMetadata();

        int spread = 0;
        int flat = 0;
        int firstPartitioned = OperationRouting.generateShardId(partitioned, "doc-0", "tenant-a");
        int firstFlat = OperationRouting.generateShardId(notPartitioned, "doc-0", "tenant-a");
        for (int i = 1; i < 200; i++) {
            if (OperationRouting.generateShardId(partitioned, "doc-" + i, "tenant-a") != firstPartitioned) {
                spread++;
            }
            if (OperationRouting.generateShardId(notPartitioned, "doc-" + i, "tenant-a") != firstFlat) {
                flat++;
            }
        }

        assertTrue("a partitioned index must spread one routing value across shards, saw " + spread, spread > 0);
        assertEquals("and an unpartitioned one must not, which is what made the omission invisible", 0, flat);
    }

    /**
     * The divisor changes where a document lands, which is the whole reason it has to be carried. Asserted
     * against a concrete disagreement rather than by reading the formula back.
     */
    public void testTheRoutingDivisorChangesWhereDocumentsLand() {
        IndexMetadata original = descriptor(5, 5, 0).toIndexMetadata();
        IndexMetadata splittable = descriptor(5, 20, 0).toIndexMetadata();

        int disagreements = 0;
        for (int i = 0; i < 200; i++) {
            if (OperationRouting.generateShardId(original, "doc-" + i, null) != OperationRouting.generateShardId(
                splittable,
                "doc-" + i,
                null
            )) {
                disagreements++;
            }
        }
        assertTrue("a wrong divisor must actually misroute documents, saw " + disagreements + " of 200", disagreements > 0);
    }

    public void testGeometrySurvivesTheWire() throws IOException {
        IndexDescriptor written = descriptor(5, 20, 4);

        IndexDescriptor read;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            written.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                read = new IndexDescriptor(in);
            }
        }

        assertEquals(20, read.routingNumShards());
        assertEquals(4, read.routingPartitionSize());
        assertEquals(5, read.shardCount());
    }

    /**
     * Round-tripping real metadata must not quietly change the geometry it came in with.
     *
     * <p>Built with {@code setRoutingNumShards} rather than the setting, which is how real creation does
     * it: {@code IndexMetadata} keeps the routing shard count as a field and derives {@code routingFactor}
     * from it, and the builder does not read it back out of the settings. A first version of this test set
     * the setting and got the shard count instead, which is the same mistake the descriptor was making.
     */
    public void testFromIndexMetadataCapturesWhatWasThere() {
        IndexMetadata source = IndexMetadata.builder("tenant")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "tenant-uuid")
                    .put(IndexMetadata.SETTING_ROUTING_PARTITION_SIZE, 3)
            )
            .numberOfShards(6)
            .numberOfReplicas(0)
            .setRoutingNumShards(12)
            .build();

        IndexDescriptor captured = IndexDescriptor.from(source);

        assertEquals(12, captured.effectiveRoutingNumShards());
        assertEquals(3, captured.effectiveRoutingPartitionSize());
        assertEquals(12, captured.toIndexMetadata().getRoutingNumShards());
        assertEquals(3, captured.toIndexMetadata().getRoutingPartitionSize());
    }
}
