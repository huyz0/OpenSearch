/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * H2b. Whether index creation records a descriptor alongside the cluster state entry.
 *
 * <p>Was {@code IndexDescriptorPublisherTests}, and renamed with the seam it covers: the publisher, the
 * creator and the updater were three static registrations that {@code DescriptorGate} installed and cleared
 * as one unit, and they are now three methods on {@link ClaimedIndexLifecycle}. Nothing this class asserts
 * changed -- the recording path is reached the same way, from the same two {@code Metadata.Builder.put}
 * overloads, with the same failure policy -- only what it is registered into.
 *
 * <p>The dual write is deliberately redundant: it exists so H2c can compare the two resolution paths
 * while the old structure is still present to be the reference. Its correctness condition is therefore
 * not "a descriptor exists" but "the descriptor says the same thing the metadata says", which is asserted
 * field by field below rather than by comparing one summary value.
 *
 * <p>The other half of the contract is that creation must not depend on it. During dual write a lost
 * descriptor costs a comparison; a failed index creation costs the user their index. That inverts in H3,
 * when the descriptor becomes the only record, and the two phases having opposite failure semantics is
 * exactly the sort of thing that gets missed when the second is written.
 */
public class ClaimedIndexRecordingTests extends OpenSearchTestCase {

    @After
    public void clearLifecycle() {
        TestClaimedIndexLifecycle.uninstall();
    }

    /** The load-bearing one: creating an index records a descriptor that agrees with the metadata. */
    public void testCreationRecordsAMatchingDescriptor() {
        AtomicReference<IndexDescriptor> recorded = new AtomicReference<>();
        TestClaimedIndexLifecycle.install().recording(recorded::set);

        IndexMetadata created = indexMetadata("logs-2024", 5, 2);
        MetadataCreateIndexService.clusterStateCreateIndex(emptyState(), Set.of(), created, noReroute(), null);

        IndexDescriptor descriptor = recorded.get();
        assertNotNull("creating an index must record a descriptor", descriptor);
        assertEquals("the name must agree, since it is the id the descriptor is stored under", "logs-2024", descriptor.name());
        assertEquals("the uuid must agree, since placement hashes it", created.getIndexUUID(), descriptor.uuid());
        assertEquals("the shard count must agree, or placement computes a different number", 5, descriptor.shardCount());
        assertEquals("search replicas must agree", 2, descriptor.searchOnlyReplicaCount());
    }

    /** With nothing installed, creation must behave exactly as it did before Area H existed. */
    public void testWithoutAPlaneCreationIsUnchanged() {
        ClusterState created = MetadataCreateIndexService.clusterStateCreateIndex(
            emptyState(),
            Set.of(),
            indexMetadata("plain", 1, 0),
            noReroute(),
            null
        );

        assertTrue("the index must still be created when no plane is installed", created.metadata().hasIndex("plain"));
    }

    /**
     * The failure semantics of this phase, which are the opposite of H3's. A recorder that throws must
     * not cost the user their index, because during dual write the descriptor is redundant.
     */
    public void testAFailingRecorderDoesNotFailCreation() {
        TestClaimedIndexLifecycle.install().recording(descriptor -> { throw new IllegalStateException("the recorder is broken"); });

        ClusterState created = MetadataCreateIndexService.clusterStateCreateIndex(
            emptyState(),
            Set.of(),
            indexMetadata("survives", 1, 0),
            noReroute(),
            null
        );

        assertTrue("a broken recorder must not prevent an index from being created", created.metadata().hasIndex("survives"));
    }

    /**
     * One descriptor per created index, no more. A recorder invoked twice would double-write every
     * index in the system, which at the populations this area targets is not a rounding error.
     */
    public void testExactlyOneDescriptorPerCreation() {
        AtomicInteger published = new AtomicInteger();
        TestClaimedIndexLifecycle.install().recording(descriptor -> published.incrementAndGet());

        MetadataCreateIndexService.clusterStateCreateIndex(emptyState(), Set.of(), indexMetadata("once", 1, 0), noReroute(), null);

        assertEquals("creation must record exactly one descriptor", 1, published.get());
    }

    /**
     * The return value exists to distinguish "nothing installed" from "installed and did nothing", since
     * both leave no descriptor behind and a caller cannot otherwise tell them apart.
     */
    public void testRecordChangeReportsWhetherAnyoneWasListening() {
        assertFalse("nothing installed means nobody was told", ClaimedIndexLifecycleRegistry.recordChange(indexMetadata("a", 1, 0)));

        TestClaimedIndexLifecycle.install().recording(descriptor -> {});
        assertTrue("an installed plane must be reported as invoked", ClaimedIndexLifecycleRegistry.recordChange(indexMetadata("b", 1, 0)));
    }

    /**
     * H4e. Rebuilding metadata from scratch must not republish every index it touches.
     *
     * <p>Gateway recovery, remote cluster state restore and reading metadata from disk all build a
     * Metadata from nothing. Publishing from those would mean a descriptor write per index on every
     * recovery, which at a hundred million indices is slower than the recovery itself. The discriminator
     * is whether the builder started from an existing metadata, which is what separates "one writer
     * changed one index" from "we are rebuilding the world".
     */
    public void testBuildingFromScratchDoesNotRepublish() {
        AtomicInteger published = new AtomicInteger();
        TestClaimedIndexLifecycle.install().recording(descriptor -> published.incrementAndGet());

        Metadata.Builder fromScratch = Metadata.builder();
        for (int i = 0; i < 10; i++) {
            fromScratch.put(indexMetadata("restored-" + i, 1, 0), false);
        }
        fromScratch.build();

        assertEquals("a restore must not write a descriptor per index it reads", 0, published.get());
    }

    /**
     * The control, and the case that must not be lost to the optimisation above. A state received from
     * the cluster manager arrives as a diff applied to the previous metadata, and the indices it changes
     * must still update their descriptors.
     */
    public void testAnIncrementalUpdateStillPublishes() {
        Metadata existing = Metadata.builder().put(indexMetadata("already-here", 1, 0), false).build();

        AtomicInteger published = new AtomicInteger();
        TestClaimedIndexLifecycle.install().recording(descriptor -> published.incrementAndGet());

        Metadata.Builder incremental = Metadata.builder(existing);
        incremental.put(indexMetadata("changed", 1, 0), false);
        incremental.build();

        assertEquals("an incremental change must still record its descriptor", 1, published.get());
    }

    // ---------------------------------------------------------------- helpers

    /** Reroute is not what these assert, so it is the identity rather than a mock with behaviour. */
    private static java.util.function.BiFunction<ClusterState, String, ClusterState> noReroute() {
        return (state, reason) -> state;
    }

    private static ClusterState emptyState() {
        return ClusterState.builder(ClusterName.DEFAULT).build();
    }

    private static IndexMetadata indexMetadata(String name, int shards, int searchReplicas) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid-000000")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, searchReplicas)
                    .build()
            )
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .build();
    }

    /** Unused, present so the import documents which blocks the creation path takes. */
    @SuppressWarnings("unused")
    private static final List<ClusterBlock> NO_BLOCKS = List.of();
}
