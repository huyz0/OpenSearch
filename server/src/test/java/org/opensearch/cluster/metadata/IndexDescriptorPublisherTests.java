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
public class IndexDescriptorPublisherTests extends OpenSearchTestCase {

    @After
    public void clearPublisher() {
        IndexDescriptorPublisher.register(null);
    }

    /** The load-bearing one: creating an index records a descriptor that agrees with the metadata. */
    public void testCreationRecordsAMatchingDescriptor() {
        AtomicReference<IndexDescriptor> recorded = new AtomicReference<>();
        IndexDescriptorPublisher.register(recorded::set);

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
    public void testWithoutAPublisherCreationIsUnchanged() {
        ClusterState created = MetadataCreateIndexService.clusterStateCreateIndex(
            emptyState(),
            Set.of(),
            indexMetadata("plain", 1, 0),
            noReroute(),
            null
        );

        assertTrue("the index must still be created when no publisher is installed", created.metadata().hasIndex("plain"));
    }

    /**
     * The failure semantics of this phase, which are the opposite of H3's. A publisher that throws must
     * not cost the user their index, because during dual write the descriptor is redundant.
     */
    public void testAFailingPublisherDoesNotFailCreation() {
        IndexDescriptorPublisher.register(descriptor -> { throw new IllegalStateException("publisher is broken"); });

        ClusterState created = MetadataCreateIndexService.clusterStateCreateIndex(
            emptyState(),
            Set.of(),
            indexMetadata("survives", 1, 0),
            noReroute(),
            null
        );

        assertTrue("a broken publisher must not prevent an index from being created", created.metadata().hasIndex("survives"));
    }

    /**
     * One descriptor per created index, no more. A publisher invoked twice would double-write every
     * index in the system, which at the populations this area targets is not a rounding error.
     */
    public void testExactlyOneDescriptorPerCreation() {
        AtomicInteger published = new AtomicInteger();
        IndexDescriptorPublisher.register(descriptor -> published.incrementAndGet());

        MetadataCreateIndexService.clusterStateCreateIndex(emptyState(), Set.of(), indexMetadata("once", 1, 0), noReroute(), null);

        assertEquals("creation must record exactly one descriptor", 1, published.get());
    }

    /**
     * The return value exists to distinguish "nothing installed" from "installed and did nothing", since
     * both leave no descriptor behind and a caller cannot otherwise tell them apart.
     */
    public void testPublishReportsWhetherAnyoneWasListening() {
        assertFalse("nothing installed means nobody was told", IndexDescriptorPublisher.publish(indexMetadata("a", 1, 0)));

        IndexDescriptorPublisher.register(descriptor -> {});
        assertTrue("an installed publisher must be reported as invoked", IndexDescriptorPublisher.publish(indexMetadata("b", 1, 0)));
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
