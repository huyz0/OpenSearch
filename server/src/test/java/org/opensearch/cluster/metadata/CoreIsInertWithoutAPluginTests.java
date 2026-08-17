/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.stats.GatedMappingStatsAggregator;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.index.mapper.UnknownFieldRefresh;
import org.opensearch.indices.cluster.IndexResidencyPolicyRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The guard for the requirement the whole approach is affordable under: with no plugin installed, core
 * behaves as it did before any of this existed.
 *
 * <h2>Why this test rather than the suites</h2>
 *
 * Until now that requirement was checked by running the core suites and observing that they pass. That is
 * evidence and not a guard. A green suite says the paths those tests exercise still work; it does not say
 * core is inert, and it cannot fail in a way that names the requirement. The design document has said from
 * the start that this invariant "can be tested cheaply and continuously, which is why it should be the one
 * guarding the rest", and nothing was guarding it.
 *
 * <h2>What it asserts, and why it clears the registries first</h2>
 *
 * Every seam this branch adds to core is a static registry holding a nullable supplier. The contract is that
 * an unset registry means core takes the branch it always took. So this clears all of them, which is the
 * state of a stock node, and then asserts both halves: nothing reports itself registered, and every query
 * answers the way it must for a traditional index.
 *
 * <p>Clearing rather than assuming is deliberate. Test JVMs are reused across classes, so a class that
 * installs a seam and forgets to remove it would otherwise make this fail for a reason that has nothing to
 * do with the requirement. Setting the state under test is what makes the assertion about core rather than
 * about test ordering.
 *
 * <p>The registries are null on a real node because nothing writes to them until a plugin does, so clearing
 * here reproduces production rather than fabricating a condition.
 */
public class CoreIsInertWithoutAPluginTests extends OpenSearchTestCase {

    /** A stock node: every seam this branch adds, unset. */
    private static void clearEverySeam() {
        AbsentIndexDescriptorSuppliers.register(null);
        AbsentIndexDescriptorSuppliers.registerExpander(null);
        AbsentIndexRoutingSuppliers.register(null);
        DescriptorPrefetch.register(null);
        IndexDescriptorPublisher.register(null);
        IndexDescriptorPublisher.registerCreator(null);
        DurableTombstones.register(null);
        GatedIndexRelease.register(null);
        MappingGenerationStore.register(null);
        UnknownFieldRefresh.register(null);
        GatedMappingStatsAggregator.register(null);
        IndexCreationStrategyRegistry.register(null);
        IndexResidencyPolicyRegistry.register(null);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clearEverySeam();
    }

    private static IndexMetadata anIndex(String name) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    /**
     * The list is spelled out rather than derived by reflection. A new seam has to be added here by hand,
     * which is weaker than scanning the package and is a far clearer failure, and the thing being defended
     * against is a seam that defaults to on rather than one nobody remembered.
     */
    public void testNoSeamIsRegisteredOnAStockNode() {
        assertFalse("descriptor resolution", AbsentIndexDescriptorSuppliers.isRegistered());
        assertFalse("wildcard expansion over gated indices", AbsentIndexDescriptorSuppliers.isExpanderRegistered());
        assertFalse("computed routing", AbsentIndexRoutingSuppliers.isRegistered());
        assertFalse("the descriptor prefetcher", DescriptorPrefetch.isRegistered());
        assertFalse("the descriptor publisher", IndexDescriptorPublisher.isRegistered());
        assertFalse("the durable tombstone writer", DurableTombstones.isRegistered());
        assertFalse("the gated index releaser", GatedIndexRelease.isRegistered());
        assertFalse("the mapping generation store", MappingGenerationStore.isRegistered());
        assertFalse("the unknown field refresher", UnknownFieldRefresh.isRegistered());
        assertFalse("the gated mapping stats aggregator", GatedMappingStatsAggregator.isRegistered());
        assertFalse("the index-creation strategy", IndexCreationStrategyRegistry.isRegistered());
        assertFalse("the index-residency policy", IndexResidencyPolicyRegistry.isRegistered());
    }

    /**
     * Phase E2 of {@code core-pluggability-refactor-plan.md}: unlike every other seam above, an unregistered
     * {@code IndexResidencyPolicy} does not answer "does nothing" -- {@code IndicesClusterStateService}'s
     * sweep timer runs on every node, plugin or not, and always has. So a stock node must see the exact
     * numeric defaults that used to be hardcoded {@code Setting} fields on that class, not merely "no
     * exception."
     */
    public void testResidencyPolicyDefaultsMatchTheHistoricalHardcodedValues() {
        assertEquals("sweep interval", TimeValue.timeValueSeconds(60), IndexResidencyPolicyRegistry.sweepInterval());
        assertEquals("idle eviction threshold", TimeValue.timeValueMinutes(30), IndexResidencyPolicyRegistry.idleEvictionAfter());
        assertEquals("max open (zero means derive one)", 0, IndexResidencyPolicyRegistry.maxOpen());
    }

    /** An index either is in cluster state or does not exist. That is main's rule and it must still hold. */
    public void testResolutionFallsBackToClusterStateAlone() {
        Metadata metadata = Metadata.builder().put(anIndex("present"), false).build();

        assertNull("no descriptor can be supplied when nothing supplies descriptors", AbsentIndexDescriptorSuppliers.supply("absent"));
        assertTrue(AbsentIndexDescriptorSuppliers.exists(metadata, "present"));
        assertFalse(
            "an absent name must stay absent rather than being looked for elsewhere",
            AbsentIndexDescriptorSuppliers.exists(metadata, "absent")
        );

        assertNotNull(AbsentIndexDescriptorSuppliers.metadataOrDescriptor(metadata, metadata.index("present").getIndex()));
        assertNull(
            "an index not in cluster state has no metadata on a stock node",
            AbsentIndexDescriptorSuppliers.metadataOrDescriptor(metadata, new Index("absent", "absent-uuid"))
        );
    }

    /** Creation writes to cluster state, which is what makes an ordinary index ordinary. */
    public void testNothingSkipsClusterStateOnCreation() {
        assertFalse(
            "with no strategy registered every index must keep its cluster state entry, and a strategy "
                + "that threw would also have to answer false rather than strand the index with no record "
                + "anywhere",
            IndexCreationStrategyRegistry.skipsClusterState(anIndex("ordinary"))
        );
        assertFalse(
            "and the claim half of the same SPI must be unclaimed for every name, not just ordinary ones, "
                + "including a name that would be in a plugin's namespace if one were installed",
            IndexCreationStrategyRegistry.claims("serverless_would-be-namespaced")
        );
    }

    /** The publisher and the tombstone writer report that nobody listened, rather than pretending. */
    public void testPublishingRecordsNothingAndSaysSo() {
        assertFalse(IndexDescriptorPublisher.publish(anIndex("ordinary")));
        assertFalse(IndexDescriptorPublisher.publishTombstone(anIndex("ordinary")));
        assertNull(
            "a null future is how the creation path is told no creator exists, which it must treat as a "
                + "failure rather than as success",
            IndexDescriptorPublisher.createGated(anIndex("ordinary"))
        );
    }

    /**
     * The two hooks that sit on request and deletion paths must complete their listeners and get out of the
     * way. A hook that forgot to complete would hang every bulk request or every delete on a stock node.
     */
    public void testRequestPathHooksCompleteImmediately() {
        AtomicBoolean prefetched = new AtomicBoolean();
        DescriptorPrefetch.prefetch(List.of("a", "b"), ActionListener.wrap(ignored -> prefetched.set(true), e -> {}));
        assertTrue("an unregistered prefetch must complete inline and not block the bulk path", prefetched.get());

        AtomicBoolean acknowledged = new AtomicBoolean();
        DurableTombstones.whenDurable(List.of(anIndex("ordinary")), ActionListener.wrap(ignored -> acknowledged.set(true), e -> {}));
        assertTrue("an ordinary delete has the graveyard behind it and must not wait on a descriptor", acknowledged.get());
    }

    /**
     * A field the mapping does not have stays unknown, so the caller rejects or dynamically infers it.
     *
     * <p>A null {@code MapperService} is safe precisely because nothing is registered: the seam must return
     * before it touches its argument. If that ever stops being true this throws, which is the right failure.
     */
    public void testNoFieldIsRefreshedFromAStoreThatDoesNotExist() {
        assertFalse(UnknownFieldRefresh.refreshed(null, "some-index-uuid", "a.new.field"));
    }
}
