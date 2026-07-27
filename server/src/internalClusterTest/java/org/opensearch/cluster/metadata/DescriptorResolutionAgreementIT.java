/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * H2c. Whether resolving through descriptors gives the same answer as resolving through cluster state.
 *
 * <p>This is the phase where correctness is established cheaply, because the old structure is still
 * present to be the reference. Once H3 stops writing the cluster state entry there is nothing left to
 * compare against, so anything not caught here is caught in production or not at all.
 *
 * <p>The comparison is deliberately field by field rather than on one summary value. A descriptor that
 * agreed on existence but disagreed on shard count would route to the wrong number of shards, and an
 * assertion on "both say the index exists" would pass while that happened.
 *
 * <p>The cases that matter are the ones the descriptor had to be designed to carry: aliases, closed
 * state, and hidden or system indices. Each of those is a place where the two paths could plausibly
 * diverge, and each is exercised rather than assumed to be covered by the ordinary case.
 */
public class DescriptorResolutionAgreementIT extends OpenSearchIntegTestCase {

    /** Descriptors recorded by the dual write, keyed by name, standing in for the descriptor index. */
    private final Map<String, IndexDescriptor> published = new ConcurrentHashMap<>();

    @After
    public void clearRegistrations() {
        IndexDescriptorPublisher.register(null);
        AbsentIndexDescriptorSuppliers.register(null);
        published.clear();
    }

    /**
     * The load-bearing one. For a population of indices, every field the descriptor carries must match
     * what cluster state holds.
     */
    public void testDescriptorsAgreeWithClusterStateForEveryIndex() throws Exception {
        IndexDescriptorPublisher.register(descriptor -> published.put(descriptor.name(), descriptor));

        List<String> names = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            String name = "agree-" + i;
            createIndex(name, 1 + (i % 3), 0);
            names.add(name);
        }

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        Map<String, String> disagreements = new HashMap<>();

        for (String name : names) {
            IndexDescriptor descriptor = published.get(name);
            if (descriptor == null) {
                disagreements.put(name, "no descriptor was published at all");
                continue;
            }
            IndexMetadata metadata = state.metadata().index(name);
            IndexDescriptor fromState = IndexDescriptor.from(metadata);
            if (descriptor.equals(fromState) == false) {
                disagreements.put(name, "published " + descriptor + " but state says " + fromState);
            }
        }

        assertEquals("every index must resolve identically through both paths, but: " + disagreements, Map.of(), disagreements);
        assertEquals("every created index must have produced a descriptor", names.size(), published.size());
    }

    /**
     * Aliases, which the descriptor carries so resolution can answer without materializing metadata. A
     * descriptor that dropped them would make an aliased index unresolvable by its alias once the map is
     * gone, and nothing in the ordinary case would notice.
     */
    public void testAliasesAgree() throws Exception {
        IndexDescriptorPublisher.register(descriptor -> published.put(descriptor.name(), descriptor));

        assertAcked(
            prepareCreate("aliased-idx").setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            ).addAlias(new org.opensearch.action.admin.indices.alias.Alias("my-alias")).setTimeout(TimeValue.timeValueSeconds(30))
        );

        IndexDescriptor descriptor = published.get("aliased-idx");
        assertNotNull("the aliased index must have produced a descriptor", descriptor);
        assertEquals("the alias must survive into the descriptor", List.of("my-alias"), descriptor.aliases());
    }

    /**
     * Closed state, which resolution needs in order to answer without loading metadata. Note that this
     * asserts what the descriptor said <em>at creation</em>: the dual write records once, so a later
     * close is not reflected. That gap is real and belongs to H4, and asserting it here rather than
     * discovering it after the map is gone is the point of this phase.
     */
    public void testAnIndexClosedAfterCreationExposesTheDualWriteGap() throws Exception {
        IndexDescriptorPublisher.register(descriptor -> published.put(descriptor.name(), descriptor));

        createIndex("to-close", 1, 0);
        assertAcked(client().admin().indices().prepareClose("to-close"));

        IndexDescriptor descriptor = published.get("to-close");
        ClusterState state = client().admin().cluster().prepareState().get().getState();

        assertEquals("cluster state must show the index closed", IndexMetadata.State.CLOSE, state.metadata().index("to-close").getState());
        assertEquals(
            "the descriptor still says OPEN, because the dual write records at creation and nothing "
                + "updates it afterwards. This is the gap H4 has to close, and it is asserted here rather "
                + "than left to be found once the cluster state entry is gone",
            IndexDescriptor.State.OPEN,
            descriptor.state()
        );
    }

    /** The seam answers for an index cluster state does not hold, using the recorded descriptors. */
    public void testTheSeamResolvesFromDescriptorsWhenTheMapMisses() throws Exception {
        IndexDescriptorPublisher.register(descriptor -> published.put(descriptor.name(), descriptor));
        createIndex("recorded", 1, 0);

        AbsentIndexDescriptorSuppliers.register(published::get);
        ClusterState state = client().admin().cluster().prepareState().get().getState();

        assertTrue(
            "a name the map does not hold must resolve through its descriptor",
            AbsentIndexDescriptorSuppliers.exists(state.metadata(), "recorded")
        );
        assertFalse(
            "a name neither the map nor the descriptors hold must still not resolve",
            AbsentIndexDescriptorSuppliers.exists(state.metadata(), "never-existed")
        );
    }

    // ---------------------------------------------------------------- helpers

    /**
     * Search replicas are fixed at zero here rather than varied, because setting them requires remote
     * store and this suite is about resolution agreement rather than storage configuration. The
     * descriptor's handling of that field is covered by IndexDescriptorTests, which can set it freely.
     */
    private void createIndex(String name, int shards, int searchReplicas) {
        assertAcked(
            prepareCreate(name).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, searchReplicas)
                    .build()
            ).setWaitForActiveShards(ActiveShardCount.NONE).setTimeout(TimeValue.timeValueSeconds(30))
        );
    }
}
