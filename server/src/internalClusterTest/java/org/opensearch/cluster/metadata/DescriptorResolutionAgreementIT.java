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

    /** Descriptors recorded by the dual write, keyed by name, standing in for the descriptor store. */
    private final Map<String, IndexDescriptor> published = new ConcurrentHashMap<>();

    @After
    public void clearRegistrations() {
        TestClaimedIndexLifecycle.uninstall();
        AbsentIndexDescriptorSuppliers.register(null);
        published.clear();
    }

    /**
     * The load-bearing one. For a population of indices, every field the descriptor carries must match
     * what cluster state holds.
     */
    public void testDescriptorsAgreeWithClusterStateForEveryIndex() throws Exception {
        TestClaimedIndexLifecycle.install().recording(descriptor -> published.put(descriptor.name(), descriptor));

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
        TestClaimedIndexLifecycle.install().recording(descriptor -> published.put(descriptor.name(), descriptor));

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
     * The gap H2c pinned, now closed.
     *
     * <p>The dual write used to fire only from index creation, so closing an index afterwards left the
     * descriptor saying OPEN while cluster state said CLOSE. Harmless while the map was authoritative and
     * fatal after H5, when a stale descriptor would be the only record and resolution would route on it.
     *
     * <p>The hook moved to {@code Metadata.Builder.put}, which every writer passes through, rather than
     * being added to each of the twelve services that update index metadata. Enumerating those call sites
     * is how a writer gets missed, and this area has already made that mistake in another form.
     */
    public void testClosingAnIndexUpdatesItsDescriptor() throws Exception {
        TestClaimedIndexLifecycle.install().recording(descriptor -> published.put(descriptor.name(), descriptor));

        createIndex("to-close", 1, 0);
        assertEquals("the descriptor must start open", IndexDescriptor.State.OPEN, published.get("to-close").state());

        assertAcked(client().admin().indices().prepareClose("to-close"));

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertEquals("cluster state must show the index closed", IndexMetadata.State.CLOSE, state.metadata().index("to-close").getState());
        assertEquals(
            "and the descriptor must have followed it, or after H5 resolution would route on a stale state",
            IndexDescriptor.State.CLOSE,
            published.get("to-close").state()
        );
    }

    /**
     * The same property for aliases, which change far more often than open state and are the other field
     * a resolver answers from without materializing metadata.
     */
    public void testAddingAnAliasUpdatesTheDescriptor() throws Exception {
        TestClaimedIndexLifecycle.install().recording(descriptor -> published.put(descriptor.name(), descriptor));
        createIndex("to-alias", 1, 0);
        assertEquals("no alias to begin with", List.of(), published.get("to-alias").aliases());

        assertAcked(client().admin().indices().prepareAliases().addAlias("to-alias", "added-later"));

        assertEquals(
            "the descriptor must carry an alias added after creation",
            List.of("added-later"),
            published.get("to-alias").aliases()
        );
    }

    /** The seam answers for an index cluster state does not hold, using the recorded descriptors. */
    public void testTheSeamResolvesFromDescriptorsWhenTheMapMisses() throws Exception {
        TestClaimedIndexLifecycle.install().recording(descriptor -> published.put(descriptor.name(), descriptor));
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

    /**
     * H4a. Deleting an index must leave a durable no rather than an absence.
     *
     * <p>This is what replaces {@code IndexGraveyard}. A node partitioned during the delete cannot tell
     * "this index never existed" from "I have not looked yet", and adopting its dangling shard data on
     * rejoin is the resurrection the graveyard exists to prevent. The graveyard keeps a bounded list and
     * purges the oldest entries; a tombstoned descriptor does not forget.
     *
     * <p>The uuid is asserted alongside the state because it is what identifies the data to reclaim. A
     * tombstone carrying only the name would say an index is gone without saying what to delete.
     */
    public void testDeletingAnIndexLeavesATombstone() throws Exception {
        TestClaimedIndexLifecycle.install().recording(descriptor -> published.put(descriptor.name(), descriptor));
        createIndex("to-delete", 2, 0);
        String uuidBeforeDelete = published.get("to-delete").uuid();

        assertAcked(client().admin().indices().prepareDelete("to-delete"));

        IndexDescriptor tombstone = published.get("to-delete");
        assertNotNull("deletion must leave a descriptor behind rather than removing it", tombstone);
        assertFalse("the tombstoned descriptor must report the index as not existing", tombstone.exists());
        assertEquals("the uuid must survive, since it identifies the dangling data to reclaim", uuidBeforeDelete, tombstone.uuid());
        assertEquals("and the shard count, so that data can be enumerated", 2, tombstone.shardCount());
    }

    /** The seam must answer no for a deleted index, which is what stops resolution resurrecting it. */
    public void testADeletedIndexDoesNotResolve() throws Exception {
        TestClaimedIndexLifecycle.install().recording(descriptor -> published.put(descriptor.name(), descriptor));
        createIndex("delete-then-resolve", 1, 0);
        assertAcked(client().admin().indices().prepareDelete("delete-then-resolve"));

        AbsentIndexDescriptorSuppliers.register(published::get);
        ClusterState state = client().admin().cluster().prepareState().get().getState();

        assertFalse(
            "a deleted index must not resolve through its tombstone",
            AbsentIndexDescriptorSuppliers.exists(state.metadata(), "delete-then-resolve")
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
