/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * What happens to gated mappings when {@code .opensearch-index-mappings} is deleted under a live cluster.
 *
 * <h2>Why this is its own class</h2>
 *
 * It deletes the mapping index, and an internal cluster test's cluster is shared across the methods of a
 * class. Any later method that creates a mapped gated index then finds the store's cached "the index exists"
 * flag still true, skips the create, and lets auto-creation rebuild the index at cluster defaults -- so a
 * geometry assertion reads 1 shard where the node asked for 3, and the failure lands in a method that did
 * nothing wrong. That is what happened when this test lived beside the others: the mutation run failed two
 * methods, only one of which was the one under test.
 *
 * <p>It also needs the plugin's own install path, which is where the watcher carrying the signal is
 * registered. Every other IT in this package installs the gate by hand with a store whose signal is
 * permanently off, so a test written there would pass while proving nothing.
 *
 * <h2>What it asserts, and what it used to</h2>
 *
 * That nothing is lost. Until T58 this index was where a gated mapping lived, so deleting it destroyed the
 * declared fields, and T48's answer was to fail the next write: the alternative was silently replacing them
 * with whichever field the next document happened to carry, which is what the code did before T48,
 * successfully and with an acknowledgement.
 *
 * <p>T58 moved the mapping into the index's descriptor, and this index became a write-behind projection kept
 * only so cluster stats can count field types in one search. Deleting it now costs a statistic, not a
 * mapping -- so the assertion inverts: the declared fields survive, the next write succeeds, and the
 * projection rebuilds itself from that write. The test kept failing after T58 because it was still demanding
 * T48's failure, and a suite that demands the old contract is how a fix like T58 gets half-reverted by
 * someone making the build green.
 *
 * <p>What it still proves is the part that would otherwise rot: {@link IndexBackedMappingStore} caches
 * "the index exists" and would go on writing into a deleted index forever without the watcher clearing that
 * latch. So the recovery assertion is the live test of the signal this class exists to wire.
 */
public class GatedMappingIndexLossIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private static volatile java.nio.file.Path sharedBasePath;

    private java.nio.file.Path basePath() {
        if (sharedBasePath == null) {
            synchronized (GatedMappingIndexLossIT.class) {
                if (sharedBasePath == null) {
                    sharedBasePath = randomRepoPath();
                }
            }
        }
        return sharedBasePath;
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath().toString())
            // The plugin installs the gate, and with it the watcher, only when this is on. That is the whole
            // point of this class: the signal has to be the one production wires, not one a test passes in.
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_NODE_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .build();
    }

    public void testAMappingSurvivesTheProjectionIndexBeingDeleted() throws Exception {
        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("serverless_gated-lost-mappings").settings(gated())
                    .mapping(Map.of("properties", Map.of("tenant", Map.of("type", "keyword"))))
            )
            .actionGet();
        String uuid = AbsentIndexDescriptorSuppliers.supply("serverless_gated-lost-mappings").uuid();

        // The projection is written behind the creation rather than inside it, so it has to have landed
        // before deleting it can prove anything -- and this wait is itself the assertion that a creation's
        // declared mapping reaches the aggregate at all, which is the hole StatsProjectingMappingStore
        // closed.
        assertBusy(
            () -> assertProjectionHolds(uuid, "the creation's mapping must reach the projection before its loss can be tested"),
            30,
            java.util.concurrent.TimeUnit.SECONDS
        );

        client().admin().indices().prepareDelete(IndexBackedMappingStore.MAPPING_INDEX).get();
        assertBusy(
            () -> assertFalse(
                "the deletion has to be visible in cluster state before the watcher can have seen it",
                client().admin().cluster().prepareState().get().getState().metadata().hasIndex(IndexBackedMappingStore.MAPPING_INDEX)
            )
        );

        // The declared fields are still there, because they are in the descriptor rather than in the index
        // that was just deleted. Read through MappingGenerationStore, since that is what every reader uses:
        // a mapping that survived somewhere unreachable through the store would not be a mapping that
        // survived.
        MappingGenerationStore.MappingGeneration afterLoss = MappingGenerationStore.currentMapping(uuid);
        assertNotNull("deleting the stats projection must not take the mapping with it", afterLoss);
        assertEquals("keyword", MappingGenerationStore.typeOf(afterLoss.fields().get("tenant")));

        // And the next write goes through rather than failing on a store that no longer owns the mapping.
        assertTrue(
            client().admin()
                .indices()
                .preparePutMapping("serverless_gated-lost-mappings")
                .setSource("amount", "type=double")
                .get()
                .isAcknowledged()
        );
        MappingGenerationStore.MappingGeneration afterWrite = MappingGenerationStore.currentMapping(uuid);
        assertEquals("keyword", MappingGenerationStore.typeOf(afterWrite.fields().get("tenant")));
        assertEquals("double", MappingGenerationStore.typeOf(afterWrite.fields().get("amount")));

        // The projection rebuilds itself from a later mapping change, which is what the watcher buys.
        // Without it the store's "the index exists" latch stays true across the deletion, every later
        // projection write is sent into an index that is not there, and the field type counts stop moving
        // with no failure anywhere -- the exact silent-statistic shape StatsProjectingMappingStore was built
        // to end.
        //
        // A second change rather than relying on the one above, because the contract is "under-reports until
        // the next mapping change", not "recovers the write that raced the deletion": the watcher's signal
        // arrives on a cluster state update, so a projection issued in the same moment as the delete can
        // still find the stale latch, fail, and be dropped by design.
        assertTrue(
            client().admin()
                .indices()
                .preparePutMapping("serverless_gated-lost-mappings")
                .setSource("region", "type=keyword")
                .get()
                .isAcknowledged()
        );
        assertBusy(
            () -> assertProjectionHolds(uuid, "the projection must rebuild itself from a later mapping change"),
            60,
            java.util.concurrent.TimeUnit.SECONDS
        );
    }

    /** The projection index exists and holds this index's document, checked in that order so a missing index retries. */
    private void assertProjectionHolds(String uuid, String why) {
        assertTrue(
            why + " -- the projection index is not there",
            client().admin().cluster().prepareState().get().getState().metadata().hasIndex(IndexBackedMappingStore.MAPPING_INDEX)
        );
        boolean holdsIt;
        try {
            holdsIt = client().prepareGet(IndexBackedMappingStore.MAPPING_INDEX, uuid).get().isExists();
        } catch (Exception e) {
            // A freshly created projection index can still be recovering. That is not the answer being
            // waited for, so it has to arrive as an AssertionError for assertBusy to retry rather than as
            // the exception it propagates out of the wait.
            throw new AssertionError(why + " -- the projection is not readable yet: " + e, e);
        }
        assertTrue(why, holdsIt);
    }

    private static Settings gated() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }
}
