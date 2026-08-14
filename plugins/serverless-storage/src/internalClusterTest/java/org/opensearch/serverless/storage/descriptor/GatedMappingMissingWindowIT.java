/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;

/**
 * T59. What a real read sees in the window between T47's deletion-time prune and the moment a stale-cache
 * node's descriptor catches up: this node's descriptor still confirms a mapping generation, and the store
 * that is supposed to back it holds nothing.
 *
 * <h2>What "real" means here, and what does not reach that bar</h2>
 *
 * Unlike {@code StoreBackedFieldRefresherIT}, which never installs {@link ServerlessStoragePlugin} at all,
 * this class installs it the way a node actually runs it -- the same as {@link GatedMappingIndexLossIT} does
 * for T48's mirror of this test -- so {@link MappingGenerationStore} and {@code AbsentIndexDescriptorSuppliers}
 * are the real seams {@code DescriptorGate.install} wires, backed by a real cluster and a real
 * {@code .opensearch-index-mappings} index, rather than test doubles.
 *
 * <p>The refresh call itself is still made directly against a live shard's {@link MapperService}, not through
 * a real {@code client().prepareIndex} write reaching it via {@code DocumentParser}. That path exists --
 * {@code DocumentParser}'s only call into {@code UnknownFieldRefresh} is inside its {@code disable_objects}
 * flattening branch, a mapping feature unrelated to gating that a document must opt into -- and driving it
 * end to end from a plain write was tried and abandoned here: it adds a second mechanism this task does not
 * own and a second failure mode ({@code StoreBackedFieldRefresher}'s own one-second recheck window, keyed
 * per index rather than per field, which a same-index probe field would silently absorb) to a test whose job
 * is the generation check, not the trigger that reaches it. {@code GatedMappingStrandedIndexTests}, T50's
 * mirror of this test on the write side, makes the same call for the same reason: reaching the seam directly
 * with everything around it real is what that task settled for over standing up two disagreeing nodes, and
 * this task settles for it over chasing an unrelated mapping feature.
 *
 * <h2>What it asserts, and what it does not</h2>
 *
 * That a read for a field this shard has never merged fails outright while the descriptor plane claims a
 * generation the store cannot back, through the real registered {@link StoreBackedFieldRefresher} against a
 * live shard's real {@link MapperService} and the real store the plugin registers -- rather than the
 * field being silently reported absent, which is what {@link StoreBackedFieldRefresher#refresh} did before
 * T59, and which would have sent a caller to infer the field fresh, replacing whatever the descriptor
 * claims with whatever it happened to see. Once the descriptor plane is consistent again -- the second half
 * of the test, standing in for the stale node's cache catching up -- the identical read succeeds and merges
 * the field the store actually has, which is the fix's other half: a refusal at the inconsistency, not a
 * standing block on the index.
 *
 * <p>This closes the read T59 owns: the only production caller of the descriptor-aware read is {@code
 * StoreBackedFieldRefresher}, reached from a document's unknown field during parsing. Nothing in this
 * codebase consults the mapping store when a search query runs, so there is no search-time symptom for this
 * mechanism to close -- the round's recurring "returns zero hits, successfully" shape is what silently
 * reporting a field absent here would eventually produce, once whatever indexes next relies on the store's
 * false "empty" answer rather than the descriptor's true one.
 */
public class GatedMappingMissingWindowIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private static final String INDEX = "missing-window-target";

    private static volatile java.nio.file.Path sharedBasePath;

    private java.nio.file.Path basePath() {
        if (sharedBasePath == null) {
            synchronized (GatedMappingMissingWindowIT.class) {
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
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath().toString())
            // What wires DescriptorGate.install, and with it the real MappingGenerationStore and
            // AbsentIndexDescriptorSuppliers registrations -- the point of this class over
            // StoreBackedFieldRefresherIT, which never installs the plugin at all.
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_NODE_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @After
    public void clearRegistrations() {
        AbsentIndexDescriptorSuppliers.register(null);
    }

    private MapperService mapperServiceFor(String index) {
        for (IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = indicesService.indexService(resolveIndex(index));
            if (indexService != null) {
                return indexService.mapperService();
            }
        }
        throw new AssertionError("no MapperService for [" + index + "]");
    }

    public void testAReadFailsInTheWindowAndSucceedsOnceResolutionCatchesUp() throws Exception {
        createIndex(INDEX);
        ensureGreen(INDEX);
        String uuid = resolveIndex(INDEX).getUUID();
        MapperService mapperService = mapperServiceFor(INDEX);

        // What the store holds for this index: nothing declared, which since T58 means its descriptor at
        // generation 0 with no fields rather than an absent document. That is the same state T47's prune
        // leaves behind, and the shape that made this test fail after T58 rather than the test being wrong:
        // the guard it drives checked for a null answer, and the descriptor-backed store answers for every
        // index that resolves. The check is stated over the generation now, so this window is reachable
        // again by the path production actually takes.
        //
        // What a stale-cache node's descriptor plane still says: generation 1, exactly what a node that
        // resolved this name moments before a still-in-flight prune, or that has simply never been told
        // fields were declared and pruned, would show. DescriptorGate.install already registered the real
        // resolver above; this replaces it, standing in for the one node whose view has not caught up, the
        // same way GatedMappingStrandedIndexTests replaces the write side's resolver for T50.
        AbsentIndexDescriptorSuppliers.register(candidate -> INDEX.equals(candidate) ? liveDescriptorAtGeneration(uuid, 1L) : null);

        MappingGenerationStore.MissingMappingException failure = expectThrows(
            MappingGenerationStore.MissingMappingException.class,
            () -> new StoreBackedFieldRefresher().refresh(mapperService, uuid, "amount")
        );
        assertTrue(
            "the failure must name the inconsistency between the descriptor and the store: " + failure.getMessage(),
            failure.getMessage().contains("descriptor claims mapping generation")
        );

        // The stale node's cache catches up: nothing else about the index changed, only what its descriptor
        // resolution says. The identical read against the real store now succeeds, because there is
        // genuinely nothing declared to be missing -- proving the fix is a refusal at the inconsistency
        // rather than a standing block on the index.
        AbsentIndexDescriptorSuppliers.register(null);

        assertFalse(
            "once resolution is consistent again, a field the store genuinely does not have is reported " + "absent rather than refused",
            new StoreBackedFieldRefresher().refresh(mapperService, uuid, "amount")
        );

        // And the same is true for a field the store actually declares: the fix does not stand in its way.
        MappingGenerationStore.updateMapping(uuid, java.util.Map.of("region", "keyword"));
        assertTrue(
            "a field the store genuinely has must still merge",
            new StoreBackedFieldRefresher().refresh(mapperService, uuid, "region")
        );
        assertNotNull(mapperService.documentMapper().mappers().getMapper("region"));
    }

    private static IndexDescriptor liveDescriptorAtGeneration(String uuid, long mappingGeneration) {
        return new IndexDescriptor(
            INDEX,
            uuid,
            1,
            0,
            true,
            IndexDescriptor.State.OPEN,
            List.of(),
            org.opensearch.Version.CURRENT.id,
            false,
            false,
            false,
            false,
            mappingGeneration,
            0L
        );
    }
}
