/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A mapping given at creation must not be accepted and then thrown away.
 *
 * <h2>What was wrong</h2>
 *
 * A descriptor carries a {@code mappingGeneration} but not the mapping: mappings live in
 * {@code MappingGenerationStore}, and {@code IndexDescriptor.from} does not copy them.
 * {@code DescriptorRepresentable} refuses to gate an index whose declared state a descriptor cannot carry --
 * a hidden index, an index with any alias -- and had no such check for mappings.
 *
 * <p>So a gated index created with an explicit mapping had that mapping parsed, validated, built into
 * {@code IndexMetadata}, and then dropped on the floor when the descriptor was written. The creation was
 * acknowledged. Nothing failed. The fields simply were not there afterwards, and the first document to
 * arrive would infer them again from its own values -- which is the same wrong answer that looks like a
 * right one this area keeps producing, except that here it silently changes the field types the user asked
 * for.
 *
 * <h2>What it does now</h2>
 *
 * Refuses, in the same place and for the same reason as the other two: an index the descriptor cannot
 * represent keeps its cluster state entry and is created as an ordinary index. The mapping survives, the
 * index works, and the cost is that this one index does not get the gated path.
 *
 * <p><b>That is a real limitation, not a fix.</b> A multi-tenant deployment that gives every tenant an
 * explicit mapping would get no gated indices at all, which is the main use case this design exists for.
 * Carrying create-time mappings into {@code MappingGenerationStore} at creation is the change that would
 * make gating usable with them, and it is a larger one: the fields have to be extracted from the built
 * metadata, written through the store, and the generation reflected on the descriptor. Refusing first is
 * what stops data being lost while that is built.
 */
public class GatedCreateTimeMappingIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private volatile java.nio.file.Path sharedBasePath;

    private java.nio.file.Path basePath() {
        if (sharedBasePath == null) {
            synchronized (this) {
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
        return List.of(org.opensearch.serverless.storage.ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(
                org.opensearch.serverless.storage.ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(),
                basePath().toString()
            )
            .build();
    }

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    public void testAMappingGivenAtCreationSurvives() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("gated-mapped").settings(gated())
                    .mapping(Map.of("properties", Map.of("tenant", Map.of("type", "keyword"), "amount", Map.of("type", "double"))))
            )
            .actionGet();

        // Through cluster state, because an index the descriptor cannot represent keeps its entry there.
        // Asserting on the mapping rather than on which path it took, since the mapping is the thing that
        // must not be lost and the path is only how that is achieved.
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexMetadata metadata = state.metadata().index("gated-mapped");
        assertNotNull(
            "an index whose mapping a descriptor cannot carry must keep its cluster state entry rather than "
                + "being gated with the mapping discarded",
            metadata
        );
        assertNotNull("and the mapping it was created with must still be there", metadata.mapping());
        String source = metadata.mapping().source().toString();
        assertTrue("the declared keyword field must have survived: " + source, source.contains("tenant"));
        assertTrue("and so must the declared double field: " + source, source.contains("amount"));
    }

    /**
     * The control: with no mapping there is nothing a descriptor cannot carry, so gating still happens.
     *
     * <p>Without this, a refusal that rejected every gated creation would satisfy the test above while
     * turning the feature off entirely, which is the failure mode of every check written as a refusal.
     */
    public void testAnIndexWithNoMappingIsStillGated() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin().indices().create(new CreateIndexRequest("gated-plain").settings(gated())).actionGet();

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertNull(
            "an index with nothing a descriptor cannot represent must still be gated, or the refusal above "
                + "has switched the whole feature off",
            state.metadata().index("gated-plain")
        );
    }

    private static Settings gated() throws Exception {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }
}
