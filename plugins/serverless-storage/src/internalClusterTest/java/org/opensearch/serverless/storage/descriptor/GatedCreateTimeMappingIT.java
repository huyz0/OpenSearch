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
 * <h2>What it does now, in two steps</h2>
 *
 * <b>First it refused</b>, in the same place and for the same reason as the other rules: an index the
 * descriptor could not represent kept its cluster state entry. That stopped the loss and left a hole big
 * enough to make the feature useless, since a deployment giving every tenant an explicit mapping got no
 * gated indices at all -- which is the case this design exists for. A refusal is a way of not losing data,
 * not a way of supporting something.
 *
 * <p><b>Then it carried them.</b> The declared fields are written to {@code MappingGenerationStore} during
 * gated creation, which is where a gated index's mapping lives anyway once documents start arriving, and
 * the refusal narrowed to what genuinely cannot round-trip.
 *
 * <p>Two orderings carry the correctness. The mapping is written before the descriptor, because the
 * descriptor is the acknowledgement: if the mapping write fails the creation fails and no index exists,
 * where the reverse order would leave a name that resolves to an index whose declared fields are missing.
 * And the extraction returns nothing at all rather than a partial map when some property will not fit,
 * because carrying the fields that do fit and dropping the rest is the original silent loss one level in.
 *
 * <p>What still cannot be carried is an object or nested field, since the store holds a flat map of field
 * name to type and a property with its own properties has nowhere to go in it. That limit is the mapping
 * store's shape rather than the descriptor's, and lifting it means changing what the store holds.
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

    /**
     * A mapping of plain typed fields is carried into the mapping store, and the index is still gated.
     *
     * <p>The first version of this asserted the opposite: that such an index kept its cluster state entry,
     * because refusing was the only thing standing between an accepted mapping and a discarded one. That
     * stopped the loss and left a hole big enough to make the feature useless -- a deployment giving every
     * tenant an explicit mapping got no gated indices at all, which is the case this design exists for.
     *
     * <p>So the fields now go to {@code MappingGenerationStore} at creation, which is where a gated index's
     * mapping lives, and the refusal narrows to what genuinely cannot round-trip.
     */
    public void testAMappingOfPlainFieldsIsCarriedAndTheIndexStaysGated() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("gated-mapped").settings(gated())
                    .mapping(Map.of("properties", Map.of("tenant", Map.of("type", "keyword"), "amount", Map.of("type", "double"))))
            )
            .actionGet();

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertNull(
            "an index whose mapping can be carried field by field must still be gated, or carrying it " + "bought nothing",
            state.metadata().index("gated-mapped")
        );

        // The fields themselves, from the store that now holds them. Asserting the index is gated proves
        // only that it took the fast path; asserting the types proves the mapping was not lost on the way,
        // which is the whole question.
        String uuid = descriptorUuid("gated-mapped");
        var generation = org.opensearch.cluster.metadata.MappingGenerationStore.currentMapping(uuid);
        assertNotNull("the declared fields must be in the mapping store after a gated creation", generation);
        assertEquals("keyword", generation.fields().get("tenant"));
        assertEquals("double", generation.fields().get("amount"));
    }

    /**
     * An object field is still refused, because the store has nowhere to put it.
     *
     * <p>The relaxation is to what round-trips, not to mappings in general.
     * {@code MappingGenerationStore} holds a flat map of field name to type, so a property with its own
     * properties has no representation there. Carrying the fields that do fit and dropping the rest would
     * be the original silent loss one level further in.
     */
    public void testAnObjectFieldIsStillRefused() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("gated-object").settings(gated())
                    .mapping(Map.of("properties", Map.of("nested", Map.of("properties", Map.of("inner", Map.of("type", "keyword"))))))
            )
            .actionGet();

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexMetadata metadata = state.metadata().index("gated-object");
        assertNotNull("an index with an object field must keep its cluster state entry rather than losing it", metadata);
        assertNotNull("and its mapping must still be there", metadata.mapping());
        assertTrue("including the object field: " + metadata.mapping().source(), metadata.mapping().source().toString().contains("inner"));
    }

    /** The descriptor's uuid for a gated name, which is the key the mapping store is written under. */
    private String descriptorUuid(String name) throws Exception {
        var descriptor = org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.supply(name);
        assertNotNull("the gated index must resolve through the descriptor seam", descriptor);
        return descriptor.uuid();
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
