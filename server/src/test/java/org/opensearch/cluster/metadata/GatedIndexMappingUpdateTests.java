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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.Set;

/**
 * H4c. What a mapping update does to an index that has no cluster state entry.
 *
 * <p>This started as an optimisation and the probe turned it into a prerequisite. H3 and H5 let an index
 * exist with no metadata entry, and every mapping update path resolves its target through
 * {@code Metadata#getIndexSafe}, which throws when the index is not in the map. So a gated index cannot
 * accept a document carrying a new field: dynamic mapping fails at the first unknown field rather than
 * degrading.
 *
 * <p>That inverts the priority. Moving mappings to a compare-and-swap on an object-store generation was
 * filed as a way to remove a global serialisation point from the write path, which is true and secondary.
 * The reason to do it is that without it H3 and H5 are not usable for any index whose mapping is not
 * fully known at creation, which is most of them.
 *
 * <p>The test asserts the failure rather than a hypothetical fix, so it documents the gap in a form that
 * fails the day someone closes it. That is the same shape as the assertion H2c used to pin the stale
 * descriptor before H4d fixed it.
 */
public class GatedIndexMappingUpdateTests extends OpenSearchTestCase {

    @After
    public void clearRegistrations() {
        DescriptorOnlyCreation.register(null);
        IndexDescriptorPublisher.register(null);
    }

    /**
     * The gap, stated as a passing test. A gated index is absent from metadata, and resolving it for a
     * mapping update throws rather than finding it.
     */
    public void testAGatedIndexCannotBeResolvedForAMappingUpdate() {
        IndexDescriptorPublisher.register(descriptor -> {});
        DescriptorOnlyCreation.register(indexMetadata -> true);

        ClusterState state = MetadataCreateIndexService.clusterStateCreateIndex(
            ClusterState.builder(ClusterName.DEFAULT).build(),
            Set.of(),
            index("gated-idx"),
            (current, reason) -> current,
            null
        );

        assertFalse("the premise: a gated index has no metadata entry", state.metadata().hasIndex("gated-idx"));

        // Every mapping update path resolves its target this way before merging anything.
        expectThrows(IndexNotFoundException.class, () -> state.getMetadata().getIndexSafe(new Index("gated-idx", "gated-idx-uuid")));
    }

    /**
     * The control. An ordinary index resolves, so the failure above is about the gate rather than about
     * the resolution mechanism being broken generally.
     */
    public void testAnOrdinaryIndexResolvesForAMappingUpdate() {
        ClusterState state = MetadataCreateIndexService.clusterStateCreateIndex(
            ClusterState.builder(ClusterName.DEFAULT).build(),
            Set.of(),
            index("ordinary-idx"),
            (current, reason) -> current,
            null
        );

        IndexMetadata resolved = state.getMetadata().getIndexSafe(state.metadata().index("ordinary-idx").getIndex());

        assertEquals("an ordinary index must still resolve for a mapping update", "ordinary-idx", resolved.getIndex().getName());
    }

    private static IndexMetadata index(String name) {
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
}
