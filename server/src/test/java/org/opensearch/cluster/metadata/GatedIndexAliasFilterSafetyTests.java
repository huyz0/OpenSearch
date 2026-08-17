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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonList;
import static org.mockito.Mockito.mock;

/**
 * Phase A2 of core-pluggability-refactor-plan.md.
 *
 * <p>{@code MetadataIndexAliasesService}'s gated branch let an index acquire an alias post-creation while
 * storing only the name -- {@link IndexDescriptor#aliases()} is a bare {@code List<String>}, with nowhere
 * to record a filter, a routing value, or a write-index flag. Meanwhile {@link Metadata} and {@link
 * IndexNameExpressionResolver#filteringAliases} both treated "this index has no cluster-state entry" as
 * "this index needs no filter enforcement." Put together: a filtered alias against a gated index looked
 * like it succeeded and its filter was never enforced anywhere. This is the regression test for closing
 * that gap by refusing the operation that creates the inconsistency, rather than by trying to enforce a
 * filter this index type has nowhere to store.
 */
public class GatedIndexAliasFilterSafetyTests extends OpenSearchTestCase {

    private final AliasValidator aliasValidator = new AliasValidator();
    private final MetadataDeleteIndexService deleteIndexService = mock(MetadataDeleteIndexService.class);
    private final MetadataIndexAliasesService service = new MetadataIndexAliasesService(
        mock(ClusterService.class),
        null,
        aliasValidator,
        deleteIndexService,
        xContentRegistry()
    );

    @After
    public void clearRegistrations() {
        AbsentIndexDescriptorSuppliers.register(null);
        IndexDescriptorPublisher.registerUpdater(null);
    }

    private static final String GATED_INDEX = "gated-index";

    private void registerGatedDescriptor() {
        IndexMetadata metadata = IndexMetadata.builder(GATED_INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, GATED_INDEX + "-uuid")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexDescriptor descriptor = IndexDescriptor.from(metadata);
        AbsentIndexDescriptorSuppliers.register(name -> GATED_INDEX.equals(name) ? descriptor : null);
    }

    public void testPlainAliasOnGatedIndexIsAccepted() {
        registerGatedDescriptor();
        AtomicReference<IndexDescriptor> updated = new AtomicReference<>();
        IndexDescriptorPublisher.registerUpdater(descriptor -> {
            updated.set(descriptor);
            return CompletableFuture.completedFuture(Boolean.TRUE);
        });

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        // No filter, no routing, no write-index -- must still work exactly as before this fix.
        service.applyAliasActions(state, singletonList(new AliasAction.Add(GATED_INDEX, "plain-alias", null, null, null, null, null)));

        assertNotNull("a name-only alias on a gated index must still be recorded", updated.get());
        assertEquals(List.of("plain-alias"), updated.get().aliases());
    }

    public void testFilteredAliasOnGatedIndexIsRefused() {
        registerGatedDescriptor();
        IndexDescriptorPublisher.registerUpdater(descriptor -> CompletableFuture.completedFuture(Boolean.TRUE));

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> service.applyAliasActions(
                state,
                singletonList(
                    new AliasAction.Add(GATED_INDEX, "filtered-alias", "{\"term\":{\"tenant\":\"a\"}}", null, null, null, null)
                )
            )
        );
        assertTrue(e.getMessage().contains("filter"));
    }

    public void testRoutedAliasOnGatedIndexIsRefused() {
        registerGatedDescriptor();
        IndexDescriptorPublisher.registerUpdater(descriptor -> CompletableFuture.completedFuture(Boolean.TRUE));

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        expectThrows(
            IllegalArgumentException.class,
            () -> service.applyAliasActions(
                state,
                singletonList(new AliasAction.Add(GATED_INDEX, "routed-alias", null, "shard-1", null, null, null))
            )
        );
    }

    public void testWriteIndexAliasOnGatedIndexIsRefused() {
        registerGatedDescriptor();
        IndexDescriptorPublisher.registerUpdater(descriptor -> CompletableFuture.completedFuture(Boolean.TRUE));

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        expectThrows(
            IllegalArgumentException.class,
            () -> service.applyAliasActions(
                state,
                singletonList(new AliasAction.Add(GATED_INDEX, "write-alias", null, null, null, Boolean.TRUE, null))
            )
        );
    }

    public void testRemovingAnAliasFromAGatedIndexIsUnaffected() {
        registerGatedDescriptor();
        // Seed the descriptor with an alias already on it, as if a previous plain Add had succeeded.
        IndexMetadata metadata = IndexMetadata.builder(GATED_INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, GATED_INDEX + "-uuid")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        AbsentIndexDescriptorSuppliers.register(
            name -> GATED_INDEX.equals(name) ? IndexDescriptor.from(metadata).withAliases(List.of("plain-alias")) : null
        );
        AtomicReference<IndexDescriptor> updated = new AtomicReference<>();
        IndexDescriptorPublisher.registerUpdater(descriptor -> {
            updated.set(descriptor);
            return CompletableFuture.completedFuture(Boolean.TRUE);
        });

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        service.applyAliasActions(state, singletonList(new AliasAction.Remove(GATED_INDEX, "plain-alias", true)));

        assertNotNull(updated.get());
        assertTrue("the alias must be gone after removal", updated.get().aliases().isEmpty());
    }
}
