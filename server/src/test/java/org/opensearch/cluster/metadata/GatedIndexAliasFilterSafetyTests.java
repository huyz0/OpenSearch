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
    /**
     * Armed per test by {@link #installStore}, and handed to the service directly rather than through the
     * registry. The service captures its lifecycle at construction, and this field initialiser runs before
     * any test body does -- so reading the registry here would capture whatever was installed before the
     * test armed anything, which is nothing.
     */
    private final TestClaimedIndexLifecycle lifecycle = new TestClaimedIndexLifecycle();
    private final MetadataIndexAliasesService service = new MetadataIndexAliasesService(
        mock(ClusterService.class),
        null,
        aliasValidator,
        deleteIndexService,
        xContentRegistry(),
        lifecycle
    );

    @After
    public void clearRegistrations() {
        AbsentIndexDescriptorSuppliers.register(null);
    }

    private static final String GATED_INDEX = "gated-index";

    /**
     * Stands in for the descriptor store: the mutation is applied to what the store holds, which is the
     * point of the seam taking a mutation rather than a finished descriptor. A caller that built the new
     * descriptor from its own cached copy could not be told apart from one that read the store first, and
     * that is how a close reverted a concurrent mapping update.
     */
    private AtomicReference<IndexDescriptor> stored;

    private void installStore(IndexDescriptor initial) {
        stored = new AtomicReference<>(initial);
        lifecycle.updating((name, mutation) -> {
            if (GATED_INDEX.equals(name) == false) {
                return CompletableFuture.completedFuture(Boolean.FALSE);
            }
            stored.updateAndGet(mutation::apply);
            return CompletableFuture.completedFuture(Boolean.TRUE);
        });
    }

    private static IndexDescriptor gatedDescriptor() {
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
        return IndexDescriptor.from(metadata);
    }

    private void registerGatedDescriptor() {
        IndexDescriptor descriptor = gatedDescriptor();
        AbsentIndexDescriptorSuppliers.register(name -> GATED_INDEX.equals(name) ? descriptor : null);
    }

    public void testPlainAliasOnGatedIndexIsAccepted() {
        registerGatedDescriptor();
        installStore(gatedDescriptor());

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        // No filter, no routing, no write-index -- must still work exactly as before this fix.
        service.applyAliasActions(state, singletonList(new AliasAction.Add(GATED_INDEX, "plain-alias", null, null, null, null, null)));

        assertEquals(List.of("plain-alias"), stored.get().aliases());
    }

    /**
     * The alias is added to what the store holds, not to the copy the caller resolved.
     *
     * <p>This is the regression the mutation-shaped seam exists for. The resolved descriptor is a cached one
     * -- on the cluster state thread it is the cached one or nothing at all -- so anything that changed
     * since, a dynamic field most obviously, is absent from it. Writing that copy back reverted the field
     * while acknowledging the alias, and a document carrying the reverted field is then unqueryable on it.
     */
    public void testTheAliasIsAppliedToWhatTheStoreHoldsRatherThanToTheCallersCopy() {
        // What resolution answers with: no mapping, because it was cached before the field was added.
        registerGatedDescriptor();
        // What the store actually holds: the same index, one dynamic field further on.
        installStore(gatedDescriptor().withMapping(7L, java.util.Map.of("age", java.util.Map.of("type", "long"))));

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        service.applyAliasActions(state, singletonList(new AliasAction.Add(GATED_INDEX, "plain-alias", null, null, null, null, null)));

        assertEquals(List.of("plain-alias"), stored.get().aliases());
        assertEquals("the concurrent mapping update must survive the alias change", 7L, stored.get().mappingGeneration());
        assertEquals(java.util.Map.of("age", java.util.Map.of("type", "long")), stored.get().initialMapping());
    }

    /** Adding an alias that is already there writes nothing, rather than rewriting the descriptor. */
    public void testAddingAnAliasThatIsAlreadyThereIsANoOp() {
        registerGatedDescriptor();
        installStore(gatedDescriptor().withAliases(List.of("plain-alias")));
        IndexDescriptor before = stored.get();

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        service.applyAliasActions(state, singletonList(new AliasAction.Add(GATED_INDEX, "plain-alias", null, null, null, null, null)));

        assertSame("an idempotent request must not produce a write", before, stored.get());
    }

    public void testFilteredAliasOnGatedIndexIsRefused() {
        registerGatedDescriptor();
        installStore(gatedDescriptor());

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> service.applyAliasActions(
                state,
                singletonList(new AliasAction.Add(GATED_INDEX, "filtered-alias", "{\"term\":{\"tenant\":\"a\"}}", null, null, null, null))
            )
        );
        assertTrue(e.getMessage().contains("filter"));
    }

    public void testRoutedAliasOnGatedIndexIsRefused() {
        registerGatedDescriptor();
        installStore(gatedDescriptor());

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
        installStore(gatedDescriptor());

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
        // Seed the descriptor with an alias already on it, as if a previous plain Add had succeeded.
        IndexDescriptor withAlias = gatedDescriptor().withAliases(List.of("plain-alias"));
        AbsentIndexDescriptorSuppliers.register(name -> GATED_INDEX.equals(name) ? withAlias : null);
        installStore(withAlias);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        service.applyAliasActions(state, singletonList(new AliasAction.Remove(GATED_INDEX, "plain-alias", true)));

        assertTrue("the alias must be gone after removal", stored.get().aliases().isEmpty());
    }

    /**
     * With no updater installed the request fails rather than acknowledging.
     *
     * <p>A gated index has no cluster state entry, so an unrecorded alias change is not a change at all.
     * {@code updateGated} used to answer null there, which every call site read as success.
     */
    public void testAnAliasChangeWithNothingToRecordItFails() throws Exception {
        registerGatedDescriptor();
        TestClaimedIndexLifecycle.uninstall();

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        AtomicReference<List<CompletableFuture<Boolean>>> writes = new AtomicReference<>();
        service.applyAliasActions(
            state,
            singletonList(new AliasAction.Add(GATED_INDEX, "plain-alias", null, null, null, null, null)),
            writes::set
        );

        assertEquals(1, writes.get().size());
        assertTrue("the write must be reported as failed rather than absent", writes.get().get(0).isCompletedExceptionally());
    }
}
