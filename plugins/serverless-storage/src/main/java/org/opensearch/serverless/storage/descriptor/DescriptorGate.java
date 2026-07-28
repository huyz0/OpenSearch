/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.stats.GatedMappingStatsAggregator;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexDescriptorPublisher;
import org.opensearch.cluster.metadata.MappingGenerationStore;

import java.util.List;

/**
 * Installs the descriptor read path, which was built and never connected.
 *
 * <p>H2e built the resolution seam and H8a taught {@code IndexNameExpressionResolver} to consult it. H17
 * built the pagination seam. Neither has ever had a supplier registered outside a test, so in production the
 * fallback returned null every time and a gated index could not be named by any request. The mechanism was
 * correct and unreachable, which is the failure this area has now shipped three times, twice caught inside a
 * single method (H7a, H8a) and once across the whole area.
 *
 * <p>Modelled on {@code ComputedPlacementGate}, which is the one installer in this plugin that already
 * works. Installing is the whole of the wiring: the registries are static, each node fills the gap locally,
 * and there is nothing to subscribe to or keep in sync.
 *
 * <p><b>Uninstalling matters and is easy to forget.</b> The registries outlive any one node, so a node that
 * closes without clearing them leaves a dead {@code Client} answering resolution for whatever runs next. In
 * a test JVM that is every subsequent suite.
 */
public final class DescriptorGate {

    private static final Logger logger = LogManager.getLogger(DescriptorGate.class);

    private static final java.util.concurrent.atomic.AtomicReference<DescriptorStore> STORE =
        new java.util.concurrent.atomic.AtomicReference<>();

    private DescriptorGate() {}

    /**
     * Resolves a name through the store, except the store's own index.
     *
     * <p><b>Without this the first resolution miss on a fresh cluster recurses until the stack overflows.</b>
     * The store reads descriptors by issuing a get against the descriptor index, and that get resolves an
     * index name, and resolution consults this supplier, which reads the descriptor index. The loop closes
     * only when the descriptor index is absent from cluster state, which is exactly the state a cluster is
     * in before its first gated index exists.
     *
     * <p>It was found by the control asserting that a missing name still raises, not by the test asserting
     * the feature works: the feature test had already created a descriptor, so the descriptor index was in
     * cluster state and resolution never reached the supplier. A gate installed on a fresh node would have
     * killed the first request that named anything unknown.
     *
     * <p>The guard is a name comparison rather than a re-entrancy flag because the recursion is not
     * incidental. The descriptor index is the one index whose metadata cannot come from descriptors, and
     * saying so once is clearer than detecting it dynamically.
     */
    private static IndexDescriptor supplyExceptForTheStoreItself(String name) {
        if (DescriptorStore.DESCRIPTOR_INDEX.equals(name)) {
            return null;
        }
        DescriptorStore store = STORE.get();
        return store == null ? null : store.get(name);
    }

    /**
     * Installs resolution and pagination against {@code store}, or does nothing when disabled.
     *
     * @param enabled whether gated indices are in use at all, so an ordinary cluster is untouched
     */
    public static void install(
        DescriptorStore store,
        MappingGenerationStore.Store mappingStore,
        GatedMappingStatsAggregator.Aggregator statsAggregator,
        boolean enabled
    ) {
        if (enabled == false) {
            return;
        }
        // The store is published before the supplier that reads it, so no resolution can observe a
        // registered supplier backed by a null store.
        STORE.set(store);
        AbsentIndexDescriptorSuppliers.register(DescriptorGate::supplyExceptForTheStoreItself);
        AbsentIndexDescriptorSuppliers.registerPager(pagerFor(store));
        // The write path. H2b dual-writes the descriptor at creation and H4 records deletions as
        // tombstones, and neither has ever had a publisher registered, so no index creation outside a test
        // has written a descriptor. Both go through put rather than create: publish records an index that
        // already exists, and a tombstone deliberately overwrites the live descriptor rather than racing
        // it. The put-if-absent path is create(), which is the uniqueness gate for gated creation (H3) and
        // a different question from recording.
        //
        // Asynchronous because this hook runs on the cluster state thread: Metadata calls it while building
        // a cluster state, so a blocking write deadlocks against the index operation it issues. Registering
        // the blocking put hung the node instead of failing, which is how the constraint was found.
        // Tombstones take the retrying path: they are the one descriptor write that is not safe to lose,
        // since for a gated index there is no cluster state entry and no graveyard entry behind them.
        IndexDescriptorPublisher.register(descriptor -> {
            if (descriptor.exists()) {
                store.putAsync(descriptor);
            } else {
                store.putTombstoneAsync(descriptor);
            }
        });
        // Mappings, which H4c required to leave cluster state and which have had no backing store since
        // H6a proved the swap converges. Blocking is safe here, unlike the publish hook above: a mapping
        // update runs on a transport thread handling a put-mapping or a dynamic field inference, not on
        // the cluster state thread where W4 found that a blocking write deadlocks.
        MappingGenerationStore.register(mappingStore);
        // Cluster stats. H19 measured that _cluster/stats reports a plausible wrong number for a gated
        // population, and H20 built a seam through which per-index iteration cannot be expressed. This is
        // the aggregate it was waiting for: one query whose cost is set by the number of field types
        // rather than by the number of indices.
        GatedMappingStatsAggregator.register(statsAggregator);
        logger.info("descriptor resolution installed against [{}]", DescriptorStore.DESCRIPTOR_INDEX);
    }

    /** Clears both registrations, which a node shutting down must do. */
    public static void uninstall() {
        // Cleared in the reverse order, so the supplier is gone before the store it reads.
        AbsentIndexDescriptorSuppliers.register(null);
        AbsentIndexDescriptorSuppliers.registerPager(null);
        IndexDescriptorPublisher.register(null);
        MappingGenerationStore.register(null);
        GatedMappingStatsAggregator.register(null);
        STORE.set(null);
    }

    /**
     * A pager over the descriptor index.
     *
     * <p>The {@code size} argument is the point of the interface rather than a convenience: H16 measured
     * that pagination sorted the whole population to make one page, so a pager that fetched everything and
     * then trimmed would satisfy the type and reintroduce the cost. This asks the index for exactly the page.
     *
     * <p>Descending order is served by fetching ascending and reversing the page. That is correct only
     * because a page is bounded, and it is the honest trade: the alternative is a second sort direction in
     * the store for a case that pagination rarely uses. Recorded rather than hidden, since a caller paging
     * descending through a large population would page through it in ascending chunks.
     */
    private static AbsentIndexDescriptorSuppliers.DescriptorPager pagerFor(DescriptorStore store) {
        return (afterName, afterCreationDate, ascending, size) -> {
            List<IndexDescriptor> page = store.findByPrefix("", afterName, size);
            if (ascending) {
                return page;
            }
            List<IndexDescriptor> reversed = new java.util.ArrayList<>(page);
            java.util.Collections.reverse(reversed);
            return reversed;
        };
    }
}
