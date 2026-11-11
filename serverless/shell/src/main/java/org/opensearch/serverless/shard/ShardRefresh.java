/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shard;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;

import java.io.IOException;

/**
 * Applies a changed mapping or changed settings to the shards {@link ShardReconciler} holds open, without closing them.
 */
public final class ShardRefresh {

    private final ShardReconciler reconciler;

    ShardRefresh(ShardReconciler reconciler) {
        this.reconciler = reconciler;
    }

    /**
     * Applies a changed mapping to whatever this node already has open for an index.
     *
     * <p><b>Why this is needed at all, rather than letting the next reconcile pass do it.</b> The pass opens
     * shards that are missing and closes shards that are no longer ours; it has never had a reason to notice
     * that an index it already holds says something different than it did. A mapping update is the first
     * change to an index that a node must apply to a shard it is already serving, and the node that accepted
     * the update is the one node guaranteed to be holding stale metadata the instant it succeeds.
     *
     * <p>Core does the applying: {@code IndexService#updateMapping} merges under
     * {@code MergeReason.MAPPING_RECOVERY} and gates on the mapping version, which is why the descriptor
     * carries one. A node with nothing open for this index has nothing to do — it will read the new
     * descriptor when it next opens a shard, which is the only moment the mapping matters to it.
     *
     * @param indexName the index whose mapping changed
     * @param descriptor the index as it now stands
     * @throws IOException if core rejects the new mapping
     */
    public void refreshMapping(String indexName, org.opensearch.serverless.cluster.IndexDescriptor descriptor) throws IOException {
        // A descriptor belongs to one incarnation. Name-only lookup can select a still-open service
        // from before delete/recreate, and scanning every held shard also makes each refresh fleet-sized.
        final IndexService indexService = reconciler.indicesService.indexService(new Index(indexName, descriptor.uuid()));
        if (indexService == null) {
            return;
        }
        if (indexService.getMetadata().getMappingVersion() >= descriptor.mappingVersion()) {
            // Already holding this mapping or a newer one. Core asserts that a mapping re-applied at the
            // same version is byte-identical to what it holds, and the descriptor's rendering is not.
            return;
        }
        indexService.updateMapping(indexService.getMetadata(), descriptor.toIndexMetadata(java.util.Map.of()));
    }

    /**
     * Applies changed settings to whatever this node already has open for an index.
     *
     * <p>The counterpart of {@link #refreshMapping}, and needed for the same reason: the reconcile pass opens
     * and closes shards, and has never had a reason to notice that an index it already holds says something
     * different than it did. Core does the applying — {@code IndexService#updateMetadata} pushes the new
     * settings into {@code IndexSettings}, notifies each shard, and re-times the refresh and fsync tasks —
     * and it gates on the settings version, which is why the descriptor carries one.
     *
     * @param indexName the index whose settings changed
     * @param descriptor the index as it now stands
     * @throws IOException if the new metadata cannot be built
     */
    public void refreshSettings(String indexName, org.opensearch.serverless.cluster.IndexDescriptor descriptor) throws IOException {
        // A descriptor belongs to one incarnation. Name-only lookup can select a still-open service
        // from before delete/recreate, and scanning every held shard also makes each refresh fleet-sized.
        final IndexService indexService = reconciler.indicesService.indexService(new Index(indexName, descriptor.uuid()));
        if (indexService == null) {
            return;
        }
        if (indexService.getMetadata().getSettingsVersion() >= descriptor.settingsVersion()) {
            return;
        }
        // The store type is how this node serves the index, not a property of the index; it travels with
        // the refreshed settings so the service's settings keep saying what its directory is.
        final IndexMetadata incoming = ShardReconciler.onBlockCache(descriptor.toIndexMetadata(java.util.Map.of()));
        // Under the index's monitor, the one a shard's open refreshes the terms under, so a term raised by an open
        // between reading the service's metadata and updating it is not lowered again here.
        final Index index = indexService.index();
        while (true) {
            final Object lock = reconciler.indexLocks.computeIfAbsent(index, ignored -> new Object());
            synchronized (lock) {
                if (reconciler.indexLocks.get(index) != lock) {
                    continue;
                }
                final IndexMetadata current = indexService.getMetadata();
                indexService.updateMetadata(current, ShardOpening.keepingHigherTerms(current, IndexMetadata.builder(incoming)));
                return;
            }
        }
    }
}
