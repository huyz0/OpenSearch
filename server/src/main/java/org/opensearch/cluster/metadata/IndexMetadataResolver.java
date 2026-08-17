/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Phase C of {@code core-pluggability-refactor-plan.md}: a plugin-supplied fallback for resolving an
 * index's {@link IndexMetadata} when it has no entry in {@link Metadata}'s own index map.
 *
 * <p>Consulted from exactly one place -- {@link Metadata#index(String)} -- on a lookup miss, so every
 * other core caller of {@code metadata.index(name)} (there were roughly twenty of them scattered across
 * {@code OperationRouting}, {@code IndexNameExpressionResolver}, the action layer, and more, before this
 * seam existed) needs no changes at all to correctly resolve an index a plugin manages the metadata for
 * outside cluster state. Before this interface existed, each of those call sites had to remember to call
 * a static registry (previously {@code AbsentIndexDescriptorSuppliers}) instead of the plain accessor --
 * this interface exists specifically to remove that "every caller has to know a second path exists"
 * failure mode, by moving the fallback into the one place every caller already goes through.
 *
 * <p>Registered via {@link org.opensearch.plugins.ClusterPlugin#getIndexMetadataResolver()}. Absent by
 * default, so an ordinary cluster with no such plugin installed resolves exactly as it always has --
 * {@link Metadata#index(String)} returns {@code null} for a genuinely nonexistent index either way.
 *
 * <p><b>Never invoked on a cluster-state-mutation thread, by construction.</b> {@link Metadata#index(String)}
 * checks {@link org.opensearch.cluster.ClusterStateMutationThreads#blockingIsUnsafeOnCurrentThread()} before
 * consulting a resolver at all -- see that class's own javadoc for the deadlock this exists to prevent. This
 * means a {@code resolve} implementation does <em>not</em> need to detect or guard against being called from
 * one of those threads itself; core already guarantees it will not be. It is, however, still on the hook for
 * every other caller of {@link Metadata#index(String)} -- which is nearly every read path in the codebase --
 * so {@code resolve} must still answer quickly (e.g. from a resolver-owned local cache) rather than perform
 * unbounded blocking I/O on an arbitrary request thread.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexMetadataResolver {

    /**
     * Called only when {@code metadata.getIndices().get(indexName)} already returned nothing, and never
     * from a thread where blocking would be unsafe -- see this interface's own javadoc.
     *
     * @param metadata  the {@link Metadata} the lookup missed against, for a resolver that needs
     *                  additional cluster-state context (e.g. to check whether the name collides with
     *                  something metadata already knows about).
     * @param indexName the index name that had no entry.
     * @return the resolved {@link IndexMetadata}, or {@code null} if this resolver has no answer either --
     *         which callers must treat identically to "the index does not exist," the same as any other
     *         {@code Metadata#index(String)} miss.
     */
    @Nullable
    IndexMetadata resolve(Metadata metadata, String indexName);
}
