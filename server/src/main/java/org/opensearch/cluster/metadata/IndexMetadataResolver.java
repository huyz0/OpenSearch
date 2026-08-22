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
 * A plugin-supplied fallback for resolving an index's {@link IndexMetadata} when it has no entry in
 * {@link Metadata}'s own index map.
 *
 * <p><b>Consulted from {@link Metadata#indexOrResolved(String)}, a separate, explicitly-named method --
 * not from {@link Metadata#index(String)} itself.</b> This is a correction made mid-session after an
 * earlier version of this design, which folded resolver consultation directly into {@code index(String)}
 * for every caller, caused a real, {@code internalClusterTest}-confirmed regression: {@code
 * MetadataDeleteIndexService#deleteIndices} relies on {@code index(String)}'s null-ness as a
 * <em>distinguishing signal</em> ("is this index gated, and does it need the durable tombstone-write path")
 * rather than a plain existence check, and auto-resolving broke that distinction silently.
 *
 * <p>The practical effect: a caller that wants the fallback (the call sites migrating off the
 * pre-existing static registry, previously {@code AbsentIndexDescriptorSuppliers}) must
 * call {@code indexOrResolved(String)} by name instead of the plain accessor. This is one extra word at
 * each of those call sites, in exchange for leaving every other caller of {@code index(String)} -- which is
 * nearly every read path in the codebase, the overwhelming majority never audited for the
 * distinguishing-signal pattern -- completely untouched, with no risk and no auditing required. A resolver
 * implementation itself does not need to know or care about this distinction; it only ever sees calls that
 * already want the fallback.
 *
 * <p>Registered via {@link org.opensearch.plugins.ClusterPlugin#getIndexMetadataResolver()}. Absent by
 * default, so an ordinary cluster with no such plugin installed resolves exactly as it always has --
 * {@link Metadata#indexOrResolved(String)} returns {@code null} for a genuinely nonexistent index either
 * way, same as {@link Metadata#index(String)} always has and still does.
 *
 * <p><b>Never invoked on a cluster-state-mutation thread, by construction.</b> {@link
 * Metadata#indexOrResolved(String)} checks {@link
 * org.opensearch.cluster.ClusterStateMutationThreads#blockingIsUnsafeOnCurrentThread()} before consulting a
 * resolver at all -- see that class's own javadoc for the deadlock this exists to prevent. This means a
 * {@code resolve} implementation does <em>not</em> need to detect or guard against being called from one of
 * those threads itself; core already guarantees it will not be. It is, however, still on the hook for every
 * other caller of {@code indexOrResolved(String)}, so {@code resolve} must still answer quickly (e.g. from a
 * resolver-owned local cache) rather than perform unbounded blocking I/O on an arbitrary request thread.
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
     *         {@code Metadata#indexOrResolved(String)} miss.
     */
    @Nullable
    IndexMetadata resolve(Metadata metadata, String indexName);
}
