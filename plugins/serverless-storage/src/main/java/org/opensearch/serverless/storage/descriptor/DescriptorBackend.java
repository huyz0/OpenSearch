/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.IndexDescriptor;

import java.util.concurrent.CompletableFuture;

/**
 * Where descriptors are actually stored, separated from {@link DescriptorStore}'s caching so the two can
 * vary independently.
 *
 * <h2>Why this is only eight operations</h2>
 *
 * T3 audited {@link DescriptorStore}'s eleven public operations against what an object store can serve.
 * Eight are point operations on a single named descriptor and map onto blob primitives directly. Three are
 * prefix searches, and no object store answers those: a bucket has no inverted index, and a LIST over a
 * hundred million keys is a rebuild path rather than a query path.
 *
 * <p>So they are a separate interface, {@link DescriptorPrefixBackend}, rather than methods here that one
 * implementation throws from. The split is the point. It makes "the blob backend cannot resolve
 * {@code logs-*}" a fact the compiler knows, instead of a runtime surprise on the one code path that is
 * hardest to test, and it states the ordering constraint in the type system: the name index tier has to
 * serve prefix resolution before the descriptor index can be removed, even though the blob backend can be
 * added alongside it long before that.
 *
 * <h2>What stays above this</h2>
 *
 * Caching, TTL, eviction and in-flight request collapsing all live in {@link DescriptorStore}. A backend
 * does I/O and nothing else. That matters more under an object store than it did under a system index:
 * collapsing turns M concurrent readers of one cold descriptor into a single round trip, and duplicating
 * that per backend would mean two chances to get it subtly different.
 *
 * <h2>Absence and unavailability are different answers</h2>
 *
 * Every read here must distinguish "there is no such descriptor" from "I could not find out". Returning
 * null for the second reports every gated index in the cluster as non-existent, and a client acting on
 * that could create an index that already exists. Implementations throw
 * {@link org.opensearch.cluster.metadata.DescriptorUnavailableException} for the second case, which is the
 * contract {@code readFromIndex} already established.
 */
public interface DescriptorBackend {

    /**
     * The descriptor for an exact name, or null if there is none.
     *
     * <p>Must be read-your-writes against this backend: an index created a moment ago has to be nameable
     * immediately, which is why a miss is never cached above this and why an implementation must not serve
     * this from anything eventually consistent.
     *
     * @throws org.opensearch.cluster.metadata.DescriptorUnavailableException if the store could not be read
     */
    IndexDescriptor get(String name);

    /**
     * Records a descriptor for a name that must not already be taken, returning whether this call took it.
     *
     * <p>This is the uniqueness mechanism, and the reason it can be one store operation is the whole of
     * H3: the store says a name is taken, not the cluster manager. A lost race is a {@code false} rather
     * than an exception, because losing is a correct outcome rather than a failure.
     */
    boolean create(IndexDescriptor descriptor);

    /** {@link #create} without blocking the caller. */
    CompletableFuture<Boolean> createAsync(IndexDescriptor descriptor);

    /** Writes a descriptor whether or not the name is taken. */
    void put(IndexDescriptor descriptor);

    /** {@link #put} without blocking the caller. */
    void putAsync(IndexDescriptor descriptor);

    /**
     * Marks a name deleted durably.
     *
     * <p>Kept distinct from {@link #putAsync} of a tombstone-shaped descriptor because the two backends
     * put it in different places: the system index writes it as another document, and a blob backend has
     * to move it out from under the descriptor prefix entirely, or every prefix listing has to read each
     * key to find out whether it is alive.
     */
    void putTombstoneAsync(IndexDescriptor tombstone);

    /**
     * Whether the backing store is ready to be read and written.
     *
     * <p>Named for the question rather than for the mechanism, because the mechanisms do not resemble each
     * other. The system index has to exist, be allocated and carry the settings W8 established. A blob
     * backend has none of that: there is nothing to bootstrap, which is one of the four problems the
     * system index charged against it that simply does not arise.
     */
    boolean available();
}
