/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexDescriptor;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Point operations from one backend, prefix searches from another.
 *
 * <h2>Why this shape and not a swap</h2>
 *
 * "Move the descriptors to the object store" reads like replacing one implementation with another, and T3
 * measured why it is not. Eight of {@link DescriptorStore}'s eleven operations are point operations on a
 * named descriptor and map onto blob primitives. Three are prefix searches, and no object store answers
 * those: a bucket has no inverted index, and a LIST over the population is a rebuild path rather than a
 * query path.
 *
 * <p>So the object store takes the eight and something with an index over names keeps the three. Today that
 * is the system index; once the name index tier serves prefix resolution it is that instead, and only the
 * second constructor argument changes. That is the ordering constraint the two SPIs were split to express,
 * now expressed as an object rather than as a comment.
 *
 * <h2>What this buys immediately</h2>
 *
 * The whole point of the exercise: the descriptor's durability moves to the object store while wildcards
 * keep working. Without it, switching backends means losing prefix resolution, which is why nothing had
 * switched and why the blob backend had been written, tested and never once used to serve a request.
 *
 * <p>It also means the system index stops being the source of truth without being deleted. It still holds
 * descriptors, because the prefix half writes them; what changes is which copy a point read believes. That
 * is deliberate for a first cut, since it makes the switch reversible: the index still has everything.
 */
public final class CompositeDescriptorBackend implements DescriptorBackend, DescriptorPrefixBackend {

    private final DescriptorBackend points;
    private final DescriptorPrefixBackend prefixes;

    public CompositeDescriptorBackend(DescriptorBackend points, DescriptorPrefixBackend prefixes) {
        this.points = points;
        this.prefixes = prefixes;
    }

    // ---------------------------------------------------------------- point operations

    @Override
    public IndexDescriptor get(String name) {
        return points.get(name);
    }

    @Override
    public boolean create(IndexDescriptor descriptor) {
        return points.create(descriptor);
    }

    @Override
    public CompletableFuture<Boolean> createAsync(IndexDescriptor descriptor) {
        return points.createAsync(descriptor);
    }

    @Override
    public void put(IndexDescriptor descriptor) {
        points.put(descriptor);
    }

    @Override
    public void putAsync(IndexDescriptor descriptor) {
        points.putAsync(descriptor);
    }

    @Override
    public void putTombstoneAsync(IndexDescriptor tombstone) {
        points.putTombstoneAsync(tombstone);
    }

    /**
     * Both halves have to be ready, because a cluster that can resolve a name but not a wildcard is not
     * usable and would report itself healthy.
     */
    @Override
    public boolean available() {
        return points.available() && prefixAvailable();
    }

    private boolean prefixAvailable() {
        return prefixes instanceof DescriptorBackend ? ((DescriptorBackend) prefixes).available() : true;
    }

    // ---------------------------------------------------------------- prefix operations

    @Override
    public List<IndexDescriptor> findByPrefix(String prefix, String afterName, int size) {
        return prefixes.findByPrefix(prefix, afterName, size);
    }

    @Override
    public List<AbsentIndexDescriptorSuppliers.PagedIndex> findNamesForPage(
        String afterName,
        long afterCreationDate,
        boolean ascending,
        int size
    ) {
        return prefixes.findNamesForPage(afterName, afterCreationDate, ascending, size);
    }

    @Override
    public AbsentIndexDescriptorSuppliers.PrefixExpansion expandPrefix(String prefix, int limit) {
        return prefixes.expandPrefix(prefix, limit);
    }

    /**
     * The half that answers prefix queries, so a caller that must write through both can reach it.
     *
     * <p>Needed because a point write to the object store leaves the prefix half not knowing the name
     * exists, and until the name index serves prefix resolution that half is a real index that has to be
     * told. See {@code DescriptorGate}, which dual-writes for exactly this reason.
     */
    public DescriptorPrefixBackend prefixBackend() {
        return prefixes;
    }
}
