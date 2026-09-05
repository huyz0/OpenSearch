/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.cluster.ShardAssignment;

import java.util.Collection;
import java.util.List;

/**
 * What one node needs to know, read from the object store.
 *
 * <p>Deliberately not "the cluster state". It contains only the indices this node owns a shard of,
 * which is the property that makes residency proportional to a node's working set rather than to the
 * number of indices in the world — the ceiling
 * {@code plan-area-h-metadata-off-cluster-state.md} measured and the cell-diet design failed to get
 * under from inside the old shell.
 */
public final class Truth {

    private final Collection<IndexDescriptor> descriptors;
    private final Collection<ShardAssignment> assignments;

    /**
     * Creates a truth snapshot.
     *
     * @param descriptors the indices this node hosts
     * @param assignments the shards it owns
     */
    public Truth(Collection<IndexDescriptor> descriptors, Collection<ShardAssignment> assignments) {
        this.descriptors = List.copyOf(descriptors);
        this.assignments = List.copyOf(assignments);
    }

    /**
     * Returns the descriptors this node needs.
     *
     * @return the descriptors
     */
    public Collection<IndexDescriptor> descriptors() {
        return descriptors;
    }

    /**
     * Returns the shards this node owns.
     *
     * @return the assignments
     */
    public Collection<ShardAssignment> assignments() {
        return assignments;
    }

    @Override
    public String toString() {
        return "Truth[" + descriptors.size() + " indices, " + assignments.size() + " shards]";
    }
}
