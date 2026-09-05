/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.membership;

import java.util.Set;

/**
 * What changed between two observations of membership.
 *
 * <p>A delta is a hint, not an event log: a node that misses one still converges on the next refresh,
 * because {@link MembershipSource#current()} is always the full truth as observed.
 */
public final class MembershipDelta {

    private final Set<NodeLease> joined;
    private final Set<NodeLease> left;

    /**
     * Creates a delta between two observations.
     *
     * @param joined leases newly observed
     * @param left leases no longer observed or newly expired
     */
    public MembershipDelta(Set<NodeLease> joined, Set<NodeLease> left) {
        this.joined = Set.copyOf(joined);
        this.left = Set.copyOf(left);
    }

    /**
     * Returns the leases newly observed.
     *
     * @return the joined leases
     */
    public Set<NodeLease> joined() {
        return joined;
    }

    /**
     * Returns the leases that disappeared or expired.
     *
     * @return the departed leases
     */
    public Set<NodeLease> left() {
        return left;
    }

    /**
     * Reports whether this delta is empty.
     *
     * @return true when nothing changed
     */
    public boolean isEmpty() {
        return joined.isEmpty() && left.isEmpty();
    }

    @Override
    public String toString() {
        return "MembershipDelta[joined=" + joined.size() + ", left=" + left.size() + "]";
    }
}
