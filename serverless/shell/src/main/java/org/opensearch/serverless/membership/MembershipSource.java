/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.membership;

import java.io.Closeable;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Where a node learns who else exists.
 *
 * <p>This deliberately replaces rather than reuses {@code DiscoveryPlugin}, whose three hooks are all
 * Coordinator concepts — seed hosts for unicast pinging, a join validator taking a {@code ClusterState},
 * and election strategies. The question they answer is "who might I ping to find a cluster to join,"
 * which a serverless node never asks. See {@code rfc-serverless-shell.md} §10.2.
 *
 * <p><b>Membership is not a decision.</b> Two nodes holding different member sets forever is the normal
 * state of the system, not a fault: no safety property reads this. Shard ownership is arbitrated
 * per-shard by compare-and-swap on the shard-head, never by agreement about who exists (§10.1).
 *
 * <p><b>A node existing is not a node owning anything.</b> Whatever this returns, ownership of a
 * specific shard is answered only by that shard's head.
 */
public interface MembershipSource extends Closeable {

    /**
     * Returns membership as of the last refresh, without performing one.
     *
     * @return the nodes currently believed alive
     */
    Set<NodeLease> current();

    /**
     * Registers a listener for membership changes. Sources that cannot push may only invoke it on
     * an explicit refresh.
     *
     * @param listener called with each change
     */
    void subscribe(Consumer<MembershipDelta> listener);

    /**
     * Recomputes membership now, notifying subscribers if it changed.
     *
     * @return the freshly computed membership
     * @throws Exception if the underlying source cannot be read
     */
    Set<NodeLease> refresh() throws Exception;

    /**
     * Refreshes only if the snapshot is older than the given age, otherwise returns it as it stands.
     *
     * @param maxAgeMillis how old a snapshot may be before it is re-read
     * @return the live members
     * @throws Exception if the source cannot be read
     */
    default Set<NodeLease> refreshIfOlderThan(long maxAgeMillis) throws Exception {
        return refresh();
    }
}
