/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

/**
 * The outcome of trying to acquire a shard.
 *
 * <p>A loser is told <em>who won</em>, not merely that it lost. That is deliberate and is the rule
 * section 6 of {@code rfc-serverless-metadata-plane.md} states: losers read the winning head and route
 * to the winner, and must not retry activation elsewhere, which would just steal the shard back. An API
 * that returned a bare boolean would make the correct behaviour harder to write than the wrong one.
 */
public final class Acquisition {

    private final boolean acquired;
    private final ShardHead head;

    private Acquisition(boolean acquired, ShardHead head) {
        this.acquired = acquired;
        this.head = head;
    }

    /**
     * The caller now owns the shard.
     *
     * @param head the head as written
     * @return the outcome
     */
    public static Acquisition won(ShardHead head) {
        return new Acquisition(true, head);
    }

    /**
     * Someone else owns the shard.
     *
     * @param head the winner's head, so the caller can route to it
     * @return the outcome
     */
    public static Acquisition heldByAnother(ShardHead head) {
        return new Acquisition(false, head);
    }

    /**
     * Reports whether the caller acquired the shard.
     *
     * @return true when acquired
     */
    public boolean acquired() {
        return acquired;
    }

    /**
     * Returns the authoritative head: the caller's if it won, the winner's if it lost.
     *
     * @return the head
     */
    public ShardHead head() {
        return head;
    }

    @Override
    public String toString() {
        return (acquired ? "won " : "lost to ") + head;
    }
}
