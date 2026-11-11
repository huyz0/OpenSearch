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
 * the metadata plane requires: losers read the winning head and route
 * to the winner, and must not retry activation elsewhere, which would just steal the shard back. An API
 * that returned a bare boolean would make the correct behaviour harder to write than the wrong one.
 */
public final class Acquisition {

    private final boolean acquired;
    private final ShardHead head;
    private final String previousOwner;

    private Acquisition(boolean acquired, ShardHead head, String previousOwner) {
        this.acquired = acquired;
        this.head = head;
        this.previousOwner = previousOwner;
    }

    /**
     * The caller now owns the shard, taken over from a head that named another owner, or this node's earlier process.
     *
     * @param head the head as written; its {@link ShardHead#writers} name every log the new owner replays
     * @param previousOwner the node the replaced head named, or null if it had none
     * @return the outcome
     */
    public static Acquisition won(ShardHead head, String previousOwner) {
        return new Acquisition(true, head, previousOwner);
    }

    /**
     * Returns the node the replaced head named as its owner.
     *
     * @return its id, or null if the head had no owner, or for a loss
     */
    public String previousOwner() {
        return previousOwner;
    }

    /**
     * The caller now owns the shard.
     *
     * @param head the head as written
     * @return the outcome
     */
    public static Acquisition won(ShardHead head) {
        return new Acquisition(true, head, null);
    }

    /**
     * Someone else owns the shard.
     *
     * @param head the winner's head, so the caller can route to it
     * @return the outcome
     */
    public static Acquisition heldByAnother(ShardHead head) {
        return new Acquisition(false, head, null);
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
