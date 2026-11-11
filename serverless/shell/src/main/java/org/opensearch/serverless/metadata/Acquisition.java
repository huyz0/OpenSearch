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
    private final boolean previousLetGo;

    private Acquisition(boolean acquired, ShardHead head, String previousOwner, boolean previousLetGo) {
        this.acquired = acquired;
        this.head = head;
        this.previousOwner = previousOwner;
        this.previousLetGo = previousLetGo;
    }

    /**
     * The caller now owns the shard, taken over from a head that named another owner, or this node's earlier process.
     *
     * @param head the head as written
     * @param previousOwner the node the replaced head named, or its last writer if it had no owner; null for neither
     * @param previousLetGo whether that node had let go of the shard, so its log needs no fence against further writes
     * @return the outcome
     */
    public static Acquisition won(ShardHead head, String previousOwner, boolean previousLetGo) {
        return new Acquisition(true, head, previousOwner, previousLetGo);
    }

    /**
     * Returns whether {@link #previousOwner} had let go of the shard rather than been taken from.
     *
     * @return true for a head that had been given back
     */
    public boolean previousLetGo() {
        return previousLetGo;
    }

    /**
     * Returns the node the replaced head named: the writer whose log may hold this shard's unpublished records.
     *
     * @return its id, or for a head that had been given back the last writer it recorded; null if neither, or for a loss
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
        return new Acquisition(true, head, null, false);
    }

    /**
     * Someone else owns the shard.
     *
     * @param head the winner's head, so the caller can route to it
     * @return the outcome
     */
    public static Acquisition heldByAnother(ShardHead head) {
        return new Acquisition(false, head, null, false);
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
