/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.reconcile;

/** How many times each driver has run. */
public final class SchedulerCounts {

    private final long renewals;
    private final long publishes;
    private final long activations;
    private final long backstops;

    SchedulerCounts(long renewals, long publishes, long activations, long backstops) {
        this.renewals = renewals;
        this.publishes = publishes;
        this.activations = activations;
        this.backstops = backstops;
    }

    /**
     * Returns how many lease renewals have run.
     *
     * @return the count
     */
    public long renewals() {
        return renewals;
    }

    /**
     * Returns how many edge-triggered publishes have run.
     *
     * @return the count
     */
    public long publishes() {
        return publishes;
    }

    /**
     * Returns how many failure-triggered activation passes have run.
     *
     * @return the count
     */
    public long activations() {
        return activations;
    }

    /**
     * Returns how many full backstop passes have run.
     *
     * @return the count
     */
    public long backstops() {
        return backstops;
    }

    @Override
    public String toString() {
        return "SchedulerCounts[renewals="
            + renewals
            + ", publishes="
            + publishes
            + ", activations="
            + activations
            + ", backstops="
            + backstops
            + "]";
    }
}
