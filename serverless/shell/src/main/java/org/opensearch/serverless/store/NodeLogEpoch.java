/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

/**
 * One epoch of a node's write log as its lease records it: what a successor needs to fence and replay it without a
 * listing.
 *
 * @param epoch the epoch
 * @param trimPoint the lowest slot that may still hold records not yet published; every slot below it can be deleted
 * @param landed a slot every slot below which is known taken, so a fence's search for the first free slot may start
 *     there; at least the trim point
 */
public record NodeLogEpoch(long epoch, long trimPoint, long landed) {

    /** Validates. */
    public NodeLogEpoch {
        if (epoch < 0) {
            throw new IllegalArgumentException("epoch must not be negative, got " + epoch);
        }
        if (trimPoint < 0 || landed < 0) {
            throw new IllegalArgumentException("trim point and landed hint must not be negative, got " + trimPoint + " and " + landed);
        }
    }
}
