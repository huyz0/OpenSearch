/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.reconcile;

/**
 * What one janitor pass did.
 *
 * @param examined shards looked at
 * @param released dead owners' shards given back with everything published, or released marks cleared
 * @param replays shards whose log was ahead of their commit, taken here to be replayed and published
 * @param claimsForgotten dead nodes' claims on shards whose head had moved on
 * @param bucket the rollup bucket read this pass, as group/bucket, or null if none was due here
 * @param full whether the pass used its whole budget, so more is waiting
 */
public record JanitorPass(int examined, int released, int replays, int claimsForgotten, String bucket, boolean full) {
}
