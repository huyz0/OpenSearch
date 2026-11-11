/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.reconcile;

/**
 * What the activation queue is doing: how many wait, how many run, and how long they have waited and run.
 *
 * @param queued activations waiting for a slot
 * @param running activations running
 * @param done activations finished since the node started
 * @param waitMillis their total time in the queue
 * @param runMillis their total time running
 * @param maxWaitMillis the longest wait since the last time these were read
 * @param maxRunMillis the longest run since the last time these were read
 * @param limit how many may run at once now
 */
public record ActivationStats(int queued, int running, long done, long waitMillis, long runMillis, long maxWaitMillis, long maxRunMillis,
    int limit) {
}
