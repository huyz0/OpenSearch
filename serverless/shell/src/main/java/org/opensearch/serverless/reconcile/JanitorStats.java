/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.reconcile;

/** What the janitor has done since the node started, and the store requests its passes made. */
public record JanitorStats(long passes, long storeRequests, long examined, long released, long replays, long claimsForgotten) {
}
