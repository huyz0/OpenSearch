/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

/**
 * How full this node is and what fullness has cost: the numbers to scale on.
 *
 * @param maxShardsHeld the cap
 * @param held shards held now
 * @param readerOpensRefused reader opens refused at the cap
 * @param activationsRefused writer activations refused at the cap
 * @param evicted shards given up to make room
 * @param handedOff shards given up for a member with room
 * @param writesSteered writes for an unheld shard sent to a member with room
 */
public record Capacity(int maxShardsHeld, int held, long readerOpensRefused, long activationsRefused, long evicted, long handedOff,
    long writesSteered) {
}
