/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

/**
 * How many writer nodes the fleet has and how many its demand wants: the number to scale on.
 *
 * @param members live writer nodes that report their load
 * @param wanted the nodes the demand of the last minute needs at {@link NodeCapacity#TARGET_UTILISATION}, or -1 if no member
 *     reports a load
 * @param demand shards used or turned away across those nodes in the last minute
 */
public record FleetSize(int members, int wanted, long demand) {
}
