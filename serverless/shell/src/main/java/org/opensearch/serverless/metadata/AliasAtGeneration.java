/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

/**
 * An alias record together with the register generation it was read at.
 *
 * <p>The two come from one register read, and that is the whole point of the type: a generation read
 * separately from the value it is meant to protect guards nothing. Read the value, then read the
 * generation, and a writer that landed between the two hands the caller a fresh token for a stale
 * value -- its compare-and-swap succeeds and the other writer's acknowledged change is gone. Worse, a
 * delete between the two reads hands over a tombstone's generation and the alias is resurrected, and
 * an index created under the name in that window is overwritten by an alias record.
 *
 * @param alias the record as read
 * @param generation the generation to compare-and-swap against
 */
public record AliasAtGeneration(org.opensearch.serverless.cluster.AliasRecord alias, long generation) {
}
