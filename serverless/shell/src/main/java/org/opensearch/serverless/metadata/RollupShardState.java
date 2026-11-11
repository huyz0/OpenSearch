/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.serverless.store.PruningDigest;

/** One shard's part of an entry: the term of a writer holding it, zero if none; and its digest, null if never published. */
public record RollupShardState(long ownerTerm, PruningDigest digest) {
}
