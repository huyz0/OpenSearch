/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction;

import org.opensearch.serverless.storage.shardstate.ShardHead;

import java.util.Optional;

/**
 * Computes the {@link ShardHead} a compaction should publish, given the shard's actual current
 * head at CAS time (which may differ from what the compactor started with, if a writer or
 * another compactor published meanwhile). Merge inputs remain valid regardless &mdash; merged
 * segments are immutable &mdash; so recomputing the manifest against a newer base is cheap; it is
 * <em>not</em> re-running the merge, only re-deriving the file map
 * (rfc-serverless-opensearch.md &sect;7.4).
 */
@FunctionalInterface
public interface CompactionPublisher {

    /**
     * @param currentHead the shard's actual current head, as of this rebase attempt
     * @return the new head to attempt to publish, or empty to abandon this compaction (e.g. the
     *         shard has moved on far enough that the merge result is no longer worth publishing)
     */
    Optional<ShardHead> computeNewHead(ShardHead currentHead);
}
