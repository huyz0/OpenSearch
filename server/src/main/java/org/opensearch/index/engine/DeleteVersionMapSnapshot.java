/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;

/**
 * Bounded, process-local recent-delete state for a quiescent clone of the exact same Lucene history.
 * Original native timestamps retain their meaning only with the same thread-pool clock. This is not
 * a durable or cross-node format and never replaces the pinned index commit or operation history.
 */
@InternalApi
public final class DeleteVersionMapSnapshot {
    final ShardId shard;
    final String historyUUID;
    final long checkpoint;
    final ThreadPool clock;
    final boolean gcDeletesEnabled;
    final long gcDeletesInMillis;
    final List<Entry> entries;
    private final long retainedBytes;

    // The UID arrays never escape the engine package. Restore copies them into its own native map.
    record Entry(byte[] uid, long version, long sequenceNumber, long term, long time) {
    }

    DeleteVersionMapSnapshot(
        ShardId shard,
        String historyUUID,
        long checkpoint,
        ThreadPool clock,
        boolean gcDeletesEnabled,
        long gcDeletesInMillis,
        List<Entry> entries,
        long retainedBytes
    ) {
        this.shard = shard;
        this.historyUUID = historyUUID;
        this.checkpoint = checkpoint;
        this.clock = clock;
        this.gcDeletesEnabled = gcDeletesEnabled;
        this.gcDeletesInMillis = gcDeletesInMillis;
        this.entries = List.copyOf(entries);
        this.retainedBytes = retainedBytes;
    }

    /**
     * Returns the number of retained recent delete identities.
     * @return retained entry count
     */
    public int size() {
        return entries.size();
    }

    /**
     * Returns the conservative reservation charged while this snapshot is resident.
     * @return charged resident bytes
     */
    public long retainedBytes() {
        return retainedBytes;
    }
}
