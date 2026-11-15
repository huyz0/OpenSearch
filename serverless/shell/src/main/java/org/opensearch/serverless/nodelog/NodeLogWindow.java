/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.nodelog;

import org.opensearch.serverless.nodelog.NodeLogBlob.ShardKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** One window's records, by shard and term, and how writing them went. */
final class NodeLogWindow {
    record Key(ShardKey shard, long term) {
    }

    final long closesAtNanos;
    final Map<Key, List<WalRecord>> batches = new LinkedHashMap<>();
    final Map<ShardKey, IOException> failures = new HashMap<>();
    long bytes;
    int records;
    /** Closed before its bounds because the records after it would not fit: written as soon as it can be. */
    boolean closeEarly;
    boolean done;
    /** Counted down once {@link #done}, for writers that wait without the lock. */
    final java.util.concurrent.CountDownLatch landedLatch = new java.util.concurrent.CountDownLatch(1);
    /** Whether a writer already waits on it under the lock, and so will write it when due; guarded by the lock. */
    boolean driven;
    long flushStartedNanos;
    long doneNanos;
    NodeLogCommitter.Position landed;
    final List<Runnable> onLanded = new ArrayList<>();

    NodeLogWindow(long closesAtNanos) {
        this.closesAtNanos = closesAtNanos;
    }

    void add(ShardKey shard, long term, List<WalRecord> more) {
        batches.computeIfAbsent(new Key(shard, term), ignored -> new ArrayList<>()).addAll(more);
        records += more.size();
        for (WalRecord record : more) {
            bytes += WalRecord.contentBytes(record.id(), record.source());
        }
    }

    boolean full(long maxBytes, int maxRecords) {
        return closeEarly || bytes >= maxBytes || records >= maxRecords;
    }

    void failAll(IOException failure) {
        for (Key key : batches.keySet()) {
            failures.putIfAbsent(key.shard, failure);
        }
    }
}
