/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.nodelog;

import org.opensearch.serverless.nodelog.NodeLogBlob.Range;
import org.opensearch.serverless.nodelog.NodeLogBlob.ShardKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The shards whose records a node's log keeps readable past their publishes, for a caller that reads a shard's records
 * since a commit while the shard goes on publishing, such as a shard split. Guarded by the committer's lock, over the
 * committer's epochs.
 */
final class NodeLogPins {

    private final ReentrantLock lock;
    private final TreeMap<Long, NodeLogEpochState> epochs;
    private final Map<ShardKey, NodeLogPin> pins = new HashMap<>();
    /** Shards whose pin lapsed and has not been closed since: reading them fails rather than missing records. */
    private final java.util.Set<ShardKey> lapsed = new java.util.HashSet<>();

    NodeLogPins(ReentrantLock lock, TreeMap<Long, NodeLogEpochState> epochs) {
        this.lock = lock;
        this.epochs = epochs;
    }

    /** Pins a shard's records; see {@link NodeLogCommitter#pin}. */
    NodeLogPin pin(ShardKey shard, long maxMillis) {
        if (maxMillis < 1) {
            throw new IllegalArgumentException("a pin lasts at least 1 ms, not " + maxMillis);
        }
        lock.lock();
        try {
            final NodeLogPin pin = new NodeLogPin(this, shard, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxMillis));
            if (pins.putIfAbsent(shard, pin) != null) {
                throw new IllegalStateException(shard + " is already pinned");
            }
            // A new pin starts its own reckoning: what an earlier one lapsed over is before it.
            lapsed.remove(shard);
            return pin;
        } finally {
            lock.unlock();
        }
    }

    /** Returns every run of a shard the log keeps readable; see {@link NodeLogCommitter#heldRuns}. */
    Map<Long, List<Range>> heldRuns(ShardKey shard) throws IOException {
        lock.lock();
        try {
            expirePinsLocked();
            if (lapsed.contains(shard)) {
                throw new IOException("the pin on " + shard + "'s node-log records lapsed; records it kept may be gone");
            }
            final Map<Long, List<Range>> out = new TreeMap<>();
            for (Map.Entry<Long, NodeLogEpochState> entry : epochs.entrySet()) {
                final List<Range> runs = new ArrayList<>(entry.getValue().retained.getOrDefault(shard, List.of()));
                runs.addAll(entry.getValue().unpublished.getOrDefault(shard, List.of()));
                if (runs.isEmpty() == false) {
                    out.put(entry.getKey(), List.copyOf(runs));
                }
            }
            return out;
        } finally {
            lock.unlock();
        }
    }

    /** Lets go of the pins past their time, so the log can trim what they kept. Called with every lease renewal. */
    void expire() {
        lock.lock();
        try {
            expirePinsLocked();
        } finally {
            lock.unlock();
        }
    }

    private void expirePinsLocked() {
        final long now = System.nanoTime();
        for (NodeLogPin pin : List.copyOf(pins.values())) {
            if (now - pin.deadlineNanos >= 0) {
                lapsed.add(pin.shard);
                unpinLocked(pin);
            }
        }
    }

    private void unpinLocked(NodeLogPin pin) {
        if (pins.remove(pin.shard, pin)) {
            for (NodeLogEpochState state : epochs.values()) {
                state.retained.remove(pin.shard);
            }
        }
    }

    /** Whether the shard is pinned now. Caller holds the lock. */
    boolean pinnedLocked(ShardKey shard) {
        return pins.containsKey(shard);
    }

    /** Closes a pin: what it kept may be trimmed, and the shard no longer reports a lapse unless pinned again. */
    void close(NodeLogPin pin) {
        lock.lock();
        try {
            unpinLocked(pin);
            if (pins.containsKey(pin.shard) == false) {
                lapsed.remove(pin.shard);
            }
        } finally {
            lock.unlock();
        }
    }

    static List<Range> keptBelow(List<Range> runs, long limit) {
        final List<Range> out = new ArrayList<>(runs.size());
        for (Range run : runs) {
            if (run.from() >= limit) {
                continue;
            }
            out.add(run.to() < limit ? run : new Range(run.from(), limit - 1));
        }
        return out;
    }
}
