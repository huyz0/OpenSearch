/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.core.action.ActionListener;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Makes a deletion's tombstone durable before the deletion is acknowledged.
 *
 * <p>The tombstone is what stops a resurrection: a node partitioned during a delete rejoins holding shard
 * data for an index cluster state no longer mentions, and without a durable no it imports that data back.
 * For a gated index there is no cluster state entry and no graveyard entry behind it, so the tombstone is
 * the only record there is.
 *
 * <p><b>Why this exists separately from the publisher.</b> The publish hook runs inside cluster state
 * construction, where a blocking write deadlocks against the index operation it issues, so tombstone writes
 * had to be asynchronous and best-effort. Retries close transient failures but not a crash between the
 * cluster state commit and the last attempt landing. Inferring the tombstone instead was considered and
 * rejected: {@code DanglingIndicesState.isDeleted} deliberately does not treat a missing descriptor as a
 * deletion, because a store that is unavailable and a store with no record give the same answer, and
 * discarding live shard data on that basis is worse than the failure it would fix.
 *
 * <p>So durability has to come from ordering rather than inference. This runs after the cluster state is
 * committed and before the client is told the delete succeeded, which is the one window where a write can
 * be both off the cluster state thread and ahead of the acknowledgement.
 *
 * <p><b>Nothing blocks.</b> The listener is deferred rather than waited on: the acknowledgement is sent
 * when the write completes, from whatever thread completes it. That is what lets this be durable without
 * reintroducing the deadlock that made the publish path asynchronous in the first place.
 *
 * <p>With nothing registered, deletion behaves exactly as it did before this existed.
 */
public final class DurableTombstones {

    /** Writes tombstones for deleted indices, completing the listener when they are durable. */
    @FunctionalInterface
    public interface Writer {
        /**
         * @param deleted    the indices whose deletion has just been committed to cluster state
         * @param whenStored completed once the tombstones are durable, or failed if they cannot be
         */
        void writeAll(List<IndexMetadata> deleted, ActionListener<Void> whenStored);
    }

    private static final AtomicReference<Writer> WRITER = new AtomicReference<>();

    private DurableTombstones() {}

    /** Installs the writer. Registering null clears it, restoring the previous behaviour exactly. */
    public static void register(Writer writer) {
        WRITER.set(writer);
    }

    public static boolean isRegistered() {
        return WRITER.get() != null;
    }

    /**
     * Runs {@code onDurable} once the tombstones for {@code deleted} are stored, or immediately when no
     * writer is installed.
     *
     * <p>A failure completes the listener exceptionally rather than silently succeeding. That is the
     * opposite of the scale-down bug this project found and left alone, where a failed operation reported
     * success because the failure path returned the unchanged state. A delete whose tombstone could not be
     * written has not achieved what a delete promises, and saying so is the whole point of doing the write
     * before the acknowledgement rather than after it.
     */
    public static void whenDurable(List<IndexMetadata> deleted, ActionListener<Void> onDurable) {
        Writer writer = WRITER.get();
        if (writer == null || deleted.isEmpty()) {
            onDurable.onResponse(null);
            return;
        }
        try {
            writer.writeAll(deleted, onDurable);
        } catch (Exception e) {
            onDurable.onFailure(e);
        }
    }
}
