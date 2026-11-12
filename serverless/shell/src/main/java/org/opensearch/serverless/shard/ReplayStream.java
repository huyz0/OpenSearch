/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shard;

import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Streams the records a writer open replays, for {@link ShardReconciler}: the shard's own fenced log, then its node
 * log's records, then any handed in for the open.
 */
final class ReplayStream {

    private ReplayStream() {}

    static void forEach(
        ShardReconciler r,
        ShardId shardId,
        org.opensearch.common.CheckedConsumer<org.opensearch.serverless.store.WalRecord, IOException> consumer
    ) throws IOException {
        final var log = r.pendingReplay.get(shardId);
        if (log == null) {
            return;
        }
        final long readingAt = System.nanoTime();
        try {
            log.forEachReplayableAfterFencing(record -> {
                consumer.accept(record);
                r.logRecordsRead.incrementAndGet();
            });
            // After the shard's own log, which holds what was written before the node log was: oldest first.
            final ShardReconciler.NodeLogReplay fromNodeLog = r.pendingNodeLogReplay.get(shardId);
            final ShardReconciler.NodeLogHooks hooks = r.nodeLog;
            if (fromNodeLog != null && hooks != null) {
                final org.opensearch.common.CheckedConsumer<org.opensearch.serverless.store.WalRecord, IOException> counting = record -> {
                    consumer.accept(record);
                    r.logRecordsRead.incrementAndGet();
                };
                // Oldest writer first, so a record a later writer replayed and wrote again comes after the original.
                final java.util.List<String> writers = fromNodeLog.writers().isEmpty()
                    ? java.util.Collections.singletonList(null)
                    : fromNodeLog.writers();
                for (String writer : writers) {
                    r.nodeLogReplayed.merge(shardId, hooks.replay(shardId, writer, fromNodeLog.term(), counting), Long::sum);
                }
            }
            // Last: records a caller handed in for this open, newer than anything the shard's logs hold.
            final java.util.List<org.opensearch.serverless.store.WalRecord> preloaded = r.preloadedReplay.remove(shardId);
            if (preloaded != null) {
                for (org.opensearch.serverless.store.WalRecord record : preloaded) {
                    consumer.accept(record);
                }
                // Counted as node-log replay, so the open publishes them before it serves: no log of this shard holds them.
                r.nodeLogReplayed.merge(shardId, (long) preloaded.size(), Long::sum);
            }
        } finally {
            r.writerOpenNanos[ShardReconciler.WRITER_OPEN_PHASES.indexOf("log_read")].addAndGet(System.nanoTime() - readingAt);
        }
    }
}
