/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.store.WalGroupCommitter;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.serverless.store.WalStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class OperationAppenderTests extends OpenSearchTestCase {

    public void testAsyncSnapshotAndCancellationDoNotDiscardAcceptedWrites() {
        final var tasks = new ArrayDeque<Runnable>();
        final var persisted = new ArrayList<WalRecord>();
        final OperationAppender appender = (term, records) -> {
            assertEquals(7L, term);
            persisted.addAll(records);
        };
        final List<WalRecord> input = new ArrayList<>(List.of(new WalRecord("id", "{}")));
        final var completion = appender.appendAsync(7, input, tasks::add);
        input.clear();
        assertTrue(completion.toCompletableFuture().cancel(false));
        assertTrue(persisted.isEmpty());
        tasks.remove().run();
        assertEquals(List.of("id"), persisted.stream().map(WalRecord::id).toList());
        // Cancellation applied only to the derived future; the operation completed durably.
        completion.toCompletableFuture().join();
    }

    public void testExecutorRejectionNeverAttemptsAppend() {
        final var calls = new AtomicInteger();
        final OperationAppender appender = (term, records) -> calls.incrementAndGet();
        final var rejected = new RejectedExecutionException("full");
        final var future = appender.appendAsync(1, List.of(new WalRecord("id", "{}")), task -> { throw rejected; }).toCompletableFuture();
        assertSame(rejected, expectThrows(CompletionException.class, future::join).getCause());
        assertEquals(0, calls.get());
    }

    public void testAppendFailurePreservesIdentity() {
        final IOException failure = new IOException("ambiguous append");
        final OperationAppender appender = (term, records) -> { throw failure; };
        final var future = appender.appendAsync(1, List.of(new WalRecord("id", "{}")), Runnable::run).toCompletableFuture();
        assertSame(failure, expectThrows(CompletionException.class, future::join).getCause());
    }

    public void testFatalAppendCompletesWaitersAndStillEscapesExecutor() {
        final var tasks = new ArrayDeque<Runnable>();
        final AssertionError failure = new AssertionError("fatal append");
        final OperationAppender appender = (term, records) -> { throw failure; };
        final var future = appender.appendAsync(1, List.of(new WalRecord("id", "{}")), tasks::add).toCompletableFuture();
        assertSame(failure, expectThrows(AssertionError.class, () -> tasks.remove().run()));
        assertSame(failure, expectThrows(CompletionException.class, future::join).getCause());
    }

    public void testS3ModeUsesTheSameAppendAndStreamingRecoveryBoundary() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var wal = new WalStore(store, BlobPath.cleanPath().add("shard"));
            wal.establish(1, () -> true);
            final DurableOperationLog log = wal;
            final List<WalRecord> expected = List.of(new WalRecord("first", "{}", 0, 1, 1), WalRecord.deletion("second", 1, 1, 1));
            new WalGroupCommitter().commitTo(new ShardId("index", "index-uuid", 0), 1, expected, log);
            final List<WalRecord> recovered = new ArrayList<>();
            log.replay(recovered::add);
            assertEquals(expected.size(), recovered.size());
            for (int i = 0; i < expected.size(); i++) {
                assertEquals(expected.get(i).toBytes(), recovered.get(i).toBytes());
            }
        }
    }
}
