/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class LogicalStreamReplayTests extends OpenSearchTestCase {
    private static final ShardLogId STREAM = new ShardLogId(new UUID(1, 2), "index-uuid", 0, new UUID(3, 4));
    private static final UUID FIRST = new UUID(5, 6);
    private static final UUID SECOND = new UUID(7, 8);

    public void testOnlyAcceptedEffectsSurviveTakeoverAndPhysicalCursorSkips() throws Exception {
        final var owner = command(LogicalCommand.Action.OWNERSHIP, FIRST, 0, -1);
        final var schema = command(LogicalCommand.Action.SCHEMA, FIRST, 0, 0);
        final var mutation = command(LogicalCommand.Action.MUTATION, FIRST, 0, 3);
        final var takeover = command(LogicalCommand.Action.OWNERSHIP, SECOND, 1, 4);
        final var duplicate = new LogicalCommand(
            mutation.action(),
            mutation.requestId(),
            mutation.firstObservedIndex(),
            mutation.requestDigest(),
            SECOND,
            1,
            new byte[] { 99 }
        );
        final var batches = new ArrayDeque<RatisJournal.ScanBatch>();
        batches.add(new RatisJournal.ScanBatch(List.of(record(0, owner, 1, 1, 0)), 2, 10));
        batches.add(new RatisJournal.ScanBatch(List.of(record(3, schema, 1, 1, 1), record(4, mutation, 1, 1, 1)), 4, 10));
        batches.add(
            new RatisJournal.ScanBatch(
                List.of(
                    record(6, takeover, 2, 2, 1),
                    record(8, duplicate, 2, 2, 1),
                    record(9, command(LogicalCommand.Action.MUTATION, FIRST, 1, 8), 1, 1, 1)
                ),
                10,
                10
            )
        );
        batches.add(new RatisJournal.ScanBatch(List.of(), 10, 10));
        final var cursors = new ArrayList<Long>();
        final var replay = new LogicalStreamReplay((after, entries, bytes) -> {
            cursors.add(after);
            assertEquals(16, entries);
            assertEquals(4096, bytes);
            return batches.removeFirst();
        }, STREAM, 8, 4096);
        assertEquals(1, replay.poll(16, 4096).effects().size());
        assertEquals(2, replay.poll(16, 4096).effects().size());
        final var last = replay.poll(16, 4096);
        assertEquals(1, last.effects().size());
        assertEquals(6, last.effects().getFirst().checkpoint().index());
        assertEquals(LogicalStreamState.Status.DUPLICATE, last.receipts().get(1).decision().status());
        assertEquals(4, last.receipts().get(1).decision().originalCheckpoint().index());
        assertEquals(LogicalStreamState.Rejection.STALE_EPOCH, last.receipts().get(2).decision().rejection());
        assertEquals(SECOND, last.state().owner());
        assertEquals(0, last.state().lastSequenceNumber());
        final var empty = replay.poll(16, 4096);
        assertTrue(empty.effects().isEmpty());
        assertTrue(empty.receipts().isEmpty());
        assertEquals(last.state(), empty.state());
        assertEquals(List.of(-1L, 2L, 4L, 10L), cursors);
        expectThrows(UnsupportedOperationException.class, () -> last.effects().clear());
        expectThrows(UnsupportedOperationException.class, () -> last.receipts().clear());
    }

    public void testIdentityLookupUsesItsFoldedPrefixWithoutScanningOrRetainingPayloads() throws Exception {
        final var calls = new AtomicInteger();
        final var owner = command(LogicalCommand.Action.OWNERSHIP, FIRST, 0, -1);
        final var schema = command(LogicalCommand.Action.SCHEMA, FIRST, 0, 0);
        final var mutation = command(LogicalCommand.Action.MUTATION, FIRST, 0, 1);
        final var replay = new LogicalStreamReplay((after, entries, bytes) -> {
            calls.incrementAndGet();
            return new RatisJournal.ScanBatch(
                List.of(record(0, owner, 1, 1, 0), record(1, schema, 1, 1, 1), record(2, mutation, 1, 1, 1)),
                8,
                8
            );
        }, STREAM, 8, 4096);
        final var unseen = replay.knownOutcome(
            mutation.requestId(),
            mutation.action(),
            mutation.firstObservedIndex(),
            mutation.requestDigest()
        );
        assertTrue(unseen.outcome().isEmpty());
        assertEquals(-1, unseen.throughIndex());
        assertEquals(0, calls.get());
        replay.poll(16, 4096);
        final var found = replay.knownOutcome(
            mutation.requestId(),
            LogicalCommand.Action.OUTCOME_ONLY,
            mutation.firstObservedIndex(),
            mutation.requestDigest()
        );
        assertEquals(8, found.throughIndex());
        assertEquals(2, found.outcome().orElseThrow().originalCheckpoint().index());
        assertEquals(1, calls.get());
    }

    public void testLegacyHistoryAndRawInjectionCannotBecomeLogicalEffects() throws Exception {
        final var raw = new RatisJournal.CommittedOperation(
            new OperationEnvelope(STREAM, OperationEnvelope.Kind.DOCUMENTS, 1, 1, 1, new byte[] { 7 }),
            new LogCheckpoint(STREAM, 0, 1)
        );
        final var owner = record(1, command(LogicalCommand.Action.OWNERSHIP, FIRST, 0, 0), 1, 1, 0);
        final var legacy = new LogicalStreamReplay(
            (after, entries, bytes) -> new RatisJournal.ScanBatch(List.of(raw, owner), 1, 1),
            STREAM,
            8,
            4096
        ).poll(16, 4096);
        assertTrue(legacy.effects().isEmpty());
        assertNull(legacy.state().owner());
        assertTrue(legacy.receipts().stream().allMatch(r -> r.decision().rejection() == LogicalStreamState.Rejection.LEGACY_HISTORY));
        final var claimed = record(0, command(LogicalCommand.Action.OWNERSHIP, FIRST, 0, -1), 1, 1, 0);
        final var injected = new RatisJournal.CommittedOperation(raw.operation(), new LogCheckpoint(STREAM, 1, 1));
        final var controlled = new LogicalStreamReplay(
            (after, entries, bytes) -> new RatisJournal.ScanBatch(List.of(claimed, injected), 1, 1),
            STREAM,
            8,
            4096
        ).poll(16, 4096);
        assertEquals(1, controlled.effects().size());
        assertEquals(FIRST, controlled.state().owner());
        assertEquals(-1, controlled.state().lastSequenceNumber());
    }

    public void testFailureAfterPartialEvaluationPermanentlyStopsTheCursor() throws Exception {
        final var owner = record(0, command(LogicalCommand.Action.OWNERSHIP, FIRST, 0, -1), 1, 1, 0);
        final var calls = new AtomicInteger();
        final var replay = new LogicalStreamReplay((after, entries, bytes) -> {
            calls.incrementAndGet();
            return new RatisJournal.ScanBatch(List.of(owner, owner), 1, 1);
        }, STREAM, 8, 4096);
        expectThrows(IOException.class, () -> replay.poll(16, 4096));
        assertTrue(expectThrows(IOException.class, () -> replay.poll(16, 4096)).getMessage().contains("earlier failure"));
        assertEquals(1, calls.get());
        expectThrows(IOException.class, () -> replay.knownOutcome(UUID.randomUUID(), LogicalCommand.Action.OWNERSHIP, -1, new byte[32]));
    }

    public void testMissingHistoryStopsInsteadOfSkippingToTheCurrentPrefix() throws Exception {
        final var calls = new AtomicInteger();
        final var replay = new LogicalStreamReplay((after, entries, bytes) -> {
            calls.incrementAndGet();
            assertEquals(-1, after);
            throw new IOException("retained history is missing");
        }, STREAM, 8, 4096);
        assertEquals("retained history is missing", expectThrows(IOException.class, () -> replay.poll(16, 4096)).getMessage());
        expectThrows(IOException.class, () -> replay.poll(16, 4096));
        assertEquals(1, calls.get());
    }

    public void testInvalidCallerBoundsDoNotScanOrStopValidReplay() throws Exception {
        final var calls = new AtomicInteger();
        final var replay = new LogicalStreamReplay((after, entries, bytes) -> {
            calls.incrementAndGet();
            return new RatisJournal.ScanBatch(List.of(), -1, -1);
        }, STREAM, 8, 4096);
        expectThrows(IllegalArgumentException.class, () -> replay.poll(0, 4096));
        expectThrows(IllegalArgumentException.class, () -> replay.poll(4097, 4096));
        expectThrows(IllegalArgumentException.class, () -> replay.poll(16, 0));
        assertEquals(0, calls.get());
        assertTrue(replay.poll(16, 4096).effects().isEmpty());
        assertEquals(1, calls.get());
    }

    public void testUncommittedOrOverflowingScanProgressIsRefused() throws Exception {
        for (var batch : List.of(
            new RatisJournal.ScanBatch(List.of(), 2, 1),
            new RatisJournal.ScanBatch(List.of(), Long.MAX_VALUE, Long.MAX_VALUE)
        )) {
            final var replay = new LogicalStreamReplay((after, entries, bytes) -> batch, STREAM, 8, 4096);
            expectThrows(IOException.class, () -> replay.poll(16, 4096));
        }
    }

    private static LogicalCommand command(LogicalCommand.Action action, UUID actor, long argument, long observed) {
        return new LogicalCommand(
            action,
            UUID.randomUUID(),
            observed,
            new byte[32],
            actor,
            argument,
            action == LogicalCommand.Action.OWNERSHIP ? new byte[0] : new byte[] { 7 }
        );
    }

    private static RatisJournal.CommittedOperation record(long index, LogicalCommand command, long epoch, long term, long schema)
        throws IOException {
        return new RatisJournal.CommittedOperation(
            new OperationEnvelope(STREAM, OperationEnvelope.Kind.LOGICAL, epoch, term, schema, command.encode(4096)),
            new LogCheckpoint(STREAM, index, 1)
        );
    }
}
