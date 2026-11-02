/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.serverless.log.LogCheckpoint;
import org.opensearch.serverless.log.LogicalCommand;
import org.opensearch.serverless.log.ShardLogId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PreparationDependencyGateTests extends OpenSearchTestCase {
    public void testResultOnlyStaysPendingThroughApplicationDespiteCancellation() throws Exception {
        final var tasks = new ArrayDeque<Runnable>();
        final var append = new CompletableFuture<LogCheckpoint>();
        final var expected = outcome(RestStatus.CONFLICT);
        final var proposal = proposal(expected);
        final var cp = checkpoint();
        final var prepared = new AtomicInteger();
        final var applied = new AtomicInteger();
        final var verified = new AtomicInteger();
        final var reference = new AtomicReference<PreparationDependencyGate<LogicalCommand, ResolvedResultOnly>>();
        final var gate = new PreparationDependencyGate<LogicalCommand, ResolvedResultOnly>(tasks::add, command -> {
            assertFalse(Thread.holdsLock(reference.get()));
            return append;
        }, (command, committed) -> {
            assertFalse(Thread.holdsLock(reference.get()));
            applied.incrementAndGet();
            return ResolvedResultOnly.decode(command.payloadReader().readAllBytes(), 4096);
        }, (command, committed, result) -> {
            assertFalse(Thread.holdsLock(reference.get()));
            assertEquals(proposal.requestId(), command.requestId());
            assertEquals(cp, committed);
            if (result.status() != expected.status() || Arrays.equals(result.json(), expected.json()) == false) {
                throw new IOException("different original result");
            }
            verified.incrementAndGet();
        });
        reference.set(gate);
        final var result = gate.submit(() -> {
            prepared.incrementAndGet();
            return proposal;
        }).toCompletableFuture();
        tasks.remove().run();
        assertEquals(PreparationDependencyGate.State.APPENDING, gate.state());
        assertTrue(result.cancel(true));
        assertFalse(append.isCancelled());
        expectThrows(
            ExecutionException.class,
            () -> gate.submit(() -> { throw new AssertionError("pending execution"); }).toCompletableFuture().get(10, TimeUnit.SECONDS)
        );
        append.complete(cp);
        assertEquals(PreparationDependencyGate.State.APPLYING, gate.state());
        assertEquals(0, applied.get());
        tasks.remove().run();
        assertEquals(1, prepared.get());
        assertEquals(1, applied.get());
        assertEquals(1, verified.get());
        assertEquals(PreparationDependencyGate.State.READY, gate.state());
        gate.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    public void testFailedResultVerificationCannotCompleteSuccessAndIsMandatory() throws Exception {
        final var expected = outcome(RestStatus.CONFLICT);
        final var wrong = outcome(RestStatus.BAD_REQUEST);
        final var gate = new PreparationDependencyGate<LogicalCommand, ResolvedResultOnly>(
            Runnable::run,
            command -> CompletableFuture.completedStage(checkpoint()),
            (command, cp) -> wrong,
            (command, cp, result) -> {
                if (result.status() != expected.status()) {
                    throw new IOException("wrong result status");
                }
            }
        );
        final var failure = expectThrows(
            ExecutionException.class,
            () -> gate.submit(() -> proposal(expected)).toCompletableFuture().get(10, TimeUnit.SECONDS)
        );
        assertTrue(failure.getCause() instanceof IOException);
        assertEquals(PreparationDependencyGate.State.FENCED, gate.state());
        expectThrows(
            ExecutionException.class,
            () -> gate.submit(() -> { throw new AssertionError("fenced execution"); }).toCompletableFuture().get(10, TimeUnit.SECONDS)
        );
        gate.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        expectThrows(
            NullPointerException.class,
            () -> new PreparationDependencyGate<LogicalCommand, ResolvedResultOnly>(
                Runnable::run,
                command -> CompletableFuture.completedStage(checkpoint()),
                (command, cp) -> expected,
                null
            )
        );
    }

    public void testCloseDrainsWorkWithoutCancelingPersistenceOrApplyingLateResult() throws Exception {
        final var append = new CompletableFuture<LogCheckpoint>();
        final var applied = new AtomicInteger();
        final var value = outcome(RestStatus.CONFLICT);
        final var gate = new PreparationDependencyGate<LogicalCommand, ResolvedResultOnly>(
            Runnable::run,
            command -> append,
            (command, cp) -> {
                applied.incrementAndGet();
                return value;
            },
            (command, cp, result) -> { throw new AssertionError("closed verification"); }
        );
        final var result = gate.submit(() -> proposal(value));
        gate.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertFalse(append.isCancelled());
        append.complete(checkpoint());
        assertEquals(0, applied.get());
        assertEquals(PreparationDependencyGate.State.CLOSED, gate.state());
        final var failure = expectThrows(ExecutionException.class, () -> result.toCompletableFuture().get(10, TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof IOException);
    }

    private static ResolvedResultOnly outcome(RestStatus status) throws IOException {
        return ResolvedResultOnly.rejected(status, "{\"error\":\"original\"}".getBytes(StandardCharsets.UTF_8), 4096);
    }

    private static LogicalCommand proposal(ResolvedResultOnly outcome) throws IOException {
        return new LogicalCommand(
            LogicalCommand.Action.OUTCOME_ONLY,
            UUID.randomUUID(),
            0,
            new byte[32],
            new UUID(5, 6),
            -1,
            outcome.encode(4096)
        );
    }

    private static LogCheckpoint checkpoint() {
        return new LogCheckpoint(new ShardLogId(new UUID(1, 2), "index", 0, new UUID(3, 4)), 1, 1);
    }
}
