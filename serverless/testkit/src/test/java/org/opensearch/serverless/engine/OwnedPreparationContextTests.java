/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.log.LogCheckpoint;
import org.opensearch.serverless.log.ShardLogId;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

@SuppressFileSystems("*")
public class OwnedPreparationContextTests extends EngineTestCase {
    private static final long RESIDENT = 64L * 1024 * 1024;
    private static final long BUFFER = 4L * 1024 * 1024;

    public void testCloseWaitsForNativeWorkAndQueuedCleanupDespiteCancellation() throws Exception {
        for (boolean blockApplication : new boolean[] { false, true }) {
            final var entered = new CountDownLatch(1);
            final var release = new CountDownLatch(1);
            final var cleanup = new LinkedBlockingQueue<Runnable>(1);
            try (var fixture = fixture()) {
                final var dispatcher = dispatcher(fixture.group);
                final var context = new OwnedPreparationContext<String, String>(
                    dispatcher,
                    cleanup::add,
                    fixture.group,
                    shardId,
                    fixture.resources,
                    proposal -> CompletableFuture.completedFuture(checkpoint()),
                    (proposal, committed) -> {
                        if (blockApplication) {
                            entered.countDown();
                            await(release);
                        }
                        assertVisibleCount(fixture.resources.engine(), 1);
                        return proposal;
                    },
                    (proposal, committed, applied) -> assertEquals(proposal, applied)
                );
                try {
                    final var pending = context.submit(100, () -> {
                        fixture.resources.index(indexForDoc(createParsedDoc("private", null)));
                        if (blockApplication == false) {
                            entered.countDown();
                            await(release);
                        }
                        return "prepared";
                    });
                    assertTrue(entered.await(10, TimeUnit.SECONDS));
                    final var canceledView = context.closeAsync().toCompletableFuture();
                    assertTrue(canceledView.cancel(true));
                    assertFalse(context.closeAsync().toCompletableFuture().isDone());
                    assertTrue(cleanup.isEmpty());
                    assertEquals(new PreparationEngineAdmission.Stats(1, RESIDENT, 0), fixture.admission.stats());
                    assertVisibleCount(fixture.resources.engine(), 1);
                    release.countDown();
                    failure(IOException.class, pending);
                    final var closeJob = cleanup.poll(10, TimeUnit.SECONDS);
                    assertNotNull(closeJob);
                    assertFalse(context.closeAsync().toCompletableFuture().isDone());
                    assertEquals(new PreparationEngineAdmission.Stats(1, RESIDENT, 0), fixture.admission.stats());
                    // The request worker can drain independently; resident admission still owns cleanup.
                    dispatcher.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
                    closeJob.run();
                    context.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
                    assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), fixture.admission.stats());
                    assertFalse(Files.exists(fixture.generation));
                    expectThrows(IllegalStateException.class, fixture.resources::engine);
                    failure(OpenSearchRejectedExecutionException.class, context.submit(1, () -> "must not run"));
                } finally {
                    release.countDown();
                    context.closeAsync();
                    dispatcher.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
                    Runnable job;
                    while ((job = cleanup.poll()) != null) {
                        job.run();
                    }
                }
            }
        }
    }

    public void testLateQuorumReplyCannotApplyAfterOwnedNativeCleanup() throws Exception {
        final var appended = new CountDownLatch(1);
        final var append = new CompletableFuture<LogCheckpoint>();
        final var applied = new java.util.concurrent.atomic.AtomicInteger();
        final var cleanup = new LinkedBlockingQueue<Runnable>(1);
        try (var fixture = fixture()) {
            final var dispatcher = dispatcher(fixture.group);
            try {
                final var context = new OwnedPreparationContext<String, String>(
                    dispatcher,
                    cleanup::add,
                    fixture.group,
                    shardId,
                    fixture.resources,
                    proposal -> {
                        appended.countDown();
                        return append;
                    },
                    (proposal, committed) -> {
                        applied.incrementAndGet();
                        fixture.resources.engine();
                        return proposal;
                    },
                    (proposal, committed, result) -> assertEquals(proposal, result)
                );
                final var pending = context.submit(100, () -> "accepted-persistence");
                assertTrue(appended.await(10, TimeUnit.SECONDS));
                context.closeAsync();
                final var closeJob = cleanup.poll(10, TimeUnit.SECONDS);
                assertNotNull(closeJob);
                closeJob.run();
                context.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
                failure(IOException.class, pending);
                assertFalse(append.isCancelled());
                assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), fixture.admission.stats());
                assertTrue(append.complete(checkpoint()));
                assertEquals(0, applied.get());
                expectThrows(IllegalStateException.class, fixture.resources::engine);
                assertVisibleCount(fixture.serving, 0);
            } finally {
                dispatcher.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            }
        }
    }

    public void testRejectedUnknownAndInlineCleanupQuarantineWithoutNativeClose() throws Exception {
        for (int mode = 0; mode < 3; mode++) {
            final int fault = mode;
            final var queued = new LinkedBlockingQueue<Runnable>(1);
            final Executor executor = task -> {
                if (fault == 0) {
                    throw new RejectedExecutionException("cleanup rejected");
                }
                if (fault == 1) {
                    queued.add(task);
                    throw new AssertionError("cleanup accepted before scheduling failed");
                }
                task.run();
            };
            try (var fixture = fixture()) {
                final var nativeEngine = fixture.resources.engine();
                final var dispatcher = dispatcher(fixture.group);
                try {
                    final var context = context(dispatcher, executor, fixture);
                    failure(fault == 1 ? AssertionError.class : RejectedExecutionException.class, context.closeAsync());
                    assertEquals(new PreparationEngineAdmission.Stats(1, RESIDENT, 1), fixture.admission.stats());
                    assertTrue(Files.exists(fixture.generation));
                    expectThrows(IllegalStateException.class, fixture.resources::engine);
                    final var canceledWrapper = queued.poll();
                    if (canceledWrapper != null) {
                        canceledWrapper.run();
                    }
                    // No native cleanup ran on the caller, or after the uncertain handoff was refused.
                    assertVisibleCount(nativeEngine, 0);
                    assertTrue(Files.exists(fixture.generation));
                    assertEquals(new PreparationEngineAdmission.Stats(1, RESIDENT, 1), fixture.admission.stats());
                    expectThrows(
                        OpenSearchRejectedExecutionException.class,
                        () -> PrivatePreparationResources.openEmpty(
                            fixture.admission,
                            fixture.group,
                            fixture.serving,
                            createTempDir(),
                            RESIDENT,
                            BUFFER
                        )
                    );
                } finally {
                    dispatcher.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
                }
                // Fixture-only cleanup closes native handles; quarantined capacity cannot be reused.
                fixture.resources.close();
                assertEquals(new PreparationEngineAdmission.Stats(1, RESIDENT, 1), fixture.admission.stats());
            }
        }
    }

    public void testKnownNativeCloseSurvivesAnExecutorThrowAfterCompletion() throws Exception {
        try (var fixture = fixture()) {
            final var dispatcher = dispatcher(fixture.group);
            try {
                final Executor executor = task -> {
                    try {
                        threadPool.generic().submit(task).get(10, TimeUnit.SECONDS);
                    } catch (Exception failed) {
                        throw new AssertionError(failed);
                    }
                    throw new AssertionError("native cleanup already finished");
                };
                final var context = context(dispatcher, executor, fixture);
                context.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
                assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), fixture.admission.stats());
                assertFalse(Files.exists(fixture.generation));
            } finally {
                dispatcher.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            }
        }
    }

    public void testScopeOrBindingRefusalLeavesResourcesCallerOwned() throws Exception {
        try (var fixture = fixture()) {
            final var dispatcher = dispatcher(fixture.group);
            final var cleanup = new LinkedBlockingQueue<Runnable>(1);
            try {
                for (boolean wrongGroup : new boolean[] { false, true }) {
                    expectThrows(
                        IllegalArgumentException.class,
                        () -> new OwnedPreparationContext<String, String>(
                            dispatcher,
                            cleanup::add,
                            wrongGroup ? UUID.randomUUID() : fixture.group,
                            wrongGroup ? shardId : new ShardId(shardId.getIndex(), shardId.id() + 1),
                            fixture.resources,
                            proposal -> CompletableFuture.completedFuture(checkpoint()),
                            (proposal, committed) -> proposal,
                            (proposal, committed, applied) -> assertEquals(proposal, applied)
                        )
                    );
                }
                final PreparationWorkDispatcher.Binding<String, String> existing = dispatcher.bind(
                    fixture.group,
                    shardId,
                    proposal -> CompletableFuture.completedFuture(checkpoint()),
                    (proposal, committed) -> proposal,
                    (proposal, committed, applied) -> assertEquals(proposal, applied)
                );
                expectThrows(OpenSearchRejectedExecutionException.class, () -> context(dispatcher, cleanup::add, fixture));
                assertTrue(cleanup.isEmpty());
                assertEquals(new PreparationEngineAdmission.Stats(1, RESIDENT, 0), fixture.admission.stats());
                assertVisibleCount(fixture.resources.engine(), 0);
                existing.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            } finally {
                dispatcher.closeAsync().toCompletableFuture().get(10, TimeUnit.SECONDS);
            }
        }
    }

    private OwnedPreparationContext<String, String> context(PreparationWorkDispatcher dispatcher, Executor cleanup, Fixture fixture) {
        return new OwnedPreparationContext<>(
            dispatcher,
            cleanup,
            fixture.group,
            shardId,
            fixture.resources,
            proposal -> CompletableFuture.completedFuture(checkpoint()),
            (proposal, committed) -> proposal,
            (proposal, committed, applied) -> assertEquals(proposal, applied)
        );
    }

    private PreparationWorkDispatcher dispatcher(UUID group) {
        return new PreparationWorkDispatcher("owned-context", Set.of(group), 1, 1, 100, 100, threadPool.getThreadContext());
    }

    private LogCheckpoint checkpoint() {
        return new LogCheckpoint(new ShardLogId(new UUID(1, 2), shardId.getIndex().getUUID(), shardId.id(), new UUID(3, 4)), 0, 1);
    }

    private static void await(CountDownLatch latch) throws IOException {
        try {
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException(failure);
        }
    }

    private static void failure(Class<? extends Throwable> type, CompletionStage<?> stage) {
        final var failure = expectThrows(ExecutionException.class, () -> stage.toCompletableFuture().get(10, TimeUnit.SECONDS));
        assertTrue(failure.getCause().toString(), type.isInstance(failure.getCause()));
    }

    private Fixture fixture() throws IOException {
        final var store = createStore(newFSDirectory(createTempDir()));
        CommittedServingEngine serving = null;
        try {
            final var configuration = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
            store.createEmpty(configuration.getIndexSettings().getIndexVersionCreated().luceneVersion);
            store.associateIndexWithNewTranslog(
                Translog.createEmptyTranslog(configuration.getTranslogConfig().getTranslogPath(), -1, shardId, primaryTerm.get())
            );
            serving = new CommittedServingEngine(configuration);
            serving.translogManager()
                .recoverFromTranslog(
                    createTranslogHandler(configuration.getIndexSettings(), serving),
                    SequenceNumbers.NO_OPS_PERFORMED,
                    Long.MAX_VALUE
                );
            final UUID group = UUID.randomUUID();
            final var admission = new PreparationEngineAdmission(Set.of(group), 1, RESIDENT, RESIDENT);
            final Path generation = createTempDir();
            final var resources = PrivatePreparationResources.openEmpty(admission, group, serving, generation, RESIDENT, BUFFER);
            return new Fixture(store, serving, group, admission, generation, resources);
        } catch (IOException | RuntimeException | Error failed) {
            IOUtils.closeWhileHandlingException(serving, store);
            throw failed;
        }
    }

    private record Fixture(Store store, CommittedServingEngine serving, UUID group, PreparationEngineAdmission admission, Path generation,
        PrivatePreparationResources resources) implements Closeable {
        @Override
        public void close() throws IOException {
            IOUtils.close(resources, serving, store);
        }
    }
}
