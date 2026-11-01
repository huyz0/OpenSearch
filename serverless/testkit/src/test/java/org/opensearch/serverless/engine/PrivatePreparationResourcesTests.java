/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.storage.utils.DirectoryUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

@SuppressFileSystems("*")
public class PrivatePreparationResourcesTests extends EngineTestCase {
    private static final long RESIDENT_BYTES = 64L * 1024 * 1024;
    private static final long BUFFER_BYTES = 4L * 1024 * 1024;

    public void testAdmittedNativeEngineClosesAndRemovesOnlyItsOwnedGeneration() throws Exception {
        final UUID group = UUID.randomUUID();
        final var admission = budget(group);
        final Path generation = createTempDir();
        try (var fixture = fixture()) {
            final var resources = PrivatePreparationResources.openEmpty(
                admission,
                group,
                fixture.serving,
                generation,
                RESIDENT_BYTES,
                BUFFER_BYTES
            );
            try (resources) {
                assertEquals(new PreparationEngineAdmission.Stats(1, RESIDENT_BYTES, 0), admission.stats());
                assertTrue(Files.exists(generation.resolve("preparation.claim")));
                assertTrue(resources.engine().config().getIndexingBufferSize().getBytes() <= BUFFER_BYTES);
                final var result = resources.engine().index(indexForDoc(createParsedDoc("speculative", null)));
                assertEquals(Engine.Result.Type.SUCCESS, result.getResultType());
                assertVisibleCount(resources.engine(), 1);
                assertVisibleCount(fixture.serving, 0);
            }
            resources.close();
            expectThrows(IllegalStateException.class, resources::engine);
            assertFalse(Files.exists(generation));
            assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), admission.stats());
            assertVisibleCount(fixture.serving, 0);
        }
    }

    public void testExhaustedOrDuplicateAdmissionDoesNotTouchTheRequestedDirectory() throws Exception {
        final UUID group = UUID.randomUUID();
        final var admission = budget(group);
        final Path refused = createTempDir();
        try (
            var fixture = fixture();
            var resources = PrivatePreparationResources.openEmpty(
                admission,
                group,
                fixture.serving,
                createTempDir(),
                RESIDENT_BYTES,
                BUFFER_BYTES
            )
        ) {
            final long unsupportedBuffer = (long) Integer.MAX_VALUE + 1;
            assertTrue(
                expectThrows(
                    IllegalArgumentException.class,
                    () -> PrivatePreparationResources.openEmpty(
                        admission,
                        group,
                        fixture.serving,
                        refused,
                        unsupportedBuffer,
                        unsupportedBuffer
                    )
                ).getMessage().contains("indexing buffer")
            );
            expectThrows(
                OpenSearchRejectedExecutionException.class,
                () -> PrivatePreparationResources.openEmpty(admission, group, fixture.serving, refused, RESIDENT_BYTES, BUFFER_BYTES)
            );
            try (var contents = Files.list(refused)) {
                assertEquals(0, contents.count());
            }
            assertVisibleCount(resources.engine(), 0);
            assertEquals(new PreparationEngineAdmission.Stats(1, RESIDENT_BYTES, 0), admission.stats());
        }
        assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), admission.stats());
    }

    public void testExistingGenerationAndServingPathsAreNeverModifiedOrAdopted() throws Exception {
        final UUID group = UUID.randomUUID();
        final var admission = budget(group);
        final Path existing = createTempDir();
        final byte[] expected = new byte[] { 1, 2, 3 };
        Files.write(existing.resolve("prior-state"), expected);
        try (var fixture = fixture()) {
            expectThrows(
                IOException.class,
                () -> PrivatePreparationResources.openEmpty(admission, group, fixture.serving, existing, RESIDENT_BYTES, BUFFER_BYTES)
            );
            assertArrayEquals(expected, Files.readAllBytes(existing.resolve("prior-state")));
            assertFalse(Files.exists(existing.resolve("preparation.claim")));
            final Path publicIndex = DirectoryUtils.unwrapFSDirectory(fixture.store.directory()).getDirectory();
            expectThrows(
                IOException.class,
                () -> PrivatePreparationResources.openEmpty(admission, group, fixture.serving, publicIndex, RESIDENT_BYTES, BUFFER_BYTES)
            );
            assertFalse(Files.exists(publicIndex.resolve("preparation.claim")));
            final Path child = Files.createDirectory(publicIndex.resolve("unowned-child"));
            expectThrows(
                IOException.class,
                () -> PrivatePreparationResources.openEmpty(admission, group, fixture.serving, child, RESIDENT_BYTES, BUFFER_BYTES)
            );
            assertFalse(Files.exists(child.resolve("preparation.claim")));
            assertVisibleCount(fixture.serving, 0);
            assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), admission.stats());
        }
    }

    public void testNonemptyServingHistoryRequiresVerifiedRebuild() throws Exception {
        final UUID group = UUID.randomUUID();
        final var admission = budget(group);
        final Path generation = createTempDir();
        try (var fixture = fixture()) {
            fixture.serving.noOp(
                new Engine.NoOp(0, primaryTerm.get(), Engine.Operation.Origin.PRIMARY, System.nanoTime(), "committed slot")
            );
            expectThrows(
                IOException.class,
                () -> PrivatePreparationResources.openEmpty(admission, group, fixture.serving, generation, RESIDENT_BYTES, BUFFER_BYTES)
            );
            assertFalse(Files.exists(generation.resolve("preparation.claim")));
            assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), admission.stats());
        }
    }

    public void testUnknownNativeInitializationRetainsStoreClaimAndReservation() throws Exception {
        final UUID group = UUID.randomUUID();
        final var admission = budget(group);
        final Path generation = createTempDir();
        final var unpublishedStore = new AtomicReference<Store>();
        final var original = new IOException("native open may still own pending IO");
        try (var fixture = fixture()) {
            assertSame(
                original,
                expectThrows(
                    IOException.class,
                    () -> PrivatePreparationResources.openEmpty(
                        admission,
                        group,
                        fixture.serving,
                        generation,
                        RESIDENT_BYTES,
                        BUFFER_BYTES,
                        configuration -> {
                            unpublishedStore.set(configuration.getStore());
                            throw original;
                        }
                    )
                )
            );
            try {
                assertTrue(Files.exists(generation.resolve("preparation.claim")));
                assertEquals(new PreparationEngineAdmission.Stats(1, RESIDENT_BYTES, 1), admission.stats());
                // A late native task must still have its open directory after initialization threw.
                try (var late = unpublishedStore.get().directory().createOutput("late-native-task", IOContext.DEFAULT)) {
                    late.writeInt(7);
                }
                assertTrue(Files.exists(generation.resolve("index/late-native-task")));
                expectThrows(
                    OpenSearchRejectedExecutionException.class,
                    () -> PrivatePreparationResources.openEmpty(
                        admission,
                        group,
                        fixture.serving,
                        createTempDir(),
                        RESIDENT_BYTES,
                        BUFFER_BYTES
                    )
                );
                // A different admission owner also cannot adopt the nonempty claimed generation.
                expectThrows(
                    IOException.class,
                    () -> PrivatePreparationResources.openEmpty(
                        budget(group),
                        group,
                        fixture.serving,
                        generation,
                        RESIDENT_BYTES,
                        BUFFER_BYTES
                    )
                );
                assertVisibleCount(fixture.serving, 0);
            } finally {
                // The simulated late task has now finished; fixture-only cleanup avoids leaking handles.
                unpublishedStore.get().close();
                IOUtils.rm(generation);
            }
        }
    }

    private PreparationEngineAdmission budget(UUID group) {
        return new PreparationEngineAdmission(Set.of(group), 1, RESIDENT_BYTES, RESIDENT_BYTES);
    }

    private Fixture fixture() throws IOException {
        final var store = createStore(newFSDirectory(createTempDir()));
        CommittedServingEngine serving = null;
        try {
            final var configuration = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
            store.createEmpty(configuration.getIndexSettings().getIndexVersionCreated().luceneVersion);
            store.associateIndexWithNewTranslog(
                Translog.createEmptyTranslog(
                    configuration.getTranslogConfig().getTranslogPath(),
                    SequenceNumbers.NO_OPS_PERFORMED,
                    shardId,
                    primaryTerm.get()
                )
            );
            serving = new CommittedServingEngine(configuration);
            serving.translogManager()
                .recoverFromTranslog(
                    createTranslogHandler(configuration.getIndexSettings(), serving),
                    SequenceNumbers.NO_OPS_PERFORMED,
                    Long.MAX_VALUE
                );
            return new Fixture(store, serving);
        } catch (IOException | RuntimeException | Error failed) {
            IOUtils.closeWhileHandlingException(serving, store);
            throw failed;
        }
    }

    private record Fixture(Store store, CommittedServingEngine serving) implements Closeable {
        @Override
        public void close() throws IOException {
            IOUtils.close(serving, store);
        }
    }
}
