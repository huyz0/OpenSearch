/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.store.BlockCache;
import org.opensearch.serverless.store.BlockCacheDirectory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@SuppressFileSystems("*")
public class PrivatePreparationRebuildTests extends EngineTestCase {
    private static final long RESIDENT_BYTES = 64L * 1024 * 1024;
    private static final long BUFFER_BYTES = 4L * 1024 * 1024;
    private static final PrivatePreparationResources.RebuildLimits LIMITS = new PrivatePreparationResources.RebuildLimits(
        10_000,
        1024 * 1024,
        1L << 30,
        10_000,
        4L * 1024 * 1024
    );

    public void testRebuildRetainsDeleteVersionsAndOldTermCasWithoutCopyingIndexData() throws Exception {
        primaryTerm.set(10);
        final UUID group = UUID.randomUUID();
        final var admission = budget(group);
        final Path generation = createTempDir();
        try (var fixture = fixture()) {
            assertSuccess(fixture.serving.index(index("live", 0, primaryTerm.get(), 3, VersionType.EXTERNAL, -2, 0)), 0, 3);
            assertSuccess(fixture.serving.delete(deletion("deleted", 1, 5)), 1, 5);
            final long newTerm = primaryTerm.get() + 1;
            try (var resources = rebuild(admission, group, fixture.serving, generation, 1, newTerm, LIMITS)) {
                final var privateEngine = resources.engine();
                assertEquals(fixture.serving.getHistoryUUID(), privateEngine.getHistoryUUID());
                assertEquals(1, privateEngine.getProcessedLocalCheckpoint());
                assertVisibleCount(privateEngine, 1);
                try (var paths = Files.list(generation.resolve("index"))) {
                    // Rebuild writes private commit metadata; immutable data files remain pinned at source.
                    assertFalse(paths.anyMatch(path -> path.getFileName().toString().startsWith("_")));
                }
                final var stale = resources.index(index("deleted", -2, newTerm, 4, VersionType.EXTERNAL, -2, 0));
                assertEquals(Engine.Result.Type.FAILURE, stale.getResultType());
                assertEquals(1, privateEngine.getProcessedLocalCheckpoint());
                assertSuccess(resources.index(index("deleted", -2, newTerm, 6, VersionType.EXTERNAL, -2, 0)), 2, 6);
                final var updated = resources.index(
                    index("live", -2, newTerm, Versions.MATCH_ANY, VersionType.INTERNAL, 0, primaryTerm.get())
                );
                assertSuccess(updated, 3, 4);
                assertEquals(newTerm, updated.getTerm());
                assertVisibleCount(privateEngine, 2);
                assertVisibleCount(fixture.serving, 1);
                try (
                    var original = fixture.serving.get(new Engine.Get(true, true, "live", newUid("live")), fixture.serving::acquireSearcher)
                ) {
                    assertEquals(3, original.version());
                    assertEquals(0, original.docIdAndVersion().seqNo);
                }
                assertEquals(new PreparationEngineAdmission.Stats(1, RESIDENT_BYTES, 0), admission.stats());
            }
            assertFalse(Files.exists(generation));
            assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), admission.stats());
        }
    }

    public void testPinnedStoreSurvivesServingCloseUntilPrivateMergeAndClose() throws Exception {
        final UUID group = UUID.randomUUID();
        final var admission = budget(group);
        try (var fixture = fixture()) {
            assertSuccess(fixture.serving.index(index("base", 0, primaryTerm.get(), 1, VersionType.EXTERNAL, -2, 0)), 0, 1);
            final int references = fixture.store.refCount();
            try (var resources = rebuild(admission, group, fixture.serving, createTempDir(), 0, primaryTerm.get(), LIMITS)) {
                assertEquals(references + 1, fixture.store.refCount());
                fixture.serving.close();
                fixture.store.close();
                assertEquals(1, fixture.store.refCount());
                assertSuccess(resources.index(index("private", -2, primaryTerm.get(), 1, VersionType.EXTERNAL, -2, 0)), 1, 1);
                resources.engine().flush(true, true);
                assertVisibleCount(resources.engine(), 2);
                // Exercise base reads after the original engine/store owners are gone.
                resources.engine().forceMerge(true, 1, false, false, false, null);
                assertVisibleCount(resources.engine(), 2);
            }
            assertEquals(0, fixture.store.refCount());
        }
    }

    public void testDeleteExpiryKeepsItsOriginalClockAcrossRebuild() throws Exception {
        final var clock = new AtomicLong(100_000);
        threadPool = spy(threadPool);
        when(threadPool.relativeTimeInMillis()).thenAnswer(invocation -> clock.get());
        final UUID group = UUID.randomUUID();
        try (var fixture = fixture()) {
            fixture.serving.config().setEnableGcDeletes(true);
            assertSuccess(fixture.serving.delete(deletion("deleted", 0, 5)), 0, 5);
            final long window = fixture.serving.config().getIndexSettings().getGcDeletesInMillis();
            assertTrue(window > 0);
            clock.addAndGet(window - 1);
            try (var resources = rebuild(budget(group), group, fixture.serving, createTempDir(), 0, primaryTerm.get(), LIMITS)) {
                assertEquals(
                    Engine.Result.Type.FAILURE,
                    resources.index(index("deleted", -2, primaryTerm.get(), 4, VersionType.EXTERNAL, -2, 0)).getResultType()
                );
                clock.addAndGet(2);
                // Rebuild must not restart the GC window at the time of installation.
                assertSuccess(resources.index(index("deleted", -2, primaryTerm.get(), 4, VersionType.EXTERNAL, -2, 0)), 1, 4);
            }
        }
    }

    public void testDisabledDeleteGcRemainsDisabledInThePrivateContext() throws Exception {
        final var clock = new AtomicLong(100_000);
        threadPool = spy(threadPool);
        when(threadPool.relativeTimeInMillis()).thenAnswer(invocation -> clock.get());
        final UUID group = UUID.randomUUID();
        try (var fixture = fixture()) {
            fixture.serving.config().setEnableGcDeletes(false);
            assertSuccess(fixture.serving.delete(deletion("deleted", 0, 5)), 0, 5);
            clock.addAndGet(fixture.serving.config().getIndexSettings().getGcDeletesInMillis() + 1);
            try (var resources = rebuild(budget(group), group, fixture.serving, createTempDir(), 0, primaryTerm.get(), LIMITS)) {
                assertFalse(resources.engine().config().isEnableGcDeletes());
                assertEquals(
                    Engine.Result.Type.FAILURE,
                    resources.index(index("deleted", -2, primaryTerm.get(), 4, VersionType.EXTERNAL, -2, 0)).getResultType()
                );
                assertEquals(0, resources.engine().getProcessedLocalCheckpoint());
            }
        }
    }

    public void testForeignHistoryAndAlreadyInitializedDeleteMapsCannotBeRestored() throws Exception {
        final UUID group = UUID.randomUUID();
        try (var source = fixture(); var foreign = fixture()) {
            assertSuccess(source.serving.delete(deletion("deleted", 0, 5)), 0, 5);
            try (var commit = source.serving.acquireLastIndexCommit(true)) {
                final var snapshot = source.serving.captureDeleteVersions(commit.get(), 100, 1024 * 1024);
                assertEquals(1, snapshot.size());
                assertTrue(snapshot.retainedBytes() > 128);
                try (
                    var empty = PrivatePreparationResources.openEmpty(
                        budget(group),
                        group,
                        foreign.serving,
                        createTempDir(),
                        RESIDENT_BYTES,
                        BUFFER_BYTES
                    )
                ) {
                    expectThrows(IOException.class, () -> empty.engine().installDeleteVersions(snapshot));
                    assertVisibleCount(empty.engine(), 0);
                }
                try (var rebuilt = rebuild(budget(group), group, source.serving, createTempDir(), 0, primaryTerm.get(), LIMITS)) {
                    expectThrows(IOException.class, () -> rebuilt.engine().installDeleteVersions(snapshot));
                    assertEquals(
                        Engine.Result.Type.FAILURE,
                        rebuilt.index(index("deleted", -2, primaryTerm.get(), 4, VersionType.EXTERNAL, -2, 0)).getResultType()
                    );
                }
            }
        }
    }

    public void testWrongPrefixTermAndHistoryGapsFailBeforeClaim() throws Exception {
        primaryTerm.set(10);
        final UUID group = UUID.randomUUID();
        final var admission = budget(group);
        try (var fixture = fixture()) {
            assertSuccess(fixture.serving.index(index("base", 0, primaryTerm.get(), 1, VersionType.EXTERNAL, -2, 0)), 0, 1);
            final Path wrongPrefix = createTempDir();
            expectThrows(IOException.class, () -> rebuild(admission, group, fixture.serving, wrongPrefix, -1, primaryTerm.get(), LIMITS));
            assertFalse(Files.exists(wrongPrefix.resolve("preparation.claim")));
            final Path wrongTerm = createTempDir();
            if (primaryTerm.get() > 1) {
                expectThrows(
                    IOException.class,
                    () -> rebuild(admission, group, fixture.serving, wrongTerm, 0, primaryTerm.get() - 1, LIMITS)
                );
                assertFalse(Files.exists(wrongTerm.resolve("preparation.claim")));
            }
            assertSuccess(fixture.serving.index(index("gap", 2, primaryTerm.get(), 1, VersionType.EXTERNAL, -2, 0)), 2, 1);
            final Path gap = createTempDir();
            expectThrows(IOException.class, () -> rebuild(admission, group, fixture.serving, gap, 2, primaryTerm.get(), LIMITS));
            assertFalse(Files.exists(gap.resolve("preparation.claim")));
            assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), admission.stats());
        }
    }

    public void testRemoteCacheSourceIsRefusedWithoutAnImmutableObjectLease() throws Exception {
        final UUID group = UUID.randomUUID();
        final var admission = budget(group);
        final Path generation = createTempDir();
        try (var fixture = fixture(true)) {
            final int references = fixture.store.refCount();
            final var refused = expectThrows(
                IOException.class,
                () -> rebuild(admission, group, fixture.serving, generation, -1, primaryTerm.get(), LIMITS)
            );
            assertTrue(refused.getMessage().contains("immutable source-file lease"));
            assertFalse(Files.exists(generation.resolve("preparation.claim")));
            assertEquals(references, fixture.store.refCount());
            assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), admission.stats());
        }
    }

    public void testUncertainNativeOpenRetainsTheSourcePinAndReservation() throws Exception {
        final UUID group = UUID.randomUUID();
        final var admission = budget(group);
        final Path generation = createTempDir();
        final var unpublished = new AtomicReference<Store>();
        final var original = new IOException("unknown native constructor completion");
        try (var fixture = fixture()) {
            assertSuccess(fixture.serving.index(index("base", 0, primaryTerm.get(), 1, VersionType.EXTERNAL, -2, 0)), 0, 1);
            final int references = fixture.store.refCount();
            assertSame(
                original,
                expectThrows(
                    IOException.class,
                    () -> PrivatePreparationResources.openCommitted(
                        admission,
                        group,
                        fixture.serving,
                        generation,
                        RESIDENT_BYTES,
                        BUFFER_BYTES,
                        0,
                        primaryTerm.get(),
                        LIMITS,
                        configuration -> {
                            unpublished.set(configuration.getStore());
                            throw original;
                        }
                    )
                )
            );
            try {
                assertEquals(references + 1, fixture.store.refCount());
                assertEquals(new PreparationEngineAdmission.Stats(1, RESIDENT_BYTES, 1), admission.stats());
                assertTrue(Files.exists(generation.resolve("preparation.claim")));
                try (var late = unpublished.get().directory().createOutput("late-native-task", IOContext.DEFAULT)) {
                    late.writeInt(7);
                }
                assertTrue(Files.exists(generation.resolve("index/late-native-task")));
                assertVisibleCount(fixture.serving, 1);
            } finally {
                // Only this injected opener proves that no unknown native work remains.
                unpublished.get().close();
                IOUtils.rm(generation);
            }
            assertEquals(references, fixture.store.refCount());
        }
    }

    public void testRejectedSnapshotBoundsReleasePinsAndOwnedGeneration() throws Exception {
        final UUID group = UUID.randomUUID();
        final var admission = budget(group);
        try (var fixture = fixture()) {
            assertSuccess(fixture.serving.delete(deletion("one", 0, 5)), 0, 5);
            assertSuccess(fixture.serving.delete(deletion("two", 1, 7)), 1, 7);
            final int references = fixture.store.refCount();
            for (var limits : new PrivatePreparationResources.RebuildLimits[] {
                new PrivatePreparationResources.RebuildLimits(1, 1024 * 1024, 1L << 30, 100, 1024 * 1024),
                new PrivatePreparationResources.RebuildLimits(10_000, 128, 1L << 30, 100, 1024 * 1024),
                new PrivatePreparationResources.RebuildLimits(10_000, 1024 * 1024, 1, 100, 1024 * 1024),
                new PrivatePreparationResources.RebuildLimits(10_000, 1024 * 1024, 1L << 30, 1, 1024 * 1024),
                new PrivatePreparationResources.RebuildLimits(10_000, 1024 * 1024, 1L << 30, 100, 128) }) {
                final Path generation = createTempDir();
                expectThrows(IOException.class, () -> rebuild(admission, group, fixture.serving, generation, 1, primaryTerm.get(), limits));
                assertFalse(Files.exists(generation));
                assertEquals(references, fixture.store.refCount());
                assertEquals(new PreparationEngineAdmission.Stats(0, 0, 0), admission.stats());
            }
            final Path unclaimed = createTempDir();
            final var tooLarge = new PrivatePreparationResources.RebuildLimits(10_000, RESIDENT_BYTES, 1L << 30, 100, 128);
            expectThrows(
                IllegalArgumentException.class,
                () -> rebuild(admission, group, fixture.serving, unclaimed, 1, primaryTerm.get(), tooLarge)
            );
            assertFalse(Files.exists(unclaimed.resolve("preparation.claim")));
        }
    }

    private Engine.Index index(String id, long seq, long term, long version, VersionType type, long ifSeq, long ifTerm) {
        final var doc = createParsedDoc(id, "route");
        return new Engine.Index(
            newUid(doc),
            doc,
            seq,
            term,
            version,
            type,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            true,
            ifSeq,
            ifTerm
        );
    }

    private Engine.Delete deletion(String id, long seq, long version) {
        return new Engine.Delete(
            id,
            newUid(id),
            seq,
            primaryTerm.get(),
            version,
            VersionType.EXTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0,
            "route"
        );
    }

    private static void assertSuccess(Engine.Result result, long sequence, long version) {
        assertEquals(Engine.Result.Type.SUCCESS, result.getResultType());
        assertEquals(sequence, result.getSeqNo());
        assertEquals(version, result.getVersion());
    }

    private PreparationEngineAdmission budget(UUID group) {
        return new PreparationEngineAdmission(Set.of(group), 1, RESIDENT_BYTES, RESIDENT_BYTES);
    }

    private static PrivatePreparationResources rebuild(
        PreparationEngineAdmission admission,
        UUID group,
        CommittedServingEngine serving,
        Path generation,
        long checkpoint,
        long term,
        PrivatePreparationResources.RebuildLimits limits
    ) throws IOException {
        return PrivatePreparationResources.openCommitted(
            admission,
            group,
            serving,
            generation,
            RESIDENT_BYTES,
            BUFFER_BYTES,
            checkpoint,
            term,
            limits
        );
    }

    private Fixture fixture() throws IOException {
        return fixture(false);
    }

    private Fixture fixture(boolean remoteCache) throws IOException {
        final var local = remoteCache ? FSDirectory.open(createTempDir()) : newFSDirectory(createTempDir());
        // An empty manifest needs no object-store calls. The guard must reject the backend itself.
        final var directory = remoteCache
            ? BlockCacheDirectory.create(local, null, BlobPath.cleanPath(), new BlockCache(4096, 1), "test", null)
            : local;
        final var store = createStore(directory);
        CommittedServingEngine serving = null;
        try {
            final var configuration = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
            store.createEmpty(configuration.getIndexSettings().getIndexVersionCreated().luceneVersion);
            store.associateIndexWithNewTranslog(
                Translog.createEmptyTranslog(configuration.getTranslogConfig().getTranslogPath(), -1, shardId, primaryTerm.get())
            );
            serving = new CommittedServingEngine(configuration);
            serving.translogManager()
                .recoverFromTranslog(createTranslogHandler(configuration.getIndexSettings(), serving), -1, Long.MAX_VALUE);
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
