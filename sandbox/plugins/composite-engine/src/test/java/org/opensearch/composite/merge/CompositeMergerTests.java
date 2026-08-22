/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.merge;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.composite.CompositeDataFormat;
import org.opensearch.composite.CompositeIndexingExecutionEngine;
import org.opensearch.composite.stats.CompositeShardStatsTracker;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.PackedRowIdMapping;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.dataformat.merge.DataFormatAwareMergePolicy;
import org.opensearch.index.engine.dataformat.merge.MergeHandler;
import org.opensearch.index.engine.dataformat.merge.OneMerge;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CompositeMerger}.
 */
public class CompositeMergerTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId(new Index("test-index", "uuid"), 0);
    private static final RowIdMapping STUB_ROW_ID_MAPPING = new PackedRowIdMapping(new long[] { 0 }, false);

    private DataFormat primaryFormat;
    private DataFormat secondaryFormat;
    private Merger primaryMerger;
    private Merger secondaryMerger;
    private CompositeIndexingExecutionEngine compositeEngine;
    private CompositeDataFormat compositeDataFormat;
    private Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        primaryFormat = stubFormat("lucene");
        secondaryFormat = stubFormat("parquet");
        primaryMerger = mock(Merger.class);
        secondaryMerger = mock(Merger.class);
        snapshotSupplier = () -> new GatedCloseable<>(null, () -> {});

        IndexingExecutionEngine<?, ?> primaryEngine = mockEngine(primaryFormat, primaryMerger);
        IndexingExecutionEngine<?, ?> secondaryEngine = mockEngine(secondaryFormat, secondaryMerger);

        compositeEngine = mock(CompositeIndexingExecutionEngine.class);
        when(compositeEngine.statsTracker()).thenReturn(new CompositeShardStatsTracker());
        doReturn(primaryEngine).when(compositeEngine).getPrimaryDelegate();
        doReturn(Set.of(secondaryEngine)).when(compositeEngine).getSecondaryDelegates();
        when(compositeEngine.getNextWriterGeneration()).thenReturn(99L);

        compositeDataFormat = new CompositeDataFormat(primaryFormat, List.of(primaryFormat, secondaryFormat));
    }

    // ========== doMerge: successful primary + secondary ==========

    public void testDoMergeSuccessWithPrimaryAndSecondary() throws IOException {
        Path tempDir = createTempDir();
        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p1.dat"), 10);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s1.dat"), 10);

        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedPrimaryWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 10);
        WriterFileSet mergedSecondaryWfs = wfs(tempDir, 99L, Set.of("ms.dat"), 10);

        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPrimaryWfs), STUB_ROW_ID_MAPPING);
        MergeResult secondaryResult = new MergeResult(Map.of(secondaryFormat, mergedSecondaryWfs));

        when(primaryMerger.merge(any())).thenReturn(primaryResult);
        when(secondaryMerger.merge(any())).thenReturn(secondaryResult);

        MergeHandler handler = createHandler();
        MergeResult result = handler.doMerge(oneMerge);

        assertNotNull(result);
        assertEquals(2, result.getMergedWriterFileSet().size());
        assertSame(mergedPrimaryWfs, result.getMergedWriterFileSetForDataformat(primaryFormat));
        assertSame(mergedSecondaryWfs, result.getMergedWriterFileSetForDataformat(secondaryFormat));
    }

    // ========== doMerge: primary only (no secondaries) ==========

    public void testDoMergePrimaryOnlyNoSecondaries() throws IOException {
        CompositeIndexingExecutionEngine engineNoSecondary = mock(CompositeIndexingExecutionEngine.class);
        when(engineNoSecondary.statsTracker()).thenReturn(new CompositeShardStatsTracker());
        IndexingExecutionEngine<?, ?> primaryEngine = mockEngine(primaryFormat, primaryMerger);
        doReturn(primaryEngine).when(engineNoSecondary).getPrimaryDelegate();
        doReturn(Set.of()).when(engineNoSecondary).getSecondaryDelegates();
        when(engineNoSecondary.getNextWriterGeneration()).thenReturn(50L);

        CompositeDataFormat primaryOnlyFormat = new CompositeDataFormat(primaryFormat, List.of(primaryFormat));

        Path tempDir = createTempDir();
        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        Segment segment = Segment.builder(0L).addSearchableFiles(primaryFormat, primaryWfs).build();
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedWfs = wfs(tempDir, 50L, Set.of("merged.dat"), 5);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedWfs));
        when(primaryMerger.merge(any())).thenReturn(primaryResult);

        MergeHandler handler = new MergeHandler(
            snapshotSupplier,
            new CompositeMerger(engineNoSecondary, primaryOnlyFormat),
            SHARD_ID,
            mock(MergeHandler.MergePolicy.class),
            mock(MergeHandler.MergeListener.class),
            () -> 1L
        );

        MergeResult result = handler.doMerge(oneMerge);
        assertNotNull(result);
        assertEquals(1, result.getMergedWriterFileSet().size());
        assertSame(mergedWfs, result.getMergedWriterFileSetForDataformat(primaryFormat));
    }

    // ========== doMerge: primary merge throws IOException ==========

    /**
     * A per-format merger's IOException must reach the caller as an IOException. CompositeMerger
     * declares {@code throws IOException}, so the executor rewrapping it as UncheckedIOException
     * meant every {@code catch (IOException)} around a composite merge silently missed it.
     */
    public void testDoMergePrimaryFailurePropagatesIOException() throws IOException {
        Path tempDir = createTempDir();
        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        when(primaryMerger.merge(any())).thenThrow(new IOException("primary disk error"));

        MergeHandler handler = createHandler();
        IOException ex = expectThrows(IOException.class, () -> handler.doMerge(oneMerge));
        assertEquals("primary disk error", ex.getMessage());
    }

    // ========== doMerge: single secondary failure ==========

    public void testDoMergeSingleSecondaryFailurePropagatesIOException() throws IOException {
        Path tempDir = createTempDir();
        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedPrimaryWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 5);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPrimaryWfs), STUB_ROW_ID_MAPPING);
        when(primaryMerger.merge(any())).thenReturn(primaryResult);
        when(secondaryMerger.merge(any())).thenThrow(new IOException("secondary disk error"));

        MergeHandler handler = createHandler();
        IOException ex = expectThrows(IOException.class, () -> handler.doMerge(oneMerge));
        assertEquals("secondary disk error", ex.getMessage());
    }

    // ========== doMerge: multiple secondaries — fails fast on first error ==========

    public void testDoMergeMultipleSecondariesFailsFastOnFirstError() throws IOException {
        DataFormat secondaryFormat2 = stubFormat("arrow");
        Merger secondaryMerger2 = mock(Merger.class);

        CompositeIndexingExecutionEngine multiEngine = mock(CompositeIndexingExecutionEngine.class);
        when(multiEngine.statsTracker()).thenReturn(new CompositeShardStatsTracker());
        IndexingExecutionEngine<?, ?> primaryEngine = mockEngine(primaryFormat, primaryMerger);
        doReturn(primaryEngine).when(multiEngine).getPrimaryDelegate();
        doReturn(Set.of(mockEngine(secondaryFormat, secondaryMerger), mockEngine(secondaryFormat2, secondaryMerger2))).when(multiEngine)
            .getSecondaryDelegates();
        when(multiEngine.getNextWriterGeneration()).thenReturn(99L);

        CompositeDataFormat multiFormat = new CompositeDataFormat(primaryFormat, List.of(primaryFormat, secondaryFormat, secondaryFormat2));

        Path tempDir = createTempDir();
        WriterFileSet pWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet sWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        WriterFileSet s2Wfs = wfs(tempDir, 1L, Set.of("s2.dat"), 5);
        Segment segment = Segment.builder(0L)
            .addSearchableFiles(primaryFormat, pWfs)
            .addSearchableFiles(secondaryFormat, sWfs)
            .addSearchableFiles(secondaryFormat2, s2Wfs)
            .build();
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedPWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 5);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPWfs), STUB_ROW_ID_MAPPING);
        when(primaryMerger.merge(any())).thenReturn(primaryResult);
        when(secondaryMerger.merge(any())).thenThrow(new IOException("parquet error"));
        when(secondaryMerger2.merge(any())).thenThrow(new IOException("arrow error"));

        MergeHandler handler = new MergeHandler(
            snapshotSupplier,
            new CompositeMerger(multiEngine, multiFormat),
            SHARD_ID,
            mock(MergeHandler.MergePolicy.class),
            mock(MergeHandler.MergeListener.class),
            () -> 1L
        );

        IOException ex = expectThrows(IOException.class, () -> handler.doMerge(oneMerge));
        // Fail-fast: only the first secondary failure is reported, no suppressed exceptions
        assertEquals(0, ex.getSuppressed().length);
    }

    // ========== doMerge: missing rowIdMapping throws IllegalStateException ==========

    public void testDoMergeMissingRowIdMappingThrowsIllegalState() throws IOException {
        Path tempDir = createTempDir();
        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedPrimaryWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 5);
        // Primary result without rowIdMapping
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPrimaryWfs));
        when(primaryMerger.merge(any())).thenReturn(primaryResult);

        MergeHandler handler = createHandler();
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> handler.doMerge(oneMerge));
        assertTrue(ex.getMessage().contains("row-ID mapping"));
        assertTrue(ex.getMessage().contains("secondaries"));
    }

    // ========== doMerge: cleanup on failure goes through the store layer ==========

    /**
     * Cleanup after a failed merge must be routed through the engine's {@code deleteFiles}, which
     * fans out to each format's {@code DataFormatStoreHandler}. It used to delete the merge output
     * with raw {@code Files.deleteIfExists}, which for parquet left its native TieredObjectStore
     * registry holding entries for files that no longer existed.
     */
    public void testDoMergeCleanupRoutesStaleMergedFilesThroughStoreLayer() throws IOException {
        Path tempDir = createTempDir();

        Path staleFile = tempDir.resolve("mp.dat");
        Files.createFile(staleFile);
        assertTrue(Files.exists(staleFile));

        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedPrimaryWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 5);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPrimaryWfs), STUB_ROW_ID_MAPPING);
        when(primaryMerger.merge(any())).thenReturn(primaryResult);
        when(secondaryMerger.merge(any())).thenThrow(new IOException("secondary fail"));
        List<Map<String, Collection<String>>> cleanupRequests = new ArrayList<>();
        when(compositeEngine.deleteFiles(any())).thenAnswer(invocation -> {
            Map<String, Collection<String>> requested = invocation.getArgument(0);
            cleanupRequests.add(Map.copyOf(requested));
            return Map.of();
        });

        MergeHandler handler = createHandler();
        IOException ex = expectThrows(IOException.class, () -> handler.doMerge(oneMerge));
        assertEquals("secondary fail", ex.getMessage());

        assertEquals(1, cleanupRequests.size());
        Map<String, Collection<String>> requested = cleanupRequests.get(0);
        assertEquals("only the format that produced output is cleaned up", Set.of(primaryFormat.name()), requested.keySet());
        assertEquals(List.of("mp.dat"), new ArrayList<>(requested.get(primaryFormat.name())));

        // The store handler owns the actual unlink; nothing deletes behind its back any more.
        assertTrue("cleanup must not bypass the store layer", Files.exists(staleFile));
    }

    /** A cleanup failure is best-effort: it is suppressed onto the merge failure, never replaces it. */
    public void testDoMergeCleanupFailureIsSuppressedOntoMergeFailure() throws IOException {
        Path tempDir = createTempDir();

        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedPrimaryWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 5);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPrimaryWfs), STUB_ROW_ID_MAPPING);
        when(primaryMerger.merge(any())).thenReturn(primaryResult);
        when(secondaryMerger.merge(any())).thenThrow(new IOException("secondary fail"));
        when(compositeEngine.deleteFiles(any())).thenThrow(new IOException("store handler refused"));

        MergeHandler handler = createHandler();
        IOException ex = expectThrows(IOException.class, () -> handler.doMerge(oneMerge));
        assertEquals("secondary fail", ex.getMessage());
        assertEquals(1, ex.getSuppressed().length);
        assertEquals("store handler refused", ex.getSuppressed()[0].getMessage());
    }

    // ========== doMerge: cleanup tolerates a store layer that reports nothing ==========

    public void testDoMergeCleanupHandlesNullCleanupResultGracefully() throws IOException {
        Path tempDir = createTempDir();

        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedPrimaryWfs = wfs(tempDir, 99L, Set.of("nonexistent.dat"), 5);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPrimaryWfs), STUB_ROW_ID_MAPPING);
        when(primaryMerger.merge(any())).thenReturn(primaryResult);
        when(secondaryMerger.merge(any())).thenThrow(new IOException("fail"));
        // compositeEngine is a mock, so deleteFiles returns null here — cleanup must not NPE.

        MergeHandler handler = createHandler();
        IOException ex = expectThrows(IOException.class, () -> handler.doMerge(oneMerge));
        assertEquals("fail", ex.getMessage());
        assertEquals(0, ex.getSuppressed().length);
    }

    // ========== doMerge: no cleanup when mergedWriterFileSet is empty ==========

    public void testDoMergeNoCleanupWhenPrimaryFails() throws IOException {
        Path tempDir = createTempDir();
        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        when(primaryMerger.merge(any())).thenThrow(new IOException("primary fail"));

        MergeHandler handler = createHandler();
        IOException ex = expectThrows(IOException.class, () -> handler.doMerge(oneMerge));
        assertEquals("primary fail", ex.getMessage());
        // Nothing was produced, so the store layer is never asked to delete anything.
        verify(compositeEngine, never()).deleteFiles(any());
    }

    // ========== doMerge: multiple segments ==========

    public void testDoMergeWithMultipleSegments() throws IOException {
        Path tempDir = createTempDir();
        WriterFileSet pWfs1 = wfs(tempDir, 1L, Set.of("p1.dat"), 5);
        WriterFileSet sWfs1 = wfs(tempDir, 1L, Set.of("s1.dat"), 5);
        WriterFileSet pWfs2 = wfs(tempDir, 2L, Set.of("p2.dat"), 5);
        WriterFileSet sWfs2 = wfs(tempDir, 2L, Set.of("s2.dat"), 5);

        Segment seg1 = buildSegment(1L, primaryFormat, pWfs1, secondaryFormat, sWfs1);
        Segment seg2 = buildSegment(2L, primaryFormat, pWfs2, secondaryFormat, sWfs2);
        OneMerge oneMerge = new OneMerge(List.of(seg1, seg2));

        WriterFileSet mergedPWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 10);
        WriterFileSet mergedSWfs = wfs(tempDir, 99L, Set.of("ms.dat"), 10);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPWfs), STUB_ROW_ID_MAPPING);
        MergeResult secondaryResult = new MergeResult(Map.of(secondaryFormat, mergedSWfs));

        when(primaryMerger.merge(any())).thenReturn(primaryResult);
        when(secondaryMerger.merge(any())).thenReturn(secondaryResult);

        MergeHandler handler = createHandler();
        MergeResult result = handler.doMerge(oneMerge);

        assertNotNull(result);
        assertEquals(2, result.getMergedWriterFileSet().size());
        verify(primaryMerger, times(1)).merge(any());
        verify(secondaryMerger, times(1)).merge(any());
    }

    // ========== doMerge: secondary format equals primary is skipped ==========

    public void testDoMergeSkipsSecondaryThatEqualsPrimary() throws IOException {
        // The duplicate secondary has the same DataFormat as primary, so it should be skipped
        // in the secondary loop. We use the same primaryMerger for both to avoid NPE in the
        // constructor's dataFormatMergerMap (last-write-wins for same key).
        IndexingExecutionEngine<?, ?> primaryEngine = mockEngine(primaryFormat, primaryMerger);
        IndexingExecutionEngine<?, ?> duplicateEngine = mockEngine(primaryFormat, primaryMerger);

        CompositeIndexingExecutionEngine dupEngine = mock(CompositeIndexingExecutionEngine.class);
        when(dupEngine.statsTracker()).thenReturn(new CompositeShardStatsTracker());
        doReturn(primaryEngine).when(dupEngine).getPrimaryDelegate();
        doReturn(Set.of(duplicateEngine)).when(dupEngine).getSecondaryDelegates();
        when(dupEngine.getNextWriterGeneration()).thenReturn(99L);

        CompositeDataFormat dupFormat = new CompositeDataFormat(primaryFormat, List.of(primaryFormat));

        Path tempDir = createTempDir();
        WriterFileSet pWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        Segment segment = Segment.builder(0L).addSearchableFiles(primaryFormat, pWfs).build();
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 5);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedWfs), STUB_ROW_ID_MAPPING);
        when(primaryMerger.merge(any())).thenReturn(primaryResult);

        MergeHandler handler = new MergeHandler(
            snapshotSupplier,
            new CompositeMerger(dupEngine, dupFormat),
            SHARD_ID,
            mock(MergeHandler.MergePolicy.class),
            mock(MergeHandler.MergeListener.class),
            () -> 1L
        );

        MergeResult result = handler.doMerge(oneMerge);
        assertNotNull(result);
        assertEquals(1, result.getMergedWriterFileSet().size());
    }

    // ========== findMerges ==========

    public void testFindMergesReturnsEmptyWhenNoSegments() {
        CatalogSnapshot catalogSnapshot = mockCatalogSnapshot(Collections.emptyList());
        snapshotSupplier = () -> new GatedCloseable<>(catalogSnapshot, () -> {});

        MergeHandler handler = createHandler();
        Collection<OneMerge> merges = handler.findMerges();
        assertNotNull(merges);
        assertTrue(merges.isEmpty());
    }

    public void testFindMergesThrowsOnSnapshotFailure() {
        snapshotSupplier = () -> { throw new RuntimeException("snapshot unavailable"); };

        MergeHandler handler = createHandler();
        RuntimeException ex = expectThrows(RuntimeException.class, handler::findMerges);
        assertTrue(ex.getMessage().contains("snapshot unavailable"));
    }

    // ========== findForceMerges ==========

    public void testFindForceMergesReturnsEmptyWhenNoSegments() {
        CatalogSnapshot catalogSnapshot = mockCatalogSnapshot(Collections.emptyList());
        snapshotSupplier = () -> new GatedCloseable<>(catalogSnapshot, () -> {});

        MergeHandler handler = createHandler();
        Collection<OneMerge> merges = handler.findForceMerges(1);
        assertNotNull(merges);
        assertTrue(merges.isEmpty());
    }

    public void testFindForceMergesThrowsOnSnapshotFailure() {
        snapshotSupplier = () -> { throw new RuntimeException("snapshot unavailable"); };

        MergeHandler handler = createHandler();
        RuntimeException ex = expectThrows(RuntimeException.class, () -> handler.findForceMerges(1));
        assertTrue(ex.getMessage().contains("snapshot unavailable"));
    }

    // ========== registerMerge / onMergeFinished / onMergeFailure ==========

    public void testRegisterMergeAndOnMergeFinished() {
        Path tempDir = createTempDir();
        WriterFileSet pWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        Segment segment = Segment.builder(0L).addSearchableFiles(primaryFormat, pWfs).build();

        CatalogSnapshot catalogSnapshot = mockCatalogSnapshot(List.of(segment));
        snapshotSupplier = () -> new GatedCloseable<>(catalogSnapshot, () -> {});

        MergeHandler handler = createHandler();
        OneMerge oneMerge = new OneMerge(List.of(segment));

        handler.registerMerge(oneMerge);
        assertTrue(handler.hasPendingMerges());

        handler.onMergeFinished(oneMerge, false);
    }

    public void testRegisterMergeAndOnMergeFailure() {
        Path tempDir = createTempDir();
        WriterFileSet pWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        Segment segment = Segment.builder(0L).addSearchableFiles(primaryFormat, pWfs).build();

        CatalogSnapshot catalogSnapshot = mockCatalogSnapshot(List.of(segment));
        snapshotSupplier = () -> new GatedCloseable<>(catalogSnapshot, () -> {});

        MergeHandler handler = createHandler();
        OneMerge oneMerge = new OneMerge(List.of(segment));

        handler.registerMerge(oneMerge);
        assertTrue(handler.hasPendingMerges());

        handler.onMergeFailure(oneMerge);
        assertFalse(handler.hasPendingMerges());
    }

    public void testGetNextMergeReturnsNullWhenEmpty() {
        MergeHandler handler = createHandler();
        assertNull(handler.getNextMerge());
        assertFalse(handler.hasPendingMerges());
    }

    public void testGetNextMergeReturnsMergeAfterRegister() {
        Path tempDir = createTempDir();
        WriterFileSet pWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        Segment segment = Segment.builder(0L).addSearchableFiles(primaryFormat, pWfs).build();

        CatalogSnapshot catalogSnapshot = mockCatalogSnapshot(List.of(segment));
        snapshotSupplier = () -> new GatedCloseable<>(catalogSnapshot, () -> {});

        MergeHandler handler = createHandler();
        OneMerge oneMerge = new OneMerge(List.of(segment));

        handler.registerMerge(oneMerge);
        OneMerge retrieved = handler.getNextMerge();
        assertNotNull(retrieved);
        assertSame(oneMerge, retrieved);
        assertFalse(handler.hasPendingMerges());
    }

    // ========== findMerges with merge candidates ==========

    public void testFindMergesReturnsMergeCandidates() throws IOException {
        Path tempDir = createTempDir();
        // Create many small segments with real files to trigger TieredMergePolicy
        List<Segment> segments = new java.util.ArrayList<>();
        for (int i = 0; i < 15; i++) {
            Path file = tempDir.resolve("seg" + i + ".dat");
            Files.write(file, new byte[100]);
            WriterFileSet pWfs = wfs(tempDir, i, Set.of("seg" + i + ".dat"), 10);
            segments.add(Segment.builder(i).addSearchableFiles(primaryFormat, pWfs).build());
        }

        CatalogSnapshot catalogSnapshot = mockCatalogSnapshot(segments);
        snapshotSupplier = () -> new GatedCloseable<>(catalogSnapshot, () -> {});

        MergeHandler handler = createHandlerWithRealPolicy();
        Collection<OneMerge> merges = handler.findMerges();
        assertNotNull(merges);
        // TieredMergePolicy should find merge candidates with 15 small segments
        assertFalse("Expected merge candidates from 15 small segments", merges.isEmpty());
        for (OneMerge merge : merges) {
            assertFalse(merge.getSegmentsToMerge().isEmpty());
        }
    }

    // ========== findForceMerges with merge candidates ==========

    public void testFindForceMergesReturnsMergeCandidates() throws IOException {
        Path tempDir = createTempDir();
        List<Segment> segments = new java.util.ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Path file = tempDir.resolve("fseg" + i + ".dat");
            Files.write(file, new byte[100]);
            WriterFileSet pWfs = wfs(tempDir, i, Set.of("fseg" + i + ".dat"), 10);
            segments.add(Segment.builder(i).addSearchableFiles(primaryFormat, pWfs).build());
        }

        CatalogSnapshot catalogSnapshot = mockCatalogSnapshot(segments);
        snapshotSupplier = () -> new GatedCloseable<>(catalogSnapshot, () -> {});

        MergeHandler handler = createHandlerWithRealPolicy();
        // Force merge down to 1 segment should produce candidates
        Collection<OneMerge> merges = handler.findForceMerges(1);
        assertNotNull(merges);
        assertFalse("Expected force merge candidates when targeting 1 segment from 5", merges.isEmpty());
    }

    // ========== cleanup: files the store layer could not delete are logged, not thrown ==========

    public void testCleanupReportsUndeletedFilesWithoutMaskingMergeFailure() throws IOException {
        Path tempDir = createTempDir();
        // The store handler cannot unlink "mp.dat" yet (e.g. still referenced), so it hands it
        // back as pending rather than throwing.
        Path dirAsFile = tempDir.resolve("mp.dat");
        Files.createDirectory(dirAsFile);
        Files.createFile(dirAsFile.resolve("child.txt"));

        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedPrimaryWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 5);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPrimaryWfs), STUB_ROW_ID_MAPPING);
        when(primaryMerger.merge(any())).thenReturn(primaryResult);
        when(secondaryMerger.merge(any())).thenThrow(new IOException("secondary fail"));

        Map<String, Collection<String>> pending = new LinkedHashMap<>();
        pending.put(primaryFormat.name(), List.of("mp.dat"));
        when(compositeEngine.deleteFiles(any())).thenReturn(pending);

        MergeHandler handler = createHandler();
        // Undeleted files are logged and retried on the next refresh; the merge failure is what
        // reaches the caller, unchanged and un-suppressed.
        IOException ex = expectThrows(IOException.class, () -> handler.doMerge(oneMerge));
        assertEquals("secondary fail", ex.getMessage());
        assertEquals(0, ex.getSuppressed().length);
        assertTrue(Files.exists(dirAsFile));
    }

    // ========== Helper methods ==========

    private MergeHandler createHandler() {
        return new MergeHandler(
            snapshotSupplier,
            new CompositeMerger(compositeEngine, compositeDataFormat),
            SHARD_ID,
            mock(MergeHandler.MergePolicy.class),
            mock(MergeHandler.MergeListener.class),
            () -> 1L
        );
    }

    private MergeHandler createHandlerWithRealPolicy() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(indexSettings.getMergePolicy(true), SHARD_ID);
        return new MergeHandler(
            snapshotSupplier,
            new CompositeMerger(compositeEngine, compositeDataFormat),
            SHARD_ID,
            policy,
            policy,
            () -> 1L
        );
    }

    private static DataFormat stubFormat(String name) {
        return new DataFormat() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public long priority() {
                return 1;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return Set.of();
            }

            @Override
            public String toString() {
                return "StubFormat{" + name + "}";
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static IndexingExecutionEngine<?, ?> mockEngine(DataFormat format, Merger merger) {
        IndexingExecutionEngine<DataFormat, ?> engine = mock(IndexingExecutionEngine.class);
        when(engine.getDataFormat()).thenReturn(format);
        when(engine.getMerger()).thenReturn(merger);
        return engine;
    }

    private static WriterFileSet wfs(Path dir, long gen, Set<String> files, long numRows) {
        return new WriterFileSet(dir.toString(), gen, files, numRows, 0L);
    }

    private static Segment buildSegment(long generation, DataFormat fmt1, WriterFileSet wfs1, DataFormat fmt2, WriterFileSet wfs2) {
        return Segment.builder(generation).addSearchableFiles(fmt1, wfs1).addSearchableFiles(fmt2, wfs2).build();
    }

    private static CatalogSnapshot mockCatalogSnapshot(List<Segment> segments) {
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(segments);
        return snapshot;
    }

    // ── Cross-format merge verification tests ──

    public void testExecutorThrowsWhenSecondaryReturnsNullButPrimaryHasOutput() throws IOException {
        Merger primaryMerger = mock(Merger.class);
        Merger secondaryMerger = mock(Merger.class);

        DataFormat primary = stubFormat("parquet", 0);
        DataFormat secondary = stubFormat("lucene", 50);

        String dir = createTempDir().toString();
        WriterFileSet primaryFiles = new WriterFileSet(dir, 10L, Set.of("file.parquet"), 100, 1L);

        RowIdMapping mapping = mock(RowIdMapping.class);
        when(mapping.size()).thenReturn(100);

        when(primaryMerger.merge(any(MergeInput.class))).thenReturn(new MergeResult(Map.of(primary, primaryFiles), mapping));
        when(secondaryMerger.merge(any(MergeInput.class))).thenReturn(new MergeResult(Map.of()));

        RecordingCleaner cleaner = new RecordingCleaner();
        CompositeMergeExecutor executor = new CompositeMergeExecutor(Map.of(primary, primaryMerger, secondary, secondaryMerger), cleaner);

        WriterFileSet inputP = new WriterFileSet(createTempDir().toString(), 1L, Set.of("in.parquet"), 50, 1L);
        WriterFileSet inputS = new WriterFileSet(createTempDir().toString(), 1L, Set.of("in.si"), 50, 1L);

        MergePlan plan = new MergePlan(10L, primary, List.of(secondary), Map.of(primary, List.of(inputP), secondary, List.of(inputS)));

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> executor.execute(plan));
        assertTrue(ex.getMessage().contains("returned null"));
        // The primary's already-written merge output is handed back to the store layer.
        assertEquals(1, cleaner.calls.size());
        assertEquals(Set.of(primary.name()), cleaner.calls.get(0).keySet());
    }

    public void testExecutorThrowsOnRowCountMismatch() throws IOException {
        Merger primaryMerger = mock(Merger.class);
        Merger secondaryMerger = mock(Merger.class);

        DataFormat primary = stubFormat("parquet", 0);
        DataFormat secondary = stubFormat("lucene", 50);

        WriterFileSet primaryFiles = new WriterFileSet(createTempDir().toString(), 10L, Set.of("file.parquet"), 100, 1L);
        WriterFileSet secondaryFiles = new WriterFileSet(createTempDir().toString(), 10L, Set.of("file.si"), 90, 1L);

        RowIdMapping mapping = mock(RowIdMapping.class);
        when(mapping.size()).thenReturn(100);

        when(primaryMerger.merge(any(MergeInput.class))).thenReturn(new MergeResult(Map.of(primary, primaryFiles), mapping));
        when(secondaryMerger.merge(any(MergeInput.class))).thenReturn(new MergeResult(Map.of(secondary, secondaryFiles)));

        RecordingCleaner cleaner = new RecordingCleaner();
        CompositeMergeExecutor executor = new CompositeMergeExecutor(Map.of(primary, primaryMerger, secondary, secondaryMerger), cleaner);

        WriterFileSet inputP = new WriterFileSet(createTempDir().toString(), 1L, Set.of("in.parquet"), 50, 1L);
        WriterFileSet inputS = new WriterFileSet(createTempDir().toString(), 1L, Set.of("in.si"), 50, 1L);

        MergePlan plan = new MergePlan(10L, primary, List.of(secondary), Map.of(primary, List.of(inputP), secondary, List.of(inputS)));

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> executor.execute(plan));
        assertTrue(ex.getMessage().contains("Row count mismatch"));
        // Both formats produced output before the mismatch was detected, so both are cleaned up.
        assertEquals(1, cleaner.calls.size());
        assertEquals(Set.of(primary.name(), secondary.name()), cleaner.calls.get(0).keySet());
    }

    /** Records the per-format file map each cleanup pass hands to the store layer. */
    private static final class RecordingCleaner implements CompositeMergeExecutor.MergeOutputCleaner {
        final List<Map<String, Collection<String>>> calls = new ArrayList<>();

        @Override
        public Map<String, Collection<String>> deleteFiles(Map<String, Collection<String>> filesByFormat) {
            calls.add(Map.copyOf(filesByFormat));
            return Map.of();
        }
    }

    private static DataFormat stubFormat(String name, long priority) {
        return new DataFormat() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public long priority() {
                return priority;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return Set.of();
            }
        };
    }
}
