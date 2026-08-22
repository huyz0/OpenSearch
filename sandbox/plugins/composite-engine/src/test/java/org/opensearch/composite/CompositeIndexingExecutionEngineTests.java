/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.WriterConfig;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CompositeIndexingExecutionEngine}.
 */
public class CompositeIndexingExecutionEngineTests extends OpenSearchTestCase {

    public void testConstructorWithPrimaryOnly() {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene");
        assertNotNull(engine.getPrimaryDelegate());
        assertTrue(engine.getSecondaryDelegates().isEmpty());
        assertEquals("composite", engine.getDataFormat().name());
    }

    public void testConstructorWithPrimaryAndSecondary() {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene", "parquet");
        assertNotNull(engine.getPrimaryDelegate());
        assertEquals(1, engine.getSecondaryDelegates().size());
        assertEquals("parquet", engine.getSecondaryDelegates().iterator().next().getDataFormat().name());
    }

    public void testConstructorWithMultipleSecondaries() {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene", "parquet", "arrow");
        assertEquals(2, engine.getSecondaryDelegates().size());
    }

    /**
     * The secondaries fan out in a fixed order — the same ascending-priority order in which
     * CompositeDataFormatPlugin.assignCapabilities lets formats claim capabilities. They used to
     * be held in a {@code Set.copyOf(...)} whose iteration order is unspecified, while
     * CompositeWriter.addDoc promised rollback "in order" and doRefresh paired two separate
     * iterations of that set up positionally.
     */
    public void testSecondaryDelegatesFanOutInPrecedenceOrder() {
        // Configured "slow" (priority 90) before "fast" (priority 10); precedence order flips them.
        CompositeIndexingExecutionEngine engine = engineWithPriorities("lucene", 1, Map.of("slow", 90L, "fast", 10L), "slow", "fast");

        List<String> order = engine.getSecondaryDelegates().stream().map(e -> e.getDataFormat().name()).toList();
        assertEquals(List.of("fast", "slow"), order);

        // The per-format document inputs are built by walking the same ordered collection, so the
        // writer's rollback order matches.
        CompositeDocumentInput input = engine.newDocumentInput();
        assertEquals(order, input.getSecondaryInputs().keySet().stream().map(DataFormat::name).toList());
        input.close();
    }

    /** Equal priorities keep the order the setting listed them in (the sort is stable). */
    public void testSecondaryDelegatesWithEqualPriorityKeepSettingOrder() {
        CompositeIndexingExecutionEngine engine = engineWithPriorities("lucene", 1, Map.of("b", 7L, "a", 7L), "b", "a");
        assertEquals(List.of("b", "a"), engine.getSecondaryDelegates().stream().map(e -> e.getDataFormat().name()).toList());
    }

    private static CompositeIndexingExecutionEngine engineWithPriorities(
        String primaryName,
        long primaryPriority,
        Map<String, Long> secondaryPriorities,
        String... secondaryNamesInSettingOrder
    ) {
        Map<String, DataFormat> formats = new HashMap<>();
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        formats.put(primaryName, CompositeTestHelper.stubFormat(primaryName, primaryPriority, Set.of()));
        plugins.put(primaryName, CompositeTestHelper.stubPlugin(primaryName, primaryPriority));
        secondaryPriorities.forEach((name, priority) -> {
            formats.put(name, CompositeTestHelper.stubFormat(name, priority, Set.of()));
            plugins.put(name, CompositeTestHelper.stubPlugin(name, priority));
        });

        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        formats.forEach((name, format) -> when(registry.format(name)).thenReturn(format));
        when(registry.getIndexingEngine(any(), any())).thenAnswer(
            invocation -> plugins.get(((DataFormat) invocation.getArgument(1)).name()).indexingEngine(null)
        );

        Settings settings = Settings.builder()
            .put("index.composite.primary_data_format", primaryName)
            .putList("index.composite.secondary_data_formats", secondaryNamesInSettingOrder)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        return new CompositeIndexingExecutionEngine(indexSettings, null, new CompositeTestHelper.StubCommitter(), registry, null, null);
    }

    public void testConstructorThrowsWhenPrimaryFormatNotRegistered() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("parquet")).thenReturn(null);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(CompositeTestHelper.stubFormat("lucene", 1, Set.of())));

        IndexSettings indexSettings = createIndexSettings("parquet");
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new CompositeIndexingExecutionEngine(indexSettings, null, new CompositeTestHelper.StubCommitter(), registry, null, null)
        );
        assertTrue(ex.getMessage().contains("parquet"));
    }

    public void testConstructorThrowsWhenSecondaryFormatNotRegistered() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("lucene")).thenReturn(CompositeTestHelper.stubFormat("lucene", 1, Set.of()));
        when(registry.format("parquet")).thenReturn(null);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(CompositeTestHelper.stubFormat("lucene", 1, Set.of())));
        when(registry.getIndexingEngine(any(), any())).thenAnswer(invocation -> {
            DataFormatPlugin plugin = CompositeTestHelper.stubPlugin("lucene", 1);
            return plugin.indexingEngine(null);
        });

        Settings settings = Settings.builder()
            .put("index.composite.primary_data_format", "lucene")
            .putList("index.composite.secondary_data_formats", "parquet")
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new CompositeIndexingExecutionEngine(indexSettings, null, new CompositeTestHelper.StubCommitter(), registry, null, null)
        );
        assertTrue(ex.getMessage().contains("parquet"));
    }

    public void testConstructorRejectsNullDataFormatRegistry() {
        IndexSettings indexSettings = createIndexSettings("lucene");
        expectThrows(
            NullPointerException.class,
            () -> new CompositeIndexingExecutionEngine(indexSettings, null, new CompositeTestHelper.StubCommitter(), null, null, null)
        );
    }

    public void testConstructorRejectsNullIndexSettings() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        expectThrows(
            NullPointerException.class,
            () -> new CompositeIndexingExecutionEngine(null, null, new CompositeTestHelper.StubCommitter(), registry, null, null)

        );
    }

    public void testValidateFormatsRegisteredAcceptsValidConfig() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("lucene")).thenReturn(CompositeTestHelper.stubFormat("lucene", 1, Set.of()));
        when(registry.format("parquet")).thenReturn(CompositeTestHelper.stubFormat("parquet", 2, Set.of()));

        CompositeIndexingExecutionEngine.validateFormatsRegistered(registry, "lucene", List.of("parquet"));
    }

    public void testValidateFormatsRegisteredRejectsMissingPrimary() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("parquet")).thenReturn(null);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(CompositeTestHelper.stubFormat("lucene", 1, Set.of())));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> CompositeIndexingExecutionEngine.validateFormatsRegistered(registry, "parquet", List.of())
        );
        assertTrue(ex.getMessage().contains("parquet"));
        assertTrue(ex.getMessage(), ex.getMessage().startsWith("Primary data format [parquet]"));
    }

    /**
     * The message must name the role that actually failed. validateFormatIsRegistered runs for
     * every secondary too, but hardcoded "Primary data format ...", so a mistyped secondary
     * reported a primary-format problem.
     */
    public void testValidateFormatsRegisteredNamesTheSecondaryRoleForAMissingSecondary() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("lucene")).thenReturn(CompositeTestHelper.stubFormat("lucene", 1, Set.of()));
        when(registry.format("parquet")).thenReturn(null);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(CompositeTestHelper.stubFormat("lucene", 1, Set.of())));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> CompositeIndexingExecutionEngine.validateFormatsRegistered(registry, "lucene", List.of("parquet"))
        );
        assertTrue(ex.getMessage(), ex.getMessage().startsWith("Secondary data format [parquet]"));
        assertFalse(ex.getMessage(), ex.getMessage().contains("Primary"));
    }

    public void testValidateFormatsRegisteredNamesTheSecondaryRoleForABlankSecondary() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("lucene")).thenReturn(CompositeTestHelper.stubFormat("lucene", 1, Set.of()));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> CompositeIndexingExecutionEngine.validateFormatsRegistered(registry, "lucene", List.of("   "))
        );
        assertEquals("Secondary data format name must not be null or blank", ex.getMessage());
    }

    public void testValidateFormatsRegisteredNamesThePrimaryRoleForABlankPrimary() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> CompositeIndexingExecutionEngine.validateFormatsRegistered(registry, "  ", List.of())
        );
        assertEquals("Primary data format name must not be null or blank", ex.getMessage());
    }

    public void testValidateFormatsRegisteredRejectsMissingSecondary() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("lucene")).thenReturn(CompositeTestHelper.stubFormat("lucene", 1, Set.of()));
        when(registry.format("parquet")).thenReturn(null);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(CompositeTestHelper.stubFormat("lucene", 1, Set.of())));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> CompositeIndexingExecutionEngine.validateFormatsRegistered(registry, "lucene", List.of("parquet"))
        );
        assertTrue(ex.getMessage().contains("parquet"));
    }

    public void testValidateFormatsRegisteredRejectsSecondaryEqualToPrimary() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("lucene")).thenReturn(CompositeTestHelper.stubFormat("lucene", 1, Set.of()));

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> CompositeIndexingExecutionEngine.validateFormatsRegistered(registry, "lucene", List.of("lucene"))
        );
        assertTrue(ex.getMessage().contains("same as primary"));
    }

    public void testCreateWriterReturnsCompositeWriter() throws IOException {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene");
        CompositeWriter writer = (CompositeWriter) engine.createWriter(new WriterConfig(42L));
        assertNotNull(writer);
        assertEquals(42L, writer.getWriterGeneration());
        writer.close();
    }

    public void testGetMergerDelegatesToPrimary() {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene");
        assertNotNull(engine.getMerger());
    }

    public void testGetNativeBytesUsedSumsAllEngines() {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene", "parquet");
        assertEquals(0L, engine.getNativeBytesUsed());
    }

    public void testGetDataFormatReturnsCompositeDataFormat() {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene", "parquet");
        CompositeDataFormat format = engine.getDataFormat();
        assertNotNull(format);
        assertEquals("composite", format.name());
        assertEquals(2, format.getDataFormats().size());
    }

    public void testNewDocumentInputReturnsCompositeDocumentInput() {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene", "parquet");
        CompositeDocumentInput input = engine.newDocumentInput();
        assertNotNull(input);
        assertNotNull(input.getPrimaryInput());
        assertEquals(1, input.getSecondaryInputs().size());
        input.close();
    }

    public void testDeleteFilesDoesNotThrow() throws Exception {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene", "parquet");
        engine.deleteFiles(Map.of());
    }

    // --- Property test — Committer is required ---

    public void testConstructorThrowsWhenCommitterNull() {
        IndexSettings indexSettings = createIndexSettings("lucene");
        DataFormatRegistry registry = mock(DataFormatRegistry.class);

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> new CompositeIndexingExecutionEngine(indexSettings, null, null, registry, null, null)
        );
        assertTrue(ex.getMessage().contains("Committer must not be null"));
    }

    // --- Property test — Refresh never calls Committer methods ---

    public void testRefreshNeverCallsCommitterMethods() throws IOException {
        TrackingCommitter tracking = new TrackingCommitter();
        DataFormat luceneFormat = CompositeTestHelper.stubFormat("lucene", 1, Set.of());
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("lucene")).thenReturn(luceneFormat);
        doReturn(new CompositeTestHelper.StubIndexingExecutionEngine(luceneFormat)).when(registry).getIndexingEngine(any(), any());
        IndexSettings indexSettings = createIndexSettings("lucene");

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(indexSettings, null, tracking, registry, null, null);

        // Reset tracking after construction (init is called during construction)
        tracking.commitCalled = false;

        RefreshInput refreshInput = RefreshInput.builder().build();
        engine.refresh(refreshInput);

        assertFalse("commit() must not be called during refresh", tracking.commitCalled);
    }

    /**
     * A Committer that tracks which methods were called, for test assertions.
     */
    private static class TrackingCommitter implements Committer {
        boolean commitCalled = false;
        boolean closeCalled = false;
        Map<String, String> lastCommitData = null;

        @Override
        public CommitResult commit(CommitInput commitData) {
            commitCalled = true;
            lastCommitData = StreamSupport.stream(commitData.userData().spliterator(), false)
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (existing, replacement) -> replacement, // Merge function for duplicate keys
                        HashMap::new
                    )
                );
            return null;
        }

        @Override
        public void close() {
            closeCalled = true;
        }

        @Override
        public Map<String, String> getLastCommittedData() {
            return Map.of();
        }

        @Override
        public CommitStats getCommitStats() {
            return null;
        }

        @Override
        public List<CatalogSnapshot> listCommittedSnapshots() {
            return List.of();
        }

        @Override
        public void deleteCommit(CatalogSnapshot snapshot) {}

        @Override
        public boolean isCommitManagedFile(String fileName) {
            return false;
        }

        @Override
        public byte[] serializeToCommitFormat(CatalogSnapshot snapshot) {
            throw new UnsupportedOperationException("stub");
        }
    }

    private IndexSettings createIndexSettings(String primaryFormat) {
        Settings settings = Settings.builder()
            .put("index.composite.primary_data_format", primaryFormat)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        return new IndexSettings(indexMetadata, Settings.EMPTY);
    }
}
