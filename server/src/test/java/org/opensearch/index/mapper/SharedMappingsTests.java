/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class SharedMappingsTests extends MapperServiceTestCase {

    private final IndexAnalyzers base = new IndexAnalyzers(
        singletonMap("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
        emptyMap(),
        emptyMap()
    );
    private final Map<CompressedXContent, Mapping> held = new ConcurrentHashMap<>();
    private final AtomicInteger released = new AtomicInteger();
    private final SharedMappings shared = new SharedMappings() {
        @Override
        public Mapping get(CompressedXContent source) {
            return held.get(source);
        }

        @Override
        public void put(CompressedXContent source, Mapping mapping) {
            held.putIfAbsent(source, mapping);
        }
    };

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return base.sharedView(released::incrementAndGet, shared);
    }

    private static final String SOURCE = "{\"_doc\":{\"properties\":{\"msg\":{\"type\":\"text\"},\"tag\":{\"type\":\"keyword\"}}}}";

    private MapperService loaded(String source) throws IOException {
        final MapperService service = createMapperService(mapping(b -> {}));
        service.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(source), MapperService.MergeReason.MAPPING_RECOVERY);
        return service;
    }

    public void testIndicesLoadingOneSourceHoldOneMapping() throws IOException {
        final MapperService first = loaded(SOURCE);
        final MapperService second = loaded(SOURCE);
        assertSame(first.documentMapper().mapping(), second.documentMapper().mapping());
        assertNotSame("each index keeps its own document mapper", first.documentMapper(), second.documentMapper());
        assertNotNull(second.fieldType("tag"));
    }

    public void testAnUpdateLeavesTheOtherIndexAlone() throws IOException {
        final MapperService first = loaded(SOURCE);
        final MapperService second = loaded(SOURCE);
        final Mapping before = second.documentMapper().mapping();
        merge(first, mapping(b -> b.startObject("added").field("type", "long").endObject()));
        assertNotNull(first.fieldType("added"));
        assertNull(second.fieldType("added"));
        assertSame(before, second.documentMapper().mapping());
        assertNotSame(before, first.documentMapper().mapping());
    }

    public void testOnlyMappingsFromIndexMetadataAreShared() throws IOException {
        final MapperService first = createMapperService(mapping(b -> {}));
        final MapperService second = createMapperService(mapping(b -> {}));
        first.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(SOURCE), MapperService.MergeReason.MAPPING_UPDATE);
        second.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(SOURCE), MapperService.MergeReason.MAPPING_UPDATE);
        assertNotSame(first.documentMapper().mapping(), second.documentMapper().mapping());
    }

    public void testClosingASharedViewReleasesInsteadOfClosing() throws IOException {
        final MapperService service = loaded(SOURCE);
        service.close();
        assertEquals(1, released.get());
        // The analyzers themselves are still open for the other indices using them.
        try (var stream = base.getDefaultIndexAnalyzer().tokenStream("f", "still open")) {
            assertNotNull(stream);
        }
        base.close();
    }
}
