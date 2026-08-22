/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Tests for {@link CompositeDataFormat}.
 */
public class CompositeDataFormatTests extends OpenSearchTestCase {

    public void testNameReturnsComposite() {
        DataFormat primary = mockFormat("lucene", 1, Set.of());
        CompositeDataFormat format = new CompositeDataFormat(primary, List.of(primary));
        assertEquals("composite", format.name());
    }

    /**
     * priority() is a precedence rank sorted ascending, so the composite — a last-resort fallback
     * for anything a concrete format can serve on its own — must sort LAST. It used to return
     * Long.MIN_VALUE while claiming "lowest priority", which sorted it first.
     */
    public void testPriorityRanksCompositeLast() {
        DataFormat primary = mockFormat("lucene", 1, Set.of());
        CompositeDataFormat format = new CompositeDataFormat(primary, List.of(primary));
        assertEquals(Long.MAX_VALUE, format.priority());

        List<DataFormat> sorted = new ArrayList<>(List.of(format, primary));
        sorted.sort(CompositeDataFormatPlugin.PRECEDENCE_ORDER);
        assertSame("the concrete format must be preferred over the composite fallback", primary, sorted.get(0));
    }

    public void testGetPrimaryDataformatReturnsPrimary() {
        DataFormat primary = mockFormat("lucene", 1, Set.of());
        DataFormat secondary = mockFormat("parquet", 2, Set.of());
        CompositeDataFormat composite = new CompositeDataFormat(primary, List.of(primary, secondary));
        assertSame(primary, composite.getPrimaryDataFormat());
    }

    public void testSupportedFieldsIsUnionAcrossFormats() {
        FieldTypeCapabilities cap1 = new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH));
        FieldTypeCapabilities cap2 = new FieldTypeCapabilities("integer", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE));
        DataFormat primary = mockFormat("lucene", 1, Set.of(cap1));
        DataFormat secondary = mockFormat("parquet", 2, Set.of(cap2));

        CompositeDataFormat composite = new CompositeDataFormat(primary, List.of(primary, secondary));
        // It used to return only dataFormats.get(0), dropping every secondary-only capability.
        assertEquals(Set.of(cap1, cap2), composite.supportedFields());
    }

    public void testSupportedFieldsUnionsCapabilitiesOfTheSameFieldType() {
        FieldTypeCapabilities luceneKeyword = new FieldTypeCapabilities("keyword", Set.of(Capability.FULL_TEXT_SEARCH));
        FieldTypeCapabilities parquetKeyword = new FieldTypeCapabilities(
            "keyword",
            Set.of(Capability.COLUMNAR_STORAGE, Capability.STORED_FIELDS)
        );
        DataFormat primary = mockFormat("lucene", 1, Set.of(luceneKeyword));
        DataFormat secondary = mockFormat("parquet", 2, Set.of(parquetKeyword));

        CompositeDataFormat composite = new CompositeDataFormat(primary, List.of(primary, secondary));
        Set<FieldTypeCapabilities> union = composite.supportedFields();
        assertEquals(1, union.size());
        FieldTypeCapabilities keyword = union.iterator().next();
        assertEquals("keyword", keyword.fieldType());
        assertEquals(Set.of(Capability.FULL_TEXT_SEARCH, Capability.COLUMNAR_STORAGE, Capability.STORED_FIELDS), keyword.capabilities());
    }

    /**
     * The instance the registry stores comes from the no-arg constructor, so it has no
     * constituents. It reports an empty set — per-field routing for a composite index is decided
     * by CompositeDataFormatPlugin.assignCapabilities from index settings, not from here.
     */
    public void testRegistryVisibleInstanceHasNoConstituentsAndReportsNoFields() {
        CompositeDataFormat registryInstance = new CompositeDataFormat();
        assertTrue(registryInstance.getDataFormats().isEmpty());
        assertEquals(Set.of(), registryInstance.supportedFields());
    }

    public void testSupportedFieldsEmptyWhenNoFormats() {
        DataFormat primary = mockFormat("lucene", 1, Set.of());
        CompositeDataFormat composite = new CompositeDataFormat(primary, List.of());
        assertEquals(Set.of(), composite.supportedFields());
    }

    public void testGetDataFormatsReturnsAllFormats() {
        DataFormat f1 = mockFormat("lucene", 1, Set.of());
        DataFormat f2 = mockFormat("parquet", 2, Set.of());
        CompositeDataFormat composite = new CompositeDataFormat(f1, List.of(f1, f2));
        assertEquals(2, composite.getDataFormats().size());
        assertSame(f1, composite.getDataFormats().get(0));
        assertSame(f2, composite.getDataFormats().get(1));
    }

    public void testGetDataFormatsIsUnmodifiable() {
        DataFormat primary = mockFormat("lucene", 1, Set.of());
        CompositeDataFormat composite = new CompositeDataFormat(primary, List.of(primary));
        expectThrows(UnsupportedOperationException.class, () -> composite.getDataFormats().add(mockFormat("x", 0, Set.of())));
    }

    public void testConstructorRejectsNullDataFormats() {
        DataFormat primary = mockFormat("lucene", 1, Set.of());
        expectThrows(NullPointerException.class, () -> new CompositeDataFormat(primary, null));
    }

    public void testConstructorRejectsNullPrimaryDataformat() {
        expectThrows(NullPointerException.class, () -> new CompositeDataFormat(null, List.of()));
    }

    public void testToStringContainsClassName() {
        DataFormat primary = mockFormat("lucene", 1, Set.of());
        CompositeDataFormat composite = new CompositeDataFormat(primary, List.of(primary));
        String str = composite.toString();
        assertTrue(str.contains("CompositeDataFormat"));
        assertTrue(str.contains("dataFormats="));
    }

    private DataFormat mockFormat(String name, long priority, Set<FieldTypeCapabilities> fields) {
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
                return fields;
            }
        };
    }
}
