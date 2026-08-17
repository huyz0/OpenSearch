/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Phase F of core-pluggability-refactor-plan.md, the fifth and final call site. {@code SourceFieldMapper.PARSER}
 * now asks {@link org.opensearch.index.engine.dataformat.DataFormat#requiresSourceEnabled()} instead of
 * unconditionally rejecting {@code "_source": {"enabled": false}} whenever the pluggable-data-format feature
 * is on.
 *
 * <p>This is a genuinely different question from {@code TextFieldMapperPluggableDataFormatTests}' stored
 * -field forcing, not a copy of it despite the near-identical wiring shape: {@code _source}'s own {@link
 * SourceFieldMapper.Defaults#FIELD_TYPE} is <em>already</em> unconditionally stored, regardless of any
 * setting or this new capability -- what {@code requiresSourceEnabled()} actually gates is whether a mapping
 * is allowed to disable {@code _source} entirely. See {@code DataFormat#requiresSourceEnabled()}'s own
 * javadoc for why this is a separate capability method from {@code requiresStoredFields()} rather than a
 * reuse of it.
 *
 * <p>Follows the same shape/rationale as {@code TextFieldMapperPluggableDataFormatTests}/{@code
 * ObjectMapperPluggableDataFormatTests} for why this extends {@link MapperServiceTestCase} rather than
 * {@link MapperTestCase}, and uses {@code createDocumentMapper(Settings, XContentBuilder)} rather than
 * {@code OpenSearchSingleNodeTestCase#createIndex} (a real index with {@code
 * index.pluggable.dataformat.enabled=true} requires an actual {@code DataFormatPlugin} to supply a committer
 * factory that no test in {@code server/} installs).
 */
public class SourceFieldMapperPluggableDataFormatTests extends MapperServiceTestCase {

    /**
     * {@code _source} is a metadata field, a sibling of {@code properties} in a real mapping, not a regular
     * field under it -- this class's own {@code mapping(...)} helper always wraps its lambda's content
     * inside {@code "properties": {...}}, so it can't express this (writing {@code _source} through it
     * would define a *field* literally named {@code _source}, colliding with the implicit metadata field of
     * the same name, exactly as the first draft of this test did, and got a "defined both as an object and
     * a field" error for its trouble). Built directly instead, matching the shape {@code mapping(...)}
     * itself produces internally.
     */
    private static XContentBuilder sourceDisabledMapping() throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_source")
            .field("enabled", false)
            .endObject()
            .endObject()
            .endObject();
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testSourceDisableRejectedWhenPluggableDataFormatEnabled() throws IOException {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(pluggableSettings, sourceDisabledMapping())
        );
        assertTrue(
            "Expected rejection message, got: " + e.getMessage(),
            e.getMessage().contains("_source can't be disabled with index.derived_source.enabled enabled index setting")
        );
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testSourceDisableAllowedWhenPluggableDataFormatDisabled() throws IOException {
        DocumentMapper mapper = createDocumentMapper(sourceDisabledMapping());

        SourceFieldMapper sourceFieldMapper = mapper.sourceMapper();
        assertFalse("Expected _source disable to be allowed without pluggable data format", sourceFieldMapper.enabled());
    }
}
