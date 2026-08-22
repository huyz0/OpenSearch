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

import java.io.IOException;

/**
 * {@code ObjectMapper.TypeParser.parseNested} rejects nested mappers whenever the
 * pluggable-data-format feature is on.
 *
 * <p>Extends {@link MapperServiceTestCase} directly rather than {@link MapperTestCase} (what {@code
 * KeywordFieldMapperTests} and friends use): {@code MapperTestCase} layers a large inherited single-field
 * -type test contract (abstract {@code minimalMapping}/{@code writeFieldValue}, generated exists-query/doc
 * -value tests, ...) on top, meant for one-field-mapper-per-class suites -- overkill and the wrong shape
 * for a single object/nested-mapping test. {@code MapperServiceTestCase} has no such contract and still
 * provides the {@code createDocumentMapper}/{@code mapping(...)} helpers this test needs.
 *
 * <p>Uses the lightweight {@code createDocumentMapper} rather than {@code
 * OpenSearchSingleNodeTestCase#createIndex} (what {@link ObjectMapperTests} itself uses): a real index
 * created with {@code index.pluggable.dataformat.enabled=true} requires an actual {@code DataFormatPlugin}
 * to supply a committer factory ({@code IndicesService#createIndexService} fails with {@code
 * IllegalStateException: multiple committer factories found: []} otherwise, as no test in {@code server/}
 * installs one -- concrete format plugins live under {@code sandbox/}) -- so the lightweight mapper-only
 * harness every other pluggable-data-format mapper test in this module already uses is the only way to
 * exercise mapping parsing under the flag without also standing up a real engine. Follows exactly the
 * pattern {@code KeywordFieldMapperTests#testPluggableDataFormatDefaultKeyword} already established:
 * {@code createDocumentMapper(Settings, XContentBuilder)}, documented as "useful for tests that need
 * specific settings like pluggable dataformat."
 */
public class ObjectMapperPluggableDataFormatTests extends MapperServiceTestCase {

    /**
     * The realistic default case: pluggable data format enabled, but no {@code DataFormatPlugin} installed
     * to resolve the configured format name -- nested must still be rejected exactly as it always has.
     */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testNestedStillRejectedWhenNoFormatResolves() throws IOException {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(pluggableSettings, mapping(b -> b.startObject("nested_field").field("type", "nested").endObject()))
        );
        assertTrue(e.getMessage().contains("nested type is not supported with pluggable data format"));
    }
}
