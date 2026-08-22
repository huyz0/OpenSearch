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
 * {@code TextFieldMapper.PARSER} force-stores {@code text} fields whenever the
 * pluggable-data-format feature is on.
 *
 * <p>Follows the same shape/rationale as {@code ObjectMapperPluggableDataFormatTests} /
 * {@code KeywordFieldMapperTests#testPluggableDataFormatDefaultKeyword} for why this extends {@code
 * MapperServiceTestCase} rather than {@code MapperTestCase}, and uses {@code createDocumentMapper(Settings,
 * XContentBuilder)}.
 *
 * <p>Reads {@code fieldType} through a {@code TextFieldMapper}-typed reference deliberately: {@code
 * TextFieldMapper} declares its own {@code protected final FieldType fieldType} field, which <em>shadows</em>
 * (does not override -- fields aren't polymorphic in Java) the inherited {@code protected FieldType fieldType}
 * declared on {@code FieldMapper} itself. That inherited field is never the one {@code TextFieldMapper}
 * actually uses -- it's left holding whatever throwaway, always-unstored, frozen {@code new FieldType()}
 * {@code ParametrizedFieldMapper}'s constructor passed up to {@code FieldMapper}'s -- so any code that reads
 * {@code ((FieldMapper) someTextMapper).fieldType} (a raw field read, not a call to the {@code fieldType()}
 * accessor method, which returns the unrelated {@code MappedFieldType}) silently observes a completely
 * different, never-touched object that has read {@code stored() == false} since construction. That
 * field-hiding trap -- not any in-place mutation of the real {@code FieldType} -- is what a prior
 * investigation session's diagnostic (identity-hash-based, but apparently comparing against the wrong
 * object) chased without resolving; this test's own diagnostic run reproduced the "true, then false" flip
 * exactly that way and confirmed the real, {@code TextFieldMapper}-owned {@code FieldType} never changes
 * after construction.
 */
public class TextFieldMapperPluggableDataFormatTests extends MapperServiceTestCase {

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testTextFieldForceStoredWhenPluggableDataFormatEnabled() throws IOException {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
        DocumentMapper mapper = createDocumentMapper(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "text").endObject())
        );

        TextFieldMapper textFieldMapper = (TextFieldMapper) mapper.mappers().getMapper("field");
        assertTrue("Expected text field to be force-stored under pluggable data format", textFieldMapper.fieldType.stored());
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testTextFieldNotForceStoredWhenPluggableDataFormatDisabled() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> b.startObject("field").field("type", "text").endObject()));

        TextFieldMapper textFieldMapper = (TextFieldMapper) mapper.mappers().getMapper("field");
        assertFalse("Expected text field not to be force-stored without pluggable data format", textFieldMapper.fieldType.stored());
    }
}
