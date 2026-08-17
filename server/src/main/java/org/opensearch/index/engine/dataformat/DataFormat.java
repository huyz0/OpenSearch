/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a data format for storing and managing index data, with declared capabilities.
 * Each data format (e.g., Lucene, Parquet) declares what storage and query capabilities it supports.
 * <p>
 * Equality is based on the format {@link #name()} — there should be one {@code DataFormat} instance
 * per unique name. This allows {@code DataFormat} to be used safely as a {@link java.util.Map} key.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class DataFormat {
    /**
     * Returns the unique name of this data format.
     *
     * @return the data format name
     */
    public abstract String name();

    /**
     * Returns the priority of this data format. Higher priority formats are preferred
     * when multiple formats can handle the same field type.
     *
     * @return the priority value
     */
    public abstract long priority();

    /**
     * Returns the set of field type capabilities supported by this data format.
     *
     * @return the supported field type capabilities
     */
    public abstract Set<FieldTypeCapabilities> supportedFields();

    /**
     * Phase F of {@code core-pluggability-refactor-plan.md}. Mapper type names (e.g. {@code "nested"}) this
     * format cannot handle -- structural, whole-mapper-tree-shape questions, distinct from {@link
     * #supportedFields()}'s per-field-type capability model, which has no vocabulary for them.
     *
     * <p>Defaults to {@code Set.of("nested")}, matching every pluggable format's current behavior exactly:
     * before this method existed, {@code ObjectMapper.TypeParser.parseNested} rejected a nested mapper
     * whenever the pluggable-data-format feature was enabled at all, uniformly across every format, with no
     * per-format opinion possible. A format that does not override this method is therefore unaffected by
     * this method's existence -- it keeps today's universal "nested is unsupported" behavior for free,
     * rather than silently gaining nested support it was never verified to handle. A format wanting to
     * declare it *does* support nested documents overrides this to exclude {@code "nested"} (or return
     * {@code Set.of()}).
     *
     * @return the mapper type names this format does not support
     */
    public Set<String> unsupportedMapperTypes() {
        return Set.of("nested");
    }

    /**
     * Phase F of {@code core-pluggability-refactor-plan.md}. Whether a {@code keyword} field under this
     * format needs a synthetic raw-value shadow field type built and populated (to reconstruct {@code
     * _source} when {@code ignore_above}/a normalizer would otherwise lose the original value) -- see {@code
     * KeywordFieldMapper}'s own {@code canConsumeRawValueForSource}.
     *
     * <p>Defaults to {@code true}, matching every pluggable format's current behavior exactly: before this
     * method existed, {@code KeywordFieldMapper.PARSER} enabled raw-value tracking whenever the
     * pluggable-data-format feature was enabled at all, uniformly across every format. A format that does
     * not override this method therefore keeps today's behavior unchanged -- it does not silently lose raw
     * -value tracking it was relying on. A format that never loses a keyword field's raw value some other
     * way (so this reconstruction is unnecessary) overrides this to return {@code false}.
     *
     * @return whether this format requires raw-value tracking for keyword fields
     */
    public boolean rawValueTrackingRequired() {
        return true;
    }

    /**
     * Phase F of {@code core-pluggability-refactor-plan.md}. Whether a dynamically-mapped {@code text}
     * value under this format should get the classic auto-added {@code .keyword} multi-field (with an
     * {@code ignore_above} of 256) alongside the {@code text} field itself -- see {@code
     * DocumentParser#builderSupplierForText}.
     *
     * <p>Defaults to {@code false}, matching every pluggable format's current behavior exactly: before this
     * method existed, {@code DocumentParser.builderSupplierForText} never added the auto-keyword multi-field
     * whenever the pluggable-data-format feature was enabled at all, uniformly across every format. A format
     * that does not override this method therefore keeps today's behavior unchanged. A format whose backend
     * benefits from (or needs) the same term-aggregation/sort-friendly keyword shape that non-pluggable
     * indices get by default overrides this to return {@code true}.
     *
     * @return whether this format wants the auto-added keyword multi-field on dynamically-mapped text
     */
    public boolean dynamicTextIncludesKeywordMultiField() {
        return false;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof DataFormat == false) return false;
        return Objects.equals(name(), ((DataFormat) o).name());
    }

    @Override
    public final int hashCode() {
        return Objects.hashCode(name());
    }
}
