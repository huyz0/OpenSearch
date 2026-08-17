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

    /**
     * Phase F of {@code core-pluggability-refactor-plan.md}. Whether {@code text} fields under this format
     * must be force-stored in Lucene -- see {@code TextFieldMapper}'s own stored-field forcing (wired via
     * {@code TextFieldMapper#requiresStoredFieldsForPluggableFormat}). {@code SourceFieldMapper}'s analogous
     * stored-field forcing still does its own unconditional settings check as of this method's introduction
     * -- migrating it to this same capability is a separate, not-yet-done follow-up (it needs its own
     * instance-field/merge-builder plumbing to carry the resolved value across a mapping merge, the way
     * {@code TextFieldMapper}/{@code KeywordFieldMapper} do; wiring it in without that would silently drop
     * the force-stored guarantee across mapping updates).
     *
     * <p>Defaults to {@code true} -- <b>not</b> {@code false}, deliberately, unlike this file's other
     * capability methods. Before this method existed, {@code TextFieldMapper} force-stored {@code text}
     * fields whenever the pluggable-data-format feature was enabled at all, uniformly across every format,
     * with no per-format opt-out. Matching that exactly means a format that does not override this method
     * must keep getting stored fields, not lose them -- the opposite of {@link #unsupportedMapperTypes()}/
     * {@link #rawValueTrackingRequired()}'s pattern only because the *sense* of "unset" differs: those two
     * default to "keep the old universal restriction/guarantee active" via a non-{@code false} default
     * ({@code Set.of("nested")}, {@code true}); this one's old universal behavior was also "active" (forced
     * stored), so its behavior-preserving default is {@code true} for the identical reason, not {@code
     * false}. A neutral-looking {@code false} default here would silently regress every currently-existing
     * concrete {@code DataFormat} (none of which override this method as of its introduction) from
     * "always force-stored" to "never force-stored" the instant this method started being consulted --
     * exactly the class of ground-rule-1 ("no behavior change") violation this phase's own methodology
     * exists to catch, and precisely the mistake an earlier, reverted attempt at this same method made
     * before being caught and abandoned; see this file's git history for that attempt's own note on why.
     * A format whose backend can reconstruct {@code text} values without Lucene's stored fields overrides
     * this to return {@code false}.
     *
     * @return whether this format requires stored fields for text fields
     */
    public boolean requiresStoredFields() {
        return true;
    }

    /**
     * Phase F of {@code core-pluggability-refactor-plan.md}. Whether this format needs {@code _source}
     * retained -- i.e. whether a mapping is rejected for setting {@code "_source": {"enabled": false}} --
     * see {@code SourceFieldMapper}'s own validation (wired via {@code SourceFieldMapper
     * #requiresSourceEnabledForPluggableFormat}).
     *
     * <p><b>Deliberately a separate method from {@link #requiresStoredFields()}, not a reuse of it,
     * even though both migrated off the same {@code PLUGGABLE_DATAFORMAT_ENABLED_SETTING} flag check.</b>
     * They ask genuinely different questions: {@code requiresStoredFields()} controls whether {@code text}
     * fields' individual Lucene {@link org.apache.lucene.document.FieldType} gets {@code setStored(true)};
     * {@code _source}'s own {@code FieldType} (see {@code SourceFieldMapper.Defaults#FIELD_TYPE}) is
     * <em>already</em> unconditionally stored regardless of this method or any setting -- what this method
     * actually gates is a mapping-time validation rejecting the document {@code _source} blob being
     * disabled entirely. A format could plausibly need one without the other (reconstructing individual
     * field values from per-field stored data is a different capability than needing the whole original
     * document blob retained), so collapsing them into one method would be answering a question this class
     * was never asked, purely to save a method declaration -- not a simplification worth making.
     *
     * <p>Defaults to {@code true}, matching every pluggable format's current behavior exactly (before this
     * method existed, {@code SourceFieldMapper} rejected {@code enabled=false} whenever the
     * pluggable-data-format feature was on at all, uniformly across every format) -- see {@link
     * #requiresStoredFields()}'s own javadoc for why the behavior-preserving default here is {@code true},
     * not the {@code false}/{@code Set.of("nested")}-style "neutral" default this file's structural
     * -question methods use; the reasoning is identical, not a copy-paste-without-checking repeat of the
     * bug that default polarity caused there.
     *
     * @return whether this format requires {@code _source} to stay enabled
     */
    public boolean requiresSourceEnabled() {
        return true;
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
