/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * What a new index inherits from the templates that match its name.
 *
 * <p><b>One template wins, and then composes.</b> That is OpenSearch's composable-template rule and it is
 * followed rather than reinvented: among the index templates whose patterns match, the highest priority is
 * chosen — not merged with the others, because two templates disagreeing about a field's type has no sensible
 * resolution and picking one is what priority is <em>for</em>. The winner's {@code composed_of} component
 * templates are then merged in the order given, and the winner's own {@code template} block goes on top of
 * them.
 *
 * <p><b>The request always wins.</b> Whatever a client sent in the create call is layered over everything a
 * template contributed. A caller who spells out a mapping is not overruled by configuration they may not know
 * exists.
 *
 * <p><b>Matching is a test, not a scan.</b> Deciding whether {@code logs-2026-09} matches {@code logs-*} costs
 * nothing and needs no listing, so the restriction to prefix patterns that governs <em>searching</em> does not
 * apply here: a template may use {@code *} anywhere. What is bounded is the number of templates, and that is
 * bounded on creation — see {@link TemplateStore#MAX_TEMPLATES}.
 */
public final class TemplateResolver {

    private TemplateResolver() {}

    /**
     * An index template composes a component template that is not there.
     *
     * <p>Its own exception type so the create path can answer 400 rather than 500: this is configuration a
     * caller can fix, by creating the component or correcting the template, not a failure of the server.
     */
    public static final class MissingComponentException extends IOException {
        /**
         * Creates the exception.
         *
         * @param template the index template naming the component
         * @param component the component that is not there
         */
        public MissingComponentException(String template, String component) {
            super("index template [" + template + "] composes component template [" + component + "], which does not exist");
        }
    }

    /**
     * What matching templates contributed, before the request is layered on top.
     *
     * @param settings the settings the templates contributed, un-normalised
     * @param mappings the mappings the templates contributed, already composed
     * @param from the name of the template these came from, for the response to attribute them
     * @param dataStream the data-stream block, or null when no matching template declared one
     */
    public record Inherited(Map<String, Object> settings, Map<String, Object> mappings, String from, Map<String, Object> dataStream) {
        /**
         * Creates an inheritance with no data-stream block.
         *
         * @param settings the settings
         * @param mappings the mappings
         * @param from the winning template, or null
         */
        public Inherited(Map<String, Object> settings, Map<String, Object> mappings, String from) {
            this(settings, mappings, from, null);
        }

        /**
         * Reports whether the winning template declares a data stream.
         *
         * @return true if it does
         */
        public boolean isDataStream() {
            return dataStream != null;
        }

        /**
         * Returns the data stream's timestamp field, core's default when the block does not name one.
         *
         * @return the field name
         */
        public String timestampField() {
            if (dataStream != null && dataStream.get("timestamp_field") instanceof Map<?, ?> field && field.get("name") != null) {
                return String.valueOf(field.get("name"));
            }
            return "@timestamp";
        }
    }

    /**
     * Resolves what an index named {@code indexName} inherits.
     *
     * @param indexTemplates every index template, by name
     * @param componentTemplates every component template, by name
     * @param indexName the index being created
     * @return what it inherits, with empty maps when nothing matched
     * @throws IOException if a stored template does not parse
     */
    public static Inherited resolve(Map<String, String> indexTemplates, Map<String, String> componentTemplates, String indexName)
        throws IOException {
        String winner = null;
        Map<String, Object> winning = null;
        long best = Long.MIN_VALUE;

        for (Map.Entry<String, String> each : indexTemplates.entrySet()) {
            final Map<String, Object> template = parse(each.getKey(), each.getValue());
            if (matches(template, indexName) == false) {
                continue;
            }
            final long priority = priority(template);
            // Ties broken by name, so the same set of templates always produces the same answer rather than
            // whichever the store happened to list first.
            if (priority > best || (priority == best && winner != null && each.getKey().compareTo(winner) < 0)) {
                best = priority;
                winner = each.getKey();
                winning = template;
            }
        }

        if (winning == null) {
            return new Inherited(Map.of(), Map.of(), null);
        }

        final Map<String, Object> settings = new LinkedHashMap<>();
        final Map<String, Object> mappings = new LinkedHashMap<>();

        for (Object name : list(winning.get("composed_of"))) {
            final String component = String.valueOf(name);
            final String source = componentTemplates.get(component);
            if (source == null) {
                // Named and absent. Reported rather than skipped: an index quietly created without the
                // analysis settings a component was supposed to contribute is the kind of wrong answer that
                // shows up much later as a query matching nothing.
                throw new MissingComponentException(winner, component);
            }
            merge(settings, mappings, parse(component, source));
        }
        merge(settings, mappings, winning);

        return new Inherited(settings, mappings, winner, winning.get("data_stream") instanceof Map<?, ?> block ? cast(block) : null);
    }

    /** Pulls the {@code template} block's settings and mappings into the accumulators. */
    private static void merge(Map<String, Object> settings, Map<String, Object> mappings, Map<String, Object> template) {
        final Object block = template.get("template");
        if (block instanceof Map<?, ?> inner) {
            if (inner.get("settings") instanceof Map<?, ?> given) {
                deepMerge(settings, cast(given));
            }
            if (inner.get("mappings") instanceof Map<?, ?> given) {
                mergeMappings(mappings, cast(given));
            }
        }
    }

    /**
     * Layers one mapping over another the way core composes templates: objects merge, fields replace.
     *
     * <p>Core merges template mappings under {@code MergeReason.INDEX_TEMPLATE}, where an object mapper
     * merges its {@code properties} field by field and a field mapper's definition <em>replaces</em> the one
     * beneath it. A merge that recursed into a field's parameter map instead composed
     * {@code {"type":"keyword","ignore_above":64}} and {@code {"type":"text"}} into a {@code text} field
     * carrying {@code ignore_above}, which core refuses — and nothing validated the merged mapping at
     * create, so the descriptor was stored and the shard failed to open. The quiet variant kept
     * {@code "index":false} from a component under a template that said only {@code "type":"long"}.
     *
     * <p>A field is an object when it says so ({@code type: object} or {@code nested}) or when it names
     * {@code properties} without naming a type, which is core's own reading of a mapping. Everything that is
     * not a field definition — {@code _source}, {@code dynamic}, {@code _meta} and the rest — merges by key.
     *
     * @param into the accumulator
     * @param from what to layer over it
     */
    public static void mergeMappings(Map<String, Object> into, Map<String, Object> from) {
        for (Map.Entry<String, Object> each : from.entrySet()) {
            final Object existing = into.get(each.getKey());
            if (existing instanceof Map<?, ?> left && each.getValue() instanceof Map<?, ?> right) {
                final Map<String, Object> merged = new LinkedHashMap<>(cast(left));
                if ("properties".equals(each.getKey())) {
                    mergeProperties(merged, cast(right));
                } else {
                    mergeMappings(merged, cast(right));
                }
                into.put(each.getKey(), merged);
            } else {
                into.put(each.getKey(), each.getValue());
            }
        }
    }

    /** Field by field: two object definitions merge, anything else the later one replaces outright. */
    private static void mergeProperties(Map<String, Object> into, Map<String, Object> from) {
        for (Map.Entry<String, Object> field : from.entrySet()) {
            final Object existing = into.get(field.getKey());
            if (existing instanceof Map<?, ?> left
                && field.getValue() instanceof Map<?, ?> right
                && isObjectField(cast(left))
                && isObjectField(cast(right))) {
                final Map<String, Object> merged = new LinkedHashMap<>(cast(left));
                mergeMappings(merged, cast(right));
                into.put(field.getKey(), merged);
            } else {
                into.put(field.getKey(), field.getValue());
            }
        }
    }

    private static boolean isObjectField(Map<String, Object> definition) {
        final Object type = definition.get("type");
        if (type == null) {
            return definition.containsKey("properties");
        }
        return "object".equals(type) || "nested".equals(type);
    }

    /**
     * Merges one map over another, recursing into nested objects.
     *
     * <p>Recursive rather than shallow so two component templates each contributing different keys under
     * one settings group both survive. A shallow merge would silently drop every key from the earlier one,
     * which is exactly what composing templates is meant to avoid. For mappings use {@link #mergeMappings},
     * which knows that a field definition is not a group to recurse into.
     *
     * @param into the accumulator
     * @param from what to layer over it
     */
    public static void deepMerge(Map<String, Object> into, Map<String, Object> from) {
        for (Map.Entry<String, Object> each : from.entrySet()) {
            final Object existing = into.get(each.getKey());
            if (existing instanceof Map<?, ?> left && each.getValue() instanceof Map<?, ?> right) {
                final Map<String, Object> merged = new LinkedHashMap<>(cast(left));
                deepMerge(merged, cast(right));
                into.put(each.getKey(), merged);
            } else {
                into.put(each.getKey(), each.getValue());
            }
        }
    }

    private static boolean matches(Map<String, Object> template, String indexName) {
        for (Object pattern : list(template.get("index_patterns"))) {
            if (glob(String.valueOf(pattern)).matcher(indexName).matches()) {
                return true;
            }
        }
        return false;
    }

    private static long priority(Map<String, Object> template) {
        final Object priority = template.get("priority");
        if (priority instanceof Number number) {
            return number.longValue();
        }
        // Absent means lowest, so a template that names a priority always beats one that does not.
        return 0L;
    }

    private static Pattern glob(String pattern) {
        final StringBuilder regex = new StringBuilder();
        for (char character : pattern.toCharArray()) {
            if (character == '*') {
                regex.append(".*");
            } else {
                regex.append(Pattern.quote(String.valueOf(character)));
            }
        }
        return Pattern.compile(regex.toString());
    }

    private static List<Object> list(Object value) {
        if (value instanceof List<?> many) {
            return new ArrayList<>(many);
        }
        if (value == null) {
            return List.of();
        }
        return List.of(value);
    }

    private static Map<String, Object> parse(String name, String source) throws IOException {
        try {
            return XContentHelper.convertToMap(new BytesArray(source.getBytes(StandardCharsets.UTF_8)), false, XContentType.JSON).v2();
        } catch (Exception e) {
            throw new IOException("stored template [" + name + "] could not be read", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> cast(Map<?, ?> map) {
        return (Map<String, Object>) map;
    }
}
