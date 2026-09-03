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

    /** What matching templates contributed, before the request is layered on top. */
    public record Inherited(Map<String, Object> settings, Map<String, Object> mappings, String from) {
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

        return new Inherited(settings, mappings, winner);
    }

    /** Pulls the {@code template} block's settings and mappings into the accumulators. */
    private static void merge(Map<String, Object> settings, Map<String, Object> mappings, Map<String, Object> template) {
        final Object block = template.get("template");
        if (block instanceof Map<?, ?> inner) {
            if (inner.get("settings") instanceof Map<?, ?> given) {
                deepMerge(settings, cast(given));
            }
            if (inner.get("mappings") instanceof Map<?, ?> given) {
                deepMerge(mappings, cast(given));
            }
        }
    }

    /**
     * Merges one map over another, recursing into nested objects.
     *
     * <p>Recursive rather than shallow so two component templates each contributing different fields under
     * {@code properties} both survive. A shallow merge would silently drop every field from the earlier one,
     * which is exactly what composing templates is meant to avoid.
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
