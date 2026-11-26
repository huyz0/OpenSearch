/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.snapshot;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads a snapshot request's body: the keys each endpoint knows and refuses, index lists, flags and names.
 * Split out of {@link SnapshotHandler}.
 */
final class SnapshotRequestBodies {

    private SnapshotRequestBodies() {}

    /** What a restore here reads. Everything else core's restore accepts is refused with its reason, below. */
    static final Set<String> RESTORE_KNOWN_KEYS = Set.of(
        "indices",
        "rename_pattern",
        "rename_replacement",
        "partial",
        "include_global_state",
        "include_aliases",
        "rename_alias_pattern",
        "rename_alias_replacement"
    );

    /**
     * Core's other restore fields, each with why it is refused rather than accepted and ignored. An
     * allowlist used to admit all of these and act on three, which told a caller -- by answering 400 for
     * an unknown key -- that a known key was honoured.
     */
    static final java.util.Map<String, String> RESTORE_REFUSED_KEYS = java.util.Map.ofEntries(
        java.util.Map.entry(
            "index_settings",
            "the settings an index had when it was captured are restored with it, unchanged; change them on the "
                + "restored index with PUT /{index}/_settings"
        ),
        java.util.Map.entry(
            "ignore_index_settings",
            "the settings an index had when it was captured are restored with it, unchanged; change them on the "
                + "restored index with PUT /{index}/_settings"
        ),
        java.util.Map.entry("settings", "repository settings are fixed when the repository is registered"),
        java.util.Map.entry(
            "storage_type",
            "a restored index is served from this deployment's own object store; there is no other storage type"
        ),
        java.util.Map.entry("source_remote_store_repository", "there is one object store here, and it is the repository"),
        java.util.Map.entry("source_remote_translog_repository", "there is one object store here, and it is the repository"),
        java.util.Map.entry(
            "alias_write_index_policy",
            "a restored index is the write index of an alias only when the snapshot said so and the alias has none; "
                + "the policy is not configurable"
        ),
        java.util.Map.entry("ignore_unavailable", "a restore names the indices it wants; one the snapshot did not capture is a 404"),
        java.util.Map.entry("allow_no_indices", "a restore names the indices it wants; patterns are not matched against the snapshot"),
        java.util.Map.entry("expand_wildcards", "a restore names the indices it wants; patterns are not matched against the snapshot")
    );

    /** What a create here reads. */
    static final Set<String> CREATE_KNOWN_KEYS = Set.of("indices", "ignore_unavailable", "partial", "include_global_state");

    static List<String> readIndices(Object field) {
        if (field instanceof String string) {
            final List<String> split = new ArrayList<>();
            for (String piece : string.split(",")) {
                if (piece.isBlank() == false) {
                    split.add(piece.trim());
                }
            }
            return split;
        }
        if (field instanceof List<?> list) {
            final List<String> result = new ArrayList<>(list.size());
            for (Object item : list) {
                result.add(String.valueOf(item));
            }
            return result;
        }
        return List.of();
    }

    static boolean readBoolean(Object field, boolean defaultValue) {
        if (field == null) {
            return defaultValue;
        }
        if (field instanceof Boolean bool) {
            return bool;
        }
        return Boolean.parseBoolean(String.valueOf(field));
    }

    /**
     * Why a repository or snapshot name is not one, or null when it is.
     *
     * @param name the name, or a comma-separated list of them when {@code list} is true
     * @param what "repository" or "snapshot", for the message
     * @param list whether several names may be given
     */
    static String invalidName(String name, String what, boolean list) {
        if (name == null) {
            return null;
        }
        for (String each : list ? name.split(",") : new String[] { name }) {
            try {
                org.opensearch.serverless.cluster.Names.validateId(each, what);
            } catch (IllegalArgumentException e) {
                return e.getMessage();
            }
        }
        return null;
    }

    /**
     * Why a restore's alias options cannot be read, or null when they can: {@code include_aliases} must be a boolean, and
     * the rename pattern, when it will be used, a regular expression.
     */
    static String invalidAliasOption(Map<String, Object> body) {
        final Object partial = body.get("partial");
        if (partial != null && partial instanceof Boolean == false && "true".equals(partial) == false && "false".equals(partial) == false) {
            return "partial must be a boolean";
        }
        final Object state = body.get("include_global_state");
        if (state != null && state instanceof Boolean == false && "true".equals(state) == false && "false".equals(state) == false) {
            return "include_global_state must be a boolean";
        }
        final Object include = body.get("include_aliases");
        if (include != null && include instanceof Boolean == false && "true".equals(include) == false && "false".equals(include) == false) {
            return "include_aliases must be a boolean";
        }
        if (readBoolean(include, false) && body.get("rename_alias_pattern") != null && body.get("rename_alias_replacement") != null) {
            try {
                java.util.regex.Pattern.compile(String.valueOf(body.get("rename_alias_pattern")));
            } catch (java.util.regex.PatternSyntaxException e) {
                return "rename_alias_pattern is not a regular expression: " + e.getDescription();
            }
        }
        return null;
    }

    static String firstUnknownKey(Map<String, Object> body, Set<String> known) {
        for (String key : body.keySet()) {
            if (known.contains(key) == false) {
                return key;
            }
        }
        return null;
    }

    static Map<String, Object> parseBody(RestRequest request) throws IOException {
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            return parser.map();
        }
    }
}
