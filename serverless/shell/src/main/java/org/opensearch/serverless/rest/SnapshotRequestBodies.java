/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

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
    static final Set<String> RESTORE_KNOWN_KEYS = Set.of("indices", "rename_pattern", "rename_replacement");

    /**
     * Core's other restore fields, each with why it is refused rather than accepted and ignored. An
     * allowlist used to admit all of these and act on three, which told a caller -- by answering 400 for
     * an unknown key -- that a known key was honoured.
     */
    static final java.util.Map<String, String> RESTORE_REFUSED_KEYS = java.util.Map.ofEntries(
        java.util.Map.entry("partial", "a restore here creates every shard of an index or refuses; there is no partial index to make"),
        java.util.Map.entry("include_global_state", "there is no global state in a snapshot here to restore"),
        java.util.Map.entry("include_aliases", "aliases are not captured in a snapshot here"),
        java.util.Map.entry("rename_alias_pattern", "aliases are not captured in a snapshot here"),
        java.util.Map.entry("rename_alias_replacement", "aliases are not captured in a snapshot here"),
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
        java.util.Map.entry("alias_write_index_policy", "aliases are not captured in a snapshot here"),
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
                org.opensearch.serverless.metadata.Names.validateId(each, what);
            } catch (IllegalArgumentException e) {
                return e.getMessage();
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
