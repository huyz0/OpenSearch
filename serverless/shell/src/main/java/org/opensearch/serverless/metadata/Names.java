/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.core.common.Strings;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * What a name may be, before it becomes an object-store path.
 *
 * <p>Every index, alias, template, pipeline, script, repository and snapshot name in this deployment is
 * used verbatim as a blob name inside a container. That is the design -- one register per name, found by
 * name -- and it is only safe when a name cannot be a path. Until this existed nothing checked, and
 * {@code PUT /..%2F..%2Fx} wrote a descriptor two directories above the deployment root on the filesystem
 * store. Core's own rules for an index name are applied here, and two characters this design uses as
 * separators are refused on top of them.
 */
public final class Names {

    /** The longest a name may be, in bytes, which is core's limit and the filesystem's. */
    public static final int MAX_BYTES = 255;

    private Names() {}

    /**
     * Refuses a name that cannot be an index or alias here.
     *
     * <p>Core's rules ({@code MetadataCreateIndexService#validateIndexOrAliasName}), plus {@code #} and
     * {@code :}, which this design uses to join a name to a shard number and a repository to a snapshot.
     *
     * @param name the name as the caller wrote it
     * @throws IllegalArgumentException naming the rule broken, in core's words where core has them
     */
    public static void validateIndexOrAlias(String name) {
        validateCommon(name, "index or alias");
        if (Strings.validFileName(name) == false) {
            throw new IllegalArgumentException(
                "Invalid index name [" + name + "], must not contain the following characters " + Strings.INVALID_FILENAME_CHARS
            );
        }
        if (name.contains("#") || name.contains(":")) {
            throw new IllegalArgumentException("Invalid index name [" + name + "], must not contain '#' or ':'");
        }
        if (name.charAt(0) == '_' || name.charAt(0) == '-' || name.charAt(0) == '+') {
            throw new IllegalArgumentException("Invalid index name [" + name + "], must not start with '_', '-', or '+'");
        }
        if (name.equals(".") || name.equals("..")) {
            throw new IllegalArgumentException("Invalid index name [" + name + "], must not be '.' or '..'");
        }
        if (name.equals(name.toLowerCase(Locale.ROOT)) == false) {
            throw new IllegalArgumentException("Invalid index name [" + name + "], must be lowercase");
        }
    }

    /**
     * Refuses a name that cannot be a template, pipeline, script, repository or snapshot here.
     *
     * <p>Looser than an index name -- core allows uppercase and most punctuation in these -- but never a
     * path: no separators, no traversal, no glob characters, no control characters, and not a name an
     * API path would claim.
     *
     * @param name the name as the caller wrote it
     * @param what what it names, for the message
     * @throws IllegalArgumentException naming the rule broken
     */
    public static void validateId(String name, String what) {
        validateCommon(name, what);
        for (int i = 0; i < name.length(); i++) {
            final char c = name.charAt(i);
            if (c == '/' || c == '\\' || c == '#' || c == '*' || c == '?' || c == '"' || c == '<' || c == '>' || c == '|' || c == ',') {
                throw new IllegalArgumentException("Invalid " + what + " name [" + name + "], must not contain '" + c + "'");
            }
        }
        if (name.equals(".") || name.equals("..")) {
            throw new IllegalArgumentException("Invalid " + what + " name [" + name + "], must not be '.' or '..'");
        }
        if (name.charAt(0) == '_') {
            throw new IllegalArgumentException("Invalid " + what + " name [" + name + "], must not start with '_'");
        }
    }

    /**
     * Refuses a prefix that cannot be listed safely: the same rules as an index name, minus the ones
     * about how a name starts, since a prefix may be empty or partial.
     *
     * @param prefix the prefix, without its trailing star
     * @throws IllegalArgumentException naming the rule broken
     */
    public static void validatePrefix(String prefix) {
        if (prefix.isEmpty()) {
            return;
        }
        if (prefix.getBytes(StandardCharsets.UTF_8).length > MAX_BYTES) {
            throw new IllegalArgumentException("Invalid index pattern [" + prefix + "*], too long");
        }
        for (int i = 0; i < prefix.length(); i++) {
            final char c = prefix.charAt(i);
            if (Strings.INVALID_FILENAME_CHARS.contains(c)
                || c == '#'
                || c == ':'
                || c == '['
                || c == ']'
                || c == '{'
                || c == '}'
                || Character.isISOControl(c)) {
                throw new IllegalArgumentException("Invalid index pattern [" + prefix + "*], must not contain '" + c + "'");
            }
        }
    }

    private static void validateCommon(String name, String what) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Invalid " + what + " name [" + name + "], must not be empty");
        }
        if (name.getBytes(StandardCharsets.UTF_8).length > MAX_BYTES) {
            throw new IllegalArgumentException(
                "Invalid "
                    + what
                    + " name ["
                    + name
                    + "], name is too long: ("
                    + name.getBytes(StandardCharsets.UTF_8).length
                    + " > "
                    + MAX_BYTES
                    + ")"
            );
        }
        for (int i = 0; i < name.length(); i++) {
            if (Character.isISOControl(name.charAt(i))) {
                throw new IllegalArgumentException("Invalid " + what + " name [" + name + "], must not contain control characters");
            }
        }
    }
}
