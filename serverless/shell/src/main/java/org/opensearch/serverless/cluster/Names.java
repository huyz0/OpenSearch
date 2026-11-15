/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.cluster;

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

    /**
     * What joins the parts of a register or container key: an index name and a shard number, a repository and a
     * snapshot name. It is safe because no name this class accepts can hold it.
     */
    public static final String KEY_SEPARATOR = "#";

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
     * Reports whether a name could be an index or alias here, without throwing.
     *
     * <p>For a caller walking names it did not mint — a node's assignments container, a listing — where a
     * stray object must be stepped over rather than abort the walk. A request naming an index still goes
     * through {@link #validateIndexOrAlias}, which says what is wrong with it.
     *
     * @param name the name
     * @return true if {@link #validateIndexOrAlias} would accept it
     */
    public static boolean isValidIndexOrAlias(String name) {
        try {
            validateIndexOrAlias(name);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
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
        validateUnicode(prefix, "index pattern");
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
        validateUnicode(name, what);
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

    private static void validateUnicode(String name, String what) {
        for (int i = 0; i < name.length(); i++) {
            final char c = name.charAt(i);
            if (Character.isSurrogate(c)) {
                if (Character.isHighSurrogate(c) && i + 1 < name.length() && Character.isLowSurrogate(name.charAt(i + 1))) {
                    i++;
                } else {
                    throw new IllegalArgumentException("Invalid " + what + ": unpaired Unicode surrogate at position " + i);
                }
            }
        }
    }

    /**
     * Reports whether a blob name is one this system would have written a record under.
     *
     * <p><b>Why a name check exists at all.</b> A record that cannot be read is treated as live — refusing
     * to delete data you cannot account for is the safe direction, and a half-written record must not have
     * its files swept out from under the caller who is about to use it. But "unreadable means it pins
     * everything" turns any stray object in this container into a deployment-wide off switch for the
     * garbage collector: one file left by a console, a backup tool, or a test filesystem, and nothing is
     * ever reclaimed again, silently.
     *
     * <p>So the two cases are separated by the only evidence available before parsing: the name. A
     * truncated record this system wrote still has the name this system gave it, so the conservative
     * treatment still covers the case it exists for. Something with a name we would never mint is not ours
     * and does not get to speak for our data.
     *
     * <p>The shape is {@link org.opensearch.common.UUIDs#randomBase64UUID}'s: twenty-two base64url characters
     * with no padding.
     *
     * @param name the blob name
     * @return true if it could be a record this system wrote
     */
    public static boolean looksLikeAnId(String name) {
        if (name == null || name.length() != 22) {
            return false;
        }
        for (int i = 0; i < name.length(); i++) {
            final char c = name.charAt(i);
            final boolean allowed = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_';
            if (allowed == false) {
                return false;
            }
        }
        return true;
    }
}
