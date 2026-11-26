/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.repository;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The aliases a snapshot recorded for one index, read for a restore.
 *
 * <p>An index entry of a record may carry {@code "aliases": {"<name>": {"is_write_index": true}}} and
 * {@code "aliases_truncated": true}. {@link SnapshotRecord} keeps both exactly as parsed, so listing, describing and
 * adopting a record never depend on them; this class is the one reader that checks them, and a field that is not what
 * a writer produces makes the record unreadable <em>to the restore that asked for aliases</em>.
 *
 * <p>The member is true only for an alias's explicit write member at the take; an implicit write target (a single
 * member with no flag) is recorded like any other member.
 */
public final class SnapshotAliases {

    /** The most aliases one index entry records; an index in more is recorded as truncated. */
    public static final int MAX_ALIASES = 64;

    /** The longest alias name a record holds, in UTF-8 bytes. */
    public static final int MAX_NAME_BYTES = 255;

    private static final SnapshotAliases NONE = new SnapshotAliases(Map.of(), false);

    private final Map<String, Boolean> writeFlags;
    private final boolean truncated;

    private SnapshotAliases(Map<String, Boolean> writeFlags, boolean truncated) {
        this.writeFlags = writeFlags;
        this.truncated = truncated;
    }

    /**
     * Reads and checks the alias fields of one index entry.
     *
     * @param index the entry
     * @return what it recorded; nothing for an entry without the fields
     * @throws IOException if either field is not what a writer produces
     */
    public static SnapshotAliases read(SnapshotRecord.SnapshottedIndex index) throws IOException {
        final Object aliases = index.aliases();
        final Object truncated = index.aliasesTruncated();
        if (truncated != null && truncated instanceof Boolean == false) {
            throw malformed("aliases_truncated must be a boolean");
        }
        if (aliases == null) {
            return truncated == null || Boolean.FALSE.equals(truncated) ? NONE : new SnapshotAliases(Map.of(), true);
        }
        if (aliases instanceof Map<?, ?> == false) {
            throw malformed("aliases must be an object");
        }
        final Map<?, ?> given = (Map<?, ?>) aliases;
        if (given.size() > MAX_ALIASES) {
            throw malformed("more than " + MAX_ALIASES + " aliases");
        }
        final Map<String, Boolean> flags = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : given.entrySet()) {
            final String name = entry.getKey() instanceof String text ? text : "";
            if (name.isEmpty() || name.getBytes(StandardCharsets.UTF_8).length > MAX_NAME_BYTES) {
                throw malformed("an alias name must be 1 to " + MAX_NAME_BYTES + " bytes");
            }
            if (entry.getValue() instanceof Map<?, ?> == false) {
                throw malformed("an alias entry must be an object");
            }
            final Object write = ((Map<?, ?>) entry.getValue()).get("is_write_index");
            if (write != null && write instanceof Boolean == false) {
                throw malformed("is_write_index must be a boolean");
            }
            flags.put(name, Boolean.TRUE.equals(write));
        }
        return new SnapshotAliases(Collections.unmodifiableMap(flags), Boolean.TRUE.equals(truncated));
    }

    /**
     * Builds the {@code aliases} member a take writes.
     *
     * @param writeFlags each alias name to whether the index was its explicit write member
     * @return the member, which a record carries and writes as given
     */
    public static Map<String, Object> encode(Map<String, Boolean> writeFlags) {
        final Map<String, Object> encoded = new LinkedHashMap<>();
        for (Map.Entry<String, Boolean> flag : writeFlags.entrySet()) {
            encoded.put(flag.getKey(), Boolean.TRUE.equals(flag.getValue()) ? Map.of("is_write_index", true) : Map.of());
        }
        return encoded;
    }

    /**
     * Returns each recorded alias and whether the index was its explicit write member.
     *
     * @return alias name to write flag, in the record's order
     */
    public Map<String, Boolean> writeFlags() {
        return writeFlags;
    }

    /**
     * Reports whether the index was in more aliases than the record names.
     *
     * @return true if the take cut the list
     */
    public boolean truncated() {
        return truncated;
    }

    private static IOException malformed(String reason) {
        return new IOException("malformed snapshot record: " + reason);
    }
}
