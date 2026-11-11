/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * The tombstone a deleted index leaves in its name's register: how one is written and recognised, the times it
 * carries, and the hourly bucket its reaping is filed under. Split out of {@link DescriptorStore}.
 */
final class Tombstones {

    private Tombstones() {}

    /**
     * How a tombstone begins. Every tombstone this store ever wrote starts this way, with or without the
     * {@code deleted_at} that later ones carry, and no descriptor or alias record does.
     */
    static final String TOMBSTONE_PREFIX = "{\"tombstone\":true";

    /** Buckets are named for the UTC hour they start at, so an operator reading a bucket can tell when. */
    static final DateTimeFormatter BUCKET_FORMAT = DateTimeFormatter.ofPattern("uuuuMMddHH", java.util.Locale.ROOT)
        .withZone(ZoneOffset.UTC);

    static boolean isTombstone(org.opensearch.core.common.bytes.BytesReference value) {
        if (value.length() < TOMBSTONE_PREFIX.length()) {
            return false;
        }
        final byte[] head = org.opensearch.core.common.bytes.BytesReference.toBytes(value.slice(0, TOMBSTONE_PREFIX.length()));
        return new String(head, java.nio.charset.StandardCharsets.UTF_8).equals(TOMBSTONE_PREFIX);
    }

    /** Renders a tombstone recording when the delete happened. */
    static org.opensearch.core.common.bytes.BytesArray tombstone(long deletedAtMillis) {
        return tombstone(deletedAtMillis, 0L);
    }

    /**
     * Renders a tombstone recording when the delete happened and, if non-zero, when a sweep claimed it for
     * removal. Still begins with {@link #TOMBSTONE_PREFIX}, so every reader that treats a tombstone as
     * absent treats a claimed one the same way.
     */
    static org.opensearch.core.common.bytes.BytesArray tombstone(long deletedAtMillis, long reapingAtMillis) {
        final String claim = reapingAtMillis == 0L ? "" : ",\"reaping_at\":" + reapingAtMillis;
        return new org.opensearch.core.common.bytes.BytesArray(
            (TOMBSTONE_PREFIX + ",\"deleted_at\":" + deletedAtMillis + claim + "}").getBytes(java.nio.charset.StandardCharsets.UTF_8)
        );
    }

    /** When a tombstone's delete happened, or 0 for one written before that was recorded. */
    static long tombstoneDeletedAt(org.opensearch.core.common.bytes.BytesReference value) throws IOException {
        return tombstoneField(value, "deleted_at");
    }

    /** When a sweep claimed a tombstone for removal, or 0 if none has. */
    static long tombstoneReapingAt(org.opensearch.core.common.bytes.BytesReference value) throws IOException {
        return tombstoneField(value, "reaping_at");
    }

    static long tombstoneField(org.opensearch.core.common.bytes.BytesReference value, String field) throws IOException {
        try (
            org.opensearch.core.xcontent.XContentParser parser = org.opensearch.common.xcontent.XContentType.JSON.xContent()
                .createParser(
                    org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                    org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    value.streamInput()
                )
        ) {
            final Object stamp = parser.map().get(field);
            return stamp instanceof Number number ? number.longValue() : 0L;
        }
    }

    /** The bucket a delete at this moment drops its marker into. */
    static String tombstoneBucket(long millis) {
        return BUCKET_FORMAT.format(Instant.ofEpochMilli(millis));
    }

    /** When a bucket starts, or -1 for a name that is not a bucket this store wrote. */
    static long tombstoneBucketStart(String bucket) {
        if (bucket.length() != 10 || bucket.chars().allMatch(Character::isDigit) == false) {
            return -1L;
        }
        try {
            return LocalDateTime.of(
                Integer.parseInt(bucket.substring(0, 4)),
                Integer.parseInt(bucket.substring(4, 6)),
                Integer.parseInt(bucket.substring(6, 8)),
                Integer.parseInt(bucket.substring(8, 10)),
                0
            ).toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (java.time.DateTimeException e) {
            return -1L;
        }
    }
}
