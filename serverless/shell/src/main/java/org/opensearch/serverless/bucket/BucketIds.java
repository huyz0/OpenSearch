/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.bucket;

/**
 * What a bucket id may be: a short name safe as one object-store path segment.
 */
public final class BucketIds {

    /** The longest bucket id accepted. */
    public static final int MAX_LENGTH = 64;

    private BucketIds() {}

    /**
     * Checks a bucket id.
     *
     * @param id the candidate
     * @return the id, unchanged
     * @throws IllegalArgumentException if it is empty, too long, or holds anything but lowercase letters, digits,
     *     {@code -} and {@code _}
     */
    public static String validate(String id) {
        if (id == null || id.isEmpty() || id.length() > MAX_LENGTH) {
            throw new IllegalArgumentException("a bucket id must be 1 to " + MAX_LENGTH + " characters, got [" + id + "]");
        }
        for (int i = 0; i < id.length(); i++) {
            final char c = id.charAt(i);
            if ((c >= 'a' && c <= 'z') == false && (c >= '0' && c <= '9') == false && c != '-' && c != '_') {
                throw new IllegalArgumentException("a bucket id may hold only a-z, 0-9, '-' and '_', got [" + id + "]");
            }
        }
        return id;
    }
}
