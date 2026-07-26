/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import java.nio.charset.StandardCharsets;

/**
 * Glob matching and byte-order comparison for index names.
 *
 * <p>Two things live here because both are easy to get subtly wrong and both need to agree with
 * {@link CompactNameIndex}'s internal comparisons.
 *
 * <p><b>Patterns are globs, not regular expressions.</b> Only {@code *} and {@code ?} are special, which
 * is what OpenSearch index patterns support. Accepting more would be a liability rather than a feature:
 * a name containing a regex metacharacter is legal, and treating it as syntax would silently change what
 * a user's pattern selects.
 *
 * <p><b>Comparison is UTF-8 byte order, unsigned.</b> This must match {@link CompactNameIndex} exactly.
 * {@link String#compareTo} is UTF-16 code unit order and disagrees above the basic multilingual plane,
 * so mixing the two produces a merge that emits results out of order and a binary search that misses
 * entries which are present.
 */
public final class NamePatterns {

    private NamePatterns() {}

    /**
     * The leading run of literal characters before the first wildcard, which is the range of the sorted
     * index a pattern can possibly match.
     *
     * <p>Returns empty for a pattern starting with a wildcard, which is the leading-wildcard case: it
     * has no seekable prefix and degenerates to a full scan.
     */
    public static String literalPrefixOf(String pattern) {
        int end = 0;
        while (end < pattern.length()) {
            char c = pattern.charAt(end);
            if (c == '*' || c == '?') {
                break;
            }
            end++;
        }
        // Never split a surrogate pair. Cutting between the high and low halves would produce a prefix
        // that is not valid UTF-16, and encoding it yields a replacement character that matches nothing.
        if (end > 0 && end < pattern.length() && Character.isHighSurrogate(pattern.charAt(end - 1))) {
            end--;
        }
        return pattern.substring(0, end);
    }

    /** Whether the pattern contains any wildcard at all. */
    public static boolean isPattern(String pattern) {
        return pattern.indexOf('*') >= 0 || pattern.indexOf('?') >= 0;
    }

    /**
     * Whether a pattern with no seekable literal prefix, which cannot use the sorted order and must scan
     * every name. Callers that need to bound cost check this before running the query.
     */
    public static boolean isLeadingWildcard(String pattern) {
        return pattern.isEmpty() == false && (pattern.charAt(0) == '*' || pattern.charAt(0) == '?');
    }

    /**
     * Glob match, iterative with backtracking rather than recursive.
     *
     * <p>Recursion here would be O(2^n) on adversarial patterns like {@code a*a*a*a*b} and could also
     * overflow the stack on a long pattern. The backtracking form keeps one restart point for the most
     * recent {@code *} and is linear in practice.
     */
    public static boolean matches(String pattern, String name) {
        int p = 0;
        int n = 0;
        int starIndex = -1;
        int nameAtStar = -1;

        while (n < name.length()) {
            if (p < pattern.length() && (pattern.charAt(p) == '?' || pattern.charAt(p) == name.charAt(n))) {
                p++;
                n++;
            } else if (p < pattern.length() && pattern.charAt(p) == '*') {
                // Record where to resume if the rest of the pattern fails, then try matching zero
                // characters here first.
                starIndex = p;
                nameAtStar = n;
                p++;
            } else if (starIndex >= 0) {
                // Backtrack: let the last star absorb one more character.
                p = starIndex + 1;
                n = ++nameAtStar;
            } else {
                return false;
            }
        }

        while (p < pattern.length() && pattern.charAt(p) == '*') {
            p++;
        }
        return p == pattern.length();
    }

    /** Unsigned UTF-8 byte comparison, matching {@link CompactNameIndex}'s ordering. */
    public static int compareUtf8(String a, String b) {
        return compareUtf8(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    /** Unsigned UTF-8 byte comparison. Unsigned matters: every continuation byte is above 0x7F. */
    public static int compareUtf8(byte[] a, byte[] b) {
        int shared = Math.min(a.length, b.length);
        for (int i = 0; i < shared; i++) {
            int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return a.length - b.length;
    }
}
