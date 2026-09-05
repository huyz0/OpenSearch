/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.serverless.metadata.MetadataPlane;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Turning what a caller typed into the index names it means, within a bound.
 *
 * <p><b>The bound is the policy, and it already existed.</b> A prefix pattern resolves through one
 * {@code listBlobsByPrefixInSortedOrder} call with a cap; more matches than the cap is a <em>refusal</em>
 * rather than a truncated list, because an answer cut off at a limit looks exactly like a complete one. That
 * rule was written for search wildcards and is reused here rather than re-decided, which is the point of this
 * class existing at all.
 *
 * <p><b>Only prefix patterns.</b> {@code logs-*} is one bounded listing; {@code *-2026-*} is a scan of every
 * name in the deployment, and there is no index over the middle of a string to make it anything else.
 *
 * <p><b>What this deliberately does not do.</b> {@code SearchHandler} has a richer expansion that also records
 * which names came from a pattern, because that decides whether a missing index is a mistake or an
 * expectation — the {@code ignore_unavailable} question. Nothing here needs that distinction, so this is the
 * smaller operation rather than a second copy of the larger one; unifying them is deliberately not done in
 * passing.
 */
final class IndexPatterns {

    private IndexPatterns() {}

    /**
     * Expands names and prefix patterns into the index names they match.
     *
     * @param metadata the metadata plane
     * @param requested what the caller typed, comma-separated
     * @param cap the most matches a single pattern may produce
     * @return the resolved names, in the order they were first seen, without duplicates
     * @throws org.opensearch.serverless.metadata.DescriptorStore.TooManyMatchesException if a pattern matches
     *     more than the cap
     * @throws IOException if listing fails
     */
    static List<String> expand(MetadataPlane metadata, String requested, int cap) throws IOException {
        final Set<String> names = new LinkedHashSet<>();
        for (String each : requested.split(",")) {
            final String name = each.trim();
            if (name.isEmpty()) {
                continue;
            }
            if (isPrefixPattern(name)) {
                names.addAll(metadata.namesWithPrefix(name.substring(0, name.length() - 1), cap));
            } else {
                names.add(name);
            }
        }
        return new ArrayList<>(names);
    }

    /**
     * Whether a name is a pattern this design can resolve with one bounded listing.
     *
     * @param name the name as typed
     * @return whether it is a trailing-star prefix pattern
     */
    static boolean isPrefixPattern(String name) {
        return name.endsWith("*") && name.indexOf('*') == name.length() - 1;
    }
}
