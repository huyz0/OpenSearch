/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.reconcile;

import org.opensearch.common.blobstore.BlobContainer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Every blob in a shard's older term containers that a sweep has to judge, known without listing them.
 *
 * <p>Within one live term nothing new lands in an older term container except a fenced zombie's leftovers,
 * which the periodic relisting finds. What can become collectable there is what the manifest or a pin named
 * last time and has stopped naming, so a sweep that remembers those names judges what a listing would have
 * found worth judging, without the listing.
 *
 * @param term the live term these names were taken under; any other term means list again
 * @param listedAtMillis when the shard's older terms were last listed
 * @param blobs every older-term blob the manifest or a pin named, or the sweep still watches
 */
public record SweepScope(long term, long listedAtMillis, Set<String> blobs) {

    /**
     * How long a sweep may judge from what it remembers before listing once more. A zombie that kept writing
     * into its fenced term after the last listing leaves blobs nothing named; this bounds how long they wait.
     */
    public static final long RELIST_AFTER_MILLIS = 60L * 60L * 1000L;

    /**
     * Copies the names.
     *
     * @param term the live term
     * @param listedAtMillis when last listed
     * @param blobs the names
     */
    public SweepScope {
        blobs = Set.copyOf(blobs);
    }

    /** One listing of the shard's container, then one of each term container older than the live one. */
    static Map<String, Set<String>> listOlderTerms(BlobContainer shardContainer, long liveTerm) throws IOException {
        final Map<String, Set<String>> byTerm = new TreeMap<>();
        for (Map.Entry<String, BlobContainer> child : shardContainer.children().entrySet()) {
            final Long term = parseTerm(child.getKey());
            if (term == null || term >= liveTerm) {
                // Not a term container, or the live term, or a higher one. A higher term container means
                // a newer writer is mid-publish and about to become the truth; leave it entirely alone.
                continue;
            }
            byTerm.put(child.getKey(), new TreeSet<>(child.getValue().listBlobs().keySet()));
        }
        return byTerm;
    }

    /** The remembered names and the watched candidates, grouped by older term container, with no request. */
    static Map<String, Set<String>> olderTermBlobs(Set<String> known, Set<String> candidates, long liveTerm) {
        final Set<String> all = new HashSet<>(known);
        if (candidates != null) {
            all.addAll(candidates);
        }
        final Map<String, Set<String>> byTerm = new TreeMap<>();
        for (String qualified : olderOnly(all, liveTerm)) {
            final int slash = qualified.indexOf('/');
            byTerm.computeIfAbsent(qualified.substring(0, slash), ignored -> new TreeSet<>()).add(qualified.substring(slash + 1));
        }
        return byTerm;
    }

    /** Only names of the form {@code t=<term>/<blob>} with a term older than the live one: node packs and the live term are not ours. */
    static Set<String> olderOnly(Set<String> qualified, long liveTerm) {
        final Set<String> older = new HashSet<>();
        for (String name : qualified) {
            final int slash = name.indexOf('/');
            if (slash <= 0 || name.indexOf('/', slash + 1) >= 0) {
                continue;
            }
            final Long term = parseTerm(name.substring(0, slash));
            if (term != null && term < liveTerm) {
                older.add(name);
            }
        }
        return Set.copyOf(older);
    }

    private static Long parseTerm(String containerName) {
        if (containerName.startsWith("t=") == false) {
            return null;
        }
        try {
            return Long.parseLong(containerName.substring(2));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
