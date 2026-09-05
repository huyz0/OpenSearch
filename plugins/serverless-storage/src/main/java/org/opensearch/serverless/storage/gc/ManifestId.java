/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.opensearch.serverless.storage.manifest.CommitManifest;

import java.util.Objects;

/**
 * Identifies one manifest by (primaryTerm, generation), independent of shard/index. Used to key
 * lease pins and durable retention pins so a pin can never be confused across a term boundary
 * (e.g. a lease recorded against term 1's generation 5 must not accidentally protect term 2's
 * generation 5 after a failover).
 */
public final class ManifestId {

    private final long primaryTerm;
    private final long generation;

    /**
     * Identifies a manifest by its term and generation.
     *
     * @param primaryTerm the primary term the manifest was published under.
     * @param generation the manifest's generation number within that term.
     */
    public ManifestId(long primaryTerm, long generation) {
        this.primaryTerm = primaryTerm;
        this.generation = generation;
    }

    /**
     * Extracts the identity of an already-published manifest.
     *
     * @param manifest the manifest to identify.
     * @return the manifest's (primaryTerm, generation) identity.
     */
    public static ManifestId of(CommitManifest manifest) {
        return new ManifestId(manifest.primaryTerm(), manifest.generation());
    }

    /** The primary term the manifest was published under. */
    public long primaryTerm() {
        return primaryTerm;
    }

    /** The manifest's generation number within its primary term. */
    public long generation() {
        return generation;
    }

    /**
     * Whether {@code this} is strictly newer than {@code other} under the total order (primaryTerm,
     * generation) &mdash; the <em>same</em> order {@link CommitManifest#isNewerThan} uses, deliberately, so
     * that "newer" means one thing everywhere GC reasons about it.
     *
     * <p>Term dominates generation: a fenced writer computes its generation from the live head but stamps
     * its own (older) term, so a higher generation under a stale term is still older overall
     * (rfc-serverless-opensearch.md &sect;6.3 fencing). Duplicating the comparison here rather than
     * converting to a {@link CommitManifest} matters because the shard head is a {@code (term, generation)}
     * pair with no manifest body behind it -- reading one just to compare would defeat the point of
     * anchoring GC to the head in the first place.
     *
     * @param other the identity to compare against.
     * @return {@code true} if {@code this} is strictly newer than {@code other}.
     */
    public boolean isNewerThan(ManifestId other) {
        if (this.primaryTerm != other.primaryTerm) {
            return this.primaryTerm > other.primaryTerm;
        }
        return this.generation > other.generation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ManifestId)) return false;
        ManifestId that = (ManifestId) o;
        return primaryTerm == that.primaryTerm && generation == that.generation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryTerm, generation);
    }

    @Override
    public String toString() {
        return primaryTerm + "-" + generation;
    }
}
