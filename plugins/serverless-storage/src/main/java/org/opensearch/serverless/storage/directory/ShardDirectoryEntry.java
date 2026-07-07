/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.directory;

import java.util.Objects;

/**
 * A routing hint: "as of a moment in time, shard S was open in role R on node N, at generation
 * G." Deliberately a hint, not a fact -- {@link ShardDirectory} makes no consistency promise
 * about it (rfc-serverless-metadata-plane.md &sect;8/&sect;9: the directory tier is "hints", the
 * shard-head CAS record is "truth"). A coordinator that routes on a stale or wrong entry doesn't
 * corrupt anything: the request lands on a node that no longer holds the shard, that node's
 * engine construction fails the mismatch, and the coordinator falls back to a shard-head lookup
 * and retries -- one wasted hop, not a correctness bug.
 */
public final class ShardDirectoryEntry {

    private final String nodeId;
    private final ShardRole role;
    private final long primaryTerm;
    private final long generation;
    private final long expiresAtMillis;

    public ShardDirectoryEntry(String nodeId, ShardRole role, long primaryTerm, long generation, long expiresAtMillis) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.role = Objects.requireNonNull(role, "role");
        this.primaryTerm = primaryTerm;
        this.generation = generation;
        this.expiresAtMillis = expiresAtMillis;
    }

    public String nodeId() {
        return nodeId;
    }

    public ShardRole role() {
        return role;
    }

    public long primaryTerm() {
        return primaryTerm;
    }

    public long generation() {
        return generation;
    }

    public long expiresAtMillis() {
        return expiresAtMillis;
    }

    public boolean isExpired(long nowMillis) {
        return nowMillis >= expiresAtMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ShardDirectoryEntry)) return false;
        ShardDirectoryEntry that = (ShardDirectoryEntry) o;
        return primaryTerm == that.primaryTerm
            && generation == that.generation
            && expiresAtMillis == that.expiresAtMillis
            && nodeId.equals(that.nodeId)
            && role == that.role;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, role, primaryTerm, generation, expiresAtMillis);
    }

    @Override
    public String toString() {
        return "ShardDirectoryEntry{node="
            + nodeId
            + ", role="
            + role
            + ", term="
            + primaryTerm
            + ", gen="
            + generation
            + ", expiresAt="
            + expiresAtMillis
            + '}';
    }
}
