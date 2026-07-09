/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.shardstate;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * The correctness-bearing "truth" for one shard (rfc-serverless-metadata-plane.md &sect;4):
 * current primary term, which node (if any) holds the writer/compactor lease and until when,
 * and the generation of the latest published commit manifest. Mutated only via
 * {@link ShardStateStore#compareAndSet}, never read-modify-write without a version check.
 */
public final class ShardHead implements Writeable {

    private final long primaryTerm;
    private final String leaseHolderNodeId;
    private final long leaseExpiryMillis;
    private final long latestManifestGeneration;

    /**
     * Constructs a shard head from its four component fields directly.
     *
     * @param primaryTerm the current primary term; must be &gt;= 1
     * @param leaseHolderNodeId the node id currently holding the writer/compactor lease, or {@code null} if no lease is held
     * @param leaseExpiryMillis the epoch millis at which the current lease (if any) expires
     * @param latestManifestGeneration the generation of the latest published commit manifest; must be &gt;= 0
     */
    public ShardHead(long primaryTerm, String leaseHolderNodeId, long leaseExpiryMillis, long latestManifestGeneration) {
        if (primaryTerm < 1) {
            throw new IllegalArgumentException("primaryTerm must be >= 1, got " + primaryTerm);
        }
        if (latestManifestGeneration < 0) {
            throw new IllegalArgumentException("latestManifestGeneration must be >= 0, got " + latestManifestGeneration);
        }
        this.primaryTerm = primaryTerm;
        this.leaseHolderNodeId = leaseHolderNodeId;
        this.leaseExpiryMillis = leaseExpiryMillis;
        this.latestManifestGeneration = latestManifestGeneration;
    }

    /** The head for a shard's very first activation: term 1, generation 0, no lease held yet. */
    public static ShardHead initial() {
        return new ShardHead(1, null, 0L, 0L);
    }

    /**
     * Deserializes a shard head previously written by {@link #writeTo}.
     *
     * @param in the stream to read the head's fields from
     * @throws IOException if reading from the stream fails
     */
    public ShardHead(StreamInput in) throws IOException {
        this(in.readVLong(), in.readOptionalString(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(primaryTerm);
        out.writeOptionalString(leaseHolderNodeId);
        out.writeVLong(leaseExpiryMillis);
        out.writeVLong(latestManifestGeneration);
    }

    /** The current primary term recorded on this head. */
    public long primaryTerm() {
        return primaryTerm;
    }

    /** The node id currently holding the writer/compactor lease, or {@code null} if no lease is held. */
    public String leaseHolderNodeId() {
        return leaseHolderNodeId;
    }

    /** The epoch millis at which the current lease (if any) expires. */
    public long leaseExpiryMillis() {
        return leaseExpiryMillis;
    }

    /** The generation of the latest published commit manifest recorded on this head. */
    public long latestManifestGeneration() {
        return latestManifestGeneration;
    }

    /**
     * Whether {@link #leaseHolderNodeId} is non-null and its lease has not yet expired as of {@code nowMillis}.
     *
     * @param nowMillis the current time in epoch millis to check the lease expiry against
     * @return {@code true} if a lease is held and still valid at {@code nowMillis}, {@code false} otherwise
     */
    public boolean isLeaseHeldAt(long nowMillis) {
        return leaseHolderNodeId != null && nowMillis < leaseExpiryMillis;
    }

    /**
     * The head after a writer acquires or renews the lease (rfc-serverless-opensearch.md &sect;16
     * Phase 4.5's lease-acquisition note), leaving {@code primaryTerm} and {@code
     * latestManifestGeneration} completely untouched. Term advancement is deliberately not this
     * method's job -- only a real publication (see {@link #withPublishedGeneration(long, long)})
     * changes {@code primaryTerm}, so the head's {@code (primaryTerm, latestManifestGeneration)}
     * pair always stays a valid pointer to the last real manifest ({@code
     * ObjectStoreCommitHeadPublisher#readLatestManifest} depends on this): if lease acquisition
     * instead wrote a newly-activating writer's not-yet-published term into the head, that pointer
     * would dangle at a manifest that does not exist yet.
     *
     * @param nodeId the node id acquiring or renewing the writer/compactor lease
     * @param leaseExpiryMillis the epoch millis at which the renewed lease expires
     * @return a new head with the lease fields updated and {@code primaryTerm}/{@code latestManifestGeneration} unchanged
     */
    public ShardHead withRenewedLease(String nodeId, long leaseExpiryMillis) {
        return new ShardHead(primaryTerm, nodeId, leaseExpiryMillis, latestManifestGeneration);
    }

    /**
     * The head after a publication advances the manifest generation under the current term/lease.
     *
     * @param generation the new manifest generation; must be strictly greater than the current {@link #latestManifestGeneration}
     * @return a new head with {@code latestManifestGeneration} updated and all other fields unchanged
     */
    public ShardHead withPublishedGeneration(long generation) {
        if (generation <= latestManifestGeneration) {
            throw new IllegalArgumentException(
                "new generation " + generation + " must be > current generation " + latestManifestGeneration
            );
        }
        return new ShardHead(primaryTerm, leaseHolderNodeId, leaseExpiryMillis, generation);
    }

    /**
     * The head after a publication advances both the manifest generation and (if the publishing
     * writer's term is newer) the lease term itself -- the one legitimate way {@code primaryTerm}
     * ever moves forward (rfc-serverless-opensearch.md &sect;16 Phase 4.5): a writer activating
     * under a newly bumped term is fenced out of publishing anything until its own first commit
     * proves it live, at which point the head should reflect that new term from then on. See {@link
     * #withPublishedGeneration(long)} for the same-term case, which this delegates to after
     * validating the term.
     *
     * @param primaryTerm the publishing writer's primary term; must be &gt;= this head's current term
     * @param generation the new manifest generation; must be strictly greater than the current {@link #latestManifestGeneration}
     * @return a new head with {@code primaryTerm} and {@code latestManifestGeneration} updated and the lease fields unchanged
     * @throws IllegalArgumentException if {@code primaryTerm} is older than this head's current
     *         term -- publishing under a term older than one already recorded here would mean a
     *         stale writer is publishing after being superseded, which must never happen.
     */
    public ShardHead withPublishedGeneration(long primaryTerm, long generation) {
        if (primaryTerm < this.primaryTerm) {
            throw new IllegalArgumentException(
                "cannot publish under stale primaryTerm " + primaryTerm + " < current term " + this.primaryTerm
            );
        }
        if (generation <= latestManifestGeneration) {
            throw new IllegalArgumentException(
                "new generation " + generation + " must be > current generation " + latestManifestGeneration
            );
        }
        return new ShardHead(primaryTerm, leaseHolderNodeId, leaseExpiryMillis, generation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ShardHead)) return false;
        ShardHead that = (ShardHead) o;
        return primaryTerm == that.primaryTerm
            && leaseExpiryMillis == that.leaseExpiryMillis
            && latestManifestGeneration == that.latestManifestGeneration
            && Objects.equals(leaseHolderNodeId, that.leaseHolderNodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryTerm, leaseHolderNodeId, leaseExpiryMillis, latestManifestGeneration);
    }

    @Override
    public String toString() {
        return "ShardHead{term="
            + primaryTerm
            + ", lease="
            + leaseHolderNodeId
            + "@"
            + leaseExpiryMillis
            + ", gen="
            + latestManifestGeneration
            + '}';
    }
}
