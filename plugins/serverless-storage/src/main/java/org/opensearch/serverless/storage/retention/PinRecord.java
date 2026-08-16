/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.serverless.storage.gc.ManifestId;

import java.io.IOException;
import java.util.Objects;

/**
 * One durable retention pin on a specific manifest generation: unlike a lease (which expires
 * when its holder dies), a durable pin survives every node and is removed only by explicit
 * policy action (a snapshot being deleted, a PITR window rolling forward) &mdash;
 * rfc-serverless-opensearch.md &sect;6.5/&sect;14. {@code pinId} identifies *why* the generation
 * is pinned (e.g. a snapshot name, or {@code "pitr"}), so independent retention reasons on the
 * same shard don't clobber each other and each can be removed independently.
 */
public final class PinRecord implements Writeable {

    /** The {@link #expiresAtMillis()} of a pin that never expires, which is what a deliberate one is. */
    public static final long NEVER_EXPIRES = 0L;

    private final String pinId;
    private final long primaryTerm;
    private final long generation;
    private final String ownerId;
    private final long expiresAtMillis;

    /**
     * Creates a pin on a specific manifest generation.
     *
     * @param pinId       identifies why the generation is pinned (e.g. a snapshot name, or {@code "pitr"}).
     * @param primaryTerm the primary term of the pinned manifest generation.
     * @param generation  the pinned manifest generation.
     */
    public PinRecord(String pinId, long primaryTerm, long generation) {
        this(pinId, primaryTerm, generation, "", NEVER_EXPIRES);
    }

    /**
     * Creates a pin that says who took it and when it stops counting.
     *
     * <h4>Why a pin needs an expiry at all</h4>
     *
     * A pin is durable and unconditional: it holds a generation against garbage collection until something
     * removes it. That is right for a pin an operator took deliberately, and wrong for one taken by an
     * operation that then died -- an index-wide pin is N shard pins written in sequence, and a coordinator
     * that stops halfway leaves pins nothing will ever release. An expiry turns that from a permanent leak
     * into a bounded one.
     *
     * <h4>Why the expiry is not on the identity</h4>
     *
     * {@link #equals} and {@link #hashCode} read the pin id, term and generation only. Owner and expiry are
     * attributes of a pin, not part of which pin it is, and that is load-bearing: {@code addPin} is
     * idempotent by set membership, so folding an expiry into equality would make re-pinning the same
     * generation add a second record rather than be the no-op it is documented to be.
     *
     * @param pinId           identifies why the generation is pinned.
     * @param primaryTerm     the primary term of the pinned manifest generation.
     * @param generation      the pinned manifest generation.
     * @param ownerId         who took it -- a node id, for diagnosing what left a pin behind. May be empty.
     * @param expiresAtMillis when it stops holding the generation, or {@link #NEVER_EXPIRES}.
     */
    public PinRecord(String pinId, long primaryTerm, long generation, String ownerId, long expiresAtMillis) {
        this.pinId = Objects.requireNonNull(pinId, "pinId");
        this.primaryTerm = primaryTerm;
        this.generation = generation;
        this.ownerId = ownerId == null ? "" : ownerId;
        this.expiresAtMillis = expiresAtMillis;
    }

    /**
     * Whether this pin still holds its generation at the given instant.
     *
     * <p>Takes the instant rather than reading a clock, so the decision is testable and so every pin in one
     * sweep is judged against the same moment.
     */
    public boolean isLiveAt(long nowMillis) {
        return expiresAtMillis == NEVER_EXPIRES || expiresAtMillis > nowMillis;
    }

    /** The same pin with a different expiry -- what confirming or renewing one produces. */
    public PinRecord withExpiry(long newExpiresAtMillis) {
        return new PinRecord(pinId, primaryTerm, generation, ownerId, newExpiresAtMillis);
    }

    /** Who took this pin, or empty for one written before pins recorded that. */
    public String ownerId() {
        return ownerId;
    }

    /** When this pin stops holding its generation, or {@link #NEVER_EXPIRES}. */
    public long expiresAtMillis() {
        return expiresAtMillis;
    }

    /**
     * Deserializes a pin record previously written by {@link #writeTo}.
     *
     * @param in the stream to read the pin record from.
     */
    public PinRecord(StreamInput in) throws IOException {
        this(in.readString(), in.readVLong(), in.readVLong(), in.readString(), in.readVLong());
    }

    /**
     * Reads a pin written before pins carried an owner and an expiry.
     *
     * <p>Such a pin is treated as never expiring, which is the only safe reading: it was taken when the only
     * way to remove a pin was to remove it, so nothing about it consented to disappearing on a timer.
     */
    public static PinRecord readLegacy(StreamInput in) throws IOException {
        return new PinRecord(in.readString(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pinId);
        out.writeVLong(primaryTerm);
        out.writeVLong(generation);
        out.writeString(ownerId);
        out.writeVLong(expiresAtMillis);
    }

    /** Why the generation is pinned (e.g. a snapshot name, or {@code "pitr"}). */
    public String pinId() {
        return pinId;
    }

    /** The primary term of the pinned manifest generation. */
    public long primaryTerm() {
        return primaryTerm;
    }

    /** The pinned manifest generation. */
    public long generation() {
        return generation;
    }

    /** Converts this pin to the {@link ManifestId} of the manifest generation it pins. */
    public ManifestId toManifestId() {
        return new ManifestId(primaryTerm, generation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PinRecord)) return false;
        PinRecord that = (PinRecord) o;
        return primaryTerm == that.primaryTerm && generation == that.generation && pinId.equals(that.pinId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pinId, primaryTerm, generation);
    }

    @Override
    public String toString() {
        return "PinRecord{pinId='" + pinId + "', term=" + primaryTerm + ", gen=" + generation + '}';
    }
}
