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

    private final String pinId;
    private final long primaryTerm;
    private final long generation;

    /**
     * Creates a pin on a specific manifest generation.
     *
     * @param pinId       identifies why the generation is pinned (e.g. a snapshot name, or {@code "pitr"}).
     * @param primaryTerm the primary term of the pinned manifest generation.
     * @param generation  the pinned manifest generation.
     */
    public PinRecord(String pinId, long primaryTerm, long generation) {
        this.pinId = Objects.requireNonNull(pinId, "pinId");
        this.primaryTerm = primaryTerm;
        this.generation = generation;
    }

    /**
     * Deserializes a pin record previously written by {@link #writeTo}.
     *
     * @param in the stream to read the pin record from.
     */
    public PinRecord(StreamInput in) throws IOException {
        this(in.readString(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pinId);
        out.writeVLong(primaryTerm);
        out.writeVLong(generation);
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
