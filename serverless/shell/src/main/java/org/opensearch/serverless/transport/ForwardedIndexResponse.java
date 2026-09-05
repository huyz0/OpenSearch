/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;

/** Acknowledgement that the owning node made a forwarded write durable. */
public final class ForwardedIndexResponse extends TransportResponse {

    private final String ownerNodeId;
    private final long seqNo;
    private final long primaryTerm;
    private final long version;
    private final boolean created;
    private final boolean found;

    /**
     * Creates a response.
     *
     * <p><b>The sequence identity travels back, and it has to.</b> A write that was forwarded is still a
     * write the caller must be able to condition a later one on. Answering a forwarded write without a
     * {@code _seq_no} while a local write carried one would make the field's presence depend on which
     * node the client happened to reach, which is exactly the kind of difference a client cannot see and
     * cannot code against.
     *
     * @param ownerNodeId the node that actually performed the write
     * @param seqNo the sequence number the owner's engine assigned
     * @param primaryTerm the primary term it ran at
     * @param version the version assigned
     * @param created whether a write created the document rather than overwriting one
     * @param found whether a deletion found anything
     */
    public ForwardedIndexResponse(String ownerNodeId, long seqNo, long primaryTerm, long version, boolean created, boolean found) {
        this.ownerNodeId = ownerNodeId;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.version = version;
        this.created = created;
        this.found = found;
    }

    /**
     * Reads a response off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public ForwardedIndexResponse(StreamInput in) throws IOException {
        this.ownerNodeId = in.readString();
        this.seqNo = in.readZLong();
        this.primaryTerm = in.readVLong();
        this.version = in.readZLong();
        this.created = in.readBoolean();
        this.found = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(ownerNodeId);
        out.writeZLong(seqNo);
        out.writeVLong(primaryTerm);
        out.writeZLong(version);
        out.writeBoolean(created);
        out.writeBoolean(found);
    }

    /**
     * Returns the sequence number the owner's engine assigned.
     *
     * @return the sequence number
     */
    public long seqNo() {
        return seqNo;
    }

    /**
     * Returns the primary term the operation ran at.
     *
     * @return the primary term
     */
    public long primaryTerm() {
        return primaryTerm;
    }

    /**
     * Returns the version the owner's engine assigned.
     *
     * @return the version
     */
    public long version() {
        return version;
    }

    /**
     * Reports whether a write created the document rather than overwriting one.
     *
     * @return true when the document did not exist before
     */
    public boolean created() {
        return created;
    }

    /**
     * Reports whether a deletion found the document.
     *
     * @return true when there was something to remove
     */
    public boolean found() {
        return found;
    }

    /**
     * Returns the node that performed the write, so a caller can see where its document went.
     *
     * @return the owning node id
     */
    public String ownerNodeId() {
        return ownerNodeId;
    }
}
