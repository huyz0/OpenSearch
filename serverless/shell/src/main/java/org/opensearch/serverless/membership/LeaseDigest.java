/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.membership;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Every live lease as one node last read them, in one register, so the rest of the fleet reads one register a
 * period instead of every lease.
 *
 * <p>Each node used to read each peer's lease whenever its last copy ran out, which across a fleet of N nodes is N
 * squared reads a lease lifetime. One node, the live member with the lowest id, now reads every lease each third of
 * a lifetime and writes what it read here; every other node reads this register as often and takes from it any lease
 * whose stamp is later than the copy it holds.
 *
 * <p>Nothing is trusted here that was not already trusted from a node's own last read. A lease's expiry only moves
 * forward, so a stamp copied from its register is never later than the register's own, and a lease taken from here
 * is live no longer than a direct read of it would have said. A digest that stops being written makes no lease look
 * live: the stamps in it run out, and readers fall back to reading those leases one by one, as they did before.
 *
 * @param writtenAtMillis when the writer read the leases
 * @param leases the leases it read live
 */
public record LeaseDigest(long writtenAtMillis, List<NodeLease> leases) {

    /**
     * The register's name in the members container. It must not start with {@link BlobLeaseMembership#LEASE_PREFIX}
     * or a listing of leases would read it as a node.
     */
    public static final String NAME = "members-digest";

    private static final byte VERSION = 1;

    /**
     * Copies the leases.
     *
     * @param writtenAtMillis when read
     * @param leases what was read
     */
    public LeaseDigest {
        leases = List.copyOf(leases);
    }

    /**
     * Encodes a digest.
     *
     * @param writtenAtMillis when the leases were read
     * @param leases the live leases
     * @return the register's value
     * @throws IOException if a lease cannot be encoded
     */
    static BytesReference encode(long writtenAtMillis, Collection<NodeLease> leases) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(VERSION);
            out.writeLong(writtenAtMillis);
            out.writeInt(leases.size());
            for (NodeLease lease : leases) {
                final byte[] one = BytesReference.toBytes(lease.toBytes());
                out.writeInt(one.length);
                out.write(one);
            }
        }
        return new BytesArray(bytes.toByteArray());
    }

    /**
     * Decodes a digest.
     *
     * @param value the register's value
     * @return the digest
     * @throws IOException if it is not a digest this node reads
     */
    static LeaseDigest decode(BytesReference value) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(BytesReference.toBytes(value)))) {
            final byte version = in.readByte();
            if (version != VERSION) {
                throw new IOException("lease digest of version " + version + "; this node reads " + VERSION);
            }
            final long writtenAt = in.readLong();
            final int count = in.readInt();
            if (count < 0) {
                throw new IOException("lease digest names " + count + " leases");
            }
            final List<NodeLease> leases = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                final int length = in.readInt();
                if (length < 0) {
                    throw new IOException("lease digest entry of " + length + " bytes");
                }
                final byte[] one = in.readNBytes(length);
                if (one.length != length) {
                    throw new IOException("truncated lease digest");
                }
                leases.add(NodeLease.fromStream(new ByteArrayInputStream(one)));
            }
            return new LeaseDigest(writtenAt, leases);
        }
    }
}
