/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.common.annotation.InternalApi;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32C;

/**
 * Durable, immutable initial placement reservation. An activation coordinate records historical
 * commitment, not current leadership or read authority. Records must never be deleted or rebound
 * to independent voters. Membership changes require a separate authorized transition protocol.
 * @param deploymentId durable deployment namespace
 * @param initialPlacement immutable three-voter genesis placement, canonicalized by voter UUID
 * @param activation first committed activation marker, or null while reserved
 */
@InternalApi
public record JournalGroupRecord(UUID deploymentId, JournalConfiguration.Group initialPlacement, LogCheckpoint activation) {
    /** Maximum encoded record size, including checksum. */
    public static final int MAX_BYTES = 8192;
    private static final int MAGIC = 0x4a475250;
    private static final int VERSION = 1;

    /**
     * Copies placement and validates activation lineage.
     * @param deploymentId deployment namespace
     * @param initialPlacement initial voter identities and placement
     * @param activation committed activation marker, or null
     */
    public JournalGroupRecord {
        Objects.requireNonNull(deploymentId);
        Objects.requireNonNull(initialPlacement);
        initialPlacement = new JournalConfiguration.Group(
            initialPlacement.id(),
            initialPlacement.peers().stream().sorted(Comparator.comparing(JournalConfiguration.Peer::id)).toList()
        );
        if (activation != null
            && (activation.logId().equals(controlIdentity(deploymentId, initialPlacement.id())) == false
                || activation.index() < 1
                || activation.entryTerm() < 1)) {
            throw new IllegalArgumentException("activation coordinate differs from reserved physical group lineage");
        }
    }

    /**
     * Returns the reserved control stream identity for one physical group.
     * @param deployment namespace
     * @param group physical group identity
     * @return identity excluded from customer proposals and replay subscriptions
     */
    public static ShardLogId controlIdentity(UUID deployment, UUID group) {
        return new ShardLogId(deployment, "__journal_group/" + group, 0, group);
    }

    /**
     * Activates this exact reservation, with idempotent retries of the original marker.
     * The trusted controller must obtain the coordinate from an authenticated journal, not a client.
     * @param checkpoint first committed activation marker
     * @return activated immutable record
     */
    public JournalGroupRecord activate(LogCheckpoint checkpoint) {
        Objects.requireNonNull(checkpoint);
        if (activation != null && activation.equals(checkpoint) == false) {
            throw new IllegalArgumentException("reserved group already has a different activation marker");
        }
        return new JournalGroupRecord(deploymentId, initialPlacement, checkpoint);
    }

    /**
     * Encodes a bounded versioned record with CRC32C over all fields.
     * @return independent encoded bytes
     * @throws IOException on encoding or record-size failure
     */
    public byte[] encode() throws IOException {
        final var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            writeUuid(out, deploymentId);
            writeUuid(out, initialPlacement.id());
            for (var peer : initialPlacement.peers()) {
                writeUuid(out, peer.id());
                out.writeUTF(peer.host());
                out.writeInt(peer.port());
                out.writeUTF(peer.availabilityZone());
            }
            out.writeByte(activation == null ? 0 : 1);
            if (activation != null) {
                out.writeLong(activation.index());
                out.writeLong(activation.entryTerm());
            }
            out.flush();
            final byte[] body = bytes.toByteArray();
            final var crc = new CRC32C();
            crc.update(body, 0, body.length);
            out.writeInt((int) crc.getValue());
        }
        if (bytes.size() > MAX_BYTES) {
            throw new IOException("journal reservation exceeds encoded size limit");
        }
        return bytes.toByteArray();
    }

    /**
     * Reads at most the record limit plus one byte and validates framing before allocating fields.
     * Modified-UTF fields have the JDK's fixed 64 KiB allocation ceiling; placement validation
     * further bounds accepted strings. Malformed or trailing bytes never become a valid reservation.
     * @param source finite register value stream
     * @return validated immutable reservation
     * @throws IOException on corruption, unknown format, invalid placement or excess bytes
     */
    public static JournalGroupRecord read(InputStream source) throws IOException {
        final byte[] bytes = source.readNBytes(MAX_BYTES + 1);
        if (bytes.length < 12 || bytes.length > MAX_BYTES) {
            throw new IOException("invalid journal reservation size");
        }
        final int bodyLength = bytes.length - Integer.BYTES;
        final var crc = new CRC32C();
        crc.update(bytes, 0, bodyLength);
        try (var trailer = new DataInputStream(new ByteArrayInputStream(bytes, bodyLength, Integer.BYTES))) {
            if (trailer.readInt() != (int) crc.getValue()) {
                throw new IOException("journal reservation checksum mismatch");
            }
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bodyLength))) {
            if (in.readInt() != MAGIC || in.readInt() != VERSION) {
                throw new IOException("unsupported journal reservation format");
            }
            final UUID deployment = readUuid(in);
            final UUID group = readUuid(in);
            final var peers = new ArrayList<JournalConfiguration.Peer>(3);
            for (int i = 0; i < 3; i++) {
                peers.add(new JournalConfiguration.Peer(readUuid(in), in.readUTF(), in.readInt(), in.readUTF()));
            }
            final int state = in.readUnsignedByte();
            final LogCheckpoint checkpoint;
            if (state == 0) {
                checkpoint = null;
            } else if (state == 1) {
                checkpoint = new LogCheckpoint(controlIdentity(deployment, group), in.readLong(), in.readLong());
            } else {
                throw new IOException("unknown journal reservation state");
            }
            if (in.read() != -1) {
                throw new IOException("trailing journal reservation bytes");
            }
            return new JournalGroupRecord(deployment, new JournalConfiguration.Group(group, peers), checkpoint);
        } catch (IllegalArgumentException failure) {
            throw new IOException("invalid journal reservation fields", failure);
        }
    }

    private static void writeUuid(DataOutputStream out, UUID value) throws IOException {
        out.writeLong(value.getMostSignificantBits());
        out.writeLong(value.getLeastSignificantBits());
    }

    private static UUID readUuid(DataInputStream in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }
}
