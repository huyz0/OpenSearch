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
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32C;

/**
 * Durable controller intent, separate from immutable genesis and native consensus configuration.
 * A pending replacement must be reconciled against consensus; it is never permission to bootstrap
 * another history, promote a learner, or regard a configuration change as committed.
 * @param genesis original activated reservation
 * @param revision completed logical controller revision; unrelated to the native configuration log index
 * @param voters last completed three-voter placement
 * @param pending serialized replacement intent, or null
 */
@InternalApi
public record JournalMembershipRecord(JournalGroupRecord genesis, long revision, JournalConfiguration.Group voters, Replacement pending) {
    /** Maximum frame size, independent of the shard population or transition history. */
    public static final int MAX_BYTES = 3 * JournalGroupRecord.MAX_BYTES + 64;
    private static final int MAGIC = 0x4a4d454d;
    private static final int VERSION = 1;

    /**
     * Canonicalizes placements and validates a single-voter transition without address rebinding.
     * @param genesis activated original reservation
     * @param revision completed controller revision
     * @param voters completed placement
     * @param pending optional next replacement
     */
    public JournalMembershipRecord {
        Objects.requireNonNull(genesis);
        Objects.requireNonNull(voters);
        if (genesis.activation() == null || revision < 0 || voters.id().equals(genesis.initialPlacement().id()) == false) {
            throw new IllegalArgumentException("membership requires an activated genesis and a nonnegative revision");
        }
        voters = canonical(genesis, voters);
        if (revision == 0 && voters.equals(genesis.initialPlacement()) == false) {
            throw new IllegalArgumentException("revision zero must retain the genesis placement");
        }
        if (pending != null) {
            if (revision == Long.MAX_VALUE || pending.target().id().equals(voters.id()) == false) {
                throw new IllegalArgumentException("replacement differs from group identity or exhausts revisions");
            }
            pending = new Replacement(pending.id(), canonical(genesis, pending.target()));
            int retained = 0;
            for (var peer : pending.target().peers()) {
                final var existing = voters.peers().stream().filter(candidate -> candidate.id().equals(peer.id())).findFirst();
                if (existing.isPresent()) {
                    if (existing.get().equals(peer) == false) {
                        throw new IllegalArgumentException("replacement cannot rebind an existing voter identity");
                    }
                    retained++;
                } else {
                    if (genesis.initialPlacement().peers().stream().anyMatch(old -> old.id().equals(peer.id()))) {
                        throw new IllegalArgumentException("a retired genesis voter identity cannot rejoin");
                    }
                    if (voters.peers().stream().anyMatch(old -> old.host().equals(peer.host()) && old.port() == peer.port())) {
                        throw new IllegalArgumentException("replacement requires a distinct transport endpoint while the old voter exists");
                    }
                }
            }
            if (retained != 2) {
                throw new IllegalArgumentException("replace exactly one voter and retain two unchanged voters");
            }
        }
    }

    /**
     * Exact durable target, including full addresses and AZ labels, bound to an idempotency nonce.
     * @param id unique controller operation identity
     * @param target resulting three-voter placement
     */
    @InternalApi
    public record Replacement(UUID id, JournalConfiguration.Group target) {
        /**
         * Validates required values; the enclosing record validates the source-to-target transition.
         * @param id operation identity
         * @param target resulting placement
         */
        public Replacement {
            Objects.requireNonNull(id);
            Objects.requireNonNull(target);
        }
    }

    /**
     * Builds the initial completed membership from an activated reservation.
     * @param genesis immutable activated reservation
     * @return revision zero with no pending replacement
     */
    public static JournalMembershipRecord initial(JournalGroupRecord genesis) {
        return new JournalMembershipRecord(genesis, 0, genesis.initialPlacement(), null);
    }

    /**
     * Serializes one replacement. Retrying the exact pending nonce and target is idempotent;
     * another nonce, placement, or expected revision cannot supersede an unresolved intent.
     * @param expectedRevision required completed controller revision
     * @param operation unique replacement identity
     * @param target resulting placement
     * @return record with the pending intent
     */
    public JournalMembershipRecord begin(long expectedRevision, UUID operation, JournalConfiguration.Group target) {
        if (expectedRevision != revision) {
            throw new IllegalArgumentException("stale journal membership revision");
        }
        final var proposed = new JournalMembershipRecord(genesis, revision, voters, new Replacement(operation, target));
        if (pending != null && pending.equals(proposed.pending()) == false) {
            throw new IllegalArgumentException("another journal replacement remains unresolved");
        }
        return proposed;
    }

    /**
     * Encodes the bounded frame and its checksum, including every placement and intent field.
     * @return independent bytes
     * @throws IOException on encoding failure
     */
    public byte[] encode() throws IOException {
        final var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            writeRecord(out, genesis);
            out.writeLong(revision);
            writeRecord(out, new JournalGroupRecord(genesis.deploymentId(), voters, null));
            out.writeBoolean(pending != null);
            if (pending != null) {
                out.writeLong(pending.id().getMostSignificantBits());
                out.writeLong(pending.id().getLeastSignificantBits());
                writeRecord(out, new JournalGroupRecord(genesis.deploymentId(), pending.target(), null));
            }
            out.flush();
            final var body = bytes.toByteArray();
            final var crc = new CRC32C();
            crc.update(body, 0, body.length);
            out.writeInt((int) crc.getValue());
        }
        if (bytes.size() > MAX_BYTES) {
            throw new IOException("journal membership exceeds encoded size bound");
        }
        return bytes.toByteArray();
    }

    /**
     * Reads one finite frame, rejecting corruption, unsupported formats and trailing data.
     * @param source register value
     * @return validated record
     * @throws IOException on malformed or excessive input
     */
    public static JournalMembershipRecord read(InputStream source) throws IOException {
        final byte[] bytes = source.readNBytes(MAX_BYTES + 1);
        if (bytes.length < 12 || bytes.length > MAX_BYTES) {
            throw new IOException("invalid journal membership size");
        }
        final int size = bytes.length - Integer.BYTES;
        final var crc = new CRC32C();
        crc.update(bytes, 0, size);
        try (var trailer = new DataInputStream(new ByteArrayInputStream(bytes, size, Integer.BYTES))) {
            if (trailer.readInt() != (int) crc.getValue()) {
                throw new IOException("journal membership checksum mismatch");
            }
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, size))) {
            if (in.readInt() != MAGIC || in.readInt() != VERSION) {
                throw new IOException("unsupported journal membership format");
            }
            final var genesis = readRecord(in);
            final long revision = in.readLong();
            final var voters = readPlacement(in, genesis);
            final int state = in.readUnsignedByte();
            final Replacement pending;
            if (state == 0) {
                pending = null;
            } else if (state == 1) {
                final var id = new UUID(in.readLong(), in.readLong());
                pending = new Replacement(id, readPlacement(in, genesis));
            } else {
                throw new IOException("unknown journal membership state");
            }
            if (in.read() != -1) {
                throw new IOException("trailing journal membership data");
            }
            return new JournalMembershipRecord(genesis, revision, voters, pending);
        } catch (IllegalArgumentException failure) {
            throw new IOException("invalid journal membership fields", failure);
        }
    }

    private static JournalConfiguration.Group canonical(JournalGroupRecord genesis, JournalConfiguration.Group placement) {
        return new JournalGroupRecord(genesis.deploymentId(), placement, null).initialPlacement();
    }

    private static void writeRecord(DataOutputStream out, JournalGroupRecord record) throws IOException {
        final byte[] bytes = record.encode();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static JournalGroupRecord readRecord(DataInputStream in) throws IOException {
        final int size = in.readInt();
        if (size < 1 || size > JournalGroupRecord.MAX_BYTES || size > in.available()) {
            throw new IOException("invalid embedded journal placement size");
        }
        return JournalGroupRecord.read(new ByteArrayInputStream(in.readNBytes(size)));
    }

    private static JournalConfiguration.Group readPlacement(DataInputStream in, JournalGroupRecord genesis) throws IOException {
        final var record = readRecord(in);
        if (record.deploymentId().equals(genesis.deploymentId()) == false || record.activation() != null) {
            throw new IOException("membership placement differs from genesis namespace or contains an activation");
        }
        return record.initialPlacement();
    }
}
