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
import java.util.zip.CRC32C;

/**
 * Historical controller receipt for a native committed configuration transition. Coordinates refer
 * to a learner barrier and a resulting configuration entry in the intent's physical group.
 * They are not current-read authority or
 * evidence that a learner is presently healthy. Publication must obtain its proof directly from
 * the authenticated journal, rather than accepting a caller-constructed receipt.
 * @param intent original durable pending replacement
 * @param learnerBarrierIndex first committed nonce-bound learner barrier index
 * @param learnerBarrierTerm term of the learner barrier
 * @param targetConfigurationIndex committed stable target configuration index
 * @param targetConfigurationTerm term of the target configuration entry
 */
@InternalApi
public record JournalReplacementReceipt(JournalMembershipRecord intent, long learnerBarrierIndex, long learnerBarrierTerm,
    long targetConfigurationIndex, long targetConfigurationTerm) {
    /** Maximum encoded receipt size, independent of the shard count and older transitions. */
    public static final int MAX_BYTES = JournalMembershipRecord.MAX_BYTES + 64;
    private static final int MAGIC = 0x4a524350;
    private static final int VERSION = 1;

    /**
     * Validates the immutable intent and chronological native configuration points.
     * @param intent original pending replacement
     * @param learnerBarrierIndex first committed learner barrier position
     * @param learnerBarrierTerm learner barrier term
     * @param targetConfigurationIndex committed stable target position
     * @param targetConfigurationTerm target entry term
     */
    public JournalReplacementReceipt {
        Objects.requireNonNull(intent);
        if (intent.pending() == null
            || learnerBarrierIndex <= intent.genesis().activation().index()
            || learnerBarrierTerm < intent.genesis().activation().entryTerm()
            || targetConfigurationIndex <= learnerBarrierIndex
            || targetConfigurationTerm < learnerBarrierTerm) {
            throw new IllegalArgumentException("replacement receipt differs from activated lineage or configuration order");
        }
    }

    /**
     * Returns the completed controller head after this exact transition, without changing genesis.
     * This receipt is historical: a later transition may already have advanced the current head.
     * @return resulting completed membership
     */
    public JournalMembershipRecord completedMembership() {
        return new JournalMembershipRecord(intent.genesis(), intent.revision() + 1, intent.pending().target(), null);
    }

    /**
     * Encodes the exact source, target, nonce and configuration points with a complete checksum.
     * @return independent bounded bytes
     * @throws IOException on encoding or size failure
     */
    public byte[] encode() throws IOException {
        final var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            final byte[] pending = intent.encode();
            out.writeInt(pending.length);
            out.write(pending);
            out.writeLong(learnerBarrierIndex);
            out.writeLong(learnerBarrierTerm);
            out.writeLong(targetConfigurationIndex);
            out.writeLong(targetConfigurationTerm);
            out.flush();
            final byte[] body = bytes.toByteArray();
            final var crc = new CRC32C();
            crc.update(body, 0, body.length);
            out.writeInt((int) crc.getValue());
        }
        if (bytes.size() > MAX_BYTES) {
            throw new IOException("journal replacement receipt exceeds encoded size bound");
        }
        return bytes.toByteArray();
    }

    /**
     * Reads one bounded receipt, rejecting corruption, unsupported formats and trailing data.
     * @param source finite catalog register stream
     * @return validated receipt
     * @throws IOException on malformed, excessive or inconsistent input
     */
    public static JournalReplacementReceipt read(InputStream source) throws IOException {
        final byte[] bytes = source.readNBytes(MAX_BYTES + 1);
        if (bytes.length < 12 || bytes.length > MAX_BYTES) {
            throw new IOException("invalid journal replacement receipt size");
        }
        final int size = bytes.length - Integer.BYTES;
        final var crc = new CRC32C();
        crc.update(bytes, 0, size);
        try (var trailer = new DataInputStream(new ByteArrayInputStream(bytes, size, Integer.BYTES))) {
            if (trailer.readInt() != (int) crc.getValue()) {
                throw new IOException("journal replacement receipt checksum mismatch");
            }
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, size))) {
            if (in.readInt() != MAGIC || in.readInt() != VERSION) {
                throw new IOException("unsupported journal replacement receipt format");
            }
            final int pendingSize = in.readInt();
            if (pendingSize < 1 || pendingSize > JournalMembershipRecord.MAX_BYTES || pendingSize > in.available()) {
                throw new IOException("invalid embedded journal replacement intent size");
            }
            final var pending = JournalMembershipRecord.read(new ByteArrayInputStream(in.readNBytes(pendingSize)));
            final var receipt = new JournalReplacementReceipt(pending, in.readLong(), in.readLong(), in.readLong(), in.readLong());
            if (in.read() != -1) {
                throw new IOException("trailing journal replacement receipt data");
            }
            return receipt;
        } catch (IllegalArgumentException failure) {
            throw new IOException("invalid journal replacement receipt fields", failure);
        }
    }
}
