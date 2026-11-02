/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32C;

/**
 * Permanent assignment of an exact logical activation to one activated physical history and fixed
 * semantic limits. This record is not a storage-mode activation, current owner or read authority.
 * The trusted host must load it from the durable catalog and separately fence migration/lifecycle.
 * A group, history or semantic-limit change requires a new, explicitly migrated activation.
 * @param stream logical deployment, index incarnation, shard and activation identity
 * @param group assigned physical group, stable across voter replacement
 * @param groupActivation original group activation checkpoint, not a current leadership proof
 * @param maximumOutcomes fixed accepted-identity retention count
 * @param maximumPayloadBytes fixed complete logical command payload limit, including its header
 */
@InternalApi
public record LogicalStreamRecord(ShardLogId stream, UUID group, LogCheckpoint groupActivation, int maximumOutcomes,
    int maximumPayloadBytes) {
    /** Bound on the complete encoded register value. */
    public static final int MAX_BYTES = 1024;
    private static final int MAGIC = 0x4c535452;
    private static final int VERSION = 1;

    /**
     * Validates immutable assignment, activation lineage and usable semantic limits.
     * @param stream exact logical activation
     * @param group assigned physical group
     * @param groupActivation original physical activation marker
     * @param maximumOutcomes fixed retry window, between 1 and 65,536
     * @param maximumPayloadBytes complete logical payload limit, between 91 bytes and 64 MiB
     */
    public LogicalStreamRecord {
        Objects.requireNonNull(stream);
        Objects.requireNonNull(group);
        Objects.requireNonNull(groupActivation);
        if (groupActivation.logId().equals(JournalGroupRecord.controlIdentity(stream.deploymentId(), group)) == false
            || groupActivation.index() < 1
            || groupActivation.entryTerm() < 1
            || stream.indexUuid().startsWith("__journal_group/")
            || stream.indexUuid().startsWith("__journal_membership/")
            || maximumOutcomes < 1
            || maximumOutcomes > 65536
            || maximumPayloadBytes < 91
            || maximumPayloadBytes > OperationEnvelope.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("invalid logical stream assignment or protocol limits");
        }
    }

    /**
     * Encodes a bounded versioned assignment with integrity over all fields.
     * @return fresh encoded register bytes
     * @throws IOException on serialization failure
     */
    public byte[] encode() throws IOException {
        final var bytes = new ByteArrayOutputStream();
        try (var out = new OutputStreamStreamOutput(bytes)) {
            out.writeInt(MAGIC);
            out.writeByte((byte) VERSION);
            stream.writeTo(out);
            out.writeLong(group.getMostSignificantBits());
            out.writeLong(group.getLeastSignificantBits());
            groupActivation.writeTo(out);
            out.writeInt(maximumOutcomes);
            out.writeInt(maximumPayloadBytes);
            out.flush();
            final byte[] body = bytes.toByteArray();
            final var crc = new CRC32C();
            crc.update(body, 0, body.length);
            out.writeInt((int) crc.getValue());
        }
        if (bytes.size() > MAX_BYTES) {
            throw new IOException("logical assignment exceeds record limit");
        }
        return bytes.toByteArray();
    }

    /**
     * Bounds the register value before parsing identities, then validates integrity and lineage.
     * @param source finite durable register stream
     * @return immutable assignment; not independently an authorization proof
     * @throws IOException on malformed, corrupt, oversized or unsupported records
     */
    public static LogicalStreamRecord read(InputStream source) throws IOException {
        final byte[] bytes = source.readNBytes(MAX_BYTES + 1);
        if (bytes.length < 9 || bytes.length > MAX_BYTES) {
            throw new IOException("invalid logical assignment size");
        }
        final int bodyLength = bytes.length - Integer.BYTES;
        final var crc = new CRC32C();
        crc.update(bytes, 0, bodyLength);
        if (ByteBuffer.wrap(bytes, bodyLength, Integer.BYTES).getInt() != (int) crc.getValue()) {
            throw new IOException("logical assignment checksum mismatch");
        }
        try (var in = new InputStreamStreamInput(new ByteArrayInputStream(bytes, 0, bodyLength))) {
            if (in.readInt() != MAGIC || in.readByte() != VERSION) {
                throw new IOException("unsupported logical assignment format");
            }
            final var stream = ShardLogId.readFrom(in);
            final var group = new UUID(in.readLong(), in.readLong());
            final var checkpoint = LogCheckpoint.readFrom(in);
            final int outcomes = in.readInt();
            final int payload = in.readInt();
            if (in.read() != -1) {
                throw new IOException("trailing logical assignment bytes");
            }
            return new LogicalStreamRecord(stream, group, checkpoint, outcomes, payload);
        } catch (IllegalArgumentException malformed) {
            throw new IOException("invalid logical assignment fields", malformed);
        }
    }
}
