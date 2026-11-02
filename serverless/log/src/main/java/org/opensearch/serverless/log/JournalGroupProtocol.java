/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.common.annotation.InternalApi;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32C;

/**
 * Immutable wire and retained-history contract shared by every voter of one physical group.
 * Node-wide memory/admission, cache, free-space and local segment settings remain local resources.
 * The current journal has one wire profile per host, so colocated groups must use that same profile.
 * This record is catalog provenance, not independent authentication or current read authority.
 * @param deploymentId durable namespace
 * @param group physical group identity
 * @param maximumPayloadBytes complete envelope payload bound
 * @param maximumPendingRequests native queue/appender count used in the retained-history safety margin
 * @param queueBytes native queue, appender batch and peer message-size basis
 * @param maximumRetainedEntries unpurged history admission bound
 */
@InternalApi
public record JournalGroupProtocol(UUID deploymentId, UUID group, int maximumPayloadBytes, int maximumPendingRequests, int queueBytes,
    long maximumRetainedEntries) {
    /** Exact encoding bound, including checksum. */
    public static final int MAX_BYTES = 61;
    private static final int MAGIC = 0x4a50524f;
    private static final byte VERSION = 1;

    /**
     * Validates the fixed wire and retained-history limits.
     * @param deploymentId namespace
     * @param group group identity
     * @param maximumPayloadBytes payload limit
     * @param maximumPendingRequests native count and history-margin basis
     * @param queueBytes native queue/batch size
     * @param maximumRetainedEntries retained log positions
     */
    public JournalGroupProtocol {
        Objects.requireNonNull(deploymentId);
        Objects.requireNonNull(group);
        if (maximumPayloadBytes < 1
            || maximumPayloadBytes > OperationEnvelope.MAX_PAYLOAD_BYTES
            || maximumPendingRequests < 1
            || maximumPendingRequests > 65536
            || queueBytes < 1024 * 1024
            || queueBytes > 128 * 1024 * 1024
            || queueBytes < maximumPayloadBytes + 1024
            || maximumRetainedEntries < 1024
            || maximumRetainedEntries > 10_000_000) {
            throw new IllegalArgumentException("invalid physical group protocol limits");
        }
    }

    /**
     * Captures the shared protocol fields of one configured voter.
     * @param deploymentId namespace
     * @param group group identity
     * @param limits voter resource configuration
     */
    public JournalGroupProtocol(UUID deploymentId, UUID group, JournalConfiguration.Limits limits) {
        this(
            deploymentId,
            group,
            Objects.requireNonNull(limits).maximumPayloadBytes(),
            limits.maximumPendingRequests(),
            limits.queueBytes(),
            limits.maximumRetainedEntries()
        );
    }

    /** Returns fresh fixed-size versioned bytes with integrity over every field. */
    public byte[] encode() {
        final var out = ByteBuffer.allocate(MAX_BYTES);
        out.putInt(MAGIC).put(VERSION);
        out.putLong(deploymentId.getMostSignificantBits()).putLong(deploymentId.getLeastSignificantBits());
        out.putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits());
        out.putInt(maximumPayloadBytes).putInt(maximumPendingRequests).putInt(queueBytes).putLong(maximumRetainedEntries);
        final var crc = new CRC32C();
        crc.update(out.array(), 0, MAX_BYTES - Integer.BYTES);
        out.putInt((int) crc.getValue());
        return out.array();
    }

    /**
     * Validates a bounded immutable protocol register.
     * @param source finite register value
     * @return decoded contract, requiring trusted catalog provenance
     * @throws IOException on malformed, corrupt, unsupported or invalid data
     */
    public static JournalGroupProtocol read(InputStream source) throws IOException {
        final byte[] bytes = source.readNBytes(MAX_BYTES + 1);
        if (bytes.length != MAX_BYTES) {
            throw new IOException("invalid physical group protocol size");
        }
        final var crc = new CRC32C();
        crc.update(bytes, 0, MAX_BYTES - Integer.BYTES);
        final var in = ByteBuffer.wrap(bytes);
        if (in.getInt(MAX_BYTES - Integer.BYTES) != (int) crc.getValue()) {
            throw new IOException("physical group protocol checksum mismatch");
        }
        if (in.getInt() != MAGIC || in.get() != VERSION) {
            throw new IOException("unsupported physical group protocol format");
        }
        final var deployment = new UUID(in.getLong(), in.getLong());
        final var group = new UUID(in.getLong(), in.getLong());
        try {
            return new JournalGroupProtocol(deployment, group, in.getInt(), in.getInt(), in.getInt(), in.getLong());
        } catch (IllegalArgumentException invalid) {
            throw new IOException("invalid physical group protocol fields", invalid);
        }
    }
}
