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

/**
 * Versioned logical-control payload, independent of engine/translog formats. The enclosing journal
 * envelope supplies stream, producer epoch, engine term, schema version and integrity. This codec
 * alone neither authorizes a producer nor implements committed ownership or duplicate detection.
 */
@InternalApi
public final class LogicalCommand {
    private static final int MAGIC = 0x4c435431;
    private static final int FORMAT = 1;
    private static final int HEADER_BYTES = 90;
    private static final int DIGEST_BYTES = 32;

    /** Explicit persistent action codes. */
    public enum Action {
        /** Claim the next producer epoch; argument is the expected previous epoch. */
        OWNERSHIP(1),
        /** Install an immutable schema definition; argument is its expected previous version. */
        SCHEMA(2),
        /** Apply an opaque resolved engine operation; argument is its assigned sequence number. */
        MUTATION(3),
        /** Persist an original result without changing document sequence state; argument is -1. */
        OUTCOME_ONLY(4),
        /** Permanently retire this stream activation; argument is -1. */
        RETIRE(5);

        private final int code;

        Action(int code) {
            this.code = code;
        }

        private static Action decode(int code) throws IOException {
            for (var action : values()) {
                if (action.code == code) {
                    return action;
                }
            }
            throw new IOException("unknown logical command action " + code);
        }
    }

    private final Action action;
    private final UUID requestId;
    private final long firstObservedIndex;
    private final byte[] requestDigest;
    private final UUID actor;
    private final long argument;
    private final byte[] payload;

    /**
     * Copies an immutable request identity and payload. Retried requests must retain the original
     * observed index and input digest, even when a new owner resolves an ambiguous outcome.
     * @param action logical operation
     * @param requestId internal identity, stable across possibly executed attempts
     * @param firstObservedIndex original physical-log observation, or -1 before any observation
     * @param requestDigest 32-byte digest of the original request, not its later resolved result
     * @param actor producer identity, checked by the committed controller
     * @param argument expected epoch/schema version, mutation sequence number, or -1 as documented
     * @param payload schema definition/resolved operation/original result; empty for claim/retirement
     */
    public LogicalCommand(
        Action action,
        UUID requestId,
        long firstObservedIndex,
        byte[] requestDigest,
        UUID actor,
        long argument,
        byte[] payload
    ) {
        this(action, requestId, firstObservedIndex, requestDigest, actor, argument, payload, false);
    }

    private LogicalCommand(
        Action action,
        UUID requestId,
        long firstObservedIndex,
        byte[] requestDigest,
        UUID actor,
        long argument,
        byte[] payload,
        boolean owned
    ) {
        this.action = Objects.requireNonNull(action);
        this.requestId = Objects.requireNonNull(requestId);
        this.actor = Objects.requireNonNull(actor);
        Objects.requireNonNull(requestDigest);
        Objects.requireNonNull(payload);
        if (firstObservedIndex < -1 || requestDigest.length != DIGEST_BYTES || payload.length > OperationEnvelope.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("invalid logical request identity or payload bound");
        }
        final boolean hasPayload = action == Action.SCHEMA || action == Action.MUTATION || action == Action.OUTCOME_ONLY;
        final boolean hasArgument = action == Action.OWNERSHIP || action == Action.SCHEMA || action == Action.MUTATION;
        if ((payload.length != 0) != hasPayload || (hasArgument ? argument < 0 : argument != -1)) {
            throw new IllegalArgumentException("logical command fields do not match its action");
        }
        this.firstObservedIndex = firstObservedIndex;
        this.argument = argument;
        this.requestDigest = owned ? requestDigest : requestDigest.clone();
        this.payload = owned ? payload : payload.clone();
    }

    /** Returns the logical operation. */
    public Action action() {
        return action;
    }

    /** Returns the internal request identity. */
    public UUID requestId() {
        return requestId;
    }

    /** Returns the original, immutable physical-log observation used by retry retention. */
    public long firstObservedIndex() {
        return firstObservedIndex;
    }

    /** Returns an independent copy of the original request digest. */
    public byte[] requestDigest() {
        return requestDigest.clone();
    }

    /** Returns the claimed producer identity; this value alone is not authorization. */
    public UUID actor() {
        return actor;
    }

    /** Returns the action-specific epoch/schema/sequence argument, or -1. */
    public long argument() {
        return argument;
    }

    /** Returns opaque payload length without copying it. */
    public int payloadLength() {
        return payload.length;
    }

    /** Returns a fresh reader over immutable payload bytes. */
    public InputStream payloadReader() {
        return new ByteArrayInputStream(payload);
    }

    /**
     * Encodes after checking the entire output size, before allocation.
     * @param maximumBytes configured enclosing payload limit
     * @return independent versioned bytes
     * @throws IOException if the complete command exceeds admission
     */
    public byte[] encode(int maximumBytes) throws IOException {
        checkLimit(maximumBytes);
        final long size = HEADER_BYTES + (long) payload.length;
        if (size > maximumBytes) {
            throw new IOException("logical command exceeds payload admission");
        }
        final var bytes = new ByteArrayOutputStream((int) size);
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeByte(FORMAT);
            out.writeByte(action.code);
            writeUuid(out, requestId);
            out.writeLong(firstObservedIndex);
            out.write(requestDigest);
            writeUuid(out, actor);
            out.writeLong(argument);
            out.writeInt(payload.length);
            out.write(payload);
        }
        return bytes.toByteArray();
    }

    /**
     * Decodes without trusting declared lengths; the outer envelope must already be verified.
     * @param encoded complete payload bytes
     * @param maximumBytes configured enclosing payload limit
     * @return owned immutable command
     * @throws IOException on unknown formats/actions, truncation, invalid fields or trailing data
     */
    public static LogicalCommand decode(byte[] encoded, int maximumBytes) throws IOException {
        checkLimit(maximumBytes);
        Objects.requireNonNull(encoded);
        if (encoded.length < HEADER_BYTES || encoded.length > maximumBytes) {
            throw new IOException("logical command length is outside admission");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(encoded))) {
            if (in.readInt() != MAGIC || in.readUnsignedByte() != FORMAT) {
                throw new IOException("unsupported logical command format");
            }
            final var action = Action.decode(in.readUnsignedByte());
            final var request = readUuid(in);
            final long observed = in.readLong();
            final byte[] digest = in.readNBytes(DIGEST_BYTES);
            final var actor = readUuid(in);
            final long argument = in.readLong();
            final int length = in.readInt();
            if (length < 0 || length != in.available()) {
                throw new IOException("logical command payload length or trailing bytes are invalid");
            }
            final byte[] payload = in.readNBytes(length);
            try {
                return new LogicalCommand(action, request, observed, digest, actor, argument, payload, true);
            } catch (IllegalArgumentException invalid) {
                throw new IOException("invalid logical command fields", invalid);
            }
        }
    }

    private static void checkLimit(int limit) {
        if (limit < 1 || limit > OperationEnvelope.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("invalid logical command admission bound");
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
