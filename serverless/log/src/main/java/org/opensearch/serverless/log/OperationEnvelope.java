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
import java.util.Objects;
import java.util.zip.CRC32C;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * Bounded, checksummed framing for a logical shard stream inside a shared physical log. CRC32C
 * detects accidental corruption; it is not authentication. Payload semantics are validated by the
 * owning state machine before admission. No mutable payload buffer escapes this object.
 */
@InternalApi
public final class OperationEnvelope {

    /** Absolute frame bound; each server should configure a smaller admission limit where possible. */
    public static final int MAX_PAYLOAD_BYTES = 64 * 1024 * 1024;

    private static final int MAGIC = 0x4c4c4f50;
    private static final int VERSION = 1;
    private static final int MAX_HEADER_BYTES = 512;

    /** Explicit wire codes; enum ordinals are never persisted. */
    public enum Kind {
        /** Resolved document mutations; engine-specific encoding belongs to the shell. */
        DOCUMENTS(1),
        /** An explicit operation slot that changes no document. */
        NO_OP(2),
        /** A committed logical-shard ownership transition. */
        OWNERSHIP(3),
        /** An exact schema dependency or schema transition. */
        SCHEMA(4),
        /** A recovery or archive cut in the committed history. */
        RECOVERY_BARRIER(5);

        private final int code;

        Kind(int code) {
            this.code = code;
        }

        private static Kind fromCode(int code) throws IOException {
            for (Kind kind : values()) {
                if (kind.code == code) {
                    return kind;
                }
            }
            throw new IOException("unsupported operation kind " + code);
        }
    }

    private final ShardLogId logId;
    private final Kind kind;
    private final long producerEpoch;
    private final long primaryTerm;
    private final long schemaVersion;
    private final byte[] payload;

    /**
     * Creates an immutable envelope, copying the caller's payload once.
     *
     * @param logId logical shard activation identity
     * @param kind operation kind
     * @param producerEpoch committed writer-ownership epoch
     * @param primaryTerm engine operation term
     * @param schemaVersion exact schema dependency
     * @param payload encoded resolved operation or control record
     */
    public OperationEnvelope(ShardLogId logId, Kind kind, long producerEpoch, long primaryTerm, long schemaVersion, byte[] payload) {
        this(logId, kind, producerEpoch, primaryTerm, schemaVersion, payload, false);
    }

    private OperationEnvelope(
        ShardLogId logId,
        Kind kind,
        long producerEpoch,
        long primaryTerm,
        long schemaVersion,
        byte[] payload,
        boolean owned
    ) {
        this.logId = Objects.requireNonNull(logId);
        this.kind = Objects.requireNonNull(kind);
        Objects.requireNonNull(payload);
        if (producerEpoch < 0 || primaryTerm < 0 || schemaVersion < 0) {
            throw new IllegalArgumentException("operation epochs, terms, and schema versions must be non-negative");
        }
        if (payload.length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("operation payload exceeds maximum frame size");
        }
        this.producerEpoch = producerEpoch;
        this.primaryTerm = primaryTerm;
        this.schemaVersion = schemaVersion;
        this.payload = owned ? payload : payload.clone();
    }

    /**
     * Returns the logical shard identity.
     * @return logical shard identity
     */
    public ShardLogId logId() {
        return logId;
    }

    /**
     * Returns the operation kind.
     * @return operation kind
     */
    public Kind kind() {
        return kind;
    }

    /**
     * Returns the writer-ownership epoch.
     * @return writer-ownership epoch
     */
    public long producerEpoch() {
        return producerEpoch;
    }

    /**
     * Returns the original engine primary term.
     * @return original engine primary term
     */
    public long primaryTerm() {
        return primaryTerm;
    }

    /**
     * Returns the required schema version.
     * @return required schema version
     */
    public long schemaVersion() {
        return schemaVersion;
    }

    /**
     * Returns the payload byte count without copying it.
     * @return payload byte count
     */
    public int payloadLength() {
        return payload.length;
    }

    /**
     * Opens an independent read cursor without exposing the mutable backing array.
     * @return independent payload read cursor
     */
    public InputStream payloadReader() {
        return new ByteArrayInputStream(payload);
    }

    /**
     * Encodes a complete frame with checksum covering identity, metadata, and payload.
     *
     * @param maximumPayloadBytes configured admission bound
     * @return encoded frame
     * @throws IOException if serialization fails or the configured bound is exceeded
     */
    public byte[] encode(int maximumPayloadBytes) throws IOException {
        validateLimit(maximumPayloadBytes);
        if (payload.length > maximumPayloadBytes) {
            throw new IOException("operation payload exceeds configured admission limit");
        }
        final var buffer = new ByteArrayOutputStream(payload.length + MAX_HEADER_BYTES);
        final var checksum = new CRC32C();
        try (var out = new OutputStreamStreamOutput(new CheckedOutputStream(buffer, checksum))) {
            out.writeInt(MAGIC);
            out.writeByte((byte) VERSION);
            logId.writeTo(out);
            out.writeByte((byte) kind.code);
            out.writeVLong(producerEpoch);
            out.writeVLong(primaryTerm);
            out.writeVLong(schemaVersion);
            out.writeVInt(payload.length);
            out.writeBytes(payload, 0, payload.length);
            out.writeInt((int) checksum.getValue());
            out.flush();
        }
        return buffer.toByteArray();
    }

    /**
     * Decodes one exact frame. Length limits are checked before payload allocation. Unsupported
     * formats, malformed headers, checksum mismatches, truncation, and trailing bytes fail closed.
     *
     * @param frame encoded frame
     * @param maximumPayloadBytes configured admission bound
     * @return immutable envelope
     * @throws IOException on malformed or oversized data
     */
    public static OperationEnvelope decode(byte[] frame, int maximumPayloadBytes) throws IOException {
        validateLimit(maximumPayloadBytes);
        if (frame.length > maximumPayloadBytes + MAX_HEADER_BYTES) {
            throw new IOException("operation frame exceeds configured admission limit");
        }
        final var checksum = new CRC32C();
        try (var in = new InputStreamStreamInput(new CheckedInputStream(new ByteArrayInputStream(frame), checksum))) {
            if (in.readInt() != MAGIC) {
                throw new IOException("invalid operation frame magic");
            }
            final int version = Byte.toUnsignedInt(in.readByte());
            if (version != VERSION) {
                throw new IOException("unsupported operation frame format " + version);
            }
            final ShardLogId id = ShardLogId.readFrom(in);
            final Kind kind = Kind.fromCode(Byte.toUnsignedInt(in.readByte()));
            final long epoch = in.readVLong();
            final long term = in.readVLong();
            final long schema = in.readVLong();
            final int length = in.readVInt();
            if (length < 0 || length > maximumPayloadBytes) {
                throw new IOException("operation payload length exceeds configured admission limit");
            }
            // Do not allocate a declared payload larger than the bytes actually supplied.
            if (length > in.available() - Integer.BYTES) {
                throw new IOException("truncated operation payload");
            }
            final byte[] payload = new byte[length];
            in.readBytes(payload, 0, length);
            final int actual = (int) checksum.getValue();
            final int expected = in.readInt();
            if (actual != expected) {
                throw new IOException("operation frame checksum mismatch");
            }
            if (in.read() != -1) {
                throw new IOException("trailing bytes after operation frame");
            }
            try {
                return new OperationEnvelope(id, kind, epoch, term, schema, payload, true);
            } catch (IllegalArgumentException e) {
                throw new IOException("invalid operation frame metadata", e);
            }
        }
    }

    private static void validateLimit(int maximumPayloadBytes) {
        if (maximumPayloadBytes < 0 || maximumPayloadBytes > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("payload limit must be between 0 and " + MAX_PAYLOAD_BYTES);
        }
    }
}
