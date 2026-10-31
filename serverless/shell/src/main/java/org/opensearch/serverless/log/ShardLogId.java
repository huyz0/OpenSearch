/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * Durable log identity, independent of node placement, index name, and leader elections. Activation
 * changes when a shard switches durable histories; a normal election keeps the same activation.
 * This value identifies a namespace but does not itself authorize access to it.
 *
 * @param deploymentId durable deployment namespace
 * @param indexUuid index incarnation, never the reusable index name
 * @param shardNumber logical shard
 * @param activationId durable activation identity, stable across leader elections
 */
@InternalApi
public record ShardLogId(UUID deploymentId, String indexUuid, int shardNumber, UUID activationId) implements Writeable {

    private static final int FORMAT_VERSION = 1;
    private static final int MAX_UUID_BYTES = 256;

    /** Validates a log identity before it can enter placement or persistence. */
    public ShardLogId {
        Objects.requireNonNull(deploymentId);
        Objects.requireNonNull(activationId);
        Objects.requireNonNull(indexUuid);
        if (indexUuid.isBlank()
            || indexUuid.length() > MAX_UUID_BYTES
            || StandardCharsets.UTF_8.newEncoder().canEncode(indexUuid) == false
            || indexUuid.getBytes(StandardCharsets.UTF_8).length > MAX_UUID_BYTES) {
            throw new IllegalArgumentException("index UUID must contain 1 to 256 valid UTF-8 bytes");
        }
        if (shardNumber < 0) {
            throw new IllegalArgumentException("shard number must be non-negative");
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte((byte) FORMAT_VERSION);
        out.writeLong(deploymentId.getMostSignificantBits());
        out.writeLong(deploymentId.getLeastSignificantBits());
        final byte[] uuid = indexUuid.getBytes(StandardCharsets.UTF_8);
        out.writeVInt(uuid.length);
        out.writeBytes(uuid, 0, uuid.length);
        out.writeVInt(shardNumber);
        out.writeLong(activationId.getMostSignificantBits());
        out.writeLong(activationId.getLeastSignificantBits());
    }

    /**
     * Reads an identity with a length bound checked before allocating the string buffer.
     *
     * @param in encoded identity
     * @return validated identity
     * @throws IOException on unknown format, excessive length, malformed UTF-8, or invalid identity
     */
    public static ShardLogId readFrom(StreamInput in) throws IOException {
        final int version = Byte.toUnsignedInt(in.readByte());
        if (version != FORMAT_VERSION) {
            throw new IOException("unsupported shard log identity format " + version);
        }
        final UUID deployment = new UUID(in.readLong(), in.readLong());
        final int length = in.readVInt();
        if (length < 1 || length > MAX_UUID_BYTES) {
            throw new IOException("index UUID length must be between 1 and 256 bytes");
        }
        final byte[] bytes = new byte[length];
        in.readBytes(bytes, 0, length);
        final String index;
        try {
            index = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            throw new IOException("invalid UTF-8 index UUID", e);
        }
        final int shard = in.readVInt();
        final UUID activation = new UUID(in.readLong(), in.readLong());
        try {
            return new ShardLogId(deployment, index, shard, activation);
        } catch (IllegalArgumentException e) {
            throw new IOException("invalid shard log identity", e);
        }
    }
}
