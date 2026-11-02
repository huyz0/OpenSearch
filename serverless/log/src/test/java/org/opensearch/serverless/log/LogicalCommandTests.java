/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.UUID;

public class LogicalCommandTests extends OpenSearchTestCase {
    public void testAllActionsRoundTripWithoutMutableAliases() throws Exception {
        for (var action : LogicalCommand.Action.values()) {
            final byte[] digest = randomByteArrayOfLength(32);
            final byte[] expectedDigest = digest.clone();
            final boolean hasData = action == LogicalCommand.Action.SCHEMA
                || action == LogicalCommand.Action.MUTATION
                || action == LogicalCommand.Action.OUTCOME_ONLY;
            final long argument = action == LogicalCommand.Action.RETIRE || action == LogicalCommand.Action.OUTCOME_ONLY
                ? -1
                : Long.MAX_VALUE;
            final byte[] payload = hasData ? randomByteArrayOfLength(128) : new byte[0];
            final byte[] expectedPayload = payload.clone();
            final UUID request = UUID.randomUUID();
            final UUID actor = UUID.randomUUID();
            final var command = new LogicalCommand(action, request, Long.MAX_VALUE, digest, actor, argument, payload);
            digest[0] ^= 1;
            if (hasData) {
                payload[0] ^= 1;
            }
            final var encoded = command.encode(1024);
            final var decoded = LogicalCommand.decode(encoded, 1024);
            encoded[30] ^= 1;
            assertEquals(action, decoded.action());
            assertEquals(request, decoded.requestId());
            assertEquals(actor, decoded.actor());
            assertEquals(Long.MAX_VALUE, decoded.firstObservedIndex());
            assertEquals(argument, decoded.argument());
            assertArrayEquals(expectedDigest, decoded.requestDigest());
            decoded.requestDigest()[0] ^= 1;
            assertArrayEquals(expectedDigest, decoded.requestDigest());
            assertArrayEquals(expectedPayload, decoded.payloadReader().readAllBytes());
            assertEquals(expectedPayload.length, decoded.payloadLength());
            final byte[] read = decoded.payloadReader().readAllBytes();
            if (hasData) {
                read[0] ^= 1;
            }
            assertArrayEquals(expectedPayload, decoded.payloadReader().readAllBytes());
        }
    }

    public void testRetirementGoldenEncoding() throws Exception {
        final byte[] digest = new byte[32];
        for (int i = 0; i < digest.length; i++) {
            digest[i] = (byte) i;
        }
        final var command = new LogicalCommand(LogicalCommand.Action.RETIRE, new UUID(1, 2), 5, digest, new UUID(3, 4), -1, new byte[0]);
        assertEquals(
            "4c4354310105000000000000000100000000000000020000000000000005"
                + "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
                + "00000000000000030000000000000004ffffffffffffffff00000000",
            HexFormat.of().formatHex(command.encode(90))
        );
    }

    public void testTruncationUnknownFieldsAndUntrustedLengthsFailBeforeAllocation() throws Exception {
        final byte[] encoded = command().encode(1024);
        for (int length = 0; length < encoded.length; length++) {
            final byte[] truncated = Arrays.copyOf(encoded, length);
            expectThrows(IOException.class, () -> LogicalCommand.decode(truncated, 1024));
        }
        for (int offset : new int[] { 0, 4, 5 }) {
            final byte[] bad = encoded.clone();
            bad[offset] = (byte) 255;
            expectThrows(IOException.class, () -> LogicalCommand.decode(bad, 1024));
        }
        final byte[] observed = encoded.clone();
        ByteBuffer.wrap(observed).putLong(22, -2);
        expectThrows(IOException.class, () -> LogicalCommand.decode(observed, 1024));
        final byte[] sequence = encoded.clone();
        ByteBuffer.wrap(sequence).putLong(78, -1);
        expectThrows(IOException.class, () -> LogicalCommand.decode(sequence, 1024));
        for (int length : new int[] { -1, 0, Integer.MAX_VALUE }) {
            final byte[] bad = encoded.clone();
            ByteBuffer.wrap(bad).putInt(86, length);
            expectThrows(IOException.class, () -> LogicalCommand.decode(bad, 1024));
        }
        expectThrows(IOException.class, () -> LogicalCommand.decode(Arrays.copyOf(encoded, encoded.length + 1), 1024));
    }

    public void testConfiguredWholeCommandLimitIncludesHeader() throws Exception {
        final var command = command();
        expectThrows(IOException.class, () -> command.encode(90));
        final byte[] encoded = command.encode(91);
        assertEquals(91, encoded.length);
        expectThrows(IOException.class, () -> LogicalCommand.decode(encoded, 90));
        expectThrows(IllegalArgumentException.class, () -> command.encode(0));
        expectThrows(IllegalArgumentException.class, () -> LogicalCommand.decode(encoded, Integer.MAX_VALUE));
    }

    public void testActionFieldsAndRetryCoordinatesAreValidated() {
        final var id = UUID.randomUUID();
        final byte[] hash = new byte[32];
        expectThrows(
            IllegalArgumentException.class,
            () -> new LogicalCommand(LogicalCommand.Action.OWNERSHIP, id, -2, hash, id, 0, new byte[0])
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new LogicalCommand(LogicalCommand.Action.RETIRE, id, -1, new byte[31], id, -1, new byte[0])
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new LogicalCommand(LogicalCommand.Action.SCHEMA, id, -1, hash, id, 0, new byte[0])
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new LogicalCommand(LogicalCommand.Action.MUTATION, id, -1, hash, id, -1, new byte[1])
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new LogicalCommand(LogicalCommand.Action.OUTCOME_ONLY, id, -1, hash, id, 0, new byte[1])
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new LogicalCommand(LogicalCommand.Action.RETIRE, id, -1, hash, id, -1, new byte[1])
        );
    }

    public void testOuterEnvelopeProvidesIntegrityForTheLogicalPayload() throws Exception {
        final var stream = new ShardLogId(UUID.randomUUID(), "index-uuid", 0, UUID.randomUUID());
        final var envelope = new OperationEnvelope(stream, OperationEnvelope.Kind.DOCUMENTS, 1, 1, 0, command().encode(1024));
        final byte[] encoded = envelope.encode(1024);
        final var decoded = OperationEnvelope.decode(encoded, 1024);
        assertEquals(command().action(), LogicalCommand.decode(decoded.payloadReader().readAllBytes(), 1024).action());
        encoded[encoded.length - 6] ^= 1;
        expectThrows(IOException.class, () -> OperationEnvelope.decode(encoded, 1024));
    }

    private static LogicalCommand command() {
        return new LogicalCommand(LogicalCommand.Action.MUTATION, new UUID(1, 2), -1, new byte[32], new UUID(3, 4), 0, new byte[] { 7 });
    }
}
