/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

public class OperationEnvelopeTests extends OpenSearchTestCase {

    private ShardLogId identity() {
        return new ShardLogId(UUID.randomUUID(), "index-uuid", randomIntBetween(0, 100), UUID.randomUUID());
    }

    public void testKindsCoordinatesAndPayloadRoundTripWithoutMutableAliases() throws Exception {
        final var id = identity();
        for (OperationEnvelope.Kind kind : OperationEnvelope.Kind.values()) {
            final byte[] original = randomByteArrayOfLength(32);
            final byte[] expected = original.clone();
            final var envelope = new OperationEnvelope(id, kind, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, original);
            original[0] ^= 1;
            final byte[] encoded = envelope.encode(32);
            final var decoded = OperationEnvelope.decode(encoded, 32);
            encoded[encoded.length - 5] ^= 1;
            assertEquals(id, decoded.logId());
            assertEquals(kind, decoded.kind());
            assertEquals(Long.MAX_VALUE, decoded.producerEpoch());
            assertEquals(Long.MAX_VALUE, decoded.primaryTerm());
            assertEquals(Long.MAX_VALUE, decoded.schemaVersion());
            assertEquals(32, decoded.payloadLength());
            assertArrayEquals(expected, decoded.payloadReader().readAllBytes());
            final byte[] read = decoded.payloadReader().readAllBytes();
            read[0] ^= 1;
            assertArrayEquals(expected, decoded.payloadReader().readAllBytes());
        }
    }

    public void testEverySingleByteCorruptionAndTruncatedPrefixFailsClosed() throws Exception {
        final var envelope = new OperationEnvelope(identity(), OperationEnvelope.Kind.DOCUMENTS, 1, 2, 3, new byte[] { 1, 2, 3 });
        final byte[] encoded = envelope.encode(3);
        for (int offset = 0; offset < encoded.length; offset++) {
            final byte[] changed = encoded.clone();
            changed[offset] ^= 1;
            expectThrows(IOException.class, () -> OperationEnvelope.decode(changed, 3));
            final byte[] truncated = Arrays.copyOf(encoded, offset);
            expectThrows(IOException.class, () -> OperationEnvelope.decode(truncated, 3));
        }
        final byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        assertTrue(expectThrows(IOException.class, () -> OperationEnvelope.decode(trailing, 3)).getMessage().contains("trailing"));
    }

    public void testDeclaredPayloadLengthIsCheckedBeforeAllocating() throws Exception {
        final var bytes = new ByteArrayOutputStream();
        try (var out = new OutputStreamStreamOutput(bytes)) {
            out.writeInt(0x4c4c4f50);
            out.writeByte((byte) 1);
            identity().writeTo(out);
            out.writeByte((byte) 1);
            out.writeVLong(1);
            out.writeVLong(1);
            out.writeVLong(1);
            out.writeVInt(Integer.MAX_VALUE);
        }
        assertTrue(
            expectThrows(IOException.class, () -> OperationEnvelope.decode(bytes.toByteArray(), 32)).getMessage().contains("length")
        );
    }

    public void testConfiguredLimitsAndEmptyPayload() throws Exception {
        final var envelope = new OperationEnvelope(identity(), OperationEnvelope.Kind.RECOVERY_BARRIER, 0, 0, 0, new byte[0]);
        assertEquals(0, OperationEnvelope.decode(envelope.encode(0), 0).payloadLength());
        expectThrows(IllegalArgumentException.class, () -> envelope.encode(-1));
        expectThrows(IllegalArgumentException.class, () -> envelope.encode(Integer.MAX_VALUE));
        final var nonEmpty = new OperationEnvelope(identity(), OperationEnvelope.Kind.DOCUMENTS, 1, 1, 1, new byte[1]);
        expectThrows(IOException.class, () -> nonEmpty.encode(0));
        expectThrows(IOException.class, () -> OperationEnvelope.decode(nonEmpty.encode(1), 0));
        expectThrows(
            IllegalArgumentException.class,
            () -> new OperationEnvelope(identity(), OperationEnvelope.Kind.DOCUMENTS, -1, 1, 1, new byte[0])
        );
    }
}
