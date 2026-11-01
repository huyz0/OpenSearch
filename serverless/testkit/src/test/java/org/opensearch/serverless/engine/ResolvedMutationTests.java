/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.log.OperationEnvelope;
import org.opensearch.serverless.log.ShardLogId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.UUID;

public class ResolvedMutationTests extends OpenSearchTestCase {
    public void testVersionOneGoldenDeleteRecord() throws Exception {
        final byte[] golden = HexFormat.of()
            .parseHex(
                "524d55540103"
                    + "00000000000000010000000000000002"
                    + "0000000000000003"
                    + "0000000000000004"
                    + "0000000000000005"
                    + "0000000169ffffffff"
            );
        final var record = new ResolvedMutation(new UUID(1, 2), new Translog.Delete("i", 3, 4, 5), ResolvedMutation.Outcome.DELETED);
        assertArrayEquals(golden, record.encode(golden.length));
        assertEquals(record.operation(), ResolvedMutation.decode(golden, golden.length).operation());
    }

    public void testEveryOutcomeRetainsIdentityRoutingAndOriginalResult() throws Exception {
        for (var outcome : ResolvedMutation.Outcome.values()) {
            for (String routing : new String[] { null, "", "routé-\uD83D\uDE80" }) {
                final Translog.Operation operation = switch (outcome) {
                    case CREATED, UPDATED -> new Translog.Index("idé-\uD83D\uDE80", 17, 9, 4, new byte[] { 0, -1, 2 }, routing, 1234);
                    case DELETED, NOT_FOUND -> new Translog.Delete("idé-\uD83D\uDE80", 17, 9, 4, routing);
                    case NO_OP -> new Translog.NoOp(17, 9, "resolved-\uD83D\uDE80");
                };
                final var original = new ResolvedMutation(UUID.randomUUID(), operation, outcome);
                final byte[] bytes = original.encode(4096);
                final var actual = ResolvedMutation.decode(bytes, bytes.length);
                assertEquals(original.operationId(), actual.operationId());
                assertEquals(outcome, actual.outcome());
                assertEquals(operation, actual.operation());
                assertArrayEquals(bytes, actual.encode(bytes.length));
                expectThrows(IOException.class, () -> original.encode(bytes.length - 1));
                expectThrows(IOException.class, () -> ResolvedMutation.decode(bytes, bytes.length - 1));
            }
        }
    }

    public void testNoSourceAliasEscapesConstructionDecodeOrOperationAccessor() throws Exception {
        final byte[] source = "{\"n\":1}".getBytes(StandardCharsets.UTF_8);
        final byte[] expected = source.clone();
        final var record = new ResolvedMutation(UUID.randomUUID(), index(source), ResolvedMutation.Outcome.CREATED);
        source[0] ^= 1;
        final byte[] encoded = record.encode(4096);
        final var decoded = ResolvedMutation.decode(encoded, 4096);
        encoded[encoded.length - 1] ^= 1;
        final byte[] read = BytesReference.toBytes(((Translog.Index) decoded.operation()).source());
        read[0] ^= 1;
        assertArrayEquals(expected, BytesReference.toBytes(((Translog.Index) record.operation()).source()));
        assertArrayEquals(expected, BytesReference.toBytes(((Translog.Index) decoded.operation()).source()));
    }

    public void testTruncationTrailingBytesAndUnknownCodesFailClosed() throws Exception {
        final byte[] bytes = new ResolvedMutation(UUID.randomUUID(), index(new byte[] { 1 }), ResolvedMutation.Outcome.UPDATED).encode(
            4096
        );
        for (int length = 0; length < bytes.length; length++) {
            final byte[] truncated = Arrays.copyOf(bytes, length);
            expectThrows(IOException.class, () -> ResolvedMutation.decode(truncated, 4096));
        }
        expectThrows(IOException.class, () -> ResolvedMutation.decode(Arrays.copyOf(bytes, bytes.length + 1), 4096));
        for (int offset : new int[] { 0, 4, 5 }) {
            final byte[] changed = bytes.clone();
            changed[offset] = (byte) 255;
            expectThrows(IOException.class, () -> ResolvedMutation.decode(changed, 4096));
        }
    }

    public void testDeclaredLengthsAndUtf8AreCheckedBeforeAllocation() throws Exception {
        final byte[] bytes = new ResolvedMutation(UUID.randomUUID(), index(new byte[] { 1 }), ResolvedMutation.Outcome.CREATED).encode(
            4096
        );
        // Fixed header is 38 bytes, followed by version(8), id length(4), one-byte id,
        // routing length(4), timestamp(8), source length(4), then source.
        for (int offset : new int[] { 46, 51, 63 }) {
            for (int length : new int[] { -2, Integer.MAX_VALUE }) {
                final byte[] changed = bytes.clone();
                ByteBuffer.wrap(changed).putInt(offset, length);
                expectThrows(IOException.class, () -> ResolvedMutation.decode(changed, 4096));
            }
        }
        final byte[] invalidId = bytes.clone();
        invalidId[50] = (byte) 0xff;
        expectThrows(IOException.class, () -> ResolvedMutation.decode(invalidId, 4096));
        final byte[] noOp = new ResolvedMutation(UUID.randomUUID(), new Translog.NoOp(0, 1, "x"), ResolvedMutation.Outcome.NO_OP).encode(
            4096
        );
        noOp[42] = (byte) 0xff;
        expectThrows(IOException.class, () -> ResolvedMutation.decode(noOp, 4096));
    }

    public void testInvalidResolvedCoordinatesAndResultsAreRefused() throws Exception {
        for (long seqNo : new long[] { -2, -1 }) {
            expectThrows(
                IllegalArgumentException.class,
                () -> new ResolvedMutation(UUID.randomUUID(), new Translog.Delete("i", seqNo, 1, 1), ResolvedMutation.Outcome.DELETED)
            );
        }
        expectThrows(
            IllegalArgumentException.class,
            () -> new ResolvedMutation(UUID.randomUUID(), new Translog.Delete("i", 0, 0, 1), ResolvedMutation.Outcome.DELETED)
        );
        for (var outcome : new ResolvedMutation.Outcome[] { ResolvedMutation.Outcome.CREATED, ResolvedMutation.Outcome.NO_OP }) {
            expectThrows(
                IllegalArgumentException.class,
                () -> new ResolvedMutation(UUID.randomUUID(), new Translog.Delete("i", 0, 1, 1), outcome)
            );
        }
        expectThrows(
            IllegalArgumentException.class,
            () -> new ResolvedMutation(UUID.randomUUID(), new Translog.Delete("i", 0, 1, -3), ResolvedMutation.Outcome.DELETED)
        );
        for (String id : new String[] { "", "x".repeat(513), "\uD800", "\uDC00" }) {
            expectThrows(
                IllegalArgumentException.class,
                () -> new ResolvedMutation(UUID.randomUUID(), new Translog.Delete(id, 0, 1, 1), ResolvedMutation.Outcome.DELETED)
            );
        }
        final byte[] invalidCoordinates = new ResolvedMutation(
            UUID.randomUUID(),
            new Translog.NoOp(0, 1, ""),
            ResolvedMutation.Outcome.NO_OP
        ).encode(4096);
        ByteBuffer.wrap(invalidCoordinates).putLong(22, -1);
        expectThrows(IOException.class, () -> ResolvedMutation.decode(invalidCoordinates, 4096));
    }

    public void testEnvelopeProtectsPayloadAndNoNativeTranslogFormatIsEmbedded() throws Exception {
        final var original = new ResolvedMutation(UUID.randomUUID(), index(new byte[] { 1 }), ResolvedMutation.Outcome.CREATED);
        final var envelope = new OperationEnvelope(
            new ShardLogId(UUID.randomUUID(), "index", 0, UUID.randomUUID()),
            OperationEnvelope.Kind.DOCUMENTS,
            1,
            1,
            1,
            original.encode(4096)
        );
        final byte[] encoded = envelope.encode(4096);
        final var decoded = OperationEnvelope.decode(encoded, 4096);
        assertEquals(original.operation(), ResolvedMutation.decode(decoded.payloadReader().readAllBytes(), 4096).operation());
        encoded[encoded.length - 5] ^= 1;
        expectThrows(IOException.class, () -> OperationEnvelope.decode(encoded, 4096));
        expectThrows(IllegalArgumentException.class, () -> original.encode(-1));
        expectThrows(IllegalArgumentException.class, () -> original.encode(Integer.MAX_VALUE));
    }

    private static Translog.Index index(byte[] source) {
        return new Translog.Index("i", 0, 1, 1, source, null, -1);
    }
}
