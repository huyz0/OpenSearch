/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import java.util.zip.CRC32C;

public class JournalGroupProtocolTests extends OpenSearchTestCase {
    public void testProtocolRoundTripAndEncodingOwnsItsBytes() throws Exception {
        final var protocol = protocol();
        final var encoded = protocol.encode();
        assertEquals(JournalGroupProtocol.MAX_BYTES, encoded.length);
        assertEquals(0x4a50524f, ByteBuffer.wrap(encoded).getInt());
        assertEquals(1, encoded[4]);
        final var decoded = JournalGroupProtocol.read(new ByteArrayInputStream(encoded));
        assertEquals(protocol, decoded);
        encoded[10] ^= 1;
        assertEquals(protocol, decoded);
        assertArrayEquals(protocol.encode(), decoded.encode());
    }

    public void testEveryCorruptionTruncationAndTrailingByteFailsClosed() throws Exception {
        final byte[] encoded = protocol().encode();
        for (int i = 0; i < encoded.length; i++) {
            final byte[] corrupt = encoded.clone();
            corrupt[i] ^= 1;
            expectThrows(IOException.class, () -> JournalGroupProtocol.read(new ByteArrayInputStream(corrupt)));
            final byte[] truncated = Arrays.copyOf(encoded, i);
            expectThrows(IOException.class, () -> JournalGroupProtocol.read(new ByteArrayInputStream(truncated)));
        }
        expectThrows(
            IOException.class,
            () -> JournalGroupProtocol.read(new ByteArrayInputStream(Arrays.copyOf(encoded, encoded.length + 1)))
        );
    }

    public void testRecomputedChecksumDoesNotAuthorizeUnknownFormatsOrInvalidLimits() throws Exception {
        for (int offset : new int[] { 0, 4, 37, 41, 45, 49 }) {
            final byte[] invalid = protocol().encode();
            if (offset < 5) {
                invalid[offset] = (byte) 255;
            } else if (offset == 49) {
                ByteBuffer.wrap(invalid).putLong(offset, Long.MAX_VALUE);
            } else {
                ByteBuffer.wrap(invalid).putInt(offset, 0);
            }
            checksum(invalid);
            expectThrows(IOException.class, () -> JournalGroupProtocol.read(new ByteArrayInputStream(invalid)));
        }
    }

    public void testLocalResourceAllowancesAreNotPartOfTheSharedWireContract() {
        final var original = JournalConfiguration.Limits.defaults();
        final var local = new JournalConfiguration.Limits(
            8,
            original.maximumPayloadBytes(),
            original.maximumPendingRequests(),
            2 * original.maximumPendingBytes(),
            original.queueBytes(),
            2 * original.segmentBytes(),
            0,
            original.maximumRetainedEntries(),
            2 * original.minimumFreeBytes()
        );
        final var deployment = UUID.randomUUID();
        final var group = UUID.randomUUID();
        assertEquals(new JournalGroupProtocol(deployment, group, original), new JournalGroupProtocol(deployment, group, local));
        expectThrows(IllegalArgumentException.class, () -> new JournalGroupProtocol(deployment, group, 0, 256, 8 * 1024 * 1024, 100_000));
        expectThrows(
            IllegalArgumentException.class,
            () -> new JournalGroupProtocol(deployment, group, 1024, 65537, 8 * 1024 * 1024, 100_000)
        );
        expectThrows(IllegalArgumentException.class, () -> new JournalGroupProtocol(deployment, group, 1024, 256, 1, 100_000));
        expectThrows(IllegalArgumentException.class, () -> new JournalGroupProtocol(deployment, group, 1024, 256, 8 * 1024 * 1024, 1023));
    }

    private static JournalGroupProtocol protocol() {
        return new JournalGroupProtocol(UUID.randomUUID(), UUID.randomUUID(), JournalConfiguration.Limits.defaults());
    }

    private static void checksum(byte[] encoded) {
        final var crc = new CRC32C();
        crc.update(encoded, 0, encoded.length - Integer.BYTES);
        ByteBuffer.wrap(encoded).putInt(encoded.length - Integer.BYTES, (int) crc.getValue());
    }
}
