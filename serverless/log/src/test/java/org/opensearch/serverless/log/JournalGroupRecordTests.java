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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.zip.CRC32C;

public class JournalGroupRecordTests extends OpenSearchTestCase {
    public void testCanonicalPlacementAndLargestAcceptedStringsRoundTrip() throws Exception {
        final var record = reservation();
        final var peers = new ArrayList<>(record.initialPlacement().peers());
        Collections.reverse(peers);
        final var reordered = new JournalGroupRecord(
            record.deploymentId(),
            new JournalConfiguration.Group(record.initialPlacement().id(), peers),
            null
        );
        assertEquals(record, reordered);
        assertArrayEquals(record.encode(), reordered.encode());
        assertEquals(record, JournalGroupRecord.read(new ByteArrayInputStream(record.encode())));
        assertTrue(record.encode().length <= JournalGroupRecord.MAX_BYTES);
    }

    public void testActivationKeepsTheOriginalPhysicalLineage() throws Exception {
        final var record = reservation();
        final var checkpoint = new LogCheckpoint(
            JournalGroupRecord.controlIdentity(record.deploymentId(), record.initialPlacement().id()),
            2,
            1
        );
        final var active = record.activate(checkpoint);
        assertEquals(active, active.activate(checkpoint));
        assertEquals(active, JournalGroupRecord.read(new ByteArrayInputStream(active.encode())));
        expectThrows(IllegalArgumentException.class, () -> active.activate(new LogCheckpoint(checkpoint.logId(), 3, 1)));
        expectThrows(IllegalArgumentException.class, () -> record.activate(new LogCheckpoint(checkpoint.logId(), 0, 1)));
        expectThrows(IllegalArgumentException.class, () -> record.activate(new LogCheckpoint(checkpoint.logId(), 2, 0)));
        final var foreign = JournalGroupRecord.controlIdentity(UUID.randomUUID(), record.initialPlacement().id());
        expectThrows(IllegalArgumentException.class, () -> record.activate(new LogCheckpoint(foreign, 2, 1)));
    }

    public void testCorruptionAndEveryTruncationAreRejected() throws Exception {
        final byte[] encoded = reservation().encode();
        for (int length = 0; length < encoded.length; length++) {
            final byte[] truncated = Arrays.copyOf(encoded, length);
            expectThrows(IOException.class, () -> JournalGroupRecord.read(new ByteArrayInputStream(truncated)));
        }
        for (int offset : List.of(0, 4, 16, encoded.length / 2, encoded.length - 1)) {
            final byte[] corrupt = encoded.clone();
            corrupt[offset] ^= 1;
            expectThrows(IOException.class, () -> JournalGroupRecord.read(new ByteArrayInputStream(corrupt)));
        }
    }

    public void testValidChecksumDoesNotPermitUnknownOrTrailingFields() throws Exception {
        final byte[] encoded = reservation().encode();
        final byte[] version = encoded.clone();
        ByteBuffer.wrap(version).putInt(4, 2);
        checksum(version);
        expectThrows(IOException.class, () -> JournalGroupRecord.read(new ByteArrayInputStream(version)));
        final byte[] state = encoded.clone();
        state[state.length - 5] = 2;
        checksum(state);
        expectThrows(IOException.class, () -> JournalGroupRecord.read(new ByteArrayInputStream(state)));
        final byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        checksum(trailing);
        expectThrows(IOException.class, () -> JournalGroupRecord.read(new ByteArrayInputStream(trailing)));
    }

    public void testOversizedInputDoesNotReadPastItsBound() throws Exception {
        final var input = new ByteArrayInputStream(new byte[JournalGroupRecord.MAX_BYTES + 100]);
        expectThrows(IOException.class, () -> JournalGroupRecord.read(input));
        assertEquals(99, input.available());
    }

    private static void checksum(byte[] frame) {
        final var crc = new CRC32C();
        crc.update(frame, 0, frame.length - 4);
        ByteBuffer.wrap(frame).putInt(frame.length - 4, (int) crc.getValue());
    }

    private static JournalGroupRecord reservation() {
        final var peers = new ArrayList<JournalConfiguration.Peer>();
        for (int i = 0; i < 3; i++) {
            peers.add(new JournalConfiguration.Peer(UUID.randomUUID(), "h".repeat(253), 9000 + i, "界".repeat(255) + i));
        }
        return new JournalGroupRecord(UUID.randomUUID(), new JournalConfiguration.Group(UUID.randomUUID(), peers), null);
    }
}
