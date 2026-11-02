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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;

public class LogicalStreamRecordTests extends OpenSearchTestCase {
    public void testAssignmentAndLimitsRoundTripIncludingMaximumIdentity() throws Exception {
        final var deployment = UUID.randomUUID();
        final var group = UUID.randomUUID();
        final var stream = new ShardLogId(deployment, "x".repeat(256), Integer.MAX_VALUE, UUID.randomUUID());
        final var cp = new LogCheckpoint(JournalGroupRecord.controlIdentity(deployment, group), Long.MAX_VALUE, Long.MAX_VALUE);
        for (var limits : new int[][] { { 1, 91 }, { 65536, OperationEnvelope.MAX_PAYLOAD_BYTES } }) {
            final var record = new LogicalStreamRecord(stream, group, cp, limits[0], limits[1]);
            final var encoded = record.encode();
            assertTrue(encoded.length <= LogicalStreamRecord.MAX_BYTES);
            assertEquals(0x4c535452, ByteBuffer.wrap(encoded).getInt());
            assertEquals(1, encoded[4]);
            final var decoded = LogicalStreamRecord.read(new ByteArrayInputStream(encoded));
            assertEquals(record, decoded);
            encoded[10] ^= 1;
            assertEquals(record, decoded);
        }
    }

    public void testEveryCorruptionAndTruncationIsRejected() throws Exception {
        final byte[] encoded = record().encode();
        for (int i = 0; i < encoded.length; i++) {
            final byte[] truncated = Arrays.copyOf(encoded, i);
            expectThrows(IOException.class, () -> LogicalStreamRecord.read(new ByteArrayInputStream(truncated)));
            final byte[] corrupt = encoded.clone();
            corrupt[i] ^= 1;
            expectThrows(IOException.class, () -> LogicalStreamRecord.read(new ByteArrayInputStream(corrupt)));
        }
        final byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        checksum(trailing);
        expectThrows(IOException.class, () -> LogicalStreamRecord.read(new ByteArrayInputStream(trailing)));
    }

    public void testValidChecksumCannotAuthorizeUnknownFormatOrInvalidLimits() throws Exception {
        final byte[] encoded = record().encode();
        for (int position : new int[] { 0, 4 }) {
            final byte[] bad = encoded.clone();
            bad[position] ^= 1;
            checksum(bad);
            expectThrows(IOException.class, () -> LogicalStreamRecord.read(new ByteArrayInputStream(bad)));
        }
        for (int[] change : new int[][] { { 12, 0 }, { 12, 65537 }, { 8, 90 }, { 8, Integer.MAX_VALUE } }) {
            final byte[] bad = encoded.clone();
            ByteBuffer.wrap(bad).putInt(bad.length - change[0], change[1]);
            checksum(bad);
            expectThrows(IOException.class, () -> LogicalStreamRecord.read(new ByteArrayInputStream(bad)));
        }
    }

    public void testUnboundedSourceIsCutOffBeforeParsing() {
        final var read = new AtomicInteger();
        final var infinite = new InputStream() {
            @Override
            public int read() {
                read.incrementAndGet();
                return 0;
            }
        };
        expectThrows(IOException.class, () -> LogicalStreamRecord.read(infinite));
        assertEquals(LogicalStreamRecord.MAX_BYTES + 1, read.get());
    }

    public void testForeignHistoryEmptyActivationAndReservedStreamsAreRefused() {
        final var record = record();
        final var wrongGroup = UUID.randomUUID();
        expectThrows(
            IllegalArgumentException.class,
            () -> new LogicalStreamRecord(record.stream(), wrongGroup, record.groupActivation(), 8, 4096)
        );
        final var foreign = new ShardLogId(UUID.randomUUID(), record.stream().indexUuid(), 0, record.stream().activationId());
        expectThrows(
            IllegalArgumentException.class,
            () -> new LogicalStreamRecord(foreign, record.group(), record.groupActivation(), 8, 4096)
        );
        for (var cp : new LogCheckpoint[] {
            new LogCheckpoint(record.groupActivation().logId(), -1, 0),
            new LogCheckpoint(record.groupActivation().logId(), 0, 1),
            new LogCheckpoint(record.groupActivation().logId(), 1, 0) }) {
            expectThrows(IllegalArgumentException.class, () -> new LogicalStreamRecord(record.stream(), record.group(), cp, 8, 4096));
        }
        for (var prefix : new String[] { "__journal_group/", "__journal_membership/" }) {
            final var reserved = new ShardLogId(record.stream().deploymentId(), prefix + "reserved", 0, UUID.randomUUID());
            expectThrows(
                IllegalArgumentException.class,
                () -> new LogicalStreamRecord(reserved, record.group(), record.groupActivation(), 8, 4096)
            );
        }
    }

    private static LogicalStreamRecord record() {
        final var deployment = UUID.randomUUID();
        final var group = UUID.randomUUID();
        final var stream = new ShardLogId(deployment, "index-uuid", 0, UUID.randomUUID());
        return new LogicalStreamRecord(
            stream,
            group,
            new LogCheckpoint(JournalGroupRecord.controlIdentity(deployment, group), 2, 1),
            8,
            4096
        );
    }

    private static void checksum(byte[] encoded) {
        final var crc = new CRC32C();
        crc.update(encoded, 0, encoded.length - Integer.BYTES);
        ByteBuffer.wrap(encoded).putInt(encoded.length - Integer.BYTES, (int) crc.getValue());
    }
}
