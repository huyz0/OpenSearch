/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;
import java.util.zip.CRC32C;

@SuppressFileSystems("*")
public class JournalDurableFloorTests extends OpenSearchTestCase {
    private record Identity(UUID deployment, UUID voter, UUID group) {
        private Identity() {
            this(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        }
    }

    public void testFixedSizeRoundTripAndClosedStoreCannotAcceptAnUpdate() throws Exception {
        final var id = new Identity();
        final Path path = createTempDir().resolve("floor");
        try (var floor = create(path, id)) {
            assertEquals(-1, floor.index());
            for (int i = 0; i < 20; i++) {
                floor.persist(i);
                assertEquals(i, floor.index());
            }
        }
        assertEquals(152, Files.size(path));
        try (var floor = open(path, id)) {
            assertEquals(19, floor.index());
            floor.close();
            expectThrows(IOException.class, () -> floor.persist(19));
        }
    }

    public void testLegitimateTruncationUsesGenerationInsteadOfGreatestIndex() throws Exception {
        final var id = new Identity();
        final Path path = createTempDir().resolve("floor");
        try (var floor = create(path, id)) {
            floor.persist(100);
            floor.persist(2);
        }
        try (var floor = open(path, id)) {
            assertEquals(2, floor.index());
            floor.persist(3);
        }
        try (var floor = open(path, id)) {
            assertEquals(3, floor.index());
        }
    }

    public void testCorruptSlotDoesNotHideANewerBoundaryBehindAnOlderValidSlot() throws Exception {
        final var id = new Identity();
        final Path path = createTempDir().resolve("floor");
        try (var floor = create(path, id)) {
            floor.persist(10);
            floor.persist(11);
        }
        final byte[] corrupted = Files.readAllBytes(path);
        corrupted[0] ^= 1;
        Files.write(path, corrupted);
        expectThrows(IOException.class, () -> open(path, id));
        assertArrayEquals(corrupted, Files.readAllBytes(path));
    }

    public void testMissingTruncatedAndOversizedFilesAreNeverReinitialized() throws Exception {
        final var id = new Identity();
        final Path path = createTempDir().resolve("floor");
        expectThrows(IOException.class, () -> open(path, id));
        assertFalse(Files.exists(path));
        try (var floor = create(path, id)) {
            floor.persist(10);
        }
        final byte[] encoded = Files.readAllBytes(path);
        for (int length : new int[] { 0, 76, 151, 153 }) {
            final byte[] malformed = Arrays.copyOf(encoded, length);
            Files.write(path, malformed);
            expectThrows(IOException.class, () -> open(path, id));
            assertArrayEquals(malformed, Files.readAllBytes(path));
        }
    }

    public void testFloorCannotMoveBetweenDeploymentsVolumesOrGroups() throws Exception {
        final var id = new Identity();
        final Path path = createTempDir().resolve("floor");
        try (var floor = create(path, id)) {
            floor.persist(10);
        }
        expectThrows(IOException.class, () -> open(path, new Identity(UUID.randomUUID(), id.voter(), id.group())));
        expectThrows(IOException.class, () -> open(path, new Identity(id.deployment(), UUID.randomUUID(), id.group())));
        expectThrows(IOException.class, () -> open(path, new Identity(id.deployment(), id.voter(), UUID.randomUUID())));
        try (var floor = open(path, id)) {
            assertEquals(10, floor.index());
        }
    }

    public void testExhaustedGenerationAndSwappedSlotsAreRejectedWithoutMutation() throws Exception {
        final var id = new Identity();
        final Path path = createTempDir().resolve("floor");
        try (var floor = create(path, id)) {
            floor.persist(10);
            floor.persist(11);
        }
        final byte[] ordinary = Files.readAllBytes(path);
        final byte[] swapped = new byte[152];
        System.arraycopy(ordinary, 0, swapped, 76, 76);
        System.arraycopy(ordinary, 76, swapped, 0, 76);
        Files.write(path, swapped);
        expectThrows(IOException.class, () -> open(path, id));
        final var bytes = ByteBuffer.wrap(ordinary);
        bytes.putLong(56, Long.MAX_VALUE - 1).putLong(64, 10).putLong(132, Long.MAX_VALUE).putLong(140, 11);
        checksum(ordinary, 0);
        checksum(ordinary, 76);
        Files.write(path, ordinary);
        try (var floor = open(path, id)) {
            assertEquals(11, floor.index());
            expectThrows(IOException.class, () -> floor.persist(12));
        }
        assertArrayEquals(ordinary, Files.readAllBytes(path));
    }

    private static void checksum(byte[] bytes, int offset) {
        final var crc = new CRC32C();
        crc.update(bytes, offset, 72);
        ByteBuffer.wrap(bytes).putInt(offset + 72, (int) crc.getValue());
    }

    private static JournalDurableFloor create(Path path, Identity id) throws IOException {
        return JournalDurableFloor.create(path, id.deployment(), id.voter(), id.group());
    }

    private static JournalDurableFloor open(Path path, Identity id) throws IOException {
        return JournalDurableFloor.open(path, id.deployment(), id.voter(), id.group());
    }
}
