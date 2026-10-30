/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.engine;

import org.opensearch.serverless.store.WalRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ReplaySpoolTests extends OpenSearchTestCase {
    public void testModernOrderAndIdenticalDuplicates() throws Exception {
        final var history = List.of(
            new WalRecord("a", "{\"n\":1}", 4, 2, 1),
            WalRecord.deletion("b", 1, 1, 2),
            new WalRecord("a", "{\"n\":1}", 4, 2, 1)
        );
        final var parent = createTempDir();
        try (var spool = ReplaySpool.capture(parent, consumer -> {
            for (var record : history) {
                consumer.accept(record);
            }
        })) {
            assertEquals(3, spool.modernCount());
            assertEquals(4, spool.highestSeqNo());
            try (var snapshot = spool.snapshot()) {
                assertEquals(4, snapshot.next().seqNo());
                assertEquals(1, snapshot.next().seqNo());
                assertEquals(4, snapshot.next().seqNo());
                assertNull(snapshot.next());
            }
        }
        try (var files = Files.list(parent)) {
            assertEquals(0, files.filter(path -> path.getFileName().toString().startsWith("wal-replay-")).count());
        }
    }

    public void testRejectsEveryKindOfConflictingSequenceIdentity() throws Exception {
        final List<List<WalRecord>> histories = List.of(
            List.of(new WalRecord("id", "{}", 0, 1, 1), new WalRecord("id", "{\"n\":1}", 0, 1, 1)),
            List.of(new WalRecord("id", "{}", 0, 1, 1), new WalRecord("id", "{}", 0, 1, 2)),
            List.of(new WalRecord("id", "{}", 0, 1, 1), new WalRecord("id", "{}", 0, 2, 1)),
            List.of(WalRecord.deletion("first", 0, 1, 1), WalRecord.deletion("other", 0, 1, 1)),
            List.of(WalRecord.deletion("id", 0, 1, 1), new WalRecord("id", "{}", 0, 1, 1))
        );
        for (var history : histories) {
            final var parent = createTempDir();
            final var failure = expectThrows(IOException.class, () -> ReplaySpool.capture(parent, consumer -> {
                for (var record : history) {
                    consumer.accept(record);
                }
            }));
            assertTrue(failure.getMessage(), failure.getMessage().contains("share a sequence number"));
            try (var files = Files.list(parent)) {
                assertEquals(0, files.filter(path -> path.getFileName().toString().startsWith("wal-replay-")).count());
            }
        }
    }

    public void testMixedReplayMatchesTheOriginalOrderingRule() throws Exception {
        final var history = new ArrayList<WalRecord>();
        final String longId = "long-" + "\uD83D\uDE03".repeat(20000);
        final List<String> ids = List.of("a", "b", "c", longId, "unpaired-\uD800");
        for (int i = 0; i < 160; i++) {
            final String id = randomFrom(ids);
            if (randomBoolean()) {
                history.add(randomBoolean() ? new WalRecord(id, "{\"n\":" + i + "}", i, 1, 1) : WalRecord.deletion(id, i, 1, 1));
            } else {
                history.add(randomBoolean() ? new WalRecord(id, "{\"n\":" + i + "}") : WalRecord.deletion(id));
            }
        }
        assertLegacyMatches(history);
    }

    public void testLegacyOnlyAndLegacyAfterModern() throws Exception {
        assertLegacyMatches(List.of(new WalRecord("a", "{}"), WalRecord.deletion("a"), new WalRecord("b", "{}")));
        assertLegacyMatches(List.of(new WalRecord("a", "{}", 0, 1, 1), new WalRecord("a", "{\"later\":1}"), WalRecord.deletion("b")));
        assertLegacyMatches(List.of());
    }

    private void assertLegacyMatches(List<WalRecord> history) throws Exception {
        final var lastModern = new HashMap<String, Integer>();
        for (int i = 0; i < history.size(); i++) {
            if (history.get(i).hasSequenceIdentity()) {
                lastModern.put(history.get(i).id(), i);
            }
        }
        final var expected = new ArrayList<WalRecord>();
        for (int i = 0; i < history.size(); i++) {
            if (history.get(i).hasSequenceIdentity() == false && lastModern.getOrDefault(history.get(i).id(), -1) <= i) {
                expected.add(history.get(i));
            }
        }
        try (var spool = ReplaySpool.capture(createTempDir(), consumer -> {
            for (var record : history) {
                consumer.accept(record);
            }
        })) {
            final var actual = new ArrayList<WalRecord>();
            spool.forEachLegacy(actual::add);
            assertEquals(expected.size(), actual.size());
            for (int i = 0; i < expected.size(); i++) {
                assertEquals(expected.get(i).id(), actual.get(i).id());
                assertEquals(expected.get(i).source(), actual.get(i).source());
                assertEquals(expected.get(i).isDeletion(), actual.get(i).isDeletion());
            }
        }
    }

    public void testConflictAcrossSortPartitions() throws Exception {
        final var parent = createTempDir();
        final var failure = expectThrows(IOException.class, () -> ReplaySpool.capture(parent, consumer -> {
            for (int i = 0; i < 750_000; i++) {
                consumer.accept(new WalRecord("same", "{}", i, 1, 1));
            }
            consumer.accept(WalRecord.deletion("different", 0, 1, 1));
        }));
        assertTrue(failure.getMessage(), failure.getMessage().contains("share a sequence number"));
        try (var files = Files.list(parent)) {
            assertEquals(0, files.filter(path -> path.getFileName().toString().startsWith("wal-replay-")).count());
        }
    }

    public void testInterruptedRecoveryFilesAreReclaimed() throws Exception {
        final var parent = createTempDir();
        final var owned = parent.resolve("wal-replay");
        Files.createDirectories(owned.resolve("interrupted"));
        Files.writeString(owned.resolve("interrupted/records"), "stale");
        final var sentinel = parent.resolve("unrelated");
        Files.writeString(sentinel, "keep");
        try (var spool = ReplaySpool.captureExclusive(owned, consumer -> consumer.accept(new WalRecord("a", "{}", 0, 1, 1)))) {
            assertFalse(Files.exists(owned.resolve("interrupted")));
            try (var snapshot = spool.snapshot()) {
                assertEquals(0, snapshot.next().seqNo());
            }
        }
        assertEquals("keep", Files.readString(sentinel));
        try (var files = Files.list(owned)) {
            assertEquals(0, files.filter(path -> path.getFileName().toString().startsWith("wal-replay-")).count());
        }
    }

    public void testCorruptSpoolCannotSupplyAnOperation() throws Exception {
        final var parent = createTempDir();
        try (var spool = ReplaySpool.capture(parent, consumer -> consumer.accept(new WalRecord("a", "{}", 0, 1, 1)))) {
            final java.nio.file.Path file;
            try (var files = Files.walk(parent)) {
                file = files.filter(path -> path.getFileName().toString().equals("records")).findFirst().orElseThrow();
            }
            final byte[] bytes = Files.readAllBytes(file);
            bytes[bytes.length - Integer.BYTES - 1] = 'x';
            Files.write(file, bytes);
            try (var snapshot = spool.snapshot()) {
                final var failure = expectThrows(IOException.class, snapshot::next);
                assertTrue(failure.getMessage(), failure.getMessage().contains("checksum"));
            }
        }
    }

    public void testCorruptLegacySupersessionCannotCompleteRecovery() throws Exception {
        final var parent = createTempDir();
        try (var spool = ReplaySpool.capture(parent, consumer -> {
            consumer.accept(new WalRecord("a", "{}"));
            consumer.accept(new WalRecord("a", "{}", 0, 1, 1));
        })) {
            final java.nio.file.Path file;
            try (var files = Files.walk(parent)) {
                file = files.filter(path -> path.getFileName().toString().equals("skipped")).findFirst().orElseThrow();
            }
            Files.write(file, new byte[] { 0 });
            final var failure = expectThrows(IOException.class, () -> spool.forEachLegacy(record -> {}));
            assertTrue(failure.getMessage(), failure.getMessage().contains("checksum"));
        }
    }

    public void testReadFailureDiscardsTheOwnedSpool() throws Exception {
        final var parent = createTempDir();
        final var sentinel = parent.resolve("unrelated");
        Files.writeString(sentinel, "keep");
        expectThrows(IOException.class, () -> ReplaySpool.capture(parent, consumer -> {
            consumer.accept(new WalRecord("a", "{}", 0, 1, 1));
            throw new IOException("injected WAL read failure");
        }));
        assertEquals("keep", Files.readString(sentinel));
        try (var files = Files.list(parent)) {
            assertEquals(0, files.filter(path -> path.getFileName().toString().startsWith("wal-replay-")).count());
        }
    }
}
