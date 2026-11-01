/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.OpenSearchVotingMetadata;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageMetadata;
import org.apache.ratis.util.SizeInBytes;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Real native term/vote persistence, independent of the journal's log-entry checksums. */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class VotingMetadataIntegrityTests extends OpenSearchTestCase {
    public void testNativePersistenceRoundTripsEmptyAndRecordedVotes() throws Exception {
        final Path root = createTempDir();
        try (var storage = formatted(root)) {
            final Path file = root.resolve("current/raft-meta");
            final var empty = OpenSearchVotingMetadata.verify(file);
            assertEquals(0, empty.getTerm());
            assertEquals("", empty.getVotedFor().toString());
            storage.getMetadataFile().persist(RaftStorageMetadata.valueOf(7, RaftPeerId.valueOf("voter-µ")));
            final var recorded = OpenSearchVotingMetadata.verify(file);
            assertEquals(7, recorded.getTerm());
            assertEquals("voter-µ", recorded.getVotedFor().toString());
        }
    }

    public void testTermVoteAndChecksumCorruptionAreRefusedWithoutChangingBytes() throws Exception {
        for (var mutation : List.of(
            new String[] { "term=7", "term=6" },
            new String[] { "votedFor=voter-a", "votedFor=voter-c" },
            new String[] { "opensearchMetadataCrc32c=", "opensearchMetadataCrc32c=0" },
            new String[] { "opensearchMetadataVersion=1", "opensearchMetadataVersion=2" }
        )) {
            final Path file = recorded();
            final String before = Files.readString(file);
            final String corrupted = before.replace(mutation[0], mutation[1]);
            assertNotEquals(before, corrupted);
            Files.writeString(file, corrupted);
            expectThrows(IOException.class, () -> OpenSearchVotingMetadata.verify(file));
            assertEquals(corrupted, Files.readString(file));
        }
    }

    public void testMissingLegacyTruncatedAndOversizedVotingStateCannotBecomeAnEmptyVote() throws Exception {
        final Path file = recorded();
        final byte[] valid = Files.readAllBytes(file);
        for (var invalid : List.of(
            "term=7\nvotedFor=voter-a\n".getBytes(java.nio.charset.StandardCharsets.UTF_8),
            java.util.Arrays.copyOf(valid, valid.length / 2),
            new byte[4097]
        )) {
            Files.write(file, invalid);
            expectThrows(IOException.class, () -> OpenSearchVotingMetadata.verify(file));
            assertArrayEquals(invalid, Files.readAllBytes(file));
        }
        Files.delete(file);
        expectThrows(IOException.class, () -> OpenSearchVotingMetadata.verify(file));
        assertFalse(Files.exists(file));
    }

    public void testAnOversizedVoterCannotReplaceThePreviousPersistedVote() throws Exception {
        final Path root = createTempDir();
        try (var storage = formatted(root)) {
            storage.getMetadataFile().persist(RaftStorageMetadata.valueOf(7, RaftPeerId.valueOf("voter-a")));
            final Path file = root.resolve("current/raft-meta");
            final byte[] previous = Files.readAllBytes(file);
            expectThrows(
                IOException.class,
                () -> storage.getMetadataFile().persist(RaftStorageMetadata.valueOf(8, RaftPeerId.valueOf("v".repeat(1025))))
            );
            assertArrayEquals(previous, Files.readAllBytes(file));
            assertEquals(7, storage.getMetadataFile().getMetadata().getTerm());
        }
    }

    private Path recorded() throws Exception {
        final Path root = createTempDir();
        try (var storage = formatted(root)) {
            storage.getMetadataFile().persist(RaftStorageMetadata.valueOf(7, RaftPeerId.valueOf("voter-a")));
        }
        return root.resolve("current/raft-meta");
    }

    @SuppressForbidden(reason = "Pinned native storage builder accepts java.io.File")
    private static RaftStorage formatted(Path root) throws IOException {
        final var storage = RaftStorage.newBuilder()
            .setDirectory(root.toFile())
            .setOption(RaftStorage.StartupOption.FORMAT)
            .setLogCorruptionPolicy(RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION)
            .setStorageFreeSpaceMin(SizeInBytes.valueOf(0))
            .build();
        try {
            storage.initialize();
            return storage;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                storage.close();
            } catch (IOException close) {
                failure.addSuppressed(close);
            }
            throw failure;
        }
    }
}
