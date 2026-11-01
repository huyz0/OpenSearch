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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@SuppressFileSystems("*")
public class JournalStorageTests extends OpenSearchTestCase {
    public void testBootstrapAndRecoveryBindEveryGroup() throws Exception {
        final var configuration = configuration();
        try (var storage = JournalStorage.open(configuration)) {
            expectThrows(IOException.class, () -> JournalDiskIdentity.discover(configuration.directory()));
            for (var group : configuration.groups()) {
                initialize(storage, group.id());
            }
        }
        final var reordered = new ArrayList<>(configuration.groups());
        Collections.reverse(reordered);
        try (var storage = JournalStorage.open(copy(configuration, reordered, false))) {
            for (var group : reordered) {
                storage.initialize(group.id());
                storage.flush(group.id());
            }
            expectThrows(IOException.class, () -> storage.groupDirectory(UUID.randomUUID()));
        }
        expectThrows(IOException.class, () -> JournalStorage.open(configuration));
    }

    public void testMissingVoteOrGroupIsNeverFormattedOnRecovery() throws Exception {
        final var configuration = configuration();
        try (var storage = JournalStorage.open(configuration)) {
            for (var group : configuration.groups()) {
                initialize(storage, group.id());
            }
        }
        final Path vote = configuration.directory().resolve(configuration.groups().getFirst().id().toString()).resolve("current/raft-meta");
        Files.delete(vote);
        expectThrows(IOException.class, () -> JournalStorage.open(copy(configuration, configuration.groups(), false)));
        assertFalse(Files.exists(vote));
        assertEquals(configuration.localVoterId(), JournalDiskIdentity.discover(configuration.directory()));
    }

    public void testReplacementVolumeCannotReuseVoterAndFailedOpenReleasesLock() throws Exception {
        final var configuration = configuration();
        final Path replacement = createTempDir();
        final var changed = new JournalConfiguration(
            configuration.deploymentId(),
            configuration.localVoterId(),
            replacement,
            configuration.groups(),
            true,
            configuration.limits()
        );
        expectThrows(IOException.class, () -> JournalStorage.open(changed));
        assertNotEquals(configuration.localVoterId(), JournalDiskIdentity.discover(replacement));
        assertFalse(Files.exists(replacement.resolve("journal-binding")));
    }

    public void testPartialBootstrapCannotRestartAsAnEmptyVoter() throws Exception {
        final var configuration = configuration();
        try (var ignored = JournalStorage.open(configuration)) {
            // Crash before any group initialized: explicit controller intervention is required.
        }
        expectThrows(IOException.class, () -> JournalStorage.open(copy(configuration, configuration.groups(), false)));
        expectThrows(IOException.class, () -> JournalStorage.open(configuration));
    }

    public void testCorruptBindingsAndChangedDeploymentFailClosed() throws Exception {
        final var configuration = configuration();
        try (var storage = JournalStorage.open(configuration)) {
            for (var group : configuration.groups()) {
                initialize(storage, group.id());
            }
        }
        final var recover = copy(configuration, configuration.groups(), false);
        final Path binding = configuration.directory().resolve("journal-binding");
        final byte[] original = Files.readAllBytes(binding);
        for (int i = 0; i < original.length; i++) {
            final byte[] corrupt = original.clone();
            corrupt[i] ^= 1;
            Files.write(binding, corrupt);
            expectThrows(IOException.class, () -> JournalStorage.open(recover));
        }
        Files.write(binding, original);
        final var changed = new JournalConfiguration(
            UUID.randomUUID(),
            recover.localVoterId(),
            recover.directory(),
            recover.groups(),
            false,
            recover.limits()
        );
        expectThrows(IOException.class, () -> JournalStorage.open(changed));
        try (var ignored = JournalStorage.open(recover)) {}
    }

    public void testRetirementPreservesHistoryAndOtherGroupsAndRejectsStaleAuthorization() throws Exception {
        final var configuration = configuration();
        try (var storage = JournalStorage.open(configuration)) {
            for (var group : configuration.groups()) {
                initialize(storage, group.id());
            }
            final UUID retiring = configuration.groups().getFirst().id();
            expectThrows(
                IOException.class,
                () -> JournalStorage.open(copy(configuration, configuration.groups(), false), Set.of(retiring))
            );
            assertTrue(Files.exists(configuration.directory().resolve(retiring.toString())));
            assertFalse(Files.exists(configuration.directory().resolve("retired")));
        }
        final UUID retired = configuration.groups().getFirst().id();
        final UUID active = configuration.groups().get(1).id();
        final Path source = configuration.directory().resolve(retired.toString());
        final byte[] original = Files.readAllBytes(source.resolve("journal-floor"));
        final var recover = copy(configuration, configuration.groups(), false);
        try (var storage = JournalStorage.open(recover, Set.of(retired))) {
            assertFalse(storage.isActiveGroup(retired));
            assertTrue(storage.isActiveGroup(active));
            expectThrows(IOException.class, () -> storage.groupDirectory(retired));
            storage.flush(active);
        }
        final Path target = configuration.directory().resolve("retired").resolve(retired.toString());
        assertFalse(Files.exists(source));
        assertArrayEquals(original, Files.readAllBytes(target.resolve("journal-floor")));
        expectThrows(IOException.class, () -> JournalStorage.open(recover));
        assertFalse(Files.exists(source));
        try (var storage = JournalStorage.open(recover, Set.of(retired))) {
            storage.initialize(active);
        }
    }

    public void testRetirementReconcilesAlreadyMovedHistoryBeforeStartingOtherGroups() throws Exception {
        final var configuration = configuration();
        try (var storage = JournalStorage.open(configuration)) {
            for (var group : configuration.groups()) {
                initialize(storage, group.id());
            }
        }
        final UUID retired = configuration.groups().getFirst().id();
        final Path archive = configuration.directory().resolve("retired");
        Files.createDirectory(archive);
        // Model a process dying after rename but before both parent directories were synchronized.
        Files.move(
            configuration.directory().resolve(retired.toString()),
            archive.resolve(retired.toString()),
            StandardCopyOption.ATOMIC_MOVE
        );
        try (var storage = JournalStorage.open(copy(configuration, configuration.groups(), false), Set.of(retired))) {
            assertFalse(storage.isActiveGroup(retired));
            storage.flush(configuration.groups().get(1).id());
        }
    }

    public void testDuplicateOrUnknownRetiredHistoryNeverGetsOverwritten() throws Exception {
        final var configuration = configuration();
        try (var storage = JournalStorage.open(configuration)) {
            for (var group : configuration.groups()) {
                initialize(storage, group.id());
            }
        }
        final UUID retired = configuration.groups().getFirst().id();
        final var recover = copy(configuration, configuration.groups(), false);
        expectThrows(IOException.class, () -> JournalStorage.open(recover, Set.of(UUID.randomUUID())));
        expectThrows(IOException.class, () -> JournalStorage.open(configuration, Set.of(retired)));
        final Path archive = configuration.directory().resolve("retired");
        Files.createDirectories(archive.resolve(retired.toString()));
        expectThrows(IOException.class, () -> JournalStorage.open(recover, Set.of(retired)));
        final Path source = configuration.directory().resolve(retired.toString());
        assertTrue(Files.exists(source.resolve("journal-initialized")));
        Files.delete(archive.resolve(retired.toString()));
        final Path unknown = archive.resolve(UUID.randomUUID().toString());
        Files.createDirectory(unknown);
        expectThrows(IOException.class, () -> JournalStorage.open(recover, Set.of(retired)));
        assertTrue(Files.exists(source.resolve("journal-initialized")));
    }

    public void testRetiredBindingCorruptionIsRefusedBeforeMovingAnyGroup() throws Exception {
        final var configuration = configuration();
        try (var storage = JournalStorage.open(configuration)) {
            for (var group : configuration.groups()) {
                initialize(storage, group.id());
            }
        }
        final UUID retired = configuration.groups().getFirst().id();
        final Path source = configuration.directory().resolve(retired.toString());
        Files.write(source.resolve("journal-initialized"), new byte[] { 1 });
        expectThrows(IOException.class, () -> JournalStorage.open(copy(configuration, configuration.groups(), false), Set.of(retired)));
        assertTrue(Files.exists(source));
        assertFalse(Files.exists(configuration.directory().resolve("retired")));
    }

    public void testNativePersistenceCannotExceedTheMetadataAllowance() throws Exception {
        final var configuration = configuration();
        final UUID group = configuration.groups().getFirst().id();
        final var limits = configuration.limits();
        final long maximumIndex = limits.maximumRetainedEntries() + 4L * limits.maximumPendingRequests() + 31;
        try (var storage = JournalStorage.open(configuration)) {
            initialize(storage, group);
            storage.recordFlushed(group, maximumIndex);
            final Path path = storage.groupDirectory(group).resolve("journal-floor");
            final byte[] before = Files.readAllBytes(path);
            expectThrows(IOException.class, () -> storage.recordFlushed(group, maximumIndex + 1));
            assertArrayEquals(before, Files.readAllBytes(path));
        }
    }

    public void testLearnerBindingSurvivesRecoveryAndCannotChangeItsTransport() throws Exception {
        final var original = configuration();
        final Path directory = createTempDir();
        final UUID identity = JournalDiskIdentity.discover(directory);
        final var joining = new JournalConfiguration.Peer(identity, "localhost", 11001, "az-a");
        final var learner = new JournalConfiguration(
            original.deploymentId(),
            identity,
            directory,
            original.groups(),
            true,
            original.limits(),
            joining
        );
        try (var storage = JournalStorage.open(learner)) {
            for (var group : learner.groups()) {
                initialize(storage, group.id());
                Files.move(
                    storage.groupDirectory(group.id()).resolve("current/raft-meta.conf"),
                    storage.groupDirectory(group.id()).resolve("current/raft-meta.bootstrap")
                );
            }
        }
        final var recovery = new JournalConfiguration(
            learner.deploymentId(),
            identity,
            directory,
            learner.groups(),
            false,
            learner.limits(),
            joining
        );
        try (var ignored = JournalStorage.open(recovery)) {
            // An initialized learner with an empty durable floor can resume before native add-listener.
        }
        final var rebound = new JournalConfiguration(
            learner.deploymentId(),
            identity,
            directory,
            learner.groups(),
            false,
            learner.limits(),
            new JournalConfiguration.Peer(identity, "localhost", 11002, "az-a")
        );
        expectThrows(IOException.class, () -> JournalStorage.open(rebound));
        Files.delete(directory.resolve(learner.groups().getFirst().id().toString()).resolve("current/raft-meta"));
        expectThrows(IOException.class, () -> JournalStorage.open(recovery));
    }

    public void testEmptyOriginalRecoveryNeedsAuthorityAndKeepsVotingMetadataMandatory() throws Exception {
        final var configuration = configuration();
        final UUID group = configuration.groups().getFirst().id();
        final Path current = configuration.directory().resolve(group.toString()).resolve("current");
        try (var storage = JournalStorage.open(configuration)) {
            for (var assigned : configuration.groups()) {
                initialize(storage, assigned.id());
            }
        }
        Files.move(current.resolve("raft-meta.conf"), current.resolve("raft-meta.bootstrap"));
        final var recovery = copy(configuration, configuration.groups(), false);
        expectThrows(IOException.class, () -> JournalStorage.open(recovery));
        try (var ignored = JournalStorage.open(recovery, Set.of(), Set.of(group))) {
            assertTrue(Files.exists(current.resolve("raft-meta")));
            assertFalse(Files.exists(current.resolve("raft-meta.conf")));
        }
        expectThrows(IOException.class, () -> JournalStorage.open(recovery, Set.of(), Set.of(UUID.randomUUID())));
        Files.delete(current.resolve("raft-meta"));
        expectThrows(IOException.class, () -> JournalStorage.open(recovery, Set.of(), Set.of(group)));
    }

    public void testAnEmptyBootstrapPermitCannotEraseAnEstablishedDurableBoundary() throws Exception {
        final var configuration = configuration();
        final UUID group = configuration.groups().getFirst().id();
        try (var storage = JournalStorage.open(configuration)) {
            for (var assigned : configuration.groups()) {
                initialize(storage, assigned.id());
            }
            storage.recordFlushed(group, 0);
        }
        final Path directory = configuration.directory().resolve(group.toString());
        Files.move(directory.resolve("current/raft-meta.conf"), directory.resolve("current/raft-meta.bootstrap"));
        final byte[] before = Files.readAllBytes(directory.resolve("journal-floor"));
        final var recovery = copy(configuration, configuration.groups(), false);
        expectThrows(IOException.class, () -> JournalStorage.open(recovery, Set.of(), Set.of(group)));
        assertArrayEquals(before, Files.readAllBytes(directory.resolve("journal-floor")));
    }

    private static void initialize(JournalStorage storage, UUID group) throws Exception {
        final Path root = storage.groupDirectory(group);
        Files.createDirectories(root.resolve("current"));
        Files.createDirectories(root.resolve("sm"));
        Files.write(root.resolve("current/raft-meta"), new byte[] { 1 });
        Files.write(root.resolve("current/raft-meta.conf"), new byte[] { 1 });
        storage.initialize(group);
    }

    private JournalConfiguration configuration() throws Exception {
        final Path directory = createTempDir();
        final UUID local = JournalDiskIdentity.discover(directory);
        final var peers = List.of(
            new JournalConfiguration.Peer(local, "localhost", 10001, "az-a"),
            new JournalConfiguration.Peer(UUID.randomUUID(), "localhost", 10002, "az-b"),
            new JournalConfiguration.Peer(UUID.randomUUID(), "localhost", 10003, "az-c")
        );
        return new JournalConfiguration(
            UUID.randomUUID(),
            local,
            directory,
            List.of(new JournalConfiguration.Group(UUID.randomUUID(), peers), new JournalConfiguration.Group(UUID.randomUUID(), peers)),
            true,
            JournalConfiguration.Limits.defaults()
        );
    }

    private static JournalConfiguration copy(
        JournalConfiguration configuration,
        List<JournalConfiguration.Group> groups,
        boolean bootstrap
    ) {
        return new JournalConfiguration(
            configuration.deploymentId(),
            configuration.localVoterId(),
            configuration.directory(),
            groups,
            bootstrap,
            configuration.limits()
        );
    }
}
