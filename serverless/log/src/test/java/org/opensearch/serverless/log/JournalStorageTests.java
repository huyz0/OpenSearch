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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
