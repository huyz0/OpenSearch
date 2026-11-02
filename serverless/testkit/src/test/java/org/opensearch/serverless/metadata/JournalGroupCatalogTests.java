/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.log.JournalConfiguration;
import org.opensearch.serverless.log.JournalGroupRecord;
import org.opensearch.serverless.log.JournalMembershipRecord;
import org.opensearch.serverless.log.LogCheckpoint;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Local conditional-register tests; not a real S3 durability qualification. */
public class JournalGroupCatalogTests extends OpenSearchTestCase {
    public void testReservationIsPermanentAndIndependentOfPeerOrder() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final UUID deployment = UUID.randomUUID();
            final var catalog = new JournalGroupCatalog(container, deployment);
            final var group = group(UUID.randomUUID());
            final var reserved = catalog.reserve(group);
            final var peers = new ArrayList<>(group.peers());
            Collections.reverse(peers);
            assertEquals(reserved, catalog.reserve(new JournalConfiguration.Group(group.id(), peers)));
            assertEquals(reserved, new JournalGroupCatalog(container, deployment).read(group.id()).orElseThrow());
            expectThrows(IOException.class, () -> catalog.reserve(group(group.id())));
            final var foreign = new JournalGroupCatalog(container, UUID.randomUUID());
            expectThrows(IOException.class, () -> foreign.reserve(group));
            expectThrows(IOException.class, () -> foreign.read(group.id()));
            assertEquals(reserved, catalog.read(group.id()).orElseThrow());
        }
    }

    public void testConflictingControllersCannotReserveIndependentVoters() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var context = new ThreadContext(Settings.EMPTY);
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final UUID deployment = UUID.randomUUID();
            final UUID identity = UUID.randomUUID();
            final var candidates = List.of(group(identity), group(identity));
            final var start = new CountDownLatch(1);
            final var pool = OpenSearchExecutors.newFixed(
                "reserve-groups",
                2,
                2,
                OpenSearchExecutors.daemonThreadFactory("reserve-groups"),
                context
            );
            try {
                final var futures = new ArrayList<Future<JournalGroupRecord>>();
                for (var candidate : candidates) {
                    futures.add(pool.submit(() -> {
                        start.await();
                        return new JournalGroupCatalog(container, deployment).reserve(candidate);
                    }));
                }
                start.countDown();
                JournalGroupRecord winner = null;
                int refused = 0;
                for (var future : futures) {
                    try {
                        final var result = future.get(10, TimeUnit.SECONDS);
                        assertNull(winner);
                        winner = result;
                    } catch (ExecutionException failure) {
                        assertTrue(failure.getCause() instanceof IOException);
                        refused++;
                    }
                }
                assertNotNull(winner);
                assertEquals(1, refused);
                assertEquals(winner, new JournalGroupCatalog(container, deployment).read(identity).orElseThrow());
            } finally {
                start.countDown();
                pool.shutdownNow();
                assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    public void testLostReservationReplyIsReconciledWithoutRebinding() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var real = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var fail = new AtomicBoolean(true);
            final var wrapper = new FilterBlobContainer(real) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    throw new AssertionError("catalog must not enumerate child containers");
                }

                @Override
                public Optional<BlobRegister> readRegister(String name) throws IOException {
                    return real.readRegister(name);
                }

                @Override
                public BlobRegisterCasResult compareAndSwapRegister(String name, long expected, BytesReference value) throws IOException {
                    final var result = real.compareAndSwapRegister(name, expected, value);
                    if (name.startsWith("group-") && result.applied() && fail.compareAndSet(true, false)) {
                        throw new IOException("injected lost reservation reply after persistence");
                    }
                    return result;
                }
            };
            final UUID deployment = UUID.randomUUID();
            final var group = group(UUID.randomUUID());
            expectThrows(IOException.class, () -> new JournalGroupCatalog(wrapper, deployment).reserve(group));
            final var expected = new JournalGroupRecord(deployment, group, null);
            assertEquals(expected, new JournalGroupCatalog(real, deployment).reserve(group));
            assertEquals(expected, new JournalGroupCatalog(wrapper, deployment).read(group.id()).orElseThrow());
        }
    }

    public void testMissingOrActiveReservationRefusesBootstrapBeforeTouchingTheVolume() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final UUID deployment = UUID.randomUUID();
            final var catalog = new JournalGroupCatalog(container, deployment);
            catalog.reserve(group(UUID.randomUUID()));
            final var group = group(UUID.randomUUID());
            final var volume = createTempDir();
            final List<java.nio.file.Path> originalFiles;
            try (var entries = Files.list(volume)) {
                originalFiles = entries.sorted().toList();
            }
            final var configuration = new JournalConfiguration(
                deployment,
                group.peers().getFirst().id(),
                volume,
                List.of(group),
                true,
                JournalConfiguration.Limits.defaults()
            );
            expectThrows(IOException.class, () -> catalog.openJournal(configuration, null, null, () -> Long.MAX_VALUE));
            final var reserved = catalog.reserve(group);
            final var active = reserved.activate(new LogCheckpoint(JournalGroupRecord.controlIdentity(deployment, group.id()), 2, 1));
            final String name = "group-" + group.id();
            final var register = container.readRegister(name).orElseThrow();
            assertTrue(container.compareAndSwapRegister(name, register.generation(), new BytesArray(active.encode())).applied());
            expectThrows(IOException.class, () -> catalog.openJournal(configuration, null, null, () -> Long.MAX_VALUE));
            try (var entries = Files.list(volume)) {
                assertEquals(originalFiles, entries.sorted().toList());
            }
            assertEquals(active, catalog.reserve(group));
            expectThrows(IOException.class, () -> catalog.reserve(group(group.id())));
        }
    }

    public void testCorruptAndMisaddressedRecordsNeverAuthorizeStartup() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final UUID deployment = UUID.randomUUID();
            final var catalog = new JournalGroupCatalog(container, deployment);
            final var valid = catalog.reserve(group(UUID.randomUUID()));
            final UUID wrongKey = UUID.randomUUID();
            assertTrue(container.createRegisterIfAbsent("group-" + wrongKey, new BytesArray(valid.encode())).applied());
            expectThrows(IOException.class, () -> catalog.read(wrongKey));
            final UUID corruptKey = UUID.randomUUID();
            assertTrue(container.createRegisterIfAbsent("group-" + corruptKey, new BytesArray(new byte[] { 1, 2 })).applied());
            expectThrows(IOException.class, () -> catalog.read(corruptKey));
            assertEquals(valid, catalog.read(valid.initialPlacement().id()).orElseThrow());
        }
    }

    public void testReplacementIntentSurvivesRestartAndBlocksStaticStartupBeforeVolumeAccess() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final UUID deployment = UUID.randomUUID();
            final var catalog = new JournalGroupCatalog(container, deployment);
            final var group = group(UUID.randomUUID());
            catalog.reserve(group);
            expectThrows(IOException.class, () -> catalog.prepareReplacement(group.id(), 0, UUID.randomUUID(), replacement(group)));
            activate(container, catalog, group);
            final var initial = catalog.membership(group.id());
            assertNull(initial.pending());
            final UUID operation = UUID.randomUUID();
            final var target = replacement(group);
            final var pending = catalog.prepareReplacement(group.id(), 0, operation, target);
            final var restarted = new JournalGroupCatalog(container, deployment);
            assertEquals(pending, restarted.membership(group.id()));
            assertEquals(pending, restarted.prepareReplacement(group.id(), 0, operation, target));
            assertEquals(initial.genesis(), restarted.reserve(group));
            expectThrows(IOException.class, () -> restarted.prepareReplacement(group.id(), 0, UUID.randomUUID(), target));
            expectThrows(IOException.class, () -> restarted.prepareReplacement(group.id(), 1, operation, target));
            final var volume = createTempDir();
            final List<java.nio.file.Path> originalFiles;
            try (var files = Files.list(volume)) {
                originalFiles = files.sorted().toList();
            }
            final var configuration = new JournalConfiguration(
                deployment,
                group.peers().getFirst().id(),
                volume,
                List.of(group),
                false,
                JournalConfiguration.Limits.defaults()
            );
            final var failure = expectThrows(
                IOException.class,
                () -> restarted.openJournal(configuration, null, null, () -> Long.MAX_VALUE)
            );
            assertTrue(failure.getMessage(), failure.getMessage().contains("replacement reconciliation"));
            try (var files = Files.list(volume)) {
                assertEquals(originalFiles, files.sorted().toList());
            }
        }
    }

    public void testLostReplacementReplyReconcilesTheSameNonceAndPlacement() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var real = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final UUID deployment = UUID.randomUUID();
            final var catalog = new JournalGroupCatalog(real, deployment);
            final var group = group(UUID.randomUUID());
            activate(real, catalog, group);
            final var fail = new AtomicBoolean(true);
            final var wrapper = new FilterBlobContainer(real) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    throw new AssertionError("membership must use direct bounded register lookups");
                }

                @Override
                public Optional<BlobRegister> readRegister(String name) throws IOException {
                    return real.readRegister(name);
                }

                @Override
                public BlobRegisterCasResult compareAndSwapRegister(String name, long expected, BytesReference value) throws IOException {
                    final var result = real.compareAndSwapRegister(name, expected, value);
                    if (name.startsWith("membership-") && result.applied()) {
                        try (var in = value.streamInput()) {
                            if (JournalMembershipRecord.read(in).pending() != null && fail.compareAndSet(true, false)) {
                                throw new IOException("lost reply after replacement intent persisted");
                            }
                        }
                    }
                    return result;
                }
            };
            final UUID operation = UUID.randomUUID();
            final var target = replacement(group);
            final var lossy = new JournalGroupCatalog(wrapper, deployment);
            expectThrows(IOException.class, () -> lossy.prepareReplacement(group.id(), 0, operation, target));
            final var observed = new JournalGroupCatalog(real, deployment).membership(group.id());
            assertEquals(operation, observed.pending().id());
            assertEquals(observed, lossy.prepareReplacement(group.id(), 0, operation, target));
            assertEquals(observed.genesis(), catalog.read(group.id()).orElseThrow());
        }
    }

    public void testCompetingReplacementControllersHaveOnlyOneDurableWinner() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final UUID deployment = UUID.randomUUID();
            final var catalog = new JournalGroupCatalog(container, deployment);
            final var group = group(UUID.randomUUID());
            activate(container, catalog, group);
            final var context = new ThreadContext(Settings.EMPTY);
            final var pool = OpenSearchExecutors.newFixed(
                "replace-voter",
                2,
                2,
                OpenSearchExecutors.daemonThreadFactory("replace-voter"),
                context
            );
            final var start = new CountDownLatch(1);
            try {
                final var futures = new ArrayList<Future<JournalMembershipRecord>>();
                for (int i = 0; i < 2; i++) {
                    final UUID operation = UUID.randomUUID();
                    final var target = replacement(group);
                    futures.add(pool.submit(() -> {
                        start.await();
                        return new JournalGroupCatalog(container, deployment).prepareReplacement(group.id(), 0, operation, target);
                    }));
                }
                start.countDown();
                JournalMembershipRecord winner = null;
                int refused = 0;
                for (var future : futures) {
                    try {
                        final var result = future.get(10, TimeUnit.SECONDS);
                        assertNull(winner);
                        winner = result;
                    } catch (ExecutionException failure) {
                        assertTrue(failure.getCause() instanceof IOException);
                        refused++;
                    }
                }
                assertNotNull(winner);
                assertEquals(1, refused);
                assertEquals(winner, catalog.membership(group.id()));
            } finally {
                start.countDown();
                pool.shutdownNow();
                assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    public void testCorruptOrForeignMembershipCannotAuthorizeStaticRecovery() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final UUID deployment = UUID.randomUUID();
            final var catalog = new JournalGroupCatalog(container, deployment);
            final var group = group(UUID.randomUUID());
            activate(container, catalog, group);
            final var other = group(UUID.randomUUID());
            activate(container, catalog, other);
            final var foreign = catalog.membership(other.id());
            final String key = "membership-" + group.id();
            final var originalHead = container.readRegister(key).orElseThrow();
            assertTrue(container.compareAndSwapRegister(key, originalHead.generation(), new BytesArray(foreign.encode())).applied());
            expectThrows(IOException.class, () -> catalog.membership(group.id()));
            expectThrows(IOException.class, () -> catalog.prepareReplacement(group.id(), 0, UUID.randomUUID(), replacement(group)));
            final var old = container.readRegister(key).orElseThrow();
            assertTrue(container.compareAndSwapRegister(key, old.generation(), new BytesArray(new byte[] { 1 })).applied());
            expectThrows(IOException.class, () -> catalog.membership(group.id()));
        }
    }

    public void testMissingMembershipHeadNeverRecreatesRevisionZero() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var catalog = new JournalGroupCatalog(container, UUID.randomUUID());
            final var group = group(UUID.randomUUID());
            activate(container, catalog, group);
            final var target = replacement(group);
            catalog.prepareReplacement(group.id(), 0, UUID.randomUUID(), target);
            final String key = "membership-" + group.id();
            final var head = container.readRegister(key).orElseThrow();
            assertTrue(container.deleteRegisterIfUnchanged(key, head.generation()));
            expectThrows(IOException.class, () -> catalog.membership(group.id()));
            expectThrows(IOException.class, () -> catalog.prepareReplacement(group.id(), 0, UUID.randomUUID(), target));
            assertTrue(container.readRegister(key).isEmpty());
        }
    }

    public void testRetiredReplacementIdentityCannotBeReusedByALaterIntent() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var catalog = new JournalGroupCatalog(container, UUID.randomUUID());
            final var group = group(UUID.randomUUID());
            activate(container, catalog, group);
            final var first = catalog.prepareReplacement(group.id(), 0, UUID.randomUUID(), replacement(group));
            // Model completed head publication to test the register protocol only. There is no
            // production completion API until native catch-up and reconfiguration are integrated.
            final var completed = new JournalMembershipRecord(first.genesis(), 1, first.pending().target(), null);
            final String key = "membership-" + group.id();
            var old = container.readRegister(key).orElseThrow();
            assertTrue(container.compareAndSwapRegister(key, old.generation(), new BytesArray(completed.encode())).applied());
            final var firstAdded = first.pending()
                .target()
                .peers()
                .stream()
                .filter(peer -> group.peers().stream().noneMatch(original -> original.id().equals(peer.id())))
                .findFirst()
                .orElseThrow();
            final var secondPeers = new ArrayList<>(completed.voters().peers());
            secondPeers.remove(firstAdded);
            secondPeers.add(new JournalConfiguration.Peer(UUID.randomUUID(), "second-replacement", 9600, firstAdded.availabilityZone()));
            final var second = catalog.prepareReplacement(
                group.id(),
                1,
                UUID.randomUUID(),
                new JournalConfiguration.Group(group.id(), secondPeers)
            );
            final var completedAgain = new JournalMembershipRecord(second.genesis(), 2, second.pending().target(), null);
            old = container.readRegister(key).orElseThrow();
            assertTrue(container.compareAndSwapRegister(key, old.generation(), new BytesArray(completedAgain.encode())).applied());
            expectThrows(IOException.class, () -> catalog.prepareReplacement(group.id(), 2, UUID.randomUUID(), first.pending().target()));
            expectThrows(IOException.class, () -> catalog.prepareReplacement(group.id(), 2, UUID.randomUUID(), group));
            assertEquals(completedAgain, catalog.membership(group.id()));
        }
    }

    public void testMissingPendingVoterReservationCannotBeReconstructed() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var catalog = new JournalGroupCatalog(container, UUID.randomUUID());
            final var group = group(UUID.randomUUID());
            activate(container, catalog, group);
            final UUID operation = UUID.randomUUID();
            final var target = replacement(group);
            final var pending = catalog.prepareReplacement(group.id(), 0, operation, target);
            final var added = target.peers()
                .stream()
                .filter(peer -> group.peers().stream().noneMatch(original -> original.id().equals(peer.id())))
                .findFirst()
                .orElseThrow();
            final String key = "replacement-" + group.id() + "-" + added.id();
            final var reservation = container.readRegister(key).orElseThrow();
            assertTrue(container.deleteRegisterIfUnchanged(key, reservation.generation()));
            expectThrows(IOException.class, () -> catalog.prepareReplacement(group.id(), 0, operation, target));
            assertTrue(container.readRegister(key).isEmpty());
            expectThrows(IOException.class, () -> catalog.membership(group.id()));
            try (var in = container.readRegister("membership-" + group.id()).orElseThrow().value().streamInput()) {
                assertEquals(pending, JournalMembershipRecord.read(in));
            }
        }
    }

    public void testCompletedHeadRequiresExactPermanentBindingsForNewVoters() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var catalog = new JournalGroupCatalog(container, UUID.randomUUID());
            final var group = group(UUID.randomUUID());
            activate(container, catalog, group);
            final var pending = catalog.prepareReplacement(group.id(), 0, UUID.randomUUID(), replacement(group));
            final var completed = new JournalMembershipRecord(pending.genesis(), 1, pending.pending().target(), null);
            final String key = "membership-" + group.id();
            var head = container.readRegister(key).orElseThrow();
            assertTrue(container.compareAndSwapRegister(key, head.generation(), new BytesArray(completed.encode())).applied());
            assertEquals(completed, catalog.membership(group.id()));
            final var added = completed.voters()
                .peers()
                .stream()
                .filter(peer -> group.peers().stream().noneMatch(original -> original.id().equals(peer.id())))
                .findFirst()
                .orElseThrow();
            final var rebound = new ArrayList<>(completed.voters().peers());
            rebound.remove(added);
            rebound.add(new JournalConfiguration.Peer(added.id(), "rebound.invalid", added.port(), added.availabilityZone()));
            final var changed = new JournalMembershipRecord(
                pending.genesis(),
                1,
                new JournalConfiguration.Group(group.id(), rebound),
                null
            );
            head = container.readRegister(key).orElseThrow();
            assertTrue(container.compareAndSwapRegister(key, head.generation(), new BytesArray(changed.encode())).applied());
            expectThrows(IOException.class, () -> catalog.membership(group.id()));
            head = container.readRegister(key).orElseThrow();
            assertTrue(container.compareAndSwapRegister(key, head.generation(), new BytesArray(completed.encode())).applied());
            final String voterKey = "replacement-" + group.id() + "-" + added.id();
            final var reservation = container.readRegister(voterKey).orElseThrow();
            assertTrue(container.deleteRegisterIfUnchanged(voterKey, reservation.generation()));
            expectThrows(IOException.class, () -> catalog.membership(group.id()));
            expectThrows(
                IOException.class,
                () -> catalog.prepareReplacement(group.id(), 1, UUID.randomUUID(), replacement(completed.voters()))
            );
            assertTrue(container.readRegister(voterKey).isEmpty());
        }
    }

    public void testOperationNonceCannotBeReusedAfterCompletionOrMissingReservation() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var catalog = new JournalGroupCatalog(container, UUID.randomUUID());
            final var group = group(UUID.randomUUID());
            activate(container, catalog, group);
            final var first = catalog.prepareReplacement(group.id(), 0, UUID.randomUUID(), replacement(group));
            final String operationKey = "transition-" + group.id() + "-" + first.pending().id();
            final var reservation = container.readRegister(operationKey).orElseThrow();
            assertTrue(container.deleteRegisterIfUnchanged(operationKey, reservation.generation()));
            expectThrows(IOException.class, () -> catalog.membership(group.id()));
            expectThrows(
                IOException.class,
                () -> catalog.prepareReplacement(group.id(), 0, first.pending().id(), first.pending().target())
            );
            assertTrue(container.readRegister(operationKey).isEmpty());
            assertTrue(container.createRegisterIfAbsent(operationKey, new BytesArray(first.encode())).applied());
            final var completed = new JournalMembershipRecord(first.genesis(), 1, first.pending().target(), null);
            final String key = "membership-" + group.id();
            final var head = container.readRegister(key).orElseThrow();
            assertTrue(container.compareAndSwapRegister(key, head.generation(), new BytesArray(completed.encode())).applied());
            expectThrows(
                IOException.class,
                () -> catalog.prepareReplacement(group.id(), 1, first.pending().id(), replacement(completed.voters()))
            );
            assertEquals(completed, catalog.membership(group.id()));
        }
    }

    private static void activate(BlobContainer container, JournalGroupCatalog catalog, JournalConfiguration.Group group)
        throws IOException {
        final var reserved = catalog.reserve(group);
        catalog.reserveProtocol(group, JournalConfiguration.Limits.defaults());
        final var active = reserved.activate(
            new LogCheckpoint(JournalGroupRecord.controlIdentity(reserved.deploymentId(), group.id()), 2, 1)
        );
        assertTrue(
            container.createRegisterIfAbsent("membership-" + group.id(), new BytesArray(JournalMembershipRecord.initial(active).encode()))
                .applied()
        );
        final String name = "group-" + group.id();
        final var old = container.readRegister(name).orElseThrow();
        assertTrue(container.compareAndSwapRegister(name, old.generation(), new BytesArray(active.encode())).applied());
    }

    private static JournalConfiguration.Group replacement(JournalConfiguration.Group group) {
        final var peers = new ArrayList<>(group.peers());
        final var removed = peers.removeLast();
        final UUID identity = UUID.randomUUID();
        peers.add(new JournalConfiguration.Peer(identity, "replacement-" + identity, 9500, removed.availabilityZone()));
        return new JournalConfiguration.Group(group.id(), peers);
    }

    private static JournalConfiguration.Group group(UUID id) {
        final var peers = new ArrayList<JournalConfiguration.Peer>();
        for (int i = 0; i < 3; i++) {
            peers.add(new JournalConfiguration.Peer(UUID.randomUUID(), "127.0.0.1", 9000 + i, "az-" + i));
        }
        return new JournalConfiguration.Group(id, peers);
    }
}
