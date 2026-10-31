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

    private static JournalConfiguration.Group group(UUID id) {
        final var peers = new ArrayList<JournalConfiguration.Peer>();
        for (int i = 0; i < 3; i++) {
            peers.add(new JournalConfiguration.Peer(UUID.randomUUID(), "127.0.0.1", 9000 + i, "az-" + i));
        }
        return new JournalConfiguration.Group(id, peers);
    }
}
