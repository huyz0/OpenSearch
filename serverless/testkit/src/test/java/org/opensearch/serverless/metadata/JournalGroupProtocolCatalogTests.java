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
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.log.JournalConfiguration;
import org.opensearch.serverless.log.JournalGroupProtocol;
import org.opensearch.serverless.log.JournalGroupRecord;
import org.opensearch.serverless.log.LogCheckpoint;
import org.opensearch.serverless.log.ShardLogId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class JournalGroupProtocolCatalogTests extends OpenSearchTestCase {
    public void testConflictingWireAndHistoryLimitsRefuseBothStartupPathsBeforeVolumeMutation() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var deployment = UUID.randomUUID();
            final var catalog = new JournalGroupCatalog(container, deployment);
            final var directory = createTempDir();
            final var voter = UUID.randomUUID();
            final var group = group(voter);
            final var base = JournalConfiguration.Limits.defaults();
            final var fixed = catalog.reserveProtocol(group, base);
            final List<java.nio.file.Path> before;
            try (var files = Files.list(directory)) {
                before = files.sorted().toList();
            }
            for (var altered : List.of(
                limits(base.maximumPayloadBytes() / 2, 256, base.queueBytes(), 100_000),
                limits(base.maximumPayloadBytes(), 257, base.queueBytes(), 100_000),
                limits(base.maximumPayloadBytes(), 256, 2 * base.queueBytes(), 100_000),
                limits(base.maximumPayloadBytes(), 256, base.queueBytes(), 100_001)
            )) {
                expectThrows(IOException.class, () -> catalog.reserveProtocol(group, altered));
                final var configuration = new JournalConfiguration(deployment, voter, directory, List.of(group), true, altered);
                assertTrue(
                    expectThrows(IOException.class, () -> catalog.openJournal(configuration, null, null, () -> Long.MAX_VALUE)).getMessage()
                        .contains("protocol")
                );
                assertTrue(
                    expectThrows(IOException.class, () -> catalog.openAssignedJournal(configuration, null, null, () -> Long.MAX_VALUE))
                        .getMessage()
                        .contains("protocol")
                );
                try (var files = Files.list(directory)) {
                    assertEquals(before, files.sorted().toList());
                }
            }
            assertEquals(fixed, new JournalGroupCatalog(container, deployment).reserveProtocol(group, base));
        }
    }

    public void testActiveMissingProtocolCannotBeRecreatedOrAuthorizeLogicalAssignment() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var deployment = UUID.randomUUID();
            final var catalog = new JournalGroupCatalog(container, deployment);
            final var directory = createTempDir();
            final var voter = UUID.randomUUID();
            final var group = group(voter);
            final var reservation = catalog.reserve(group);
            final var active = reservation.activate(new LogCheckpoint(JournalGroupRecord.controlIdentity(deployment, group.id()), 2, 1));
            final var register = container.readRegister("group-" + group.id()).orElseThrow();
            assertTrue(
                container.compareAndSwapRegister("group-" + group.id(), register.generation(), new BytesArray(active.encode())).applied()
            );
            expectThrows(IOException.class, () -> catalog.reserveProtocol(group, JournalConfiguration.Limits.defaults()));
            expectThrows(
                IOException.class,
                () -> catalog.reserveStream(new ShardLogId(deployment, "index", 0, UUID.randomUUID()), group.id(), 8, 4096)
            );
            final var recovery = new JournalConfiguration(
                deployment,
                voter,
                directory,
                List.of(group),
                false,
                JournalConfiguration.Limits.defaults()
            );
            assertTrue(
                expectThrows(IOException.class, () -> catalog.openJournal(recovery, null, null, () -> Long.MAX_VALUE)).getMessage()
                    .contains("protocol")
            );
            assertTrue(
                expectThrows(IOException.class, () -> catalog.openAssignedJournal(recovery, null, null, () -> Long.MAX_VALUE)).getMessage()
                    .contains("protocol")
            );
            assertTrue(container.readRegister("protocol-" + group.id()).isEmpty());
            assertFalse(Files.exists(directory.resolve("journal-binding")));
        }
    }

    public void testLogicalStreamCannotExceedPhysicalFrameLimit() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var deployment = UUID.randomUUID();
            final var catalog = new JournalGroupCatalog(container, deployment);
            final var group = group(UUID.randomUUID());
            final var limits = JournalConfiguration.Limits.defaults();
            catalog.reserveProtocol(group, limits);
            final var reservation = catalog.read(group.id()).orElseThrow();
            final var active = reservation.activate(new LogCheckpoint(JournalGroupRecord.controlIdentity(deployment, group.id()), 2, 1));
            final var register = container.readRegister("group-" + group.id()).orElseThrow();
            assertTrue(
                container.compareAndSwapRegister("group-" + group.id(), register.generation(), new BytesArray(active.encode())).applied()
            );
            final var stream = new ShardLogId(deployment, "index", 0, UUID.randomUUID());
            expectThrows(IOException.class, () -> catalog.reserveStream(stream, group.id(), 8, limits.maximumPayloadBytes() + 1));
            assertTrue(catalog.readStream(stream).isEmpty());
            assertNotNull(catalog.reserveStream(stream, group.id(), 8, limits.maximumPayloadBytes()));
        }
    }

    public void testLostProtocolReplyReconcilesTheSameImmutableContract() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var real = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var deployment = UUID.randomUUID();
            final var once = new AtomicBoolean(true);
            final var wrapper = new FilterBlobContainer(real) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    throw new AssertionError("protocol reservation must not enumerate groups");
                }

                @Override
                public Optional<BlobRegister> readRegister(String name) throws IOException {
                    return real.readRegister(name);
                }

                @Override
                public BlobRegisterCasResult compareAndSwapRegister(String name, long expected, BytesReference value) throws IOException {
                    final var result = real.compareAndSwapRegister(name, expected, value);
                    if (name.startsWith("protocol-") && result.applied() && once.compareAndSet(true, false)) {
                        throw new IOException("lost protocol reservation reply");
                    }
                    return result;
                }
            };
            final var catalog = new JournalGroupCatalog(wrapper, deployment);
            final var group = group(UUID.randomUUID());
            final var limits = JournalConfiguration.Limits.defaults();
            expectThrows(IOException.class, () -> catalog.reserveProtocol(group, limits));
            assertEquals(
                new JournalGroupProtocol(deployment, group.id(), limits),
                new JournalGroupCatalog(real, deployment).reserveProtocol(group, limits)
            );
        }
    }

    public void testCorruptOrMisaddressedProtocolCannotBeOverwritten() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var deployment = UUID.randomUUID();
            final var catalog = new JournalGroupCatalog(container, deployment);
            final var group = group(UUID.randomUUID());
            final var limits = JournalConfiguration.Limits.defaults();
            final var valid = catalog.reserveProtocol(group, limits);
            final var key = "protocol-" + group.id();
            final byte[] corrupt = valid.encode();
            corrupt[10] ^= 1;
            for (var bytes : List.of(
                corrupt,
                new JournalGroupProtocol(deployment, UUID.randomUUID(), limits).encode(),
                new JournalGroupProtocol(UUID.randomUUID(), group.id(), limits).encode()
            )) {
                final var register = container.readRegister(key).orElseThrow();
                assertTrue(container.compareAndSwapRegister(key, register.generation(), new BytesArray(bytes)).applied());
                expectThrows(IOException.class, () -> catalog.reserveProtocol(group, limits));
                assertEquals(new BytesArray(bytes), container.readRegister(key).orElseThrow().value());
            }
        }
    }

    private static JournalConfiguration.Limits limits(int payload, int requests, int queue, long retained) {
        return new JournalConfiguration.Limits(
            4,
            payload,
            requests,
            128L * 1024 * 1024,
            queue,
            Math.max(16 * 1024 * 1024, queue),
            0,
            retained,
            1
        );
    }

    private static JournalConfiguration.Group group(UUID voter) {
        return new JournalConfiguration.Group(
            UUID.randomUUID(),
            List.of(
                new JournalConfiguration.Peer(voter, "127.0.0.1", 10001, "az-a"),
                new JournalConfiguration.Peer(UUID.randomUUID(), "127.0.0.1", 10002, "az-b"),
                new JournalConfiguration.Peer(UUID.randomUUID(), "127.0.0.1", 10003, "az-c")
            )
        );
    }
}
