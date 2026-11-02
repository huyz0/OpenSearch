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
import org.opensearch.serverless.log.JournalGroupRecord;
import org.opensearch.serverless.log.LogCheckpoint;
import org.opensearch.serverless.log.LogicalStreamRecord;
import org.opensearch.serverless.log.ShardLogId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LogicalStreamCatalogTests extends OpenSearchTestCase {
    public void testAssignmentLimitsAndActivationNonceCannotBeRebound() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var deployment = UUID.randomUUID();
            final var catalog = new JournalGroupCatalog(container, deployment);
            final var first = activate(catalog, container, deployment);
            final var second = activate(catalog, container, deployment);
            final var stream = stream(deployment);
            final var expected = catalog.reserveStream(stream, first, 8, 4096);
            assertEquals(expected, catalog.reserveStream(stream, first, 8, 4096));
            assertEquals(expected, new JournalGroupCatalog(container, deployment).readStream(stream).orElseThrow());
            expectThrows(IOException.class, () -> catalog.reserveStream(stream, second, 8, 4096));
            expectThrows(IOException.class, () -> catalog.reserveStream(stream, first, 9, 4096));
            expectThrows(IOException.class, () -> catalog.reserveStream(stream, first, 8, 8192));
            final var otherShard = new ShardLogId(deployment, stream.indexUuid(), 1, stream.activationId());
            expectThrows(IOException.class, () -> catalog.reserveStream(otherShard, first, 8, 4096));
            expectThrows(IOException.class, () -> catalog.readStream(otherShard));
            assertEquals(expected, catalog.readStream(stream).orElseThrow());
        }
    }

    public void testConcurrentDifferentGroupReservationsHaveOnePermanentWinner() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var deployment = UUID.randomUUID();
            final var catalog = new JournalGroupCatalog(container, deployment);
            final var first = activate(catalog, container, deployment);
            final var second = activate(catalog, container, deployment);
            final var stream = stream(deployment);
            final var start = new CountDownLatch(1);
            final var pool = Executors.newFixedThreadPool(2);
            try {
                final var attempts = List.of(pool.submit(() -> {
                    start.await();
                    return catalog.reserveStream(stream, first, 8, 4096);
                }), pool.submit(() -> {
                    start.await();
                    return catalog.reserveStream(stream, second, 8, 4096);
                }));
                start.countDown();
                LogicalStreamRecord winner = null;
                int refused = 0;
                for (var attempt : attempts) {
                    try {
                        final var accepted = attempt.get(10, TimeUnit.SECONDS);
                        assertNull(winner);
                        winner = accepted;
                    } catch (ExecutionException failure) {
                        assertTrue(failure.getCause() instanceof IOException);
                        refused++;
                    }
                }
                assertNotNull(winner);
                assertEquals(1, refused);
                assertEquals(winner, new JournalGroupCatalog(container, deployment).readStream(stream).orElseThrow());
            } finally {
                start.countDown();
                pool.shutdownNow();
                assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    public void testLostReplyIsReconciledWithoutChangingAssignmentOrLimits() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var real = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var deployment = UUID.randomUUID();
            final var catalog = new JournalGroupCatalog(real, deployment);
            final var group = activate(catalog, real, deployment);
            final var stream = stream(deployment);
            final var once = new AtomicBoolean(true);
            final var wrapper = new FilterBlobContainer(real) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    throw new AssertionError("logical assignment must not enumerate child containers");
                }

                @Override
                public Optional<BlobRegister> readRegister(String name) throws IOException {
                    return real.readRegister(name);
                }

                @Override
                public BlobRegisterCasResult compareAndSwapRegister(String name, long expected, BytesReference value) throws IOException {
                    final var result = real.compareAndSwapRegister(name, expected, value);
                    if (name.startsWith("stream-") && result.applied() && once.compareAndSet(true, false)) {
                        throw new IOException("lost reply after durable assignment");
                    }
                    return result;
                }
            };
            final var uncertain = new JournalGroupCatalog(wrapper, deployment);
            expectThrows(IOException.class, () -> uncertain.reserveStream(stream, group, 8, 4096));
            final var recovered = uncertain.readStream(stream).orElseThrow();
            assertEquals(recovered, catalog.reserveStream(stream, group, 8, 4096));
            expectThrows(IOException.class, () -> catalog.reserveStream(stream, group, 9, 4096));
            assertEquals(8, recovered.maximumOutcomes());
        }
    }

    public void testMissingInactiveForeignAndCorruptHistoryCannotAuthorizeAssignment() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var deployment = UUID.randomUUID();
            final var catalog = new JournalGroupCatalog(container, deployment);
            final var inactive = group();
            catalog.reserve(inactive);
            final var stream = stream(deployment);
            assertTrue(catalog.readStream(stream).isEmpty());
            expectThrows(IOException.class, () -> catalog.reserveStream(stream, UUID.randomUUID(), 8, 4096));
            expectThrows(IOException.class, () -> catalog.reserveStream(stream, inactive.id(), 8, 4096));
            expectThrows(IOException.class, () -> catalog.reserveStream(stream(UUID.randomUUID()), inactive.id(), 8, 4096));
            final var active = activate(catalog, container, deployment);
            final var record = catalog.reserveStream(stream, active, 8, 4096);
            final byte[] corrupt = record.encode();
            corrupt[10] ^= 1;
            final var name = "stream-" + stream.activationId();
            final var register = container.readRegister(name).orElseThrow();
            assertTrue(container.compareAndSwapRegister(name, register.generation(), new BytesArray(corrupt)).applied());
            expectThrows(IOException.class, () -> catalog.readStream(stream));
            expectThrows(IOException.class, () -> catalog.reserveStream(stream, active, 8, 4096));
        }
    }

    // Codec/catalog unit fixture only; the native runtime suite separately verifies real activation.
    private static UUID activate(JournalGroupCatalog catalog, BlobContainer container, UUID deployment) throws IOException {
        final var group = group();
        final var reserved = catalog.reserve(group);
        catalog.reserveProtocol(group, JournalConfiguration.Limits.defaults());
        final var active = reserved.activate(new LogCheckpoint(JournalGroupRecord.controlIdentity(deployment, group.id()), 2, 1));
        final var name = "group-" + group.id();
        final var register = container.readRegister(name).orElseThrow();
        assertTrue(container.compareAndSwapRegister(name, register.generation(), new BytesArray(active.encode())).applied());
        return group.id();
    }

    private static ShardLogId stream(UUID deployment) {
        return new ShardLogId(deployment, "index-uuid", 0, UUID.randomUUID());
    }

    private static JournalConfiguration.Group group() {
        return new JournalConfiguration.Group(
            UUID.randomUUID(),
            List.of(
                new JournalConfiguration.Peer(UUID.randomUUID(), "127.0.0.1", 10001, "az-a"),
                new JournalConfiguration.Peer(UUID.randomUUID(), "127.0.0.1", 10002, "az-b"),
                new JournalConfiguration.Peer(UUID.randomUUID(), "127.0.0.1", 10003, "az-c")
            )
        );
    }
}
