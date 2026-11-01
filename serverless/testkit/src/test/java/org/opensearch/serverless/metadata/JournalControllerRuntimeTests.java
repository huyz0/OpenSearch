/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.log.JournalConfiguration;
import org.opensearch.serverless.log.JournalDiskIdentity;
import org.opensearch.serverless.log.OperationEnvelope;
import org.opensearch.serverless.log.ShardLogId;
import org.opensearch.serverless.shell.JournalController;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Automatic controller/native integration, using real sockets and local conditional metadata. */
@SuppressFileSystems("*")
@ThreadLeakLingering(linger = 70_000)
@ThreadLeakFilters(filters = JournalGroupCatalogRuntimeTests.GlobalTimerFilter.class)
public class JournalControllerRuntimeTests extends OpenSearchTestCase {
    public void testAutomaticActivationReplacementAndCatalogPollingBudget() throws Exception {
        final var fixture = fixture(2);
        final var controllers = new ArrayList<JournalController>();
        final var pools = new ArrayList<ThreadPool>();
        final var clock = new AtomicLong();
        final var reads = new AtomicInteger();
        final var failReads = new AtomicBoolean();
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var real = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var container = new FilterBlobContainer(real) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    throw new AssertionError("journal control must not enumerate children");
                }

                @Override
                public BlobRegisterCasResult compareAndSwapRegister(String name, long generation, BytesReference value) throws IOException {
                    return real.compareAndSwapRegister(name, generation, value);
                }

                @Override
                public BlobRegisterCasResult createRegisterIfAbsent(String name, BytesReference value) throws IOException {
                    return real.createRegisterIfAbsent(name, value);
                }

                @Override
                public Optional<BlobRegister> readRegister(String name) throws IOException {
                    reads.incrementAndGet();
                    if (failReads.get()) {
                        throw new IOException("injected catalog read outage");
                    }
                    return real.readRegister(name);
                }
            };
            try {
                for (var configuration : fixture.configurations()) {
                    final var pool = pool("journal-auto-" + pools.size(), 1);
                    pools.add(pool);
                    final var controller = controller(fixture, configuration, container, pool, clock);
                    controllers.add(controller);
                    controller.start();
                    controller.awaitOpened(); // Does not wait for the remaining hosts' activation.
                }
                final var groups = fixture.configurations().getFirst().groups();
                assertBusy(() -> {
                    for (var controller : controllers) {
                        for (var group : groups) {
                            assertEquals(JournalController.State.ACTIVE, controller.state(group.id()));
                        }
                    }
                }, 40, TimeUnit.SECONDS);
                final int steadyReads = reads.get();
                final long before = controllers.getFirst().reconciliationPasses();
                for (int i = 0; i < 10_000; i++) {
                    controllers.getFirst().requestReconcile();
                }
                assertBusy(() -> assertTrue(controllers.getFirst().reconciliationPasses() > before));
                assertEquals("native progress edges must not poll S3", steadyReads, reads.get());

                failReads.set(true);
                controllers.getFirst().requestCatalogRefresh();
                assertBusy(() -> {
                    for (var assigned : groups) {
                        assertEquals(JournalController.State.UNAVAILABLE, controllers.getFirst().state(assigned.id()));
                    }
                });
                assertTrue(controllers.getFirst().failure(groups.getFirst().id()).orElseThrow().contains("injected catalog read outage"));
                final int failedReads = reads.get();
                final long failedPass = controllers.getFirst().reconciliationPasses();
                for (int i = 0; i < 10_000; i++) {
                    controllers.getFirst().requestReconcile();
                }
                assertBusy(() -> assertTrue(controllers.getFirst().reconciliationPasses() > failedPass));
                assertEquals("metadata failures must honor backoff", failedReads, reads.get());
                failReads.set(false);
                clock.addAndGet(TimeUnit.SECONDS.toNanos(5) + 1);
                controllers.getFirst().requestReconcile();
                assertBusy(() -> assertEquals(JournalController.State.ACTIVE, controllers.getFirst().state(groups.getFirst().id())));

                final var group = groups.getFirst();
                final var oldLeader = leader(controllers, group.id());
                final var removedIndex = oldLeader == controllers.getFirst() ? 1 : 0;
                final var removed = fixture.configurations().get(removedIndex).localPeer();
                final Path directory = createTempDir();
                final JournalConfiguration.Peer joining;
                try (var port = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"))) {
                    joining = new JournalConfiguration.Peer(
                        JournalDiskIdentity.discover(directory),
                        "127.0.0.1",
                        port.getLocalPort(),
                        removed.availabilityZone()
                    );
                }
                final var target = new ArrayList<>(group.peers());
                target.remove(removed);
                target.add(joining);
                final var catalog = new JournalGroupCatalog(container, fixture.deployment());
                catalog.prepareReplacement(group.id(), 0, UUID.randomUUID(), new JournalConfiguration.Group(group.id(), target));
                // The same new disk can be a learner here and a genesis voter in a new group.
                // It must reserve only its own genesis assignment, not independently rebootstrap this one.
                final var cohort = fixture(1);
                final var companions = cohort.configurations()
                    .stream()
                    .filter(value -> value.localPeer().availabilityZone().equals(joining.availabilityZone()) == false)
                    .toList();
                final var mixedGroup = new JournalConfiguration.Group(
                    UUID.randomUUID(),
                    List.of(joining, companions.get(0).localPeer(), companions.get(1).localPeer())
                );
                final var learnerConfiguration = new JournalConfiguration(
                    fixture.deployment(),
                    joining.id(),
                    directory,
                    List.of(group, mixedGroup),
                    true,
                    fixture.configurations().getFirst().limits(),
                    joining
                );
                final var learnerPool = pool("journal-auto-learner", 1);
                pools.add(learnerPool);
                final var learner = controller(fixture, learnerConfiguration, container, learnerPool, clock);
                controllers.add(learner);
                learner.start();
                learner.awaitOpened();
                assertNotNull(catalog.read(mixedGroup.id()).orElseThrow());
                for (var companion : companions) {
                    final var companionPool = pool("journal-mixed-" + pools.size(), 1);
                    pools.add(companionPool);
                    final var companionConfiguration = new JournalConfiguration(
                        fixture.deployment(),
                        companion.localVoterId(),
                        companion.directory(),
                        List.of(mixedGroup),
                        true,
                        companion.limits()
                    );
                    final var companionController = controller(fixture, companionConfiguration, container, companionPool, clock);
                    controllers.add(companionController);
                    companionController.start();
                    companionController.awaitOpened();
                }
                controllers.forEach(JournalController::requestCatalogRefresh);
                assertBusy(() -> {
                    try {
                        assertEquals(1, catalog.membership(group.id()).revision());
                        assertNull(catalog.membership(group.id()).pending());
                        assertEquals(JournalController.State.ACTIVE, learner.state(group.id()));
                        assertEquals(JournalController.State.ACTIVE, learner.state(mixedGroup.id()));
                    } catch (IOException waiting) {
                        throw new AssertionError("waiting for automatic replacement", waiting);
                    }
                }, 40, TimeUnit.SECONDS);
                controllers.forEach(JournalController::requestCatalogRefresh);
                assertBusy(() -> assertEquals(JournalController.State.RETIRED, controllers.get(removedIndex).state(group.id())));
                final var stream = new ShardLogId(fixture.deployment(), "automatic-controller", 0, UUID.randomUUID());
                leader(controllers, group.id()).awaitOpened()
                    .append(group.id(), operation(stream, 0))
                    .toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
                oldLeader.close();
                // Only the replacement and one retained voter form this group's surviving quorum.
                final var surviving = leader(controllers, group.id()).awaitOpened();
                surviving.append(group.id(), operation(stream, 1)).toCompletableFuture().get(10, TimeUnit.SECONDS);
                assertEquals(2, surviving.scan(group.id(), -1, Set.of(stream), 100, 65536).operations().size());
                final var other = groups.get(1);
                final var otherStream = new ShardLogId(fixture.deployment(), "other-automatic-group", 0, UUID.randomUUID());
                leader(controllers, other.id()).awaitOpened()
                    .append(other.id(), operation(otherStream, 2))
                    .toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
                final var externallyClosed = controllers.get(removedIndex);
                externallyClosed.awaitOpened().close();
                externallyClosed.requestReconcile();
                assertBusy(() -> assertEquals(JournalController.State.UNAVAILABLE, externallyClosed.state(other.id())));
                assertTrue(externallyClosed.failure(other.id()).isPresent());
            } finally {
                close(controllers, pools);
            }
        }
    }

    public void testCloseDuringCatalogOpeningCannotStartNativeStorageLater() throws Exception {
        final var fixture = fixture(1);
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var pool = pool("journal-close-opening", 1);
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var real = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var container = new FilterBlobContainer(real) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    throw new AssertionError("no child enumeration");
                }

                @Override
                public BlobRegisterCasResult compareAndSwapRegister(String name, long generation, BytesReference value) throws IOException {
                    return real.compareAndSwapRegister(name, generation, value);
                }

                @Override
                public BlobRegisterCasResult createRegisterIfAbsent(String name, BytesReference value) throws IOException {
                    return real.createRegisterIfAbsent(name, value);
                }

                @Override
                public Optional<BlobRegister> readRegister(String name) throws IOException {
                    entered.countDown();
                    try {
                        if (release.await(10, TimeUnit.SECONDS) == false) {
                            throw new IOException("test did not release catalog opening");
                        }
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        throw new IOException(failure);
                    }
                    return real.readRegister(name);
                }
            };
            final var configuration = fixture.configurations().getFirst();
            try (var controller = controller(fixture, configuration, container, pool, new AtomicLong())) {
                controller.start();
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                controller.close();
                expectThrows(IOException.class, controller::awaitOpened);
                release.countDown();
                assertBusy(
                    () -> assertEquals(0, ((ThreadPoolExecutor) pool.executor(ServerlessNode.JOURNAL_CONTROLLER_POOL)).getActiveCount())
                );
                assertFalse(Files.exists(configuration.directory().resolve("journal-binding")));
                assertEquals(configuration.localVoterId(), JournalDiskIdentity.discover(configuration.directory()));
                assertEquals(JournalController.State.CLOSED, controller.state(configuration.groups().getFirst().id()));
            } finally {
                release.countDown();
            }
        } finally {
            release.countDown();
            ThreadPool.terminate(pool, 10, TimeUnit.SECONDS);
        }
    }

    public void testCloseDuringNativeOpeningClosesTheLateServerAndKeepsItsVolumeQuarantined() throws Exception {
        final var fixture = fixture(1);
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var pool = pool("journal-close-native-opening", 1);
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var catalog = new JournalGroupCatalog(
                store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath())),
                fixture.deployment()
            );
            final var configuration = fixture.configurations().getFirst();
            try (var controller = new JournalController(configuration, catalog, fixture.key(), fixture.trust(), () -> {
                entered.countDown();
                try {
                    if (release.await(10, TimeUnit.SECONDS) == false) {
                        throw new IOException("test did not release native opening");
                    }
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IOException(failure);
                }
                return Long.MAX_VALUE;
            }, pool, System::nanoTime)) {
                controller.start();
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                controller.close();
                expectThrows(IOException.class, controller::awaitOpened);
                release.countDown();
                assertBusy(
                    () -> assertEquals(0, ((ThreadPoolExecutor) pool.executor(ServerlessNode.JOURNAL_CONTROLLER_POOL)).getActiveCount()),
                    20,
                    TimeUnit.SECONDS
                );
                assertTrue(controller.journal().isEmpty());
                // Native startup was attempted: only process exit permits this identity to reopen.
                expectThrows(IOException.class, () -> JournalDiskIdentity.discover(configuration.directory()));
                try (var socket = new ServerSocket(configuration.localPeer().port(), 0, InetAddress.getByName("127.0.0.1"))) {
                    assertTrue(socket.isBound());
                }
            } finally {
                release.countDown();
            }
        } finally {
            release.countDown();
            ThreadPool.terminate(pool, 10, TimeUnit.SECONDS);
        }
    }

    public void testOperatorCannotOverrideTheControllerQueueBound() throws Exception {
        final var fixture = fixture(1);
        final var pool = pool("journal-invalid-queue", 2);
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            expectThrows(
                IllegalArgumentException.class,
                () -> controller(fixture, fixture.configurations().getFirst(), container, pool, new AtomicLong())
            );
            assertFalse(Files.exists(fixture.configurations().getFirst().directory().resolve("journal-binding")));
        } finally {
            ThreadPool.terminate(pool, 10, TimeUnit.SECONDS);
        }
    }

    public void testBootstrapOwnsTheJournalAndFailedAuthorizationReleasesNodeHome() throws Exception {
        final var fixture = fixture(1);
        final var configuration = fixture.configurations().getFirst();
        final Path storeDirectory = createTempDir();
        final var settings = Settings.builder()
            .put("node.name", "journal-bootstrap")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", 0)
            .put("transport.port", 0)
            .put(ServerlessBootstrap.STORE_PATH, storeDirectory)
            .build();
        final var assignment = new ServerlessBootstrap.JournalAssignment(
            configuration,
            fixture.key(),
            fixture.trust(),
            () -> Long.MAX_VALUE
        );
        final JournalController controller;
        try (var bootstrap = ServerlessBootstrap.start(settings, assignment)) {
            controller = bootstrap.node().journalController().orElseThrow();
            assertNotNull(controller.awaitOpened());
            assertBusy(() -> assertEquals(JournalController.State.ACTIVATING, controller.state(configuration.groups().getFirst().id())));
        }
        assertEquals(JournalController.State.CLOSED, controller.state(configuration.groups().getFirst().id()));
        try (var legacyRestart = ServerlessBootstrap.start(settings)) {
            assertTrue(legacyRestart.node().isStarted());
            assertTrue(legacyRestart.node().journalController().isEmpty());
        }
        final var other = fixture.configurations().get(1);
        final var foreign = new JournalConfiguration(
            UUID.randomUUID(),
            other.localVoterId(),
            other.directory(),
            other.groups(),
            true,
            other.limits()
        );
        final var refused = new ServerlessBootstrap.JournalAssignment(foreign, fixture.key(), fixture.trust(), () -> Long.MAX_VALUE);
        expectThrows(IOException.class, () -> ServerlessBootstrap.start(settings, refused));
        assertFalse(Files.exists(other.directory().resolve("journal-binding")));
        try (var recovered = ServerlessBootstrap.start(settings)) {
            assertTrue(recovered.node().isStarted());
        }
    }

    public void testFileAssignmentStartsThroughNormalBootstrapWithQuorumTls() throws Exception {
        final var fixture = fixture(1);
        final var configuration = fixture.configurations().getFirst();
        final Path files = createTempDir();
        for (var entry : java.util.Map.of("member.p12", "keystore", "trust.p12", "truststore").entrySet()) {
            try (var input = getClass().getResourceAsStream("/org/opensearch/serverless/testkit/tls-test-" + entry.getValue() + ".p12")) {
                Files.copy(input, files.resolve(entry.getKey()));
            }
        }
        final var groups = configuration.groups()
            .stream()
            .map(
                group -> java.util.Map.of(
                    "id",
                    group.id().toString(),
                    "peers",
                    group.peers()
                        .stream()
                        .map(
                            peer -> java.util.Map.of(
                                "id",
                                peer.id().toString(),
                                "host",
                                peer.host(),
                                "port",
                                peer.port(),
                                "availability_zone",
                                peer.availabilityZone()
                            )
                        )
                        .toList()
                )
            )
            .toList();
        try (var builder = org.opensearch.common.xcontent.json.JsonXContent.contentBuilder()) {
            builder.map(
                java.util.Map.of(
                    "format_version",
                    1,
                    "deployment_id",
                    configuration.deploymentId().toString(),
                    "local_voter_id",
                    configuration.localVoterId().toString(),
                    "directory",
                    configuration.directory().toString(),
                    "bootstrap",
                    true,
                    "groups",
                    groups,
                    "limits",
                    java.util.Map.of("minimum_free_bytes", 1)
                )
            );
            Files.write(files.resolve("assignment.json"), BytesReference.toBytes(BytesReference.bytes(builder)));
        }
        try (var secure = org.opensearch.common.settings.KeyStoreWrapper.create()) {
            secure.setString("serverless.journal.keystore.secure_password", "testonly".toCharArray());
            secure.setString("serverless.journal.truststore.secure_password", "testonly".toCharArray());
            secure.save(files, new char[0]);
        }
        final Path storeDirectory = createTempDir();
        final var settings = Settings.builder()
            .put("node.name", "journal-file-bootstrap")
            .put("path.home", createTempDir())
            .put(ServerlessBootstrap.CONFIG_PATH, files)
            .put("network.host", "127.0.0.1")
            .put("http.port", 0)
            .put("transport.port", 0)
            .put(ServerlessBootstrap.STORE_PATH, storeDirectory)
            .put("serverless.journal.assignment_file", files.resolve("assignment.json"))
            .put("serverless.journal.keystore.path", files.resolve("member.p12"))
            .put("serverless.journal.truststore.path", files.resolve("trust.p12"))
            .build();
        final var companions = new ArrayList<JournalController>();
        final var pools = new ArrayList<ThreadPool>();
        try (var store = new FsBlobStore(1024, storeDirectory, false); var bootstrap = ServerlessBootstrap.start(settings)) {
            final var local = bootstrap.node().journalController().orElseThrow();
            try {
                for (var other : fixture.configurations().subList(1, 3)) {
                    final var pool = pool("journal-file-companion-" + pools.size(), 1);
                    pools.add(pool);
                    final var companion = controller(
                        fixture,
                        other,
                        store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath())),
                        pool,
                        new AtomicLong()
                    );
                    companions.add(companion);
                    companion.start();
                    companion.awaitOpened();
                }
                final UUID group = configuration.groups().getFirst().id();
                assertBusy(() -> {
                    assertEquals(JournalController.State.ACTIVE, local.state(group));
                    companions.forEach(controller -> assertEquals(JournalController.State.ACTIVE, controller.state(group)));
                }, 40, TimeUnit.SECONDS);
                final var cohort = new ArrayList<>(companions);
                cohort.add(local);
                final var stream = new ShardLogId(fixture.deployment(), "file-configured-journal", 0, UUID.randomUUID());
                final var leader = leader(cohort, group);
                leader.awaitOpened().append(group, operation(stream, 1)).toCompletableFuture().get(10, TimeUnit.SECONDS);
                assertEquals(1, leader.awaitOpened().scan(group, -1, Set.of(stream), 100, 65536).operations().size());
            } finally {
                close(companions, pools);
            }
        }
    }

    private static OperationEnvelope operation(ShardLogId stream, int value) {
        return new OperationEnvelope(stream, OperationEnvelope.Kind.DOCUMENTS, 1, 1, 1, new byte[] { (byte) value });
    }

    private static JournalController controller(
        Fixture fixture,
        JournalConfiguration configuration,
        BlobContainer container,
        ThreadPool pool,
        AtomicLong clock
    ) {
        return new JournalController(
            configuration,
            new JournalGroupCatalog(container, fixture.deployment()),
            fixture.key(),
            fixture.trust(),
            () -> Long.MAX_VALUE,
            pool,
            clock::get
        );
    }

    private static ThreadPool pool(String name, int queue) {
        final var settings = Settings.builder().put("node.name", name).build();
        return new ThreadPool(
            settings,
            new FixedExecutorBuilder(
                settings,
                ServerlessNode.JOURNAL_CONTROLLER_POOL,
                1,
                queue,
                "thread_pool." + ServerlessNode.JOURNAL_CONTROLLER_POOL
            )
        );
    }

    private static JournalController leader(List<JournalController> controllers, UUID group) throws Exception {
        final var result = new JournalController[1];
        assertBusy(() -> {
            for (var controller : controllers) {
                try {
                    if (controller.journal().isPresent() && controller.journal().get().isLeaderReady(group)) {
                        result[0] = controller;
                        return;
                    }
                } catch (IOException unavailable) {
                    // Closed, retired or unassigned here.
                }
            }
            fail("waiting for a ready journal leader");
        }, 30, TimeUnit.SECONDS);
        return result[0];
    }

    private static void close(List<JournalController> controllers, List<ThreadPool> pools) throws IOException {
        IOException first = null;
        for (var controller : controllers) {
            try {
                controller.close();
            } catch (IOException failure) {
                if (first == null) {
                    first = failure;
                } else {
                    first.addSuppressed(failure);
                }
            }
        }
        for (var pool : pools) {
            ThreadPool.terminate(pool, 10, TimeUnit.SECONDS);
        }
        if (first != null) {
            throw first;
        }
    }

    private record Fixture(UUID deployment, List<JournalConfiguration> configurations, X509KeyManager key, X509TrustManager trust) {
    }

    private Fixture fixture(int groupCount) throws Exception {
        final UUID deployment = UUID.randomUUID();
        final var directories = new ArrayList<Path>();
        final var peers = new ArrayList<JournalConfiguration.Peer>();
        final var ports = new ArrayList<ServerSocket>();
        try {
            for (int i = 0; i < 3; i++) {
                final Path directory = createTempDir();
                directories.add(directory);
                final var port = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"));
                ports.add(port);
                peers.add(
                    new JournalConfiguration.Peer(JournalDiskIdentity.discover(directory), "127.0.0.1", port.getLocalPort(), "az-" + i)
                );
            }
        } finally {
            for (var port : ports) {
                port.close();
            }
        }
        final var groups = new ArrayList<JournalConfiguration.Group>();
        for (int i = 0; i < groupCount; i++) {
            groups.add(new JournalConfiguration.Group(UUID.randomUUID(), peers));
        }
        final var limits = new JournalConfiguration.Limits(2, 16384, 32, 1024 * 1024, 1024 * 1024, 2 * 1024 * 1024, 0, 10_000, 1);
        final var configurations = new ArrayList<JournalConfiguration>();
        for (int i = 0; i < 3; i++) {
            configurations.add(new JournalConfiguration(deployment, peers.get(i).id(), directories.get(i), groups, true, limits));
        }
        final var keys = KeyStore.getInstance("PKCS12");
        try (var input = getClass().getResourceAsStream("/org/opensearch/serverless/testkit/tls-test-keystore.p12")) {
            keys.load(input, "testonly".toCharArray());
        }
        final var keyFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyFactory.init(keys, "testonly".toCharArray());
        final var trust = KeyStore.getInstance("PKCS12");
        try (var input = getClass().getResourceAsStream("/org/opensearch/serverless/testkit/tls-test-truststore.p12")) {
            trust.load(input, "testonly".toCharArray());
        }
        final var trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trust);
        return new Fixture(
            deployment,
            List.copyOf(configurations),
            (X509KeyManager) keyFactory.getKeyManagers()[0],
            (X509TrustManager) trustFactory.getTrustManagers()[0]
        );
    }
}
