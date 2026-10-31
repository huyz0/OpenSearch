/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.log.JournalConfiguration;
import org.opensearch.serverless.log.JournalDiskIdentity;
import org.opensearch.serverless.log.RatisJournal;
import org.opensearch.test.OpenSearchTestCase;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Real local journal activation plus conditional object-store metadata, not a three-AZ test. */
@SuppressFileSystems("*")
@ThreadLeakLingering(linger = 70_000)
@ThreadLeakFilters(filters = JournalGroupCatalogRuntimeTests.GlobalTimerFilter.class)
public class JournalGroupCatalogRuntimeTests extends OpenSearchTestCase {
    /** Only known idle JVM-global library threads without a public shutdown API are excluded. */
    public static class GlobalTimerFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            final var group = thread.getThreadGroup();
            if (thread.isDaemon() && thread.getName().equals("grpc-shared-destroyer-0") && thread.getState() == Thread.State.WAITING) {
                // SharedResourceHolder keeps its cleanup scheduler after all client executors retire.
                for (var frame : thread.getStackTrace()) {
                    if (frame.getClassName().equals("java.util.concurrent.ScheduledThreadPoolExecutor$DelayedWorkQueue")
                        && frame.getMethodName().equals("take")) {
                        return true;
                    }
                }
            }
            if (thread.isDaemon() == false
                || thread.getName().matches("timer[0-7]") == false
                || group == null
                || group.getName().matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}@group-[0-9A-F]{12}") == false) {
                return false;
            }
            for (var frame : thread.getStackTrace()) {
                if (frame.getClassName().equals("java.util.TimerThread")) {
                    return true;
                }
            }
            return false;
        }
    }

    public void testOnlyCommittedActivationCanBePublishedAndLostReplyIsIdempotent() throws Exception {
        final var directories = new ArrayList<Path>();
        final var peers = new ArrayList<JournalConfiguration.Peer>();
        final var ports = new ArrayList<ServerSocket>();
        try {
            for (int i = 0; i < 3; i++) {
                final var directory = createTempDir();
                directories.add(directory);
                final var port = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"));
                ports.add(port);
                peers.add(
                    new JournalConfiguration.Peer(JournalDiskIdentity.discover(directory), "127.0.0.1", port.getLocalPort(), "az-" + i)
                );
            }
        } finally {
            IOUtils.close(ports);
        }
        final var group = new JournalConfiguration.Group(UUID.randomUUID(), peers);
        final UUID deployment = UUID.randomUUID();
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
        final var key = (X509KeyManager) keyFactory.getKeyManagers()[0];
        final var trusted = (X509TrustManager) trustFactory.getTrustManagers()[0];
        final var limits = new JournalConfiguration.Limits(2, 16384, 32, 1024 * 1024, 1024 * 1024, 2 * 1024 * 1024, 0, 10_000, 1);
        final var journals = new ArrayList<RatisJournal>();
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var real = store.blobContainer(RegisterMap.journalGroups(BlobPath.cleanPath()));
            final var loseReply = new AtomicBoolean();
            final var wrapper = new FilterBlobContainer(real) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    throw new AssertionError("catalog must not list children");
                }

                @Override
                public Optional<BlobRegister> readRegister(String name) throws IOException {
                    return real.readRegister(name);
                }

                @Override
                public BlobRegisterCasResult compareAndSwapRegister(String name, long expected, BytesReference value) throws IOException {
                    final var result = real.compareAndSwapRegister(name, expected, value);
                    if (name.startsWith("group-") && result.applied() && loseReply.compareAndSet(true, false)) {
                        throw new IOException("lost activation reply after persistence");
                    }
                    return result;
                }
            };
            final var catalog = new JournalGroupCatalog(wrapper, deployment);
            final var reservation = catalog.reserve(group);
            try {
                for (int i = 0; i < 3; i++) {
                    final var configuration = new JournalConfiguration(
                        deployment,
                        peers.get(i).id(),
                        directories.get(i),
                        List.of(group),
                        true,
                        limits
                    );
                    journals.add(catalog.openJournal(configuration, key, trusted, () -> Long.MAX_VALUE));
                }
                expectThrows(IOException.class, () -> catalog.publishActivation(journals.getFirst(), group.id()));
                assertEquals(reservation, catalog.read(group.id()).orElseThrow());
                final var leader = new RatisJournal[1];
                assertBusy(() -> {
                    for (var journal : journals) {
                        if (journal.isLeaderReady(group.id())) {
                            leader[0] = journal;
                            return;
                        }
                    }
                    fail("no ready journal leader");
                }, 30, TimeUnit.SECONDS);
                leader[0].activateGroup(group.id()).toCompletableFuture().get(20, TimeUnit.SECONDS);
                loseReply.set(true);
                expectThrows(IOException.class, () -> catalog.publishActivation(leader[0], group.id()));
                final var active = catalog.read(group.id()).orElseThrow();
                assertNotNull(active.activation());
                assertEquals(leader[0].groupActivation(group.id()).checkpoint(), active.activation());
                final var restartedController = new JournalGroupCatalog(real, deployment);
                assertEquals(active, restartedController.publishActivation(leader[0], group.id()));
                assertEquals(active, restartedController.reserve(group));
                final var configuration = new JournalConfiguration(
                    deployment,
                    peers.getFirst().id(),
                    directories.getFirst(),
                    List.of(group),
                    true,
                    limits
                );
                final var refused = expectThrows(
                    IOException.class,
                    () -> restartedController.openJournal(configuration, key, trusted, () -> Long.MAX_VALUE)
                );
                assertTrue(refused.getMessage(), refused.getMessage().contains("activated physical group"));
            } finally {
                IOUtils.close(journals);
            }
        }
    }
}
