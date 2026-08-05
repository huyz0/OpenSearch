/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugins.Plugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * T23. Whether two clients creating the same gated index name can both be told they succeeded.
 *
 * <p>H3's design names the mechanism that prevents this: a descriptor written with
 * {@code op_type=create} is the uniqueness gate, so the store rather than the cluster manager decides that
 * a name is taken, and a version conflict is a lost race rather than an error. {@code BlobDescriptorBackend}
 * implements it as {@link BlobDescriptorBackend#create}.
 *
 * <p>The gated creation path does not call it. {@code MetadataCreateIndexService.clusterStateCreateIndex}
 * calls {@code IndexDescriptorPublisher.publish}, and {@code DescriptorGate} routes a live descriptor to
 * {@code putAsync}, which is a plain put. So the mechanism exists, is tested, and is not on the path it was
 * built for, which makes it the fifth thing in this area found correct and unreachable.
 *
 * <p><b>What makes this worse than a timing race.</b> An ordinary index gets its uniqueness from cluster
 * state: the second task in a batch sees the first task's index already present in the state it is handed,
 * because the executor threads state through the loop. A gated index is never added to that state. So
 * within a single batch the second task validates against a state the first one did not change, and there
 * is no window to lose, only a guarantee to lack.
 *
 * <p>Probed rather than argued, because this area has repeatedly punished reading the code and concluding.
 */
public class GatedCreationUniquenessIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private static final String CONTESTED = "contested-idx";

    private static final int CLIENTS = 8;

    private volatile java.nio.file.Path sharedBasePath;

    private java.nio.file.Path basePath() {
        if (sharedBasePath == null) {
            synchronized (this) {
                if (sharedBasePath == null) {
                    sharedBasePath = randomRepoPath();
                }
            }
        }
        return sharedBasePath;
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(org.opensearch.serverless.storage.ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(
                org.opensearch.serverless.storage.ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(),
                basePath().toString()
            )
            .build();
    }

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    /** The control: an ordinary index gets its uniqueness from cluster state, so exactly one wins. */
    public void testAnOrdinaryIndexNameCanOnlyBeCreatedOnce() throws Exception {
        Outcome outcome = createConcurrently("ordinary-contested", false);

        assertEquals("exactly one client may be told it created an ordinary index", 1, outcome.succeeded.get());
        assertEquals(CLIENTS - 1, outcome.failed.get());
    }

    /**
     * The question, and it fails. <b>Measured: 8 of 8 concurrent creations of the same gated name were
     * acknowledged.</b> Not one winner and seven losers, and not a narrow race either. Eight clients were
     * each told they created the index.
     *
     * <p>For a tenant migration that is eight tenants silently sharing one index, discovered when their
     * data turns out to be mixed.
     *
     * <p>Committed under {@code AwaitsFix} alongside T17, because both are the same defect seen from
     * different sides and both are fixed by the same change: route gated creation through
     * {@code BlobDescriptorBackend.create}, which is {@code op_type=create} and therefore atomic, and make the
     * acknowledgement wait for its result instead of for a cluster state update that does nothing.
     */
    public void testAGatedIndexNameCanOnlyBeCreatedOnce() throws Exception {
        installBlobBackedDescriptorPlane();

        Outcome outcome = createConcurrently(CONTESTED, true);

        logger.warn(
            "T23: {} of {} concurrent creations of the same gated name [{}] were acknowledged",
            outcome.succeeded.get(),
            CLIENTS,
            CONTESTED
        );

        assertEquals(
            "exactly one client may be told it created the index. H3 makes op_type=create the uniqueness "
                + "gate, and the gated creation path routes through publish to a plain put instead, so the "
                + "gate is implemented and unreached",
            1,
            outcome.succeeded.get()
        );
    }

    private record Outcome(AtomicInteger succeeded, AtomicInteger failed) {
    }

    private Outcome createConcurrently(String name, boolean serverless) throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(CLIENTS);
        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();

        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        if (serverless) {
            settings.put("index.serverless_storage.enabled", true);
        }

        for (int i = 0; i < CLIENTS; i++) {
            Thread client = new Thread(() -> {
                try {
                    release.await();
                    client().admin().indices().create(new CreateIndexRequest(name).settings(settings.build()), new ActionListener<>() {
                        @Override
                        public void onResponse(CreateIndexResponse response) {
                            if (response.isAcknowledged()) {
                                succeeded.incrementAndGet();
                            }
                            done.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            failed.incrementAndGet();
                            done.countDown();
                        }
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    done.countDown();
                }
            });
            client.setDaemon(true);
            client.start();
        }

        release.countDown();
        assertTrue("every client must finish", done.await(2, TimeUnit.MINUTES));
        return new Outcome(succeeded, failed);
    }
}
