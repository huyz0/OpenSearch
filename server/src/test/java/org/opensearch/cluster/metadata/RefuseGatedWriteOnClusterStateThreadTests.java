/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Phase A1 of core-pluggability-refactor-plan.md: {@code clusterStateCreateIndex}'s gated branch
 * documented a refusal ("The refusal below is what stands in for the argument") that was never actually
 * called. This is the regression test for that fix -- it drives the gated-creation code path from a
 * thread named the same way {@code ClusterService} names its own update-task thread, and asserts the
 * write is refused rather than attempted, and that an ordinary thread is unaffected.
 */
public class RefuseGatedWriteOnClusterStateThreadTests extends OpenSearchTestCase {

    private final TestIndexCreationStrategy strategy = new TestIndexCreationStrategy();

    @Before
    public void registerStrategy() {
        // Phase D3 of core-pluggability-refactor-plan.md: a test-local IndexCreationStrategy, not
        // DescriptorOnlyCreation (relocated into plugins/serverless-storage) -- see TestIndexCreationStrategy's
        // own javadoc.
        IndexCreationStrategyRegistry.register(strategy);
    }

    @After
    public void clearRegistrations() {
        strategy.deactivate();
        IndexDescriptorPublisher.register(null);
        IndexDescriptorPublisher.registerCreator(null);
        IndexCreationStrategyRegistry.register(null);
    }

    public void testGatedCreationOnClusterStateThreadIsRefusedNotBlocked() throws Exception {
        strategy.activate(indexMetadata -> true);
        IndexDescriptorPublisher.registerCreator(descriptor -> CompletableFuture.completedFuture(Boolean.TRUE));

        AtomicReference<Throwable> caught = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        // Matched by name only, same as ClusterStateMutationThreads#blockingIsUnsafeOnCurrentThread itself --
        // this is exactly how a real cluster-manager update-task thread would be named.
        Thread updateThread = new Thread(() -> {
            try {
                MetadataCreateIndexService.clusterStateCreateIndex(
                    ClusterState.builder(ClusterName.DEFAULT).build(),
                    Set.of(),
                    index("gated-on-update-thread"),
                    (current, reason) -> current,
                    null,
                    write -> fail("a refused creation must never hand over a descriptor-write future")
                );
            } catch (Throwable t) {
                caught.set(t);
            } finally {
                done.countDown();
            }
        }, "opensearch[test][clusterManagerService#updateTask][T#1]");
        updateThread.start();
        assertTrue(
            "the update-task thread must finish quickly, not block on a store write",
            done.await(10, java.util.concurrent.TimeUnit.SECONDS)
        );
        updateThread.join();

        assertNotNull("a gated creation reached on the cluster-state-update thread must be refused", caught.get());
        assertTrue(
            "the refusal must be the documented tripwire, not some other failure: " + caught.get(),
            caught.get() instanceof IllegalStateException
                && caught.get().getMessage() != null
                && caught.get().getMessage().contains("cluster state updates")
        );
    }

    public void testGatedCreationOffClusterStateThreadStillWorks() {
        strategy.activate(indexMetadata -> true);
        IndexDescriptorPublisher.registerCreator(descriptor -> CompletableFuture.completedFuture(Boolean.TRUE));

        AtomicReference<CompletableFuture<Boolean>> handedOver = new AtomicReference<>();
        ClusterState result = MetadataCreateIndexService.clusterStateCreateIndex(
            ClusterState.builder(ClusterName.DEFAULT).build(),
            Set.of(),
            index("gated-off-update-thread"),
            (current, reason) -> current,
            null,
            handedOver::set
        );

        assertNotNull("an ordinary (non cluster-state-update) thread must still be handed the write", handedOver.get());
        assertEquals("and it must leave no cluster state entry, exactly as before this fix", 0, result.metadata().indices().size());
    }

    private static IndexMetadata index(String name) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
