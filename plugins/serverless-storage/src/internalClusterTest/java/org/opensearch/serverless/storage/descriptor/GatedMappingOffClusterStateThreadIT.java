/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * T44. Which thread reaches the mapping store, asserted rather than argued.
 *
 * <h2>Why this needs a test at all</h2>
 *
 * The store's reads and writes block, and blocking is safe only because of a claim about its callers:
 * that a mapping update runs on a transport thread or on GENERIC, never on the cluster state update
 * thread whose serialization is the ceiling this whole design exists to remove.
 *
 * <p>That claim was false for one of the two callers and stayed false for a while. A put-mapping on a gated
 * index ran inside {@code MetadataMappingService.PutMappingExecutor#execute}, which <em>is</em> the cluster
 * manager's update thread, so every one made two blocking round trips on it. It tripped an assertion rather
 * than merely being slow, and it went unnoticed because the only test of the path drove the executor with an
 * in-memory double -- a double has no round trips, so the one property that mattered was the one the test
 * could not see. T19 moved the work to {@code MetadataMappingService#putMapping}, which dispatches to
 * GENERIC before submitting anything.
 *
 * <p>Nothing checked it afterwards either. This does, through the real cluster: a recording store notes the
 * calling thread of every call, a real gated creation and a real put-mapping drive it, and the assertion is
 * on the thread names collected.
 *
 * <p><b>What this proves is narrower than "no gated creation blocks that thread", and the gap is not
 * hypothetical.</b> The creation here carries {@code index.serverless_storage.enabled} in its own request
 * settings, which is what {@code DescriptorGate}'s admission check reads, so it is admitted onto GENERIC. An
 * index made gated by a matching template carries nothing in its request that says so, is deliberately not
 * admitted, and reaches {@code clusterStateCreateIndex} on the cluster manager's update thread -- where the
 * finished settings do say it is gated, so it calls the store from exactly the thread this class exists to
 * keep it off. T49 covers that shape; this class does not, and saying so is the point of this paragraph.
 *
 * <h2>The two ways a proof like this passes for the wrong reason</h2>
 *
 * It can record nothing, because the path stopped reaching the store, and then "no call came from the
 * cluster state thread" is true and worthless. So each phase asserts it recorded at least one call.
 *
 * <p>It can also record only the phase that was already safe. Creation and put-mapping are separate callers
 * that were fixed at different times for different reasons, so they are counted separately rather than
 * summed.
 */
public class GatedMappingOffClusterStateThreadIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    /**
     * The threads a blocking store call must never come from.
     *
     * <p>Matched on a substring because the runtime names carry the node name as a prefix. Both are listed
     * rather than only the cluster manager's: the applier thread is equally fatal to block, and an
     * implementation that moved this work into a cluster state applier would be a regression that a check
     * for the manager thread alone would wave through.
     */
    /** Long enough that a healthy request never trips it, short enough that a stalled one is a failure. */
    private static final org.opensearch.common.unit.TimeValue REQUEST_DEADLINE = org.opensearch.common.unit.TimeValue.timeValueSeconds(60);

    private static final List<String> FORBIDDEN = List.of(
        "clusterManagerService#updateTask",
        "clusterApplierService#updateTask",
        // The pre-rename spelling. Kept because this list exists to agree with
        // AbsentIndexDescriptorSuppliers#blockingIsUnsafeHere, which carries it, and a check that is the
        // narrower of two copies of the same idea is the one that lets something through.
        "masterService#updateTask"
    );

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(org.opensearch.serverless.storage.ServerlessStoragePlugin.class);
    }

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    public void testNeitherCreationNorPutMappingTouchesTheStoreFromAClusterStateThread() throws Exception {
        installBlobBackedDescriptorPlane();
        RecordingStore store = new RecordingStore(new IndexBackedMappingStore(client()));
        MappingGenerationStore.register(store);

        checkPhase(
            store,
            "gated creation",
            () -> client().admin()
                .indices()
                .create(
                    new CreateIndexRequest("gated-threading").settings(gated())
                        .mapping(Map.of("properties", Map.of("tenant", Map.of("type", "keyword"))))
                )
                .actionGet(REQUEST_DEADLINE)
        );

        checkPhase(
            store,
            "put-mapping",
            () -> client().admin()
                .indices()
                .preparePutMapping("gated-threading")
                .setSource("amount", "type=double")
                .execute()
                .actionGet(REQUEST_DEADLINE)
        );
    }

    /**
     * Runs one phase and checks the threads it used, whether or not the phase returned.
     *
     * <p><b>The bounded wait and the finally are the whole reason this test is worth anything.</b> The first
     * version waited on each request without a deadline and read the recorded threads afterwards, and
     * against the build this is supposed to catch it never got that far: a blocking store call on the
     * cluster manager's update thread trips {@code BaseFuture.blockingAllowed}, which throws an
     * {@code AssertionError}, which {@code PutMappingExecutor}'s {@code catch (Exception)} does not catch,
     * so the task is never answered and the caller waits forever. The mutation was killed at ten minutes.
     *
     * <p>A proof that reports a timeout is barely better than the defect it is proving absent -- the
     * pre-T19 bug also presented as things not finishing. The evidence exists the moment the store is
     * entered, because the thread is recorded before the call is delegated, so the check runs in a finally
     * and names the offending thread even when the request never comes back.
     */
    private void checkPhase(RecordingStore store, String phase, Runnable request) {
        try {
            request.run();
        } finally {
            Set<String> threads = store.takeThreads();
            assertNoneForbidden(phase, threads);
            assertFalse(phase + " never reached the mapping store, so this proves nothing about it", threads.isEmpty());
        }
    }

    private static void assertNoneForbidden(String phase, Set<String> threads) {
        for (String thread : threads) {
            for (String forbidden : FORBIDDEN) {
                assertFalse(
                    phase + " called the mapping store from [" + thread + "], which blocks the thread this design exists to free",
                    thread.contains(forbidden)
                );
            }
        }
    }

    /** Records the calling thread of every store call, delegating the call itself to the real store. */
    private static final class RecordingStore implements MappingGenerationStore.Store {

        private final MappingGenerationStore.Store delegate;
        private final Set<String> threads = ConcurrentHashMap.newKeySet();

        RecordingStore(MappingGenerationStore.Store delegate) {
            this.delegate = delegate;
        }

        @Override
        public MappingGenerationStore.MappingGeneration read(String indexUuid) {
            threads.add(Thread.currentThread().getName());
            return delegate.read(indexUuid);
        }

        @Override
        public void delete(String indexUuid) {
            threads.add(Thread.currentThread().getName());
            delegate.delete(indexUuid);
        }

        @Override
        public boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGenerationStore.MappingGeneration updated) {
            threads.add(Thread.currentThread().getName());
            return delegate.compareAndSwap(indexUuid, expectedGeneration, updated);
        }

        /**
         * What has been recorded since the last call, so one phase's threads are not read as another's.
         *
         * <p>Drained by removing each element rather than copy-then-clear, which would drop anything
         * recorded between the two.
         */
        Set<String> takeThreads() {
            Set<String> taken = new java.util.HashSet<>();
            for (java.util.Iterator<String> each = threads.iterator(); each.hasNext();) {
                taken.add(each.next());
                each.remove();
            }
            return taken;
        }
    }

    private static Settings gated() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }
}
