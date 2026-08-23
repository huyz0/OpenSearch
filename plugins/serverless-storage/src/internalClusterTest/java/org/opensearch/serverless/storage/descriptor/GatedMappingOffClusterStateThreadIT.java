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
 * <p><b>Both ways an index becomes gated are driven, and the second one is why T49 exists.</b> A request
 * carrying {@code index.serverless_storage.enabled} says it is gated and is admitted off-thread on that
 * basis. An index gated only by a matching template says nothing, and used to take the ordinary road and
 * meet the gate at the bottom on the cluster manager's update thread -- where being gated means a blocking
 * write to the mapping store, from the thread this class exists to keep it off, by a write that can submit
 * a cluster state update of its own and wait for the thread it is standing on. T49 made admission resolve
 * templates, so that road is no longer taken; the template phase below is what holds it to that.
 *
 * <h2>The two ways a proof like this passes for the wrong reason</h2>
 *
 * It can record nothing, because the path stopped reaching the store, and then "no call came from the
 * cluster state thread" is true and worthless. So each phase asserts it recorded at least one call.
 *
 * <p>It can also record only the phase that was already safe. Creation and put-mapping are separate callers
 * that were fixed at different times for different reasons, so they are counted separately rather than
 * summed.
 *
 * <p><b>T58 turned the first of those into a live failure rather than a hypothetical.</b> A creation's
 * mapping rides its descriptor now and never reaches {@code MappingGenerationStore}, so the creation phases
 * recorded nothing at all and passed on the strength of it. Both planes are watched since: the mapping store,
 * and the descriptor backend the gate's hooks write through. The backend's blocking calls answer the thread
 * question; its asynchronous ones answer only "this phase reached the plane", because handing a descriptor
 * write to an executor from the cluster state thread is this design working rather than the defect it was
 * built to prevent.
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

    private static volatile java.nio.file.Path sharedBasePath;

    private java.nio.file.Path basePath() {
        if (sharedBasePath == null) {
            synchronized (GatedMappingOffClusterStateThreadIT.class) {
                if (sharedBasePath == null) {
                    sharedBasePath = randomRepoPath();
                }
            }
        }
        return sharedBasePath;
    }

    /**
     * A storage base path, which this class did without until the template phase needed it.
     *
     * <p>The class did without one until the template phase, and the reason first written here was wrong:
     * that a template-gated creation falls back to the ordinary path and opens a shard. It does not. The
     * assertion below confirms the index is gated, and removing this setting still fails the run with
     * "serverless storage enabled but no usable container", so something in this class does open a gated
     * shard. Which one is not established, and the setting is kept because it is required rather than
     * because the explanation is understood.
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
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

    public void testNeitherCreationNorPutMappingTouchesTheStoreFromAClusterStateThread() throws Exception {
        // Two recorders, because since T58 the two phases write to different places. A put-mapping goes
        // through MappingGenerationStore; a creation's mapping rides the descriptor its creation writes, so
        // it never reaches that store at all -- which is why the premise guard below reads both. Watching
        // only the mapping store leaves the creation phases proving nothing, and a phase that records
        // nothing passes every thread check there is.
        java.util.concurrent.atomic.AtomicReference<RecordingBackend> recorded = new java.util.concurrent.atomic.AtomicReference<>();
        var plane = installBlobBackedDescriptorPlane(true, delegate -> {
            RecordingBackend recording = new RecordingBackend(delegate);
            recorded.set(recording);
            return recording;
        });
        RecordingBackend backend = recorded.get();
        // Descriptor-backed rather than index-backed, because that is the half of the registered store a
        // mapping write actually goes through since T58. An index-backed one here also raced the stats
        // projection for the creation of .opensearch-index-mappings and failed the put-mapping on a shard
        // that was still recovering -- a fight between two stores that production does not have.
        RecordingStore store = new RecordingStore(new DescriptorBackedMappingStore(plane::points, null));
        MappingGenerationStore.register(store);

        checkPhase(
            store,
            backend,
            "gated creation",
            () -> client().admin()
                .indices()
                .create(
                    new CreateIndexRequest("serverless_gated-threading").settings(gated())
                        .mapping(Map.of("properties", Map.of("tenant", Map.of("type", "keyword"))))
                )
                .actionGet(REQUEST_DEADLINE)
        );

        // The shape T44 could not cover and T49 closed. Nothing in this request says the index is gated;
        // the template does, and admission now resolves templates rather than reading the request alone.
        client().admin()
            .indices()
            .preparePutTemplate("gated-by-template")
            .setPatterns(List.of("serverless_templated-*"))
            .setSettings(gated())
            .get();

        checkPhase(
            store,
            backend,
            "template-gated creation",
            () -> client().admin()
                .indices()
                .create(
                    new CreateIndexRequest("serverless_templated-threading").mapping(
                        Map.of("properties", Map.of("tenant", Map.of("type", "keyword")))
                    )
                )
                .actionGet(REQUEST_DEADLINE)
        );

        // The phase means nothing unless the template actually gated the index: an index that fell back to
        // the ordinary path would still have reached the store once, off-thread, during the attempt.
        assertNull(
            "the template must be what gates this index, or the phase above proves nothing about the " + "template path",
            client().admin().cluster().prepareState().get().getState().metadata().index("serverless_templated-threading")
        );

        checkPhase(
            store,
            backend,
            "put-mapping",
            () -> client().admin()
                .indices()
                .preparePutMapping("serverless_gated-threading")
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
    private void checkPhase(RecordingStore store, RecordingBackend backend, String phase, Runnable request) {
        Throwable failure = null;
        try {
            request.run();
        } catch (Throwable e) {
            // Throwable rather than Exception. What this class exists to catch raises an AssertionError,
            // and a catch that misses it is the exact mistake this file criticises PutMappingExecutor for.
            // Rethrown below, after the evidence has been read.
            failure = e;
        }
        // Only calls that block are held to the thread rule. A descriptor write handed to an executor from
        // the cluster state thread is the design working, not a violation of it, so recording those here
        // would fail the very shape T58 introduced -- while a phase that reached the plane only that way
        // still counts as evidence for the premise below.
        Set<String> threads = new java.util.HashSet<>(store.takeThreads());
        threads.addAll(backend.takeBlockingThreads());
        Set<String> touched = backend.takeAnyThreads();
        assertNoneForbidden(phase, threads);
        // The request's own failure goes into this message rather than replacing it. With the tripwire in
        // place a regression presents as a refused creation rather than a blocked thread, so the store is
        // never reached, and "never reached the mapping store" on its own sends a reader to the wrong
        // place. The reason is right here.
        assertFalse(
            phase
                + " reached neither the mapping store nor the descriptor backend, so this proves nothing"
                + (failure == null ? "" : ", and it failed with: " + failure),
            threads.isEmpty() && touched.isEmpty()
        );
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new AssertionError(phase + " failed", failure);
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

    /**
     * The same for the descriptor plane, recording only the calls that block.
     *
     * <p>The asynchronous ones are safe from this class's question by construction -- they hand the work to
     * an executor and return -- and recording them would put GENERIC in the evidence for a call that never
     * touched it. What matters here is {@code get}, {@code create} and {@code put}: they do the I/O on the
     * caller's thread, which is exactly the property that made the pre-T19 put-mapping fatal.
     */
    private static final class RecordingBackend implements DescriptorBackend {

        private final DescriptorBackend delegate;
        private final Set<String> blocking = ConcurrentHashMap.newKeySet();
        private final Set<String> any = ConcurrentHashMap.newKeySet();

        RecordingBackend(DescriptorBackend delegate) {
            this.delegate = delegate;
        }

        /** A call that does its I/O on this thread, and so is subject to the question this class asks. */
        private void record() {
            blocking.add(Thread.currentThread().getName());
            touch();
        }

        /** A call that only hands work to an executor. Evidence the phase reached the plane, nothing more. */
        private void touch() {
            any.add(Thread.currentThread().getName());
        }

        @Override
        public org.opensearch.cluster.metadata.IndexDescriptor get(String name) {
            record();
            return delegate.get(name);
        }

        @Override
        public org.opensearch.cluster.metadata.IndexDescriptor getIfFresh(String name) {
            touch();
            return delegate.getIfFresh(name);
        }

        @Override
        public boolean create(org.opensearch.cluster.metadata.IndexDescriptor descriptor) {
            record();
            return delegate.create(descriptor);
        }

        @Override
        public java.util.concurrent.CompletableFuture<Boolean> createAsync(org.opensearch.cluster.metadata.IndexDescriptor descriptor) {
            touch();
            return delegate.createAsync(descriptor);
        }

        @Override
        public void put(org.opensearch.cluster.metadata.IndexDescriptor descriptor) {
            record();
            delegate.put(descriptor);
        }

        @Override
        public void putAsync(org.opensearch.cluster.metadata.IndexDescriptor descriptor) {
            touch();
            delegate.putAsync(descriptor);
        }

        @Override
        public void putAsync(
            org.opensearch.cluster.metadata.IndexDescriptor descriptor,
            org.opensearch.core.action.ActionListener<Void> listener
        ) {
            touch();
            delegate.putAsync(descriptor, listener);
        }

        @Override
        public void putTombstoneAsync(org.opensearch.cluster.metadata.IndexDescriptor tombstone) {
            touch();
            delegate.putTombstoneAsync(tombstone);
        }

        @Override
        public void putTombstoneAsync(
            org.opensearch.cluster.metadata.IndexDescriptor tombstone,
            org.opensearch.core.action.ActionListener<Void> whenDurable
        ) {
            touch();
            delegate.putTombstoneAsync(tombstone, whenDurable);
        }

        @Override
        public boolean available() {
            return delegate.available();
        }

        @Override
        public void warmAsync(java.util.Collection<String> names, org.opensearch.core.action.ActionListener<Void> listener) {
            delegate.warmAsync(names, listener);
        }

        @Override
        public void invalidate(String name) {
            delegate.invalidate(name);
        }

        Set<String> takeBlockingThreads() {
            return drain(blocking);
        }

        Set<String> takeAnyThreads() {
            return drain(any);
        }

        private static Set<String> drain(Set<String> from) {
            Set<String> taken = new java.util.HashSet<>();
            for (java.util.Iterator<String> each = from.iterator(); each.hasNext();) {
                taken.add(each.next());
                each.remove();
            }
            return taken;
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
