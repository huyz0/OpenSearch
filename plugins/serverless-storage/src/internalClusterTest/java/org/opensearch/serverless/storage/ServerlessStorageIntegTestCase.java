/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.serverless.storage.descriptor.BlobDescriptorBackend;
import org.opensearch.serverless.storage.descriptor.DescriptorBackedMappingStore;
import org.opensearch.serverless.storage.descriptor.DescriptorBackend;
import org.opensearch.serverless.storage.descriptor.DescriptorEnumerator;
import org.opensearch.serverless.storage.descriptor.DescriptorGate;
import org.opensearch.serverless.storage.descriptor.FailableDescriptorContainer;
import org.opensearch.serverless.storage.descriptor.IndexBackedMappingStatsAggregator;
import org.opensearch.serverless.storage.descriptor.IndexBackedMappingStore;
import org.opensearch.serverless.storage.descriptor.StatsProjectingMappingStore;
import org.opensearch.serverless.storage.descriptor.StoreBackedFieldRefresher;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.Executor;

/**
 * Shared base for every serverless-storage {@code internalClusterTest} that creates a real
 * serverless-storage index: enables {@link RemoteClusterStateService#REMOTE_CLUSTER_STATE_ENABLED_SETTING}
 * on every node, since {@link ServerlessStorageIndexSettingProvider} now enforces it as mandatory
 * (rfc-serverless-opensearch.md &sect;10) and rejects index creation otherwise. That setting is
 * {@code Property.Final} -- it can only be set at node startup, so it has to be threaded in here
 * via {@link #nodeSettings}, not set dynamically once a test cluster is already running.
 *
 * <p>Subclasses that already override {@link #nodeSettings(int)} for their own reasons must call
 * {@code super.nodeSettings(nodeOrdinal)} and layer their own settings on top, the same as every
 * other {@code nodeSettings} override in this test suite already does for its own superclass.
 */
public abstract class ServerlessStorageIntegTestCase extends OpenSearchIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .build();
    }

    /**
     * Never the framework's mock engine, because this plugin supplies an engine factory of its own.
     *
     * <p>{@code IndicesService.getEngineFactory} refuses outright when two plugins claim one index --
     * "multiple engine factories provided" -- so on the seeds where the randomizer installs
     * {@code MockEngineFactoryPlugin}, opening a serverless index is impossible. For a gated index that is
     * not a failed test so much as a hung one: the index has no cluster state entry, so nothing tells the
     * write it will never be servable, and it retries against a shard that cannot open until the suite
     * times out twenty minutes later.
     *
     * <p>Seventy-one subclasses had already worked this out and overridden it individually. Three had not,
     * and one of those, {@code ServerlessStorageAffinityForwardingIT}, was carried on the round's flaky list
     * for exactly this -- failing about one run in three, which is how often that seed comes up, and being
     * read as an intermittent property of the system rather than a missing line in one file. Declared here
     * so the next test to be written cannot omit it.
     */
    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    /**
     * The two halves of the descriptor plane a test installed, so it can assert against either.
     *
     * <p>Two objects rather than one because they answer different questions: point reads and writes go to
     * {@code points}, wildcard expansion to {@code prefixes}. Both address the same {@code descriptors/}
     * prefix of the same store, so there is no second copy to keep in step -- which is the whole reason the
     * descriptor system index could be removed.
     */
    protected record InstalledDescriptorPlane(BlobDescriptorBackend points, DescriptorEnumerator prefixes) {
    }

    /**
     * Installs the descriptor plane the plugin actually runs, backed by an object store.
     *
     * <p>Every test here used to install an index-backed descriptor store, since removed, so the suite
     * exercised a backend production does not use. That is the defect this branch keeps finding elsewhere --
     * a mechanism tested and unreachable, or reached and untested -- so the fixture is now the wiring itself
     * rather than something adjacent to it.
     *
     * <p>The executor is the node's {@code GENERIC} pool rather than the calling thread, for the reason
     * {@link BlobDescriptorBackend}'s own javadoc gives: the gate registers write hooks that run on the
     * cluster state thread, and a blocking write there hangs the node rather than failing. A same-thread
     * executor would keep that invisible against a local filesystem and make it real against S3.
     */
    protected InstalledDescriptorPlane installBlobBackedDescriptorPlane() throws IOException {
        return installBlobBackedDescriptorPlane(true);
    }

    /**
     * The same, with the gate's own enabled flag, for the test that asserts an installed-but-disabled gate
     * registers nothing at all.
     */
    protected InstalledDescriptorPlane installBlobBackedDescriptorPlane(boolean enabled) throws IOException {
        return installBlobBackedDescriptorPlane(enabled, java.util.function.UnaryOperator.identity());
    }

    /**
     * The same, with the backend the gate is given passed through {@code wrapForGate} first.
     *
     * <p>For tests that need to see what the gate's own hooks do to the descriptor plane -- which thread they
     * call from, or how often -- now that a creation's mapping rides its descriptor rather than going through
     * {@code MappingGenerationStore}. Only the gate's copy is wrapped: the mapping store keeps the raw
     * backend, so a test can tell a descriptor write made by a creation from one made by a mapping update.
     */
    protected InstalledDescriptorPlane installBlobBackedDescriptorPlane(
        boolean enabled,
        java.util.function.UnaryOperator<DescriptorBackend> wrapForGate
    ) throws IOException {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        Executor generic = internalCluster().getInstance(ThreadPool.class).executor(ThreadPool.Names.GENERIC);
        BlobDescriptorBackend points = new BlobDescriptorBackend(blobStore.blobContainer(BlobPath.cleanPath()), generic);
        DescriptorEnumerator prefixes = new DescriptorEnumerator(blobStore::blobContainer, BlobPath.cleanPath());
        DescriptorGate.install(
            wrapForGate.apply(points),
            prefixes,
            mappingStoreAsProductionBuildsIt(points, generic),
            new IndexBackedMappingStatsAggregator(client()),
            new StoreBackedFieldRefresher(),
            enabled
        );
        return new InstalledDescriptorPlane(points, prefixes);
    }

    /**
     * The plane installed over a container whose operations can be made to fail.
     *
     * <p>For the two tests that used to make the descriptor store unavailable by closing its system index.
     * A bucket cannot be closed, so the injection moved to the container rather than the property being
     * dropped: an unreadable store still must not read as an empty one, and a creation still must not be
     * acknowledged when its write cannot land.
     */
    protected InstalledDescriptorPlane installOverFailableContainer(FailableDescriptorContainer container) throws Exception {
        DescriptorEnumerator prefixes = new DescriptorEnumerator(path -> container, BlobPath.cleanPath());
        Executor generic = internalCluster().getInstance(ThreadPool.class).executor(ThreadPool.Names.GENERIC);
        BlobDescriptorBackend points = new BlobDescriptorBackend(container, generic);
        DescriptorGate.install(
            points,
            prefixes,
            mappingStoreAsProductionBuildsIt(points, generic),
            new IndexBackedMappingStatsAggregator(client()),
            new StoreBackedFieldRefresher(),
            true
        );
        return new InstalledDescriptorPlane(points, prefixes);
    }

    /**
     * The mapping store exactly as {@code ServerlessStoragePlugin} composes it.
     *
     * <p>This fixture used to hand the gate a bare {@link IndexBackedMappingStore}, and for a while that did
     * not matter because the gate ignored the argument and built its own descriptor-backed store. So the
     * suite was exercising a store production did not register, which is the same shape of defect as a
     * mechanism that is tested and unreachable -- and it is what let the mapping stats projection go missing
     * without a test noticing. Building it here the way the plugin builds it is the only version of this
     * fixture that can catch the next one.
     */
    private MappingGenerationStore.Store mappingStoreAsProductionBuildsIt(BlobDescriptorBackend points, Executor generic) {
        StatsProjectingMappingStore store = new StatsProjectingMappingStore(
            new DescriptorBackedMappingStore(() -> points, null),
            new IndexBackedMappingStore(client()),
            generic
        );
        installedMappingStore = store;
        return store;
    }

    private volatile StatsProjectingMappingStore installedMappingStore;

    /**
     * Lets the projection finish before the cluster is torn down.
     *
     * <p>The projection is asynchronous in production and the fixture keeps it that way, so a test can end
     * with a write still in flight against {@code .opensearch-index-mappings}. The cluster then shuts down
     * underneath it and the framework reports "shard is still locked", which points at the mapping index
     * rather than at the test that left work running. Draining here is not making the test synchronous --
     * the write still ran on {@code GENERIC} -- it is waiting for it before pulling the cluster away.
     */
    @org.junit.After
    public void drainMappingStatsProjection() {
        StatsProjectingMappingStore store = installedMappingStore;
        installedMappingStore = null;
        if (store != null) {
            store.awaitQuiescence(30_000);
        }
        // And the one the plugin installed, for the classes that let the node wire itself rather than
        // installing a plane by hand. Those were the ones this drain missed: it only knew about stores this
        // fixture built, so a projection left running by a plugin-installed store still tore the cluster
        // down underneath itself and surfaced as "shard is still locked" in a class that had nothing to do
        // with mappings.
        if (DescriptorGate.installedMappingStore() instanceof StatsProjectingMappingStore installed) {
            installed.awaitQuiescence(30_000);
        }
    }

    /** A failable container over a fresh temporary directory, for the caller to hold and flip. */
    protected FailableDescriptorContainer newFailableDescriptorContainer() throws IOException {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FailableDescriptorContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    /**
     * A backend over its own store, with nothing registered.
     *
     * <p>For the tests whose whole point is that the gate is <em>not</em> installed: writing a descriptor and
     * then finding the name still unresolvable is what production looked like before any of this existed, and
     * a helper that installed on the way past would quietly turn that assertion into its opposite.
     */
    protected BlobDescriptorBackend blobBackendWithoutInstalling() throws IOException {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new BlobDescriptorBackend(
            blobStore.blobContainer(BlobPath.cleanPath()),
            internalCluster().getInstance(ThreadPool.class).executor(ThreadPool.Names.GENERIC)
        );
    }

    /**
     * A descriptor backend with a named clock and cache capacity, for the tests that exercise the cache.
     *
     * <p>These were seams on the index-backed store and nowhere else, which is why the tests using them were
     * the last ones pinned to it. Both backends wrap the same {@code DescriptorCache}, so what they assert
     * was never index-specific -- only the constructor that reached it was.
     */
    protected BlobDescriptorBackend blobBackend(java.util.function.LongSupplier clock, int cacheCapacity) throws IOException {
        return blobBackend(clock, cacheCapacity, org.opensearch.serverless.storage.descriptor.DescriptorCache.DEFAULT_BYTES);
    }

    /** The same, additionally naming the byte budget, which is the bound that actually holds. */
    protected BlobDescriptorBackend blobBackend(java.util.function.LongSupplier clock, int cacheCapacity, long cacheBytes)
        throws IOException {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new BlobDescriptorBackend(
            blobStore.blobContainer(BlobPath.cleanPath()),
            internalCluster().getInstance(ThreadPool.class).executor(ThreadPool.Names.GENERIC),
            BlobDescriptorBackend.DEFAULT_CACHE_TTL_NANOS,
            clock,
            cacheCapacity,
            cacheBytes
        );
    }

    /**
     * A bare, hand-built {@link org.opensearch.cluster.ClusterState}, plus the same {@code
     * SupplierBackedIndexCatalog} registration a real node performs at startup (wired in {@code Node.java}
     * from {@code ClusterPlugin#getIndexCatalog()}).
     *
     * <p>Several tests in this suite build their own {@code ClusterState} directly, deliberately bypassing a
     * real cluster round-trip to test {@code IndexNameExpressionResolver}'s resolution logic in isolation.
     * Before Phase C4b of {@code core-pluggability-refactor-plan.md}, that was harmless: resolution asked
     * the static {@code AbsentIndexDescriptorSuppliers} registry directly, which does not care which {@code
     * ClusterState}/{@code Metadata} instance is in hand. Once {@code IndexNameExpressionResolver} was
     * migrated to ask {@code Metadata#indexOrResolved}/{@code #existsOrResolved} instead, that stopped being
     * true (real cluster tests here found this: {@code DescriptorGateIT}/{@code DescriptorLifecycleIT}/
     * {@code GatedWildcardVisibilityIT} all failed with a spurious {@code IndexNotFoundException} until this
     * helper existed). The catalog is node-scoped rather than per-state now, so what a hand-built state was
     * missing is a registration rather than an attachment -- but a unit-scope test still has to perform it,
     * which is what this helper does. Use this in place of {@code
     * ClusterState.builder(ClusterName.DEFAULT).build()} wherever a test needs a bare state that still
     * resolves gated names correctly.
     */
    protected static org.opensearch.cluster.ClusterState emptyClusterStateWithDescriptorResolver() {
        // One registration covers both halves, so ClusterState#getIndexRoutingTable/#resolveShard resolve a
        // gated index's computed placement the way a real applied state does: name to descriptor to
        // synthesised metadata to supplied routing.
        org.opensearch.cluster.metadata.IndexCatalogRegistry.register(new org.opensearch.cluster.metadata.SupplierBackedIndexCatalog());
        org.opensearch.cluster.metadata.Metadata metadata = org.opensearch.cluster.metadata.Metadata.builder().build();
        org.opensearch.cluster.routing.RoutingTable routingTable = org.opensearch.cluster.routing.RoutingTable.builder().build();
        return org.opensearch.cluster.ClusterState.builder(org.opensearch.cluster.ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
    }
}
