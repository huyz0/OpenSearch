/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.serverless.storage.descriptor.BlobDescriptorBackend;
import org.opensearch.serverless.storage.descriptor.DescriptorEnumerator;
import org.opensearch.serverless.storage.descriptor.DescriptorGate;
import org.opensearch.serverless.storage.descriptor.FailableDescriptorContainer;
import org.opensearch.serverless.storage.descriptor.IndexBackedMappingStatsAggregator;
import org.opensearch.serverless.storage.descriptor.IndexBackedMappingStore;
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
     * <p>Every test here used to install an index-backed {@code DescriptorStore} instead, so the suite
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
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        Executor generic = internalCluster().getInstance(ThreadPool.class).executor(ThreadPool.Names.GENERIC);
        BlobDescriptorBackend points = new BlobDescriptorBackend(blobStore.blobContainer(BlobPath.cleanPath()), generic);
        DescriptorEnumerator prefixes = new DescriptorEnumerator(blobStore::blobContainer, BlobPath.cleanPath());
        DescriptorGate.install(
            points,
            prefixes,
            new IndexBackedMappingStore(client()),
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
        BlobDescriptorBackend points = new BlobDescriptorBackend(
            container,
            internalCluster().getInstance(ThreadPool.class).executor(ThreadPool.Names.GENERIC)
        );
        DescriptorGate.install(
            points,
            prefixes,
            new IndexBackedMappingStore(client()),
            new IndexBackedMappingStatsAggregator(client()),
            new StoreBackedFieldRefresher(),
            true
        );
        return new InstalledDescriptorPlane(points, prefixes);
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
}
