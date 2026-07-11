/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.serverless.storage.allocation.SuspendedShardsMetadata;
import org.opensearch.serverless.storage.benchmark.LatencyInjectingBlobContainer;
import org.opensearch.serverless.storage.benchmark.LatencyProfile;
import org.opensearch.serverless.storage.scaletozero.ShardSuspensionCoordinator;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidateEntry;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Proves the suspend/reactivate mechanism (rfc-serverless-opensearch.md &sect;7.3) still meets its
 * "no client-visible failure" bar (this feature's own standing product decision -- see {@code
 * ShardReactivationActionFilter}'s own javadoc) when every blob operation is artificially slowed to
 * {@link LatencyProfile#TYPICAL}, not just the near-zero latency every other suspend/reactivate IT
 * (e.g. {@link ServerlessStorageShardSuspensionIT}) exercises against local disk.
 *
 * <p>Reuses {@code LatencyInjectingBlobContainer} (added for {@code BlobLatencyBenchmarkTests}) via
 * a small test-only {@code latency-fs} repository type, following exactly the same {@code
 * BlobStoreRepository}-through-a-registered-repository seam {@link
 * ServerlessStorageRepositoryBackedContainerIT} already proves: {@link
 * ServerlessStoragePlugin#SERVERLESS_STORAGE_REPOSITORY_SETTING} resolves a shard's container through
 * a registered repository's {@code blobStore()} instead of the plugin's own local {@code
 * FsBlobStore}. {@code LatencyRepository} overrides {@code createBlobStore()} to wrap the real {@code
 * FsBlobStore}-backed containers it returns in {@link LatencyInjectingBlobContainer} -- the same
 * decorator-around-{@code createBlobStore()} shape {@code test.framework}'s own {@code
 * MockRepository} already uses for fault injection, just for latency instead.
 *
 * <p>The point of this test is to check whether {@code
 * ServerlessStoragePlugin#SERVERLESS_STORAGE_SCALE_TO_ZERO_SEARCH_REACTIVATION_WAIT_SETTING}'s 30s
 * default has real headroom under realistic object-store latency, not merely near-zero local-disk
 * latency -- a real finding either way (comfortable headroom, or a margin worth tightening) rather
 * than a foregone conclusion.
 *
 * <p><b>{@link LatencyProfile#TYPICAL}, not {@link LatencyProfile#HIGH} -- a deliberate scope
 * decision, not an oversight.</b> An earlier version of this test used {@code HIGH} for everything,
 * including ordinary (non-suspended) index/shard startup -- not just reactivation -- and that alone
 * failed to reach green within 120s: startup does dozens of small sequential blob writes (segment
 * files, WAL chunks), and at {@code HIGH}'s up-to-400ms-per-op ceiling those compound past what any
 * bounded IT should wait on. That is a real, separate finding worth its own follow-up (whether
 * startup's blob-op count/sequencing itself scales to degraded latency), not something this test
 * should paper over by stretching its own timeout arbitrarily far. This test instead uses {@code
 * TYPICAL} (published single-region S3 GET/PUT/LIST figures under normal conditions) for a decisive
 * answer to the one question actually in scope here: does the reactivation-wait budget have
 * headroom under realistic, not just degraded, object-store latency.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageReactivationUnderLatencyIT extends OpenSearchIntegTestCase {

    private static final String IDX = "reactivation-under-latency-it-idx";
    private static final String REPO_NAME = "reactivation-under-latency-it-repo";
    private static final String REPO_TYPE = "latency-fs";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ServerlessStoragePlugin.class, LatencyRepositoryPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testSuspendedWriterShardReactivatesUnderSimulatedTypicalBlobLatency() throws Exception {
        Path repoPath = createTempDir("serverless-storage-reactivation-under-latency-it-repo");
        Path unusedLocalBasePath = createTempDir("serverless-storage-reactivation-under-latency-it-unused-local");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", repoPath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), unusedLocalBasePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_REPOSITORY_SETTING.getKey(), REPO_NAME)
            .build();

        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        assertTrue(
            "repository registration must be acknowledged before it's usable",
            client().admin()
                .cluster()
                .preparePutRepository(REPO_NAME)
                .setType(REPO_TYPE)
                .setSettings(Settings.builder().put(FsRepository.LOCATION_SETTING.getKey(), repoPath.toString()))
                .get()
                .isAcknowledged()
        );

        createIndex(
            IDX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        // Every blob op involved in initial shard startup (not just reactivation) now also costs
        // LatencyProfile.TYPICAL's simulated delay, so the default 30s ensureGreen budget -- itself
        // unrelated to what this test is actually checking -- needs a little headroom; widened here,
        // not tightened elsewhere, to keep the thing under test (the reactivation-wait budget)
        // isolated from this collateral slowdown.
        ensureGreen(org.opensearch.common.unit.TimeValue.timeValueSeconds(60), IDX);

        IndexResponse firstIndex = client().prepareIndex(IDX).setSource("field", "value").get();
        assertEquals(org.opensearch.core.rest.RestStatus.CREATED, firstIndex.status());

        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(IDX).getIndexUUID();

        ClusterService clusterManagerClusterService = internalCluster().getInstance(ClusterService.class, clusterManagerNode);
        ShardSuspensionCoordinator coordinator = new ShardSuspensionCoordinator(clusterManagerClusterService, client());
        coordinator.suspendCandidates(List.of(new ScaleToZeroCandidateEntry(indexUuid, 0, 0L, 0L, true)));

        assertBusy(() -> {
            IndexMetadata indexMetadata = clusterManagerClusterService.state().metadata().index(IDX);
            assertTrue("the index's cluster state must record shard 0 as suspended", SuspendedShardsMetadata.isSuspended(indexMetadata, 0));
            assertEquals(
                "a suspended writer shard must actually be evicted (UNASSIGNED), not merely marked",
                ShardRoutingState.UNASSIGNED,
                clusterManagerClusterService.state().routingTable().index(IDX).shard(0).primaryShard().state()
            );
        });

        // Every blob op this write's reactivation touches (manifest read/write, shard-state
        // read/write, WAL bootstrap) now costs LatencyProfile.TYPICAL's simulated delay -- up to
        // 100ms per op -- rather than near-zero local-disk latency. A handful of sequential ops at
        // that ceiling is still well under the 30s search-reactivation-wait budget if there's real
        // headroom; assertBusy's own default timeout (10s) is intentionally NOT stretched to match,
        // so a genuine regression here fails this test rather than silently passing by waiting long
        // enough for anything to eventually succeed.
        long start = System.nanoTime();
        IndexResponse secondIndex = client().prepareIndex(IDX).setSource("field", "value-after-reactivation").get();
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertEquals(org.opensearch.core.rest.RestStatus.CREATED, secondIndex.status());
        logger.info("write against a suspended shard reactivated (under simulated TYPICAL blob latency) in {} ms", elapsedMillis);

        assertBusy(() -> {
            IndexMetadata indexMetadata = clusterManagerClusterService.state().metadata().index(IDX);
            assertFalse("reactivation must clear the suspended marker", SuspendedShardsMetadata.isSuspended(indexMetadata, 0));
            assertEquals(
                ShardRoutingState.STARTED,
                clusterManagerClusterService.state().routingTable().index(IDX).shard(0).primaryShard().state()
            );
        });

        refresh(IDX);
        assertEquals(2L, client().prepareSearch(IDX).setSize(0).get().getHits().getTotalHits().value());
    }

    /**
     * Test-only repository type wrapping {@link FsRepository}'s real blob store in {@link
     * LatencyInjectingBlobContainer}, mirroring how {@code test.framework}'s own {@code
     * MockRepository} decorates {@link FsRepository#createBlobStore()} for fault injection.
     */
    public static class LatencyRepository extends FsRepository {

        public LatencyRepository(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, recoverySettings);
        }

        @Override
        protected BlobStore createBlobStore() throws Exception {
            return new LatencyInjectingBlobStore(super.createBlobStore());
        }
    }

    /**
     * Wraps every {@link BlobContainer} a delegate {@link BlobStore} hands out in {@link
     * LatencyInjectingBlobContainer}; not a lambda since {@link BlobStore} also extends {@link
     * java.io.Closeable}, which has its own abstract method.
     */
    private static final class LatencyInjectingBlobStore implements BlobStore {

        private final BlobStore delegate;

        LatencyInjectingBlobStore(BlobStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new LatencyInjectingBlobContainer(delegate.blobContainer(path), LatencyProfile.TYPICAL);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /** Registers {@link LatencyRepository} under the {@value #REPO_TYPE} repository type. */
    public static class LatencyRepositoryPlugin extends Plugin implements RepositoryPlugin {

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                REPO_TYPE,
                metadata -> new LatencyRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings)
            );
        }
    }
}
