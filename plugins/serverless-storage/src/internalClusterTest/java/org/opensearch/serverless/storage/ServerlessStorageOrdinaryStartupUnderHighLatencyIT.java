/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.serverless.storage.benchmark.LatencyInjectingBlobContainer;
import org.opensearch.serverless.storage.benchmark.LatencyProfile;
import org.opensearch.serverless.storage.security.action.NodeObjectStoreRequestStatsAction;
import org.opensearch.serverless.storage.security.action.NodeObjectStoreRequestStatsRequest;
import org.opensearch.serverless.storage.security.action.NodeObjectStoreRequestStatsResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * The diagnostic investigation {@code ServerlessStorageReactivationUnderLatencyIT}'s own javadoc
 * left explicitly open: "whether that blob-op count/sequencing during ordinary startup itself
 * scales to degraded object-store latency is an open question worth its own future investigation
 * -- this IT sidesteps it by using {@code TYPICAL} instead." This class answers that question
 * directly, using the exact same {@code LatencyProfile.HIGH} that made an earlier draft of that
 * other test fail to reach green within 120s.
 *
 * <p>Reuses the same {@code LatencyInjectingBlobContainer}-wrapped test-only repository seam {@code
 * ServerlessStorageReactivationUnderLatencyIT} established, just with {@link LatencyProfile#HIGH}
 * instead of {@link LatencyProfile#TYPICAL}, and additionally reads real per-shape object-store
 * request counts via {@link NodeObjectStoreRequestStatsAction} (rfc-serverless-opensearch.md
 * &sect;18 risk #1) so this test's own log line reports not just how long ordinary startup took
 * under {@code HIGH}, but how many real blob-shaped requests it actually cost -- the "sequencing"
 * half of the open question, not just the "count" half.
 *
 * <p>{@link #testShardRecoveryWithManySegmentFilesUnderSimulatedHighBlobLatency} closes the
 * remaining half of that same open question: whether the per-file blob-op count/sequencing
 * observed for a single-segment shard above actually scales to a shard with hundreds of real
 * segment files, not just whether ordinary startup and single-segment recovery complete at all.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageOrdinaryStartupUnderHighLatencyIT extends ServerlessStorageIntegTestCase {

    private static final String IDX = "ordinary-startup-under-high-latency-it-idx";
    private static final String REPO_NAME = "ordinary-startup-under-high-latency-it-repo";
    private static final String REPO_TYPE = "high-latency-fs";
    // Generous on purpose: this test's own point is to observe and report real elapsed time under
    // HIGH, not to enforce a tight SLA -- a real regression (startup that no longer completes at
    // all) still fails this bound, but a merely slow-but-working result is exactly the finding this
    // diagnostic exists to surface via its own log line, not something to fail the build over.
    private static final TimeValue STARTUP_BOUND = TimeValue.timeValueSeconds(180);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ServerlessStoragePlugin.class, HighLatencyRepositoryPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testOrdinaryShardStartupUnderSimulatedHighBlobLatency() throws Exception {
        Path repoPath = createTempDir("serverless-storage-ordinary-startup-under-high-latency-it-repo");
        Path unusedLocalBasePath = createTempDir("serverless-storage-ordinary-startup-under-high-latency-it-unused-local");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", repoPath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), unusedLocalBasePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_REPOSITORY_SETTING.getKey(), REPO_NAME)
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        String dataNodeName = internalCluster().startDataOnlyNode(nodeSettings);

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

        // Baseline: this node's own real object-store request counts before this index (and
        // therefore its shard) has ever been created, so the delta below is attributable entirely
        // to this one shard's own ordinary startup, not repository-registration overhead.
        NodeObjectStoreRequestStatsResponse before = internalCluster().client(dataNodeName)
            .execute(NodeObjectStoreRequestStatsAction.INSTANCE, new NodeObjectStoreRequestStatsRequest())
            .get();

        long start = System.nanoTime();
        createIndex(
            IDX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        // Ordinary startup only -- no suspend/reactivate cycle, no ingest -- deliberately isolating
        // exactly the "how many blob ops does a shard's own first-ever open cost, and how long does
        // that take under degraded latency" question this test exists to answer.
        ensureGreen(STARTUP_BOUND, IDX);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        NodeObjectStoreRequestStatsResponse after = internalCluster().client(dataNodeName)
            .execute(NodeObjectStoreRequestStatsAction.INSTANCE, new NodeObjectStoreRequestStatsRequest())
            .get();

        long putDelta = after.putCount() - before.putCount();
        long getDelta = after.getCount() - before.getCount();
        long listDelta = after.listCount() - before.listCount();
        long deleteDelta = after.deleteCount() - before.deleteCount();
        logger.info(
            "ordinary (non-suspended) shard startup under simulated HIGH blob latency took {} ms and cost "
                + "{} PUTs, {} GETs, {} LISTs, {} DELETEs (bounded at {})",
            elapsedMillis,
            putDelta,
            getDelta,
            listDelta,
            deleteDelta,
            STARTUP_BOUND
        );

        // The real pass/fail criterion is ensureGreen(STARTUP_BOUND, IDX) above: it throws (failing
        // this test) if startup doesn't reach green within the bound, rather than waiting
        // indefinitely -- reaching this line at all is already proof startup completed in time.
        client().prepareIndex(IDX).setSource("field", "value").get();
        refresh(IDX);
        assertEquals(1L, client().prepareSearch(IDX).setSize(0).get().getHits().getTotalHits().value());
    }

    /**
     * The more representative half of the open question this class answers: a brand-new empty
     * shard's own first-ever open (the test above) has nothing to materialize yet, so it's a
     * degenerate case -- cheap almost by construction. The scenario this section's own "startup
     * does dozens of small sequential blob writes (segment files, WAL chunks)" concern actually
     * describes is recovery of a shard that already has real durable data: a fresh node picking up
     * a primary after its old node died, materializing an existing manifest's real segment files
     * from the object store rather than opening an empty {@code Directory}. Mirrors {@code
     * ServerlessStorageWriterFailoverIT#testWriterShardSurvivesItsNodeBeingKilled}'s own real
     * cross-node-kill mechanics, but through this class's {@code HIGH}-latency-injecting repository
     * instead of that test's near-zero-latency local {@code base_path}.
     */
    public void testShardRecoveryOntoAFreshNodeUnderSimulatedHighBlobLatency() throws Exception {
        Path repoPath = createTempDir("serverless-storage-recovery-under-high-latency-it-repo");
        Path unusedLocalBasePath = createTempDir("serverless-storage-recovery-under-high-latency-it-unused-local");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", repoPath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), unusedLocalBasePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_REPOSITORY_SETTING.getKey(), REPO_NAME)
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);
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
            IDX + "-recovery",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(STARTUP_BOUND, IDX + "-recovery");

        client().prepareIndex(IDX + "-recovery").setId("1").setSource("field", "value1").get();
        // Publishing (rfc-serverless-opensearch.md &sect;7.1) only happens on flush -- this is what
        // gives the survivor node a real, durable manifest (and real segment files) to materialize.
        client().admin().indices().prepareFlush(IDX + "-recovery").get();

        String primaryNodeId = internalCluster().clusterService()
            .state()
            .routingTable()
            .index(IDX + "-recovery")
            .shard(0)
            .primaryShard()
            .currentNodeId();
        String primaryNodeName = internalCluster().clusterService().state().nodes().get(primaryNodeId).getName();

        NodeObjectStoreRequestStatsResponse survivorBefore = internalCluster().client(survivorNodeName(primaryNodeName))
            .execute(NodeObjectStoreRequestStatsAction.INSTANCE, new NodeObjectStoreRequestStatsRequest())
            .get();

        long start = System.nanoTime();
        internalCluster().stopRandomNode(settings -> primaryNodeName.equals(settings.get("node.name")));
        // The survivor node starts with a completely empty local Store -- this ensureGreen only
        // succeeds once it has fully materialized the flushed manifest's real segment files from
        // the (simulated HIGH-latency) object store, exactly the "segment files" half of the
        // concern this class exists to investigate.
        ensureGreen(STARTUP_BOUND, IDX + "-recovery");
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        NodeObjectStoreRequestStatsResponse survivorAfter = internalCluster().client(survivorNodeName(primaryNodeName))
            .execute(NodeObjectStoreRequestStatsAction.INSTANCE, new NodeObjectStoreRequestStatsRequest())
            .get();

        logger.info(
            "cross-node shard recovery (existing flushed data) under simulated HIGH blob latency took {} ms and cost "
                + "{} PUTs, {} GETs, {} LISTs, {} DELETEs on the survivor node (bounded at {})",
            elapsedMillis,
            survivorAfter.putCount() - survivorBefore.putCount(),
            survivorAfter.getCount() - survivorBefore.getCount(),
            survivorAfter.listCount() - survivorBefore.listCount(),
            survivorAfter.deleteCount() - survivorBefore.deleteCount(),
            STARTUP_BOUND
        );

        refresh(IDX + "-recovery");
        assertHitCount(client().prepareSearch(IDX + "-recovery").setSize(0).get(), 1);
    }

    /**
     * The previous test's shard has nothing but a single tiny segment to materialize -- a
     * degenerate case that doesn't actually exercise this section's own "startup does dozens of
     * small sequential blob writes (segment files, WAL chunks)" concern about many-file recovery.
     * This test forces a real many-segment commit (merges disabled via generous {@code
     * index.merge.policy} tuning, one segment per individually-refreshed document) so the flushed
     * manifest this survivor node materializes genuinely references hundreds of real segment files,
     * not one -- the actual scaling question {@code ServerlessStorageReactivationUnderLatencyIT}'s
     * own javadoc left open.
     */
    public void testShardRecoveryWithManySegmentFilesUnderSimulatedHighBlobLatency() throws Exception {
        // Generous on purpose, same reasoning as STARTUP_BOUND: this test's real bound-breaking
        // failure mode is startup that never completes, not a merely-slow-but-working result --
        // hundreds of sequential per-file GETs under HIGH easily dwarfs STARTUP_BOUND's 180s.
        TimeValue manySegmentStartupBound = TimeValue.timeValueSeconds(300);
        int documentCount = 100;

        Path repoPath = createTempDir("serverless-storage-recovery-many-segments-under-high-latency-it-repo");
        Path unusedLocalBasePath = createTempDir("serverless-storage-recovery-many-segments-under-high-latency-it-unused-local");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", repoPath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), unusedLocalBasePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_REPOSITORY_SETTING.getKey(), REPO_NAME)
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);
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

        String manySegmentsIdx = IDX + "-many-segments";
        createIndex(
            manySegmentsIdx,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                // Merges deliberately suppressed: the whole point is a many-segment commit, so
                // TieredMergePolicy must never consolidate the per-document segments below into one.
                .put("index.merge.policy.segments_per_tier", (double) (documentCount * 10))
                .put("index.merge.policy.max_merge_at_once", documentCount * 10)
                .build()
        );
        ensureGreen(STARTUP_BOUND, manySegmentsIdx);

        for (int i = 0; i < documentCount; i++) {
            client().prepareIndex(manySegmentsIdx).setId(Integer.toString(i)).setSource("field", "value" + i).get();
            // One segment per document: this loop's whole purpose is producing a many-segment
            // commit, not testing indexing throughput, so an individual refresh after every write
            // is deliberate, not an oversight.
            client().admin().indices().prepareRefresh(manySegmentsIdx).get();
        }
        // Publishing (rfc-serverless-opensearch.md &sect;7.1) only happens on flush -- a single
        // flush here bundles every one of the documentCount un-merged segments above into one real,
        // durable, many-segment-file manifest for the survivor node to materialize below.
        client().admin().indices().prepareFlush(manySegmentsIdx).get();

        String primaryNodeId = internalCluster().clusterService()
            .state()
            .routingTable()
            .index(manySegmentsIdx)
            .shard(0)
            .primaryShard()
            .currentNodeId();
        String primaryNodeName = internalCluster().clusterService().state().nodes().get(primaryNodeId).getName();

        NodeObjectStoreRequestStatsResponse survivorBefore = internalCluster().client(survivorNodeName(primaryNodeName))
            .execute(NodeObjectStoreRequestStatsAction.INSTANCE, new NodeObjectStoreRequestStatsRequest())
            .get();

        long start = System.nanoTime();
        internalCluster().stopRandomNode(settings -> primaryNodeName.equals(settings.get("node.name")));
        // The survivor node starts with a completely empty local Store -- this ensureGreen only
        // succeeds once it has fully materialized every one of the many flushed segment files from
        // the (simulated HIGH-latency) object store, the actual "scales to hundreds of segment
        // files" question this test exists to answer.
        ensureGreen(manySegmentStartupBound, manySegmentsIdx);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        NodeObjectStoreRequestStatsResponse survivorAfter = internalCluster().client(survivorNodeName(primaryNodeName))
            .execute(NodeObjectStoreRequestStatsAction.INSTANCE, new NodeObjectStoreRequestStatsRequest())
            .get();

        logger.info(
            "cross-node shard recovery ({} pre-flushed un-merged segments) under simulated HIGH blob latency took "
                + "{} ms and cost {} PUTs, {} GETs, {} LISTs, {} DELETEs on the survivor node (bounded at {})",
            documentCount,
            elapsedMillis,
            survivorAfter.putCount() - survivorBefore.putCount(),
            survivorAfter.getCount() - survivorBefore.getCount(),
            survivorAfter.listCount() - survivorBefore.listCount(),
            survivorAfter.deleteCount() - survivorBefore.deleteCount(),
            manySegmentStartupBound
        );

        refresh(manySegmentsIdx);
        assertHitCount(client().prepareSearch(manySegmentsIdx).setSize(0).get(), documentCount);
    }

    /** The data node name that ISN'T {@code deadNodeName} -- both data nodes started by {@link #testShardRecoveryOntoAFreshNodeUnderSimulatedHighBlobLatency} and {@link #testShardRecoveryWithManySegmentFilesUnderSimulatedHighBlobLatency} are equally-eligible candidates, but only one is actually still alive by the time this is called. */
    private String survivorNodeName(String deadNodeName) {
        for (String nodeName : internalCluster().getNodeNames()) {
            if (nodeName.equals(deadNodeName) == false && nodeName.equals(internalCluster().getClusterManagerName()) == false) {
                return nodeName;
            }
        }
        throw new AssertionError("no surviving data node found");
    }

    /** Same shape as {@code ServerlessStorageReactivationUnderLatencyIT.LatencyRepository}, just parameterized with {@link LatencyProfile#HIGH}. */
    public static class HighLatencyRepository extends FsRepository {

        public HighLatencyRepository(
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
            return new HighLatencyInjectingBlobStore(super.createBlobStore());
        }
    }

    /** Wraps every {@link BlobContainer} a delegate {@link BlobStore} hands out in a {@link LatencyProfile#HIGH}-configured {@link LatencyInjectingBlobContainer}. */
    private static final class HighLatencyInjectingBlobStore implements BlobStore {

        private final BlobStore delegate;

        HighLatencyInjectingBlobStore(BlobStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new LatencyInjectingBlobContainer(delegate.blobContainer(path), LatencyProfile.HIGH);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /** Registers {@link HighLatencyRepository} under the {@value #REPO_TYPE} repository type. */
    public static class HighLatencyRepositoryPlugin extends Plugin implements RepositoryPlugin {

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                REPO_TYPE,
                metadata -> new HighLatencyRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings)
            );
        }
    }
}
