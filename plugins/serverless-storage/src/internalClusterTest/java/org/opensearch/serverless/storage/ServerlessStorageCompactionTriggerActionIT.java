/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.compaction.action.CompactionTriggerAction;
import org.opensearch.serverless.storage.compaction.action.CompactionTriggerRequest;
import org.opensearch.serverless.storage.compaction.action.CompactionTriggerResponse;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Proves {@link CompactionTriggerAction} genuinely works over the transport layer, in a real
 * cluster -- not just a direct unit-level call to {@code CompactionSchedulerTask.maybeCompact} --
 * covering the Guice injection of {@link ServerlessStoragePlugin} into {@code
 * TransportCompactionTriggerAction} and the {@code ThreadPool.Names#GENERIC} dispatch, both of
 * which only exist once the action actually runs inside a real node (rfc-serverless-opensearch.md
 * &sect;7.4, &sect;16 Phase 4.5).
 *
 * <p><b>Also proves {@code TransportCompactionTriggerAction}'s shard-existence precondition</b>:
 * the index used here is a real, cluster-created {@link IndexMetadata} and the trigger is issued
 * against its real UUID, not an arbitrary string. The {@code indexUuid} the request carries becomes
 * a blob-path segment, so accepting any string at all made the path -- rather than the shard -- the
 * caller's to choose; the negative tests below prove a uuid naming no real index is now rejected
 * loudly and immediately.
 *
 * <p>This cluster deliberately never starts a data node: the index created below stays permanently
 * unassigned, so nothing ever opens a real engine against it. That's essential here, not
 * incidental -- this test writes a shard's manifests and head directly into its object-store
 * location, bypassing any real {@code IndexShard}; a real writer engine racing to activate the same
 * shard would corrupt that synthetic state via conflicting CAS writes. The transport action itself
 * needs no data node (it is pure object-store work -- see its own javadoc), exactly as {@link
 * ServerlessStorageShardSplitActionIT} already relies on.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageCompactionTriggerActionIT extends ServerlessStorageIntegTestCase {

    private static final String INDEX_NAME = "compaction-trigger-it-idx";
    private static final int SHARD_ID = 0;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    private static BlobContainer blobContainerFor(Path basePath, String indexUuid, int shardId) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024 * 1024, basePath, false);
        BlobPath shardPath = BlobPath.cleanPath().add(indexUuid).add(String.valueOf(shardId));
        return blobStore.blobContainer(shardPath);
    }

    /**
     * Creates a real serverless-storage index with one shard, deliberately never assigned to any
     * node (this cluster has no data node) -- see this class's own javadoc for why that matters.
     *
     * @param name the index name to create.
     * @return the index's real, cluster-assigned UUID.
     */
    private String createUnassignedServerlessIndex(String name) {
        client().admin()
            .indices()
            .prepareCreate(name)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
            )
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .get();
        return client().admin().cluster().prepareState().get().getState().metadata().index(name).getIndexUUID();
    }

    public void testCompactionTriggerActionMergesARealFragmentedShardOverTransport() throws Exception {
        Path basePath = createTempDir("serverless-storage-compaction-trigger-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);

        String indexUuid = createUnassignedServerlessIndex(INDEX_NAME);
        BlobContainer container = blobContainerFor(basePath, indexUuid, SHARD_ID);
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(container);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(container);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);

        // Ten separate flushes from the same continuing writer -- ten real, separate Lucene
        // segments -- well past CompactionPolicy.withDefaults()'s minSegmentCountToCompact (10),
        // so this shard is a genuine compaction candidate, not a synthetic one.
        CommitManifest latestManifest = null;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (
                IndexWriter writer = new IndexWriter(
                    writerDirectory,
                    new IndexWriterConfig().setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE)
                )
            ) {
                for (int generation = 1; generation <= 10; generation++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", Integer.toString(generation), Field.Store.YES));
                    writer.addDocument(doc);
                    writer.commit();
                    SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
                    latestManifest = publisher.publishCommit(
                        writerDirectory,
                        segmentInfos,
                        indexUuid,
                        SHARD_ID,
                        1,
                        generation,
                        generation - 1,
                        generation - 1,
                        new WalPosition("epoch-0", 0),
                        0,
                        PruningStats.empty()
                    );
                    if (generation == 1) {
                        assertEquals(
                            CasResult.SUCCESS,
                            shardStateStore.compareAndSet(
                                indexUuid,
                                SHARD_ID,
                                Optional.empty(),
                                new ShardHead(1, null, 0L, latestManifest.generation())
                            )
                        );
                    } else {
                        long currentVersion = shardStateStore.get(indexUuid, SHARD_ID).orElseThrow().version();
                        assertEquals(
                            CasResult.SUCCESS,
                            shardStateStore.compareAndSet(
                                indexUuid,
                                SHARD_ID,
                                Optional.of(currentVersion),
                                new ShardHead(1, null, 0L, latestManifest.generation())
                            )
                        );
                    }
                }
            }
        }
        assertEquals(10, org.opensearch.serverless.storage.compaction.ManifestSegmentMetrics.from(latestManifest).segmentCount);

        CompactionTriggerResponse response = client().execute(
            CompactionTriggerAction.INSTANCE,
            new CompactionTriggerRequest(indexUuid, SHARD_ID)
        ).get();
        assertTrue("a 10-segment shard must be recognized as a compaction candidate", response.attempted());

        VersionedShardHead afterCompaction = shardStateStore.get(indexUuid, SHARD_ID).orElseThrow();
        assertTrue(
            "compaction must have published a strictly newer generation",
            afterCompaction.head().latestManifestGeneration() > latestManifest.generation()
        );
        CommitManifest compactedManifest = manifestStore.readManifest(1, afterCompaction.head().latestManifestGeneration());
        assertEquals(
            "the ten fragmented segments must have been merged down to one",
            1,
            org.opensearch.serverless.storage.compaction.ManifestSegmentMetrics.from(compactedManifest).segmentCount
        );
    }

    public void testCompactionTriggerActionIsANoOpForAShardThatWasNeverActivated() throws Exception {
        Path basePath = createTempDir("serverless-storage-compaction-trigger-it-never-activated");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);

        // A real index whose shard has simply never published a head. "No head yet" and "no such
        // shard" are genuinely different answers and must not collapse into one: this one is a safe
        // no-op, the one below is an error.
        String indexUuid = createUnassignedServerlessIndex(INDEX_NAME);

        CompactionTriggerResponse response = client().execute(
            CompactionTriggerAction.INSTANCE,
            new CompactionTriggerRequest(indexUuid, SHARD_ID)
        ).get();
        assertFalse("a shard with no published head must be a safe no-op, never an error", response.attempted());
    }

    /**
     * The traversal-shaped case. {@code indexUuid} is concatenated into a blob path
     * ({@code BlobPath.cleanPath().add(indexUuid)}) whose segments are resolved without
     * normalisation, so before the metadata check a {@code ../..} uuid genuinely walked out of the
     * repository base on a filesystem repository. It must be refused, and refused before any
     * object-store work happens.
     */
    public void testCompactionTriggerActionRejectsATraversalShapedIndexUuid() throws Exception {
        Path basePath = createTempDir("serverless-storage-compaction-trigger-it-traversal");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);

        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(CompactionTriggerAction.INSTANCE, new CompactionTriggerRequest("../../../etc", SHARD_ID)).get()
        );
        assertTrue(
            "a traversal-shaped uuid must be refused, not resolved into a blob path: " + failure.getCause(),
            failure.getCause() instanceof IllegalArgumentException
        );
    }

    /**
     * The plausible-looking case, which no character check would ever catch: a well-formed string
     * that simply names no index in this cluster. Only resolving it through cluster metadata can
     * tell the difference, which is why that -- not a charset test -- is the actual control.
     */
    public void testCompactionTriggerActionRejectsAnIndexUuidThatNamesNoRealIndex() throws Exception {
        Path basePath = createTempDir("serverless-storage-compaction-trigger-it-unknown-uuid");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        createUnassignedServerlessIndex(INDEX_NAME);

        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(CompactionTriggerAction.INSTANCE, new CompactionTriggerRequest("not-a-real-index-uuid", SHARD_ID)).get()
        );
        assertTrue(
            "must fail with the shard-existence precondition error, not something else: " + failure.getCause(),
            failure.getCause() instanceof IllegalArgumentException
        );
        assertTrue(failure.getCause().getMessage().contains("does not exist"));
    }

    /**
     * The unrelated-but-real case: a uuid that resolves to a genuine index, but names a shard that
     * index does not have. Without the bound this reached another index's shard directory with no
     * scoping at all.
     */
    public void testCompactionTriggerActionRejectsAShardIdOutOfBoundsForARealIndex() throws Exception {
        Path basePath = createTempDir("serverless-storage-compaction-trigger-it-oob");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);

        // A real index, but it only has one shard (id 0) -- shard id 5 is out of bounds for it.
        String indexUuid = createUnassignedServerlessIndex(INDEX_NAME);

        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(CompactionTriggerAction.INSTANCE, new CompactionTriggerRequest(indexUuid, 5)).get()
        );
        assertTrue(
            "must fail with the shard-existence precondition error, not something else: " + failure.getCause(),
            failure.getCause() instanceof IllegalArgumentException
        );
        assertTrue(failure.getCause().getMessage().contains("out of bounds"));
    }
}
