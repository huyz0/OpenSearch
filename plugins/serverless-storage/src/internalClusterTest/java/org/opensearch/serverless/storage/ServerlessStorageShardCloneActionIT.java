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
import org.opensearch.serverless.storage.clone.BlobContainerCloneLineageStore;
import org.opensearch.serverless.storage.clone.action.ShardCloneAction;
import org.opensearch.serverless.storage.clone.action.ShardCloneRequest;
import org.opensearch.serverless.storage.clone.action.ShardCloneResponse;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
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
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Proves {@link ShardCloneAction} genuinely works over the transport layer, in a real cluster --
 * not just a direct unit-level call to {@code ShardCloner.clone} -- covering the Guice injection
 * of {@link ServerlessStoragePlugin} into {@code TransportShardCloneAction} and the {@code
 * ThreadPool.Names#GENERIC} dispatch, both of which only exist once the action actually runs
 * inside a real node (rfc-serverless-opensearch.md &sect;14).
 *
 * <p><b>Also proves {@code TransportShardCloneAction}'s provisioning-precondition check</b>: both
 * indices used here are real, cluster-created {@link IndexMetadata}s, not arbitrary strings. Each
 * {@code indexUuid} becomes a blob-path segment, so accepting any string at all made the path --
 * rather than the shard -- the caller's to choose; the negative tests below prove a uuid naming no
 * real index is rejected loudly and immediately, rather than silently writing object-store state
 * nothing could ever open. This still does not create or allocate the target shard: an operator
 * must create the real target index first, exactly as {@link ServerlessStorageShardSplitActionIT}
 * already requires for the split this is modelled on.
 *
 * <p>This cluster deliberately never starts a data node: every index created below stays
 * permanently unassigned, so nothing ever opens a real engine against it. That's essential here,
 * not incidental -- this test writes a shard's manifest and head directly into its object-store
 * location, bypassing any real {@code IndexShard}; a real writer engine racing to activate the same
 * shard would corrupt that synthetic state via conflicting CAS writes.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageShardCloneActionIT extends ServerlessStorageIntegTestCase {

    private static final String SOURCE_INDEX_NAME = "action-it-source-idx";
    private static final String TARGET_INDEX_NAME = "action-it-target-idx";
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

    public void testShardCloneActionClonesARealPublishedSourceOverTransport() throws Exception {
        Path basePath = createTempDir("serverless-storage-clone-action-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);

        String sourceIndexUuid = createUnassignedServerlessIndex(SOURCE_INDEX_NAME);
        String targetIndexUuid = createUnassignedServerlessIndex(TARGET_INDEX_NAME);

        BlobContainer sourceContainer = blobContainerFor(basePath, sourceIndexUuid, SHARD_ID);
        BlobContainerBundleStore sourceBundleStore = new BlobContainerBundleStore(sourceContainer);
        BlobContainerManifestStore sourceManifestStore = new BlobContainerManifestStore(sourceContainer);
        ShardStateStore sourceShardStateStore = new BlobContainerShardStateStore(sourceContainer);
        DurablePinRegistry sourcePinRegistry = new BlobContainerDurablePinRegistry(sourceContainer);

        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(sourceBundleStore, sourceManifestStore);
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            CommitManifest sourceManifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                sourceIndexUuid,
                SHARD_ID,
                1,
                1,
                1,
                1,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
            assertEquals(
                CasResult.SUCCESS,
                sourceShardStateStore.compareAndSet(
                    sourceIndexUuid,
                    SHARD_ID,
                    Optional.empty(),
                    new ShardHead(1, null, 0L, sourceManifest.generation())
                )
            );
        }

        ShardCloneResponse response = client().execute(
            ShardCloneAction.INSTANCE,
            new ShardCloneRequest(sourceIndexUuid, SHARD_ID, targetIndexUuid, SHARD_ID)
        ).get();
        assertTrue(response.acknowledged());

        Set<org.opensearch.serverless.storage.retention.PinRecord> pins = sourcePinRegistry.getPins(sourceIndexUuid, SHARD_ID);
        assertEquals(1, pins.size());

        BlobContainer targetContainer = blobContainerFor(basePath, targetIndexUuid, SHARD_ID);
        ShardStateStore targetShardStateStore = new BlobContainerShardStateStore(targetContainer);
        Optional<VersionedShardHead> targetHead = targetShardStateStore.get(targetIndexUuid, SHARD_ID);
        assertTrue(targetHead.isPresent());
        assertTrue(new BlobContainerCloneLineageStore(targetContainer).readLineage().isPresent());
    }

    public void testShardCloneActionFailsWithoutSwallowingWhenSourceHasNoPublishedManifest() throws Exception {
        Path basePath = createTempDir("serverless-storage-clone-action-it-missing-source");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);

        // Both indices are real; the source simply never published a manifest. "No manifest yet"
        // and "no such index" are genuinely different failures and must not collapse into one.
        String sourceIndexUuid = createUnassignedServerlessIndex(SOURCE_INDEX_NAME);
        String targetIndexUuid = createUnassignedServerlessIndex(TARGET_INDEX_NAME);

        expectThrows(
            Exception.class,
            () -> client().execute(ShardCloneAction.INSTANCE, new ShardCloneRequest(sourceIndexUuid, SHARD_ID, targetIndexUuid, SHARD_ID))
                .get()
        );
    }

    /**
     * The traversal-shaped case. {@code indexUuid} is concatenated into a blob path
     * ({@code BlobPath.cleanPath().add(indexUuid)}) whose segments are resolved without
     * normalisation, so before this check a {@code ../..} uuid genuinely walked out of the
     * repository base on a filesystem repository -- and a clone <em>writes</em> there.
     */
    public void testShardCloneActionRejectsATraversalShapedIndexUuid() throws Exception {
        Path basePath = createTempDir("serverless-storage-clone-action-it-traversal");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        String sourceIndexUuid = createUnassignedServerlessIndex(SOURCE_INDEX_NAME);

        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(ShardCloneAction.INSTANCE, new ShardCloneRequest(sourceIndexUuid, SHARD_ID, "../../../etc", SHARD_ID))
                .get()
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
    public void testShardCloneActionRejectsATargetUuidThatNamesNoRealIndex() throws Exception {
        Path basePath = createTempDir("serverless-storage-clone-action-it-unknown-uuid");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        String sourceIndexUuid = createUnassignedServerlessIndex(SOURCE_INDEX_NAME);
        String bogusTargetUuid = "does-not-exist-as-a-real-index";

        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(ShardCloneAction.INSTANCE, new ShardCloneRequest(sourceIndexUuid, SHARD_ID, bogusTargetUuid, SHARD_ID))
                .get()
        );
        assertTrue(
            "must fail with the provisioning-precondition error, not something else: " + failure.getCause(),
            failure.getCause() instanceof IllegalArgumentException
        );
        assertTrue(failure.getCause().getMessage().contains("does not exist"));

        BlobContainer targetContainer = blobContainerFor(basePath, bogusTargetUuid, SHARD_ID);
        assertTrue(
            "a rejected clone must never write any object-store state for the bogus target",
            new BlobContainerShardStateStore(targetContainer).get(bogusTargetUuid, SHARD_ID).isEmpty()
        );
    }

    /**
     * The unrelated-but-real case: a uuid that resolves to a genuine index, but names a shard that
     * index does not have. Without the bound this reached another index's shard directory with no
     * scoping at all.
     */
    public void testShardCloneActionRejectsAShardIdOutOfBoundsForARealIndex() throws Exception {
        Path basePath = createTempDir("serverless-storage-clone-action-it-oob");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        String sourceIndexUuid = createUnassignedServerlessIndex(SOURCE_INDEX_NAME);
        // A real index, but it only has one shard (id 0) -- shard id 5 is out of bounds for it.
        String targetIndexUuid = createUnassignedServerlessIndex(TARGET_INDEX_NAME);

        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(ShardCloneAction.INSTANCE, new ShardCloneRequest(sourceIndexUuid, SHARD_ID, targetIndexUuid, 5)).get()
        );
        assertTrue(
            "must fail with the provisioning-precondition error, not something else: " + failure.getCause(),
            failure.getCause() instanceof IllegalArgumentException
        );
        assertTrue(failure.getCause().getMessage().contains("out of bounds"));
    }
}
