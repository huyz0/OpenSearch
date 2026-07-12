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
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.resharding.BlobContainerShardPartitionStore;
import org.opensearch.serverless.storage.resharding.ShardPartitionDescriptor;
import org.opensearch.serverless.storage.resharding.action.ShardSplitAction;
import org.opensearch.serverless.storage.resharding.action.ShardSplitRequest;
import org.opensearch.serverless.storage.resharding.action.ShardSplitResponse;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
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
 * Proves {@link ShardSplitAction} genuinely works over the transport layer, in a real cluster --
 * not just a direct unit-level call to {@code ShardSplitter.split} -- covering the Guice injection
 * of {@link ServerlessStoragePlugin} into {@code TransportShardSplitAction} and the {@code
 * ThreadPool.Names#GENERIC} dispatch, the same fidelity {@link ServerlessStorageShardCloneActionIT}
 * already established for plain clones (rfc-serverless-opensearch.md &sect;16 Phase 5). Splitting
 * one source three ways and checking every target's descriptor and the source's pin count is the
 * one thing a single clone's own IT doesn't need to cover: that a single source can be split more
 * than once without pins from one target clobbering another's.
 *
 * <p><b>Also proves {@code TransportShardSplitAction}'s provisioning-precondition check</b> (RFC
 * &sect;16 Phase 4's own gap note): every source/target index used here is a real, cluster-created
 * {@link IndexMetadata}, not an arbitrary string, and the negative tests below prove a split
 * request naming an index/shard that doesn't really exist is rejected loudly and immediately,
 * rather than silently writing object-store state nothing could ever open.
 *
 * <p>This cluster deliberately never starts a data node: every index created below stays
 * permanently unassigned, so nothing ever opens a real engine against it. That's essential here,
 * not incidental -- this test (like {@code ShardSplitter.split} itself) writes a shard's manifest
 * and head directly into its object-store location, bypassing any real {@code IndexShard}
 * entirely; a real writer engine racing to activate the same shard concurrently would corrupt that
 * synthetic state via conflicting CAS writes. See {@link #createUnassignedServerlessIndex}.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageShardSplitActionIT extends ServerlessStorageIntegTestCase {

    private static final String SOURCE_INDEX_NAME = "split-action-it-source-idx";
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

    /** Synthetically publishes a one-document manifest directly against {@code indexUuid}'s object-store location, as though a real writer had. */
    private static CommitManifest publishSyntheticManifest(BlobContainer sourceContainer, String indexUuid, int shardId) throws Exception {
        BlobContainerBundleStore sourceBundleStore = new BlobContainerBundleStore(sourceContainer);
        BlobContainerManifestStore sourceManifestStore = new BlobContainerManifestStore(sourceContainer);
        ShardStateStore sourceShardStateStore = new BlobContainerShardStateStore(sourceContainer);

        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(sourceBundleStore, sourceManifestStore);
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            CommitManifest manifest;
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                indexUuid,
                shardId,
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
                sourceShardStateStore.compareAndSet(indexUuid, shardId, Optional.empty(), new ShardHead(1, null, 0L, manifest.generation()))
            );
            return manifest;
        }
    }

    public void testShardSplitActionSplitsARealPublishedSourceThreeWaysOverTransport() throws Exception {
        Path basePath = createTempDir("serverless-storage-split-action-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);

        String sourceIndexUuid = createUnassignedServerlessIndex(SOURCE_INDEX_NAME);
        BlobContainer sourceContainer = blobContainerFor(basePath, sourceIndexUuid, SHARD_ID);
        DurablePinRegistry sourcePinRegistry = new BlobContainerDurablePinRegistry(sourceContainer);
        CommitManifest sourceManifest = publishSyntheticManifest(sourceContainer, sourceIndexUuid, SHARD_ID);

        int numPartitions = 3;
        for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
            String targetIndexUuid = createUnassignedServerlessIndex(SOURCE_INDEX_NAME + "-part-" + partitionIndex);
            ShardSplitResponse response = client().execute(
                ShardSplitAction.INSTANCE,
                new ShardSplitRequest(sourceIndexUuid, SHARD_ID, targetIndexUuid, SHARD_ID, partitionIndex, numPartitions)
            ).get();
            assertTrue("split target " + partitionIndex + " must be acknowledged", response.acknowledged());

            BlobContainer targetContainer = blobContainerFor(basePath, targetIndexUuid, SHARD_ID);
            ShardStateStore targetShardStateStore = new BlobContainerShardStateStore(targetContainer);
            Optional<VersionedShardHead> targetHead = targetShardStateStore.get(targetIndexUuid, SHARD_ID);
            assertTrue("split target " + partitionIndex + " must have a real published head", targetHead.isPresent());

            BlobContainerManifestStore targetManifestStore = new BlobContainerManifestStore(targetContainer);
            CommitManifest targetManifest = targetManifestStore.readManifest(
                targetHead.get().head().primaryTerm(),
                targetHead.get().head().latestManifestGeneration()
            );
            assertEquals(
                "every split target must reference the exact same bundle files as the source",
                sourceManifest.files(),
                targetManifest.files()
            );

            Optional<ShardPartitionDescriptor> descriptor = new BlobContainerShardPartitionStore(targetContainer).readDescriptor();
            assertTrue(descriptor.isPresent());
            assertEquals(partitionIndex, descriptor.get().partitionIndex());
            assertEquals(numPartitions, descriptor.get().numPartitions());
        }

        Set<PinRecord> pins = sourcePinRegistry.getPins(sourceIndexUuid, SHARD_ID);
        assertEquals(
            "the source must carry one independent pin per split target, not one shared/clobbered pin",
            numPartitions,
            pins.size()
        );
    }

    public void testShardSplitActionRejectsATargetThatIsNotARealIndex() throws Exception {
        Path basePath = createTempDir("serverless-storage-split-action-it-no-target");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);

        String sourceIndexUuid = createUnassignedServerlessIndex(SOURCE_INDEX_NAME);
        BlobContainer sourceContainer = blobContainerFor(basePath, sourceIndexUuid, SHARD_ID);
        publishSyntheticManifest(sourceContainer, sourceIndexUuid, SHARD_ID);

        String bogusTargetUuid = "does-not-exist-as-a-real-index";
        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(
                ShardSplitAction.INSTANCE,
                new ShardSplitRequest(sourceIndexUuid, SHARD_ID, bogusTargetUuid, SHARD_ID, 0, 2)
            ).get()
        );
        assertTrue(
            "must fail with the provisioning-precondition error, not something else: " + failure.getCause(),
            failure.getCause() instanceof IllegalArgumentException
        );
        assertTrue(failure.getCause().getMessage().contains("does not exist"));

        BlobContainer targetContainer = blobContainerFor(basePath, bogusTargetUuid, SHARD_ID);
        assertTrue(
            "a rejected split must never write any object-store state for the bogus target",
            new BlobContainerShardStateStore(targetContainer).get(bogusTargetUuid, SHARD_ID).isEmpty()
        );
    }

    public void testShardSplitActionRejectsASourceThatIsNotARealIndex() throws Exception {
        Path basePath = createTempDir("serverless-storage-split-action-it-no-source");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);

        String targetIndexUuid = createUnassignedServerlessIndex(SOURCE_INDEX_NAME + "-part-0");
        String bogusSourceUuid = "does-not-exist-as-a-real-index-either";

        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(
                ShardSplitAction.INSTANCE,
                new ShardSplitRequest(bogusSourceUuid, SHARD_ID, targetIndexUuid, SHARD_ID, 0, 2)
            ).get()
        );
        assertTrue(
            "must fail with the provisioning-precondition error, not something else: " + failure.getCause(),
            failure.getCause() instanceof IllegalArgumentException
        );
        assertTrue(failure.getCause().getMessage().contains("does not exist"));
    }

    public void testShardSplitActionRejectsATargetShardIdOutOfBoundsForARealIndex() throws Exception {
        Path basePath = createTempDir("serverless-storage-split-action-it-oob");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);

        String sourceIndexUuid = createUnassignedServerlessIndex(SOURCE_INDEX_NAME);
        BlobContainer sourceContainer = blobContainerFor(basePath, sourceIndexUuid, SHARD_ID);
        publishSyntheticManifest(sourceContainer, sourceIndexUuid, SHARD_ID);

        // A real index, but it only has one shard (id 0) -- shard id 5 is out of bounds for it.
        String targetIndexUuid = createUnassignedServerlessIndex(SOURCE_INDEX_NAME + "-part-0");
        int outOfBoundsShardId = 5;

        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(
                ShardSplitAction.INSTANCE,
                new ShardSplitRequest(sourceIndexUuid, SHARD_ID, targetIndexUuid, outOfBoundsShardId, 0, 2)
            ).get()
        );
        assertTrue(
            "must fail with the provisioning-precondition error, not something else: " + failure.getCause(),
            failure.getCause() instanceof IllegalArgumentException
        );
        assertTrue(failure.getCause().getMessage().contains("out of bounds"));
    }
}
