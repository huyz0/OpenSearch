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
import org.opensearch.cluster.metadata.AliasMetadata;
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
import org.opensearch.serverless.storage.resharding.WritePartitionRoutingMetadata;
import org.opensearch.serverless.storage.resharding.action.OrchestrateShardSplitAction;
import org.opensearch.serverless.storage.resharding.action.OrchestrateShardSplitRequest;
import org.opensearch.serverless.storage.resharding.action.OrchestrateShardSplitResponse;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Proves {@link OrchestrateShardSplitAction} genuinely chains {@code ProvisionSplitTargetsAction}
 * -&gt; per-target {@code ShardSplitAction} -&gt; {@code CutoverSplitRoutingAction} -&gt;
 * {@code EnableWritePartitionRoutingAction} into one real, resumable sequence over the transport
 * layer -- the A15 orchestration gap
 * <code>write-routing-and-term-authority-progress.md</code> flagged as the one remaining piece
 * once {@code ProvisionSplitTargetsAction} closed the auto-provisioning blocker.
 *
 * <p>Follows {@link ServerlessStorageShardSplitActionIT}'s own "no data node, synthetic published
 * manifest written directly against the object store" pattern for the same reason that class
 * documents: a real writer engine racing to activate the same shard concurrently would corrupt
 * this test's own synthetic state via conflicting CAS writes.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageOrchestrateShardSplitActionIT extends ServerlessStorageIntegTestCase {

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

    private static void publishSyntheticManifest(BlobContainer sourceContainer, String indexUuid, int shardId) throws Exception {
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
        }
    }

    public void testOrchestratesTheFullSequenceAndIsResumableOnRetry() throws Exception {
        Path basePath = createTempDir("serverless-storage-orchestrate-split-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);

        String sourceIndexName = "orchestrate-split-source";
        String sourceIndexUuid = createUnassignedServerlessIndex(sourceIndexName);
        BlobContainer sourceContainer = blobContainerFor(basePath, sourceIndexUuid, SHARD_ID);
        publishSyntheticManifest(sourceContainer, sourceIndexUuid, SHARD_ID);

        List<String> targetIndexNames = List.of("orchestrate-split-target-a", "orchestrate-split-target-b");
        String routingAlias = "orchestrate-split-alias";

        OrchestrateShardSplitResponse response = client().execute(
            OrchestrateShardSplitAction.INSTANCE,
            new OrchestrateShardSplitRequest(sourceIndexName, targetIndexNames, routingAlias, true)
        ).get();
        assertTrue("targets must have been provisioned", response.targetsProvisioned());
        assertTrue("every target must have been split", response.allTargetsSplit());
        assertTrue("the alias cutover must have completed", response.cutover());
        assertTrue("write-routing must have been enabled since the request asked for it", response.writeRoutingEnabled());

        // Provisioning: both real target indices must now exist.
        assertTrue(client().admin().cluster().prepareState().get().getState().metadata().hasIndex("orchestrate-split-target-a"));
        assertTrue(client().admin().cluster().prepareState().get().getState().metadata().hasIndex("orchestrate-split-target-b"));

        // Split: both targets must have a real published head in the object store.
        for (String targetIndexName : targetIndexNames) {
            String targetIndexUuid = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .metadata()
                .index(targetIndexName)
                .getIndexUUID();
            BlobContainer targetContainer = blobContainerFor(basePath, targetIndexUuid, SHARD_ID);
            assertTrue(
                "split target [" + targetIndexName + "] must have a real published head",
                new BlobContainerShardStateStore(targetContainer).get(targetIndexUuid, SHARD_ID).isPresent()
            );
        }

        // Cutover: the alias must now point at both real targets.
        IndexMetadata targetAMetadata = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("orchestrate-split-target-a");
        IndexMetadata targetBMetadata = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("orchestrate-split-target-b");
        AliasMetadata aliasOnA = targetAMetadata.getAliases().get(routingAlias);
        AliasMetadata aliasOnB = targetBMetadata.getAliases().get(routingAlias);
        assertTrue("the routing alias must be assigned on target A", aliasOnA != null);
        assertTrue("the routing alias must be assigned on target B", aliasOnB != null);

        // Write routing: both targets must carry a real write-routing assignment for the alias.
        assertTrue("target A must carry a write-routing assignment", WritePartitionRoutingMetadata.isWriteRoutingTarget(targetAMetadata));
        assertTrue("target B must carry a write-routing assignment", WritePartitionRoutingMetadata.isWriteRoutingTarget(targetBMetadata));
        assertEquals(routingAlias, WritePartitionRoutingMetadata.writeRoutingAlias(targetAMetadata));
        assertEquals(routingAlias, WritePartitionRoutingMetadata.writeRoutingAlias(targetBMetadata));

        // Resumability: re-issuing the exact same request must succeed cleanly, not fail on any
        // already-completed stage (provisioning refuses an existing target; splitting refuses an
        // already-published head; cutover/write-routing were already idempotent).
        OrchestrateShardSplitResponse retryResponse = client().execute(
            OrchestrateShardSplitAction.INSTANCE,
            new OrchestrateShardSplitRequest(sourceIndexName, targetIndexNames, routingAlias, true)
        ).get();
        assertTrue("a resumed retry must still report every stage complete", retryResponse.targetsProvisioned());
        assertTrue(retryResponse.allTargetsSplit());
        assertTrue(retryResponse.cutover());
        assertTrue(retryResponse.writeRoutingEnabled());
    }

    public void testRefusesAMultiShardSource() throws Exception {
        Settings nodeSettings = Settings.builder().build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);

        client().admin()
            .indices()
            .prepareCreate("orchestrate-split-multishard-source")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .get();

        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(
                OrchestrateShardSplitAction.INSTANCE,
                new OrchestrateShardSplitRequest(
                    "orchestrate-split-multishard-source",
                    List.of("multishard-target-a", "multishard-target-b"),
                    "multishard-alias",
                    false
                )
            ).get()
        );
        assertTrue(
            "must fail with the single-shard-source precondition error, not something else: " + failure.getCause(),
            failure.getCause() instanceof IllegalArgumentException
        );
        assertTrue(failure.getCause().getMessage().contains("single-shard source"));
        assertFalse(
            "a refused orchestration must never have provisioned anything",
            client().admin().cluster().prepareState().get().getState().metadata().hasIndex("multishard-target-a")
        );
    }
}
