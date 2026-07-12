/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.migration.action.MigrateShardAction;
import org.opensearch.serverless.storage.migration.action.MigrateShardRequest;
import org.opensearch.serverless.storage.migration.action.MigrateShardResponse;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Proves {@link MigrateShardAction} genuinely works over the real transport layer, against a real,
 * currently-open, locally-recovered classic (non-serverless-storage) shard -- not just a direct
 * unit-level call to {@code ClassicIndexMigrator.migrate} against a synthetic {@code Directory}
 * ({@code ClassicIndexMigratorTests} already covers that). This is the piece {@code
 * ClassicIndexMigrator}'s own javadoc named as deliberately not built yet: resolving a real {@code
 * IndexShard} from a shard id alone via {@code IndicesService} (rfc-serverless-opensearch.md
 * &sect;16 Phase 6).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageMigrateShardActionIT extends ServerlessStorageIntegTestCase {

    private static final String INDEX_NAME = "migrate-action-it-classic-idx";
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

    /** The data node hosting {@link #INDEX_NAME}'s only shard, and that shard's real UUID. */
    private record IndexedClassicShard(String indexUuid, String dataNodeName) {
    }

    /**
     * {@link MigrateShardAction} requires routing to the specific node hosting the shard's live
     * copy (same contract as {@code PollNowAction}/{@code WaitForGenerationAction} -- see {@code
     * TransportMigrateShardAction}'s own javadoc): every caller in this test class must dispatch
     * through {@code internalCluster().client(dataNodeName)} for that reason, never the plain
     * {@code client()} helper, which the test framework deliberately routes to an arbitrary node
     * (including, in this two-node cluster, the cluster-manager-only node that never hosts any
     * shard at all).
     */
    private IndexedClassicShard startClusterAndIndexOneRealDocument(Path basePath) throws Exception {
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        String dataNodeName = internalCluster().startDataOnlyNode(nodeSettings);

        // A genuinely ordinary classic index -- no serverless-storage setting at all -- with a
        // real writer engine actually running, exactly what this action is meant to adopt in place.
        createIndex(
            INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();
        return new IndexedClassicShard(indexUuid, dataNodeName);
    }

    public void testMigrateShardActionAdoptsARealClassicShardInPlace() throws Exception {
        Path basePath = createTempDir("serverless-storage-migrate-action-it");
        IndexedClassicShard shard = startClusterAndIndexOneRealDocument(basePath);
        String indexUuid = shard.indexUuid();

        MigrateShardResponse response = internalCluster().client(shard.dataNodeName())
            .execute(MigrateShardAction.INSTANCE, new MigrateShardRequest(indexUuid, SHARD_ID))
            .get();
        assertTrue("the migration must be acknowledged", response.acknowledged());

        BlobContainer container = blobContainerFor(basePath, indexUuid, SHARD_ID);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);
        Optional<VersionedShardHead> head = shardStateStore.get(indexUuid, SHARD_ID);
        assertTrue("migration must adopt a real serverless-storage head for this exact shard identity", head.isPresent());
        assertEquals(1L, head.get().head().latestManifestGeneration());

        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(container);
        CommitManifest manifest = manifestStore.readManifest(head.get().head().primaryTerm(), head.get().head().latestManifestGeneration());
        assertFalse("the adopted manifest must reference real bundle files, not an empty shard", manifest.files().isEmpty());
        // A single indexed document has seqNo/local-checkpoint 0 -- proves the transport action
        // extracted these from the shard's own committed segmentInfos.userData (matching
        // ObjectStoreWriterEngine#commitIndexWriter's own established pattern), not some other,
        // possibly-inconsistent source like the shard's live in-memory state.
        assertEquals("adopted manifest must carry the real committed maxSeqNo, not a placeholder", 0L, manifest.maxSeqNo());
        assertEquals("adopted manifest must carry the real committed localCheckpoint, not a placeholder", 0L, manifest.localCheckpoint());

        // The real proof, matching ClassicIndexMigratorTests' own bar: materializing the adopted
        // manifest into a fresh Lucene directory and opening a real DirectoryReader over it must
        // actually find the document that was indexed through the classic engine before migration
        // -- not just "a manifest object with plausible-looking fields."
        org.apache.lucene.store.Directory materialized = new org.apache.lucene.store.ByteBuffersDirectory();
        new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(container)).materialize(manifest, materialized);
        try (org.apache.lucene.index.DirectoryReader reader = org.apache.lucene.index.DirectoryReader.open(materialized)) {
            assertEquals("the migrated commit must contain exactly the one document indexed before migration", 1, reader.numDocs());
        }

        // Also confirm the classic engine (this action does not cut routing over -- see
        // rfc-serverless-opensearch.md's own note on that) is completely untouched by the adoption.
        SearchResponse classicSearch = client().prepareSearch(INDEX_NAME).get();
        assertHitCount(classicSearch, 1);
    }

    public void testMigrateShardActionRejectsAnIndexThatDoesNotExist() throws Exception {
        Path basePath = createTempDir("serverless-storage-migrate-action-it-no-index");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(MigrateShardAction.INSTANCE, new MigrateShardRequest("does-not-exist-as-a-real-index", SHARD_ID)).get()
        );
        assertTrue(
            "must fail with a clear precondition error, not something else: " + failure.getCause(),
            failure.getCause() instanceof IllegalArgumentException
        );
        assertTrue(failure.getCause().getMessage().contains("does not exist"));
    }

    public void testMigrateShardActionRefusesAShardThatAlreadyHasAServerlessStorageHead() throws Exception {
        Path basePath = createTempDir("serverless-storage-migrate-action-it-twice");
        IndexedClassicShard shard = startClusterAndIndexOneRealDocument(basePath);
        String indexUuid = shard.indexUuid();
        var dataNodeClient = internalCluster().client(shard.dataNodeName());

        MigrateShardResponse first = dataNodeClient.execute(MigrateShardAction.INSTANCE, new MigrateShardRequest(indexUuid, SHARD_ID))
            .get();
        assertTrue(first.acknowledged());

        ExecutionException secondAttempt = expectThrows(
            ExecutionException.class,
            () -> dataNodeClient.execute(MigrateShardAction.INSTANCE, new MigrateShardRequest(indexUuid, SHARD_ID)).get()
        );
        assertTrue(
            "a shard already migrated must refuse a second migration, not silently overwrite: " + secondAttempt.getCause(),
            secondAttempt.getCause() instanceof IllegalStateException
        );
        assertTrue(secondAttempt.getCause().getMessage().contains("already has a serverless-storage head"));
    }
}
