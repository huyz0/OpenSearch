/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotPinAction;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotPinRequest;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotPinResponse;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotReleaseAction;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotReleaseRequest;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotReleaseResponse;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotRestoreAction;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotRestoreRequest;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotRestoreResponse;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Proves the index-wide "snapshot every shard under one name, all-or-nothing" orchestration layer
 * (rfc-serverless-opensearch.md &sect;14) genuinely fans out across every shard of a real
 * multi-shard index, and that a pin failure on one shard rolls back every shard already pinned in
 * that same call rather than leaving a partial, inconsistent snapshot behind -- see {@link
 * IndexSnapshotPinAction}'s own javadoc for the exact guarantee this proves.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageIndexSnapshotActionIT extends ServerlessStorageIntegTestCase {

    private static final String INDEX_NAME = "index-snapshot-it-idx";
    private static final String SNAPSHOT_ID = "test-index-snapshot-1";
    private static final int NUMBER_OF_SHARDS = 3;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testIndexWidePinAndReleaseCoverEveryShardOfARealMultiShardIndex() throws Exception {
        Path basePath = createTempDir("serverless-storage-index-snapshot-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUMBER_OF_SHARDS)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);
        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();

        // Enough docs, unrouted, that with 3 shards every shard gets at least one and therefore
        // publishes a real manifest on flush -- not relying on a single doc happening to land on
        // every shard.
        for (int i = 0; i < 30; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        IndexSnapshotPinResponse pinned = client().execute(
            IndexSnapshotPinAction.INSTANCE,
            new IndexSnapshotPinRequest(INDEX_NAME, SNAPSHOT_ID)
        ).get();
        assertEquals(NUMBER_OF_SHARDS, pinned.shardCount());

        for (int shardId = 0; shardId < NUMBER_OF_SHARDS; shardId++) {
            BlobContainer container = internalCluster().getDataNodeInstance(ServerlessStoragePlugin.class)
                .blobContainerForDirectoryFactory(indexUuid, shardId);
            Set<PinRecord> pins = new BlobContainerDurablePinRegistry(container).getPins(indexUuid, shardId);
            assertTrue(
                "every shard of the index must have been pinned by the index-wide call, not just shard 0",
                pins.stream().anyMatch(pin -> pin.pinId().equals(SNAPSHOT_ID))
            );
        }

        IndexSnapshotReleaseResponse released = client().execute(
            IndexSnapshotReleaseAction.INSTANCE,
            new IndexSnapshotReleaseRequest(INDEX_NAME, SNAPSHOT_ID)
        ).get();
        assertEquals(NUMBER_OF_SHARDS, released.shardCount());

        for (int shardId = 0; shardId < NUMBER_OF_SHARDS; shardId++) {
            BlobContainer container = internalCluster().getDataNodeInstance(ServerlessStoragePlugin.class)
                .blobContainerForDirectoryFactory(indexUuid, shardId);
            Set<PinRecord> pins = new BlobContainerDurablePinRegistry(container).getPins(indexUuid, shardId);
            assertTrue(
                "every shard's pin must actually be gone after the index-wide release, not just reported gone",
                pins.stream().noneMatch(pin -> pin.pinId().equals(SNAPSHOT_ID))
            );
        }
    }

    public void testIndexWidePinRollsBackEveryAlreadyPinnedShardWhenAnyShardFails() throws Exception {
        Path basePath = createTempDir("serverless-storage-index-snapshot-it-rollback");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUMBER_OF_SHARDS)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);
        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();

        for (int i = 0; i < 30; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        // Every shard gets an initial published manifest as part of normal shard creation/flush,
        // so "never published" (the only failure SnapshotPinAction itself can hit) can't be
        // reached through the public index/document APIs here -- corrupt shard 2's register
        // directly (garbage bytes where a serialized ShardHead is expected) to force a genuine,
        // late-in-the-fan-out read failure once shards 0 and 1 have already been pinned, and prove
        // the rollback actually undoes them rather than just failing fast on shard 0.
        BlobContainer corruptedShardContainer = internalCluster().getDataNodeInstance(ServerlessStoragePlugin.class)
            .blobContainerForDirectoryFactory(indexUuid, NUMBER_OF_SHARDS - 1);
        String corruptedRegisterName = "head-" + indexUuid + "-" + (NUMBER_OF_SHARDS - 1);
        long currentGeneration = corruptedShardContainer.readRegister(corruptedRegisterName)
            .orElseThrow(() -> new AssertionError("shard must already have a published head to corrupt"))
            .generation();
        corruptedShardContainer.compareAndSwapRegister(
            corruptedRegisterName,
            currentGeneration,
            new org.opensearch.core.common.bytes.BytesArray(new byte[] { 1, 2, 3 })
        );

        ExecutionException e = expectThrows(
            ExecutionException.class,
            () -> client().execute(IndexSnapshotPinAction.INSTANCE, new IndexSnapshotPinRequest(INDEX_NAME, SNAPSHOT_ID)).get()
        );
        assertNotNull(e.getCause());

        for (int shardId = 0; shardId < NUMBER_OF_SHARDS; shardId++) {
            BlobContainer container = internalCluster().getDataNodeInstance(ServerlessStoragePlugin.class)
                .blobContainerForDirectoryFactory(indexUuid, shardId);
            Set<PinRecord> pins = new BlobContainerDurablePinRegistry(container).getPins(indexUuid, shardId);
            assertTrue(
                "a failed index-wide pin must leave no shard pinned under the failed snapshotId",
                pins.stream().noneMatch(pin -> pin.pinId().equals(SNAPSHOT_ID))
            );
        }
    }

    public void testIndexWideRestoreRestoresEveryShardToItsPinnedGeneration() throws Exception {
        Path basePath = createTempDir("serverless-storage-index-snapshot-it-restore");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUMBER_OF_SHARDS)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);
        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();

        for (int i = 0; i < 30; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().execute(IndexSnapshotPinAction.INSTANCE, new IndexSnapshotPinRequest(INDEX_NAME, SNAPSHOT_ID)).get();

        for (int i = 30; i < 60; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        assertTrue(client().admin().indices().prepareClose(INDEX_NAME).get().isAcknowledged());

        IndexSnapshotRestoreResponse restored = assertBusyReturning(
            () -> client().execute(IndexSnapshotRestoreAction.INSTANCE, new IndexSnapshotRestoreRequest(INDEX_NAME, SNAPSHOT_ID)).get()
        );
        assertEquals(NUMBER_OF_SHARDS, restored.shardCount());

        for (int shardId = 0; shardId < NUMBER_OF_SHARDS; shardId++) {
            BlobContainer container = internalCluster().getDataNodeInstance(ServerlessStoragePlugin.class)
                .blobContainerForDirectoryFactory(indexUuid, shardId);
            ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);
            Set<PinRecord> pins = new BlobContainerDurablePinRegistry(container).getPins(indexUuid, shardId);
            PinRecord pin = pins.stream().filter(p -> p.pinId().equals(SNAPSHOT_ID)).findFirst().orElseThrow();

            Optional<VersionedShardHead> head = shardStateStore.get(indexUuid, shardId);
            assertTrue(head.isPresent());
            // Every shard's head must carry its own pinned commit's files after the index-wide restore.
            // Asserted on the files rather than the generation number because a restore publishes a new
            // generation carrying the pinned commit's segments rather than rewinding onto the pinned
            // generation itself (RestoreManifestSynthesis has the reasoning) -- and because "the head names
            // the right bytes" is what the index-wide restore is actually claiming per shard.
            org.opensearch.serverless.storage.manifest.BlobContainerManifestStore manifestStore =
                new org.opensearch.serverless.storage.manifest.BlobContainerManifestStore(container);
            assertTrue(
                "shard " + shardId + " must have published forward",
                head.get().head().latestManifestGeneration() > pin.generation()
            );
            assertEquals(
                "shard " + shardId + "'s head must name exactly its own pinned commit's files",
                manifestStore.readManifest(pin.primaryTerm(), pin.generation()).files(),
                manifestStore.readManifest(head.get().head().primaryTerm(), head.get().head().latestManifestGeneration()).files()
            );
        }
    }

    /**
     * Same shape as {@code ServerlessStorageSnapshotRestoreActionIT}'s own helper: {@code
     * assertBusy}'s retry loop only catches {@code AssertionError}, not the plain {@code
     * ExecutionException} restoring against a just-closed (lease not yet expired) index throws.
     */
    private <T> T assertBusyReturning(Callable<T> callable) throws Exception {
        AtomicReference<T> result = new AtomicReference<>();
        assertBusy(() -> {
            try {
                result.set(callable.call());
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }, 40, TimeUnit.SECONDS);
        return result.get();
    }
}
