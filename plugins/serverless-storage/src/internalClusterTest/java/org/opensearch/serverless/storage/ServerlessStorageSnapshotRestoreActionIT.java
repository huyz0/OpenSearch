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
import org.opensearch.serverless.storage.retention.action.SnapshotPinAction;
import org.opensearch.serverless.storage.retention.action.SnapshotPinRequest;
import org.opensearch.serverless.storage.retention.action.SnapshotPinResponse;
import org.opensearch.serverless.storage.retention.action.SnapshotRestoreAction;
import org.opensearch.serverless.storage.retention.action.SnapshotRestoreRequest;
import org.opensearch.serverless.storage.retention.action.SnapshotRestoreResponse;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Proves the restore-in-place half of "snapshot = pinned manifest set"
 * (rfc-serverless-opensearch.md &sect;14) genuinely works against a real writer shard's real
 * published manifests over the transport layer: pin an early generation, advance the shard past
 * it, restore, and confirm the shard's real head (not a hand-built one) now points back at exactly
 * the pinned generation. Also proves the lease-held safety refusal actually fires against a real
 * live writer, not just a hand-built {@code ShardHead}.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageSnapshotRestoreActionIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "snapshot-restore-it-idx";
    private static final String SNAPSHOT_ID = "test-snapshot-restore-1";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testRestoreRefusesWhileTheWriterLeaseIsHeldThenSucceedsOnceTheIndexIsClosed() throws Exception {
        Path basePath = createTempDir("serverless-storage-snapshot-restore-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);
        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();

        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        SnapshotPinResponse pinned = client().execute(SnapshotPinAction.INSTANCE, new SnapshotPinRequest(indexUuid, 0, SNAPSHOT_ID)).get();

        client().prepareIndex(INDEX_NAME).setId("2").setSource("field", "value2").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        // While the index is open, its writer engine holds and actively renews the lease -- restore
        // must refuse rather than fight a live writer for the head.
        ExecutionException whileOpen = expectThrows(
            ExecutionException.class,
            () -> client().execute(SnapshotRestoreAction.INSTANCE, new SnapshotRestoreRequest(indexUuid, 0, SNAPSHOT_ID)).get()
        );
        assertTrue(
            "restore must refuse while a live writer holds the lease, not silently fight it for the head",
            whileOpen.getCause().getMessage().contains("active writer lease")
        );

        // Closing the index stops the shard's engine from renewing its lease, but does not
        // retroactively clear the lease already recorded in the shard head -- a deliberate
        // fencing-safety property (this plugin has no way to distinguish a graceful close from an
        // ungraceful one from the object store's perspective, so an already-published lease must
        // stay formally valid until its own TTL naturally elapses, the same guarantee that makes
        // WAL replay fencing sound). Restore must therefore wait out the lease TTL
        // (ObjectStoreWriterEngine#LEASE_TTL_MILLIS = 30s), not assume close() releases it
        // instantly -- discovered by this test itself failing against that wrong assumption.
        assertTrue(client().admin().indices().prepareClose(INDEX_NAME).get().isAcknowledged());

        SnapshotRestoreResponse restored = assertBusyReturning(
            () -> client().execute(SnapshotRestoreAction.INSTANCE, new SnapshotRestoreRequest(indexUuid, 0, SNAPSHOT_ID)).get()
        );
        assertEquals(pinned.primaryTerm(), restored.primaryTerm());
        assertEquals(pinned.generation(), restored.generation());

        BlobContainer container = internalCluster().getDataNodeInstance(ServerlessStoragePlugin.class)
            .blobContainerForDirectoryFactory(indexUuid, 0);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);
        Optional<VersionedShardHead> headAfterRestore = shardStateStore.get(indexUuid, 0);
        assertTrue(headAfterRestore.isPresent());
        assertEquals(
            "the shard's real, durable head must actually reflect the restored generation, not just the response",
            pinned.generation(),
            headAfterRestore.get().head().latestManifestGeneration()
        );
    }

    /**
     * Polls {@code callable} until it stops throwing, same intent as {@link #assertBusy(CheckedRunnable)}
     * but for a call that also needs to hand back a value once it succeeds -- {@code assertBusy}
     * itself only accepts a {@code void}-returning runnable. Also re-wraps any exception as an
     * {@link AssertionError}: {@code assertBusy}'s own retry loop only catches {@code
     * AssertionError} and lets every other exception propagate on the very first attempt (found by
     * this test itself failing immediately, well before the lease TTL it's supposed to be waiting
     * out, when {@code callable} throws the plain {@link ExecutionException} {@code client().execute}
     * wraps failures in).
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

    public void testRestoreFailsForASnapshotThatWasNeverPinned() throws Exception {
        Path basePath = createTempDir("serverless-storage-snapshot-restore-it-no-pin");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);
        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();

        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        assertTrue(client().admin().indices().prepareClose(INDEX_NAME).get().isAcknowledged());

        ExecutionException e = expectThrows(
            ExecutionException.class,
            () -> client().execute(SnapshotRestoreAction.INSTANCE, new SnapshotRestoreRequest(indexUuid, 0, "never-pinned")).get()
        );
        assertTrue(
            "restoring a never-pinned snapshot name must fail loudly, not silently restore to something arbitrary",
            e.getCause().getMessage().contains("no snapshot")
        );
    }
}
