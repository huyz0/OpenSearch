/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.serverless.storage.retention.action.SnapshotPinAction;
import org.opensearch.serverless.storage.retention.action.SnapshotPinRequest;
import org.opensearch.serverless.storage.retention.action.SnapshotPinResponse;
import org.opensearch.serverless.storage.retention.action.SnapshotReleaseAction;
import org.opensearch.serverless.storage.retention.action.SnapshotReleaseRequest;
import org.opensearch.serverless.storage.retention.action.SnapshotReleaseResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Proves the "snapshot = pinned manifest set" feature (rfc-serverless-opensearch.md &sect;14)
 * genuinely works over the transport layer against a real writer shard's real published manifest
 * -- not a hand-built {@link PinRecord} -- covering both halves ({@link SnapshotPinAction} and
 * {@link SnapshotReleaseAction}) of the only feature this plugin exposes for the &sect;14 design
 * bullet that, until this pass, had nothing but its underlying generic pin mechanism built.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageSnapshotPinActionIT extends ServerlessStorageIntegTestCase {

    private static final String INDEX_NAME = "snapshot-pin-it-idx";
    private static final String SNAPSHOT_ID = "test-snapshot-1";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testSnapshotPinAndReleaseRoundTripAgainstARealPublishedManifest() throws Exception {
        Path basePath = createTempDir("serverless-storage-snapshot-pin-it");
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
        // The pin action reads the shard's *published* head (ShardStateStore#get), not whatever
        // the local engine has buffered -- an explicit flush is what actually publishes a manifest,
        // exactly as every other IT in this plugin that depends on a real published head requires.
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        SnapshotPinResponse pinResponse = client().execute(SnapshotPinAction.INSTANCE, new SnapshotPinRequest(indexUuid, 0, SNAPSHOT_ID))
            .get();
        assertTrue("a real published manifest must pin at generation > 0", pinResponse.generation() > 0);

        BlobContainer container = internalCluster().getDataNodeInstance(ServerlessStoragePlugin.class)
            .blobContainerForDirectoryFactory(indexUuid, 0);
        BlobContainerDurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(container);

        Set<PinRecord> pinsAfterPin = pinRegistry.getPins(indexUuid, 0);
        assertTrue(
            "the exact (snapshotId, primaryTerm, generation) the pin action reported must actually be durably recorded",
            pinsAfterPin.contains(new PinRecord(SNAPSHOT_ID, pinResponse.primaryTerm(), pinResponse.generation()))
        );

        SnapshotReleaseResponse releaseResponse = client().execute(
            SnapshotReleaseAction.INSTANCE,
            new SnapshotReleaseRequest(indexUuid, 0, SNAPSHOT_ID)
        ).get();
        assertTrue(releaseResponse.acknowledged());

        Set<PinRecord> pinsAfterRelease = pinRegistry.getPins(indexUuid, 0);
        assertTrue(
            "releasing the snapshot must actually remove the durable pin, not just report success",
            pinsAfterRelease.stream().noneMatch(pin -> pin.pinId().equals(SNAPSHOT_ID))
        );
    }

    public void testRePinningUnderTheSameSnapshotIdReplacesTheOldGenerationRatherThanAccumulating() throws Exception {
        Path basePath = createTempDir("serverless-storage-snapshot-pin-it-replace");
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
        SnapshotPinResponse firstPin = client().execute(SnapshotPinAction.INSTANCE, new SnapshotPinRequest(indexUuid, 0, SNAPSHOT_ID))
            .get();

        client().prepareIndex(INDEX_NAME).setId("2").setSource("field", "value2").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        SnapshotPinResponse secondPin = client().execute(SnapshotPinAction.INSTANCE, new SnapshotPinRequest(indexUuid, 0, SNAPSHOT_ID))
            .get();
        assertTrue(
            "the second flush must have actually advanced the generation, or this test isn't exercising the replace path",
            secondPin.generation() > firstPin.generation()
        );

        BlobContainer container = internalCluster().getDataNodeInstance(ServerlessStoragePlugin.class)
            .blobContainerForDirectoryFactory(indexUuid, 0);
        Set<PinRecord> pinsUnderThisSnapshotId = new BlobContainerDurablePinRegistry(container).getPins(indexUuid, 0)
            .stream()
            .filter(pin -> pin.pinId().equals(SNAPSHOT_ID))
            .collect(java.util.stream.Collectors.toSet());

        assertEquals(
            "re-pinning under the same snapshotId must leave exactly one pin behind (the new generation), "
                + "not accumulate the old one alongside it",
            java.util.Set.of(new PinRecord(SNAPSHOT_ID, secondPin.primaryTerm(), secondPin.generation())),
            pinsUnderThisSnapshotId
        );
    }

    public void testSnapshotPinFailsForAShardThatHasNeverPublished() throws Exception {
        Path basePath = createTempDir("serverless-storage-snapshot-pin-it-never-published");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        // No data node: the index below stays permanently unassigned, so no engine ever activates
        // it and it genuinely never publishes a manifest -- which is the state under test. It must
        // still be a *real* index, because a uuid naming no index at all is now a different (and
        // earlier) failure, covered separately below.
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        String indexUuid = createUnassignedServerlessIndex(INDEX_NAME);

        Exception e = expectThrows(
            Exception.class,
            () -> client().execute(SnapshotPinAction.INSTANCE, new SnapshotPinRequest(indexUuid, 0, SNAPSHOT_ID)).get()
        );
        assertTrue(
            "pinning a shard with no published manifest must fail loudly, not silently pin nothing",
            e.getMessage().contains("never published")
        );
    }

    /**
     * The traversal-shaped case. {@code indexUuid} is concatenated into a blob path
     * ({@code BlobPath.cleanPath().add(indexUuid)}) whose segments are resolved without
     * normalisation, so before this check a {@code ../..} uuid genuinely walked out of the
     * repository base on a filesystem repository.
     */
    public void testSnapshotPinRejectsATraversalShapedIndexUuid() throws Exception {
        Path basePath = createTempDir("serverless-storage-snapshot-pin-it-traversal");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);

        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(SnapshotPinAction.INSTANCE, new SnapshotPinRequest("../../../etc", 0, SNAPSHOT_ID)).get()
        );
        assertTrue(
            "a traversal-shaped uuid must be refused, not resolved into a blob path: " + failure.getCause(),
            failure.getCause() instanceof IllegalArgumentException
        );
    }

    /**
     * The plausible-looking case, which no character check would ever catch: a well-formed string
     * that simply names no index in this cluster. Only resolving it through cluster metadata can
     * tell the difference, which is why that -- not a charset test -- is the actual control. The
     * release half is checked in the same test because it shares exactly the same guard.
     */
    public void testSnapshotPinAndReleaseRejectAnIndexUuidThatNamesNoRealIndex() throws Exception {
        Path basePath = createTempDir("serverless-storage-snapshot-pin-it-unknown-uuid");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        createUnassignedServerlessIndex(INDEX_NAME);

        ExecutionException pinFailure = expectThrows(
            ExecutionException.class,
            () -> client().execute(SnapshotPinAction.INSTANCE, new SnapshotPinRequest("not-a-real-index-uuid", 0, SNAPSHOT_ID)).get()
        );
        assertTrue(pinFailure.getCause() instanceof IllegalArgumentException);
        assertTrue(pinFailure.getCause().getMessage().contains("does not exist"));

        ExecutionException releaseFailure = expectThrows(
            ExecutionException.class,
            () -> client().execute(SnapshotReleaseAction.INSTANCE, new SnapshotReleaseRequest("not-a-real-index-uuid", 0, SNAPSHOT_ID))
                .get()
        );
        assertTrue(releaseFailure.getCause() instanceof IllegalArgumentException);
        assertTrue(releaseFailure.getCause().getMessage().contains("does not exist"));
    }

    /**
     * The unrelated-but-real case: a uuid that resolves to a genuine index, but names a shard that
     * index does not have. Without the bound this reached another index's shard directory with no
     * scoping at all.
     */
    public void testSnapshotPinRejectsAShardIdOutOfBoundsForARealIndex() throws Exception {
        Path basePath = createTempDir("serverless-storage-snapshot-pin-it-oob");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        // A real index, but it only has one shard (id 0) -- shard id 5 is out of bounds for it.
        String indexUuid = createUnassignedServerlessIndex(INDEX_NAME);

        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(SnapshotPinAction.INSTANCE, new SnapshotPinRequest(indexUuid, 5, SNAPSHOT_ID)).get()
        );
        assertTrue(failure.getCause() instanceof IllegalArgumentException);
        assertTrue(failure.getCause().getMessage().contains("out of bounds"));
    }

    /**
     * Creates a real serverless-storage index with one shard, deliberately never assigned to any
     * node (the tests that use it start no data node), so nothing ever activates an engine against
     * it and it never publishes a manifest of its own.
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
}
