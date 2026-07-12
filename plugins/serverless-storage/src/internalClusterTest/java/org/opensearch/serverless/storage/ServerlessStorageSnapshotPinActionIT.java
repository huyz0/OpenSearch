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

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        Exception e = expectThrows(
            Exception.class,
            () -> client().execute(SnapshotPinAction.INSTANCE, new SnapshotPinRequest("never-published-idx", 0, SNAPSHOT_ID)).get()
        );
        assertTrue(
            "pinning a shard with no published manifest must fail loudly, not silently pin nothing",
            e.getMessage().contains("never published")
        );
    }
}
