/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Proves {@link ServerlessStorageRemoteClusterStateValidator} enforces rfc-serverless-opensearch.md
 * &sect;10's mandatory-remote-cluster-state requirement directly against real {@link IndexSettings}
 * (index settings plus node settings merged the same way core does it for every other {@link
 * org.opensearch.index.IndexCreationValidator}), not through any injected/mocked state -- this
 * class needs none, unlike the {@code IndexSettingProvider}-based check it replaced.
 */
public class ServerlessStorageRemoteClusterStateValidatorTests extends OpenSearchTestCase {

    private IndexSettings newIndexSettings(boolean serverlessStorageEnabled, boolean remoteClusterStateEnabled) {
        Settings.Builder indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        if (serverlessStorageEnabled) {
            indexSettings.put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true);
        }
        IndexMetadata indexMetadata = IndexMetadata.builder("my-index").settings(indexSettings).build();
        Settings nodeSettings = Settings.builder()
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), remoteClusterStateEnabled)
            .build();
        return new IndexSettings(indexMetadata, nodeSettings);
    }

    public void testRejectsServerlessStorageWhenRemoteClusterStateIsNotEnabled() {
        IndexSettings indexSettings = newIndexSettings(true, false);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ServerlessStorageRemoteClusterStateValidator().validate(null, indexSettings)
        );
        assertTrue(e.getMessage().contains(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey()));
        assertTrue(e.getMessage().contains(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey()));
    }

    public void testAllowsServerlessStorageWhenRemoteClusterStateIsEnabled() {
        IndexSettings indexSettings = newIndexSettings(true, true);

        // Must not throw.
        new ServerlessStorageRemoteClusterStateValidator().validate(null, indexSettings);
    }

    public void testMissingRemoteClusterStateDoesNotAffectAnOrdinaryIndex() {
        IndexSettings indexSettings = newIndexSettings(false, false);

        // Must not throw: an index that never opted into serverless storage is never rejected on this basis.
        new ServerlessStorageRemoteClusterStateValidator().validate(null, indexSettings);
    }
}
