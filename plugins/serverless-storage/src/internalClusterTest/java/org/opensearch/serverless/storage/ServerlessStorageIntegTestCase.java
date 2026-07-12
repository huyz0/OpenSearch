/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.test.OpenSearchIntegTestCase;

/**
 * Shared base for every serverless-storage {@code internalClusterTest} that creates a real
 * serverless-storage index: enables {@link RemoteClusterStateService#REMOTE_CLUSTER_STATE_ENABLED_SETTING}
 * on every node, since {@link ServerlessStorageIndexSettingProvider} now enforces it as mandatory
 * (rfc-serverless-opensearch.md &sect;10) and rejects index creation otherwise. That setting is
 * {@code Property.Final} -- it can only be set at node startup, so it has to be threaded in here
 * via {@link #nodeSettings}, not set dynamically once a test cluster is already running.
 *
 * <p>Subclasses that already override {@link #nodeSettings(int)} for their own reasons must call
 * {@code super.nodeSettings(nodeOrdinal)} and layer their own settings on top, the same as every
 * other {@code nodeSettings} override in this test suite already does for its own superclass.
 */
public abstract class ServerlessStorageIntegTestCase extends OpenSearchIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .build();
    }
}
