/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.serverless.storage.readerengine.ReaderEngineFactory;
import org.opensearch.serverless.storage.writerengine.WriterEngineFactory;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.Optional;

public class ServerlessStoragePluginTests extends OpenSearchTestCase {

    private ServerlessStoragePlugin newPlugin(Path basePath) {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin();
        if (basePath != null) {
            Settings nodeSettings = Settings.builder()
                .put("path.home", createTempDir().toString())
                .putList("path.repo", basePath.toString())
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
                .build();
            Environment environment = TestEnvironment.newEnvironment(nodeSettings);
            plugin.createComponents(null, null, null, null, null, null, environment, null, null, null, null);
        }
        return plugin;
    }

    private IndexSettings indexSettings(boolean enabled) {
        Index index = new Index("test-index", "test-index-uuid");
        Settings.Builder indexSettingsBuilder = Settings.builder();
        if (enabled) {
            indexSettingsBuilder.put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true);
        }
        return IndexSettingsModule.newIndexSettings(
            index,
            indexSettingsBuilder.build(),
            Settings.EMPTY,
            ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING
        );
    }

    public void testIndicesNotOptedInGetNoEngineFactory() {
        ServerlessStoragePlugin plugin = newPlugin(createTempDir());
        Optional<EngineFactory> factory = plugin.getEngineFactory(indexSettings(false), null);
        assertTrue(factory.isEmpty());
    }

    public void testOptedInIndexWithoutBasePathConfiguredFailsLoudly() {
        ServerlessStoragePlugin plugin = newPlugin(null);
        expectThrows(IllegalStateException.class, () -> plugin.getEngineFactory(indexSettings(true), null));
    }

    public void testWriterShardGetsAWriterEngineFactory() {
        ServerlessStoragePlugin plugin = newPlugin(createTempDir());
        IndexSettings settings = indexSettings(true);
        ShardId shardId = new ShardId(settings.getIndex(), 0);
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);

        Optional<EngineFactory> factory = plugin.getEngineFactory(settings, primaryRouting);
        assertTrue(factory.isPresent());
        assertTrue(factory.get() instanceof WriterEngineFactory);
    }

    public void testSearchOnlyShardGetsAReaderEngineFactory() {
        ServerlessStoragePlugin plugin = newPlugin(createTempDir());
        IndexSettings settings = indexSettings(true);
        ShardId shardId = new ShardId(settings.getIndex(), 0);
        ShardRouting searchOnlyRouting = TestShardRouting.newShardRouting(
            shardId,
            "node-1",
            false,
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );

        Optional<EngineFactory> factory = plugin.getEngineFactory(settings, searchOnlyRouting);
        assertTrue(factory.isPresent());
        assertTrue(factory.get() instanceof ReaderEngineFactory);
    }

    public void testNoShardRoutingDefaultsToAWriterEngineFactory() {
        ServerlessStoragePlugin plugin = newPlugin(createTempDir());
        Optional<EngineFactory> factory = plugin.getEngineFactory(indexSettings(true), null);
        assertTrue(factory.isPresent());
        assertTrue(factory.get() instanceof WriterEngineFactory);
    }
}
