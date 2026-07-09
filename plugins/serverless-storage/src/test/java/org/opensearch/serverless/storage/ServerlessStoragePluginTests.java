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
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.serverless.storage.format.InMemoryPlaintextBundleCache;
import org.opensearch.serverless.storage.readerengine.ReaderEngineFactory;
import org.opensearch.serverless.storage.writerengine.WriterEngineFactory;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.Base64;
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

    public void testEncryptionKeyConfiguredDoesNotBreakEngineFactoryConstruction() throws Exception {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin();
        Path basePath = createTempDir();

        MockSecureSettings secureSettings = new MockSecureSettings();
        byte[] rawKeyBytes = new byte[32];
        random().nextBytes(rawKeyBytes);
        secureSettings.setString(
            ServerlessStoragePlugin.SERVERLESS_STORAGE_ENCRYPTION_KEY_SETTING.getKey(),
            Base64.getEncoder().encodeToString(rawKeyBytes)
        );
        Settings nodeSettings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .setSecureSettings(secureSettings)
            .build();
        Environment environment = TestEnvironment.newEnvironment(nodeSettings);
        plugin.createComponents(null, null, null, null, null, null, environment, null, null, null, null);

        IndexSettings settings = indexSettings(true);
        ShardId shardId = new ShardId(settings.getIndex(), 0);
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);

        Optional<EngineFactory> factory = plugin.getEngineFactory(settings, primaryRouting);
        assertTrue(factory.isPresent());
        assertTrue(factory.get() instanceof WriterEngineFactory);
    }

    /**
     * Proves the plugin actually threads its own {@code encryptionKeyProvider} into {@link
     * WriterEngineFactory} when both encryption and WAL mirroring are configured together -- the
     * specific wiring point {@code getEngineFactory} gained when {@code EncryptingWalChunkService}
     * was wired into the real writer engine. Neither {@link #testEncryptionKeyConfiguredDoesNotBreakEngineFactoryConstruction}
     * nor {@link #testWalMirroringEnabledDoesNotBreakEngineFactoryConstruction} alone exercises this
     * combination or inspects anything beyond the factory's type.
     */
    public void testEncryptionAndWalMirroringTogetherWireTheEncryptionKeyProviderIntoTheWriterEngineFactory() throws Exception {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin();
        Path basePath = createTempDir();

        MockSecureSettings secureSettings = new MockSecureSettings();
        byte[] rawKeyBytes = new byte[32];
        random().nextBytes(rawKeyBytes);
        secureSettings.setString(
            ServerlessStoragePlugin.SERVERLESS_STORAGE_ENCRYPTION_KEY_SETTING.getKey(),
            Base64.getEncoder().encodeToString(rawKeyBytes)
        );
        Settings nodeSettings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
            .setSecureSettings(secureSettings)
            .build();
        Environment environment = TestEnvironment.newEnvironment(nodeSettings);
        plugin.createComponents(null, null, null, null, null, null, environment, null, null, null, null);

        IndexSettings settings = indexSettings(true);
        ShardId shardId = new ShardId(settings.getIndex(), 0);
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);

        Optional<EngineFactory> factory = plugin.getEngineFactory(settings, primaryRouting);
        assertTrue(factory.isPresent());
        WriterEngineFactory writerEngineFactory = (WriterEngineFactory) factory.get();
        assertNotNull(
            "the plugin's own encryptionKeyProvider must reach WriterEngineFactory so WAL-mirrored records get encrypted",
            writerEngineFactory.encryptionKeyProviderForTesting()
        );
    }

    public void testWalMirroringEnabledDoesNotBreakEngineFactoryConstruction() {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin();
        Path basePath = createTempDir();
        Settings nodeSettings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
            .build();
        Environment environment = TestEnvironment.newEnvironment(nodeSettings);
        plugin.createComponents(null, null, null, null, null, null, environment, null, null, null, null);

        IndexSettings settings = indexSettings(true);
        ShardId shardId = new ShardId(settings.getIndex(), 0);
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);

        Optional<EngineFactory> factory = plugin.getEngineFactory(settings, primaryRouting);
        assertTrue(factory.isPresent());
        assertTrue(factory.get() instanceof WriterEngineFactory);
    }

    public void testWalPerShardBudgetSettingDefaultsToDisabled() {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin();
        Path basePath = createTempDir();
        Settings nodeSettings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
            .build();
        Environment environment = TestEnvironment.newEnvironment(nodeSettings);
        plugin.createComponents(null, null, null, null, null, null, environment, null, null, null, null);

        assertEquals(0L, plugin.sharedWalChunkServiceForTesting().perShardBudgetBytesForTesting());
    }

    public void testWalPerShardBudgetSettingIsThreadedIntoTheSharedWalChunkService() {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin();
        Path basePath = createTempDir();
        Settings nodeSettings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_PER_SHARD_BUDGET_SETTING.getKey(), "512kb")
            .build();
        Environment environment = TestEnvironment.newEnvironment(nodeSettings);
        plugin.createComponents(null, null, null, null, null, null, environment, null, null, null, null);

        assertEquals(512L * 1024, plugin.sharedWalChunkServiceForTesting().perShardBudgetBytesForTesting());
    }

    public void testWalMirroringDisabledByDefaultStillConstructsAWriterEngineFactory() {
        // The default-off setting must never be a hard requirement: every existing deployment of
        // this plugin (and every other test in this class) constructs a WriterEngineFactory with
        // WAL mirroring untouched, and that must keep working unchanged.
        ServerlessStoragePlugin plugin = newPlugin(createTempDir());
        IndexSettings settings = indexSettings(true);
        ShardId shardId = new ShardId(settings.getIndex(), 0);
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);

        Optional<EngineFactory> factory = plugin.getEngineFactory(settings, primaryRouting);
        assertTrue(factory.isPresent());
        assertTrue(factory.get() instanceof WriterEngineFactory);
    }

    public void testBundleCacheSizeSettingDefaultsToAPositiveFractionOfHeap() {
        Environment environment = TestEnvironment.newEnvironment(buildEnvSettings(Settings.EMPTY));
        long defaultBytes = ServerlessStoragePlugin.SERVERLESS_STORAGE_BUNDLE_CACHE_SIZE_SETTING.get(environment.settings()).getBytes();
        assertTrue("the 5% default must resolve to a real, positive byte budget", defaultBytes > 0);
    }

    public void testBundleCacheSizeSettingHonorsAnExplicitAbsoluteValue() {
        Settings nodeSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BUNDLE_CACHE_SIZE_SETTING.getKey(), "2kb")
            .build();
        Environment environment = TestEnvironment.newEnvironment(buildEnvSettings(nodeSettings));
        long bytes = ServerlessStoragePlugin.SERVERLESS_STORAGE_BUNDLE_CACHE_SIZE_SETTING.get(environment.settings()).getBytes();
        assertEquals(2048, bytes);
    }

    public void testTheSharedBundleCacheIsOneInstanceThatServesBundlesFromDifferentShardsWithoutCollision() throws Exception {
        Settings nodeSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BUNDLE_CACHE_SIZE_SETTING.getKey(), "1mb")
            .build();
        Environment environment = TestEnvironment.newEnvironment(buildEnvSettings(nodeSettings));
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin();
        plugin.createComponents(null, null, null, null, null, null, environment, null, null, null, null);

        InMemoryPlaintextBundleCache sharedCache = plugin.sharedBundleCacheForTesting();
        assertNotNull("createComponents must construct the shared cache", sharedCache);

        // Two different "shards" (distinguished by bundle name, which is already index/shard-scoped
        // in real callers) reading through the same shared instance must not collide or interfere.
        byte[] shardAPayload = "shard-a-bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] shardBPayload = "shard-b-bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        org.opensearch.serverless.storage.format.BundleFileEntry entry = new org.opensearch.serverless.storage.format.BundleFileEntry(
            "f",
            0,
            shardAPayload.length,
            0
        );
        java.util.concurrent.atomic.AtomicInteger callsA = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger callsB = new java.util.concurrent.atomic.AtomicInteger();

        byte[] firstA = sharedCache.readFile("bundle-shard-a", entry, (bundleName, e) -> {
            callsA.incrementAndGet();
            return shardAPayload;
        });
        byte[] firstB = sharedCache.readFile("bundle-shard-b", entry, (bundleName, e) -> {
            callsB.incrementAndGet();
            return shardBPayload;
        });
        byte[] secondA = sharedCache.readFile("bundle-shard-a", entry, (bundleName, e) -> {
            callsA.incrementAndGet();
            return shardAPayload;
        });

        assertArrayEquals(shardAPayload, firstA);
        assertArrayEquals(shardBPayload, firstB);
        assertArrayEquals(shardAPayload, secondA);
        assertEquals("second shard-a read must be a cache hit, not a re-fetch", 1, callsA.get());
        assertEquals(1, callsB.get());
    }
}
