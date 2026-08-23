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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Setting;
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
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServerlessStoragePluginTests extends OpenSearchTestCase {

    private ServerlessStoragePlugin newPlugin(Path basePath) {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin(Settings.EMPTY);
        if (basePath != null) {
            Settings nodeSettings = Settings.builder()
                .put("path.home", createTempDir().toString())
                .putList("path.repo", basePath.toString())
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
                .build();
            Environment environment = TestEnvironment.newEnvironment(nodeSettings);
            plugin.createComponents(null, clusterServiceFor(plugin), null, null, null, null, environment, null, null, null, null);
        }
        return plugin;
    }

    /**
     * A cluster service that can answer for the plugin's own settings.
     *
     * <p>This used to be null, along with every other collaborator, and the tests passed because
     * {@code createComponents} happened never to touch it. It does now: T28's wildcard cap registers a
     * settings update consumer, so the dynamic half of that setting is wired through here. Nineteen tests
     * then failed with a null cluster service, which reads like a regression and is not one. The fixture
     * was asserting against a shape a real node never has.
     *
     * <p>Only the plugin's node-scoped settings are registered, which is what a real node's
     * {@code ClusterSettings} would carry for it. {@code addSettingsUpdateConsumer} rejects a setting it
     * does not know, so this fails loudly if the plugin ever registers a consumer for something it forgot
     * to declare in {@code getSettings}, which is a real defect worth catching here rather than at
     * startup.
     */
    private static ClusterService clusterServiceFor(ServerlessStoragePlugin plugin) {
        Set<Setting<?>> nodeScoped = plugin.getSettings()
            .stream()
            .filter(Setting::hasNodeScope)
            .collect(java.util.stream.Collectors.toSet());
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, nodeScoped);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return clusterService;
    }

    /**
     * A serverless index's settings as a real one has them.
     *
     * <p>{@code index.recovery.strategy} is set alongside the opt-in because a real serverless index always
     * carries both -- {@code ServerlessStorageIndexSettingProvider} injects the strategy at creation, and
     * {@code getEngineFactory} refuses to build an engine without it (an index whose durable copy is in the
     * object store cannot be recovered by core's {@code local-lucene} strategy, and the plugin fails loudly
     * rather than let that happen quietly). Building the settings without it here would be constructing an
     * index shape that cannot exist.
     */
    private IndexSettings indexSettings(boolean enabled) {
        Index index = new Index("test-index", "test-index-uuid");
        Settings.Builder indexSettingsBuilder = Settings.builder();
        if (enabled) {
            indexSettingsBuilder.put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true);
            indexSettingsBuilder.put(
                org.opensearch.index.IndexModule.INDEX_RECOVERY_STRATEGY_SETTING.getKey(),
                org.opensearch.serverless.storage.writerengine.ObjectStoreShardRecoveryStrategy.NAME
            );
        }
        return IndexSettingsModule.newIndexSettings(
            index,
            indexSettingsBuilder.build(),
            Settings.EMPTY,
            ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING,
            org.opensearch.index.IndexModule.INDEX_RECOVERY_STRATEGY_SETTING
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

    /**
     * Bug hunt regression, round 1, finding 3: {@code getEngineFactory} is called with a null {@code
     * shardRouting} for purely administrative work (mapping validation/update -- {@code
     * IndicesService#createMapperServiceForValidation}/{@code #createIndexMapperService}, not a real
     * shard being opened), and the pre-existing shardIdValue fallback ("null routing defaults to
     * shard 0") must not be read by the pin-ledger sweep wiring as "this node hosts shard 0" -- a real
     * shard-0 assignment must still start the task.
     */
    public void testPinLedgerSweepIsNotSpawnedForAdministrativeNullShardRoutingCalls() throws Exception {
        org.opensearch.threadpool.ThreadPool threadPool = new org.opensearch.threadpool.TestThreadPool(getTestName());
        try {
            ServerlessStoragePlugin plugin = newPluginWithPinLedgerSweep(threadPool);
            IndexSettings settings = indexSettings(true);
            String indexUuid = settings.getIndex().getUUID();

            plugin.getEngineFactory(settings, null);
            assertNull("a null shardRouting must not be read as this node hosting shard 0", plugin.pinLedgerSweepTaskForTesting(indexUuid));

            ShardId shardId = new ShardId(settings.getIndex(), 0);
            ShardRouting primaryRouting = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);
            plugin.getEngineFactory(settings, primaryRouting);
            assertNotNull("a real shard-0 assignment must start the sweep task", plugin.pinLedgerSweepTaskForTesting(indexUuid));
        } finally {
            org.opensearch.threadpool.ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    /**
     * Bug hunt regression, round 2, finding 1: a serverless index is forced to zero ordinary replicas
     * and instead scales reads via search-only replicas of the SAME shard, so a search-only replica of
     * shard 0 hits this same wiring with {@code shardIdValue == 0} and a non-null routing entry -- the
     * two conditions the round 1 fix above already checks. Without also checking {@code
     * shardRouting.primary()}, every reader-replica copy of shard 0 would independently start its own
     * sweep task for the same index, scaling sweep traffic with read fan-out instead of staying at
     * roughly one sweeper per index.
     */
    public void testPinLedgerSweepIsNotSpawnedForASearchOnlyReplicaOfShardZero() throws Exception {
        org.opensearch.threadpool.ThreadPool threadPool = new org.opensearch.threadpool.TestThreadPool(getTestName());
        try {
            ServerlessStoragePlugin plugin = newPluginWithPinLedgerSweep(threadPool);
            IndexSettings settings = indexSettings(true);
            String indexUuid = settings.getIndex().getUUID();
            ShardId shardId = new ShardId(settings.getIndex(), 0);

            ShardRouting searchOnlyRouting = TestShardRouting.newShardRouting(
                shardId,
                "node-1",
                false,
                true,
                ShardRoutingState.INITIALIZING,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE
            );
            plugin.getEngineFactory(settings, searchOnlyRouting);
            assertNull(
                "a search-only replica of shard 0 must not start its own sweep task",
                plugin.pinLedgerSweepTaskForTesting(indexUuid)
            );

            ShardRouting primaryRouting = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);
            plugin.getEngineFactory(settings, primaryRouting);
            assertNotNull("the primary of shard 0 must still start the sweep task", plugin.pinLedgerSweepTaskForTesting(indexUuid));
        } finally {
            org.opensearch.threadpool.ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    /**
     * Bug hunt regression, round 1, finding 1: the pin-ledger sweep task was wired against the
     * delete-denied {@code scopedContainer} ("GET+PUT but no DELETE"), so {@code
     * PinLedgerSweeper#sweepOnce}'s {@code ledgerStore.delete(...)} call -- the entire point of the
     * sweep -- threw {@code SecurityException} on every pass. Proven through the real production
     * wiring: a ledger naming no live pins, written the same way {@code
     * TransportIndexSnapshotPinAction} writes one, must actually be gone after one real sweep pass.
     */
    public void testPinLedgerSweepCanActuallyDeleteAnAbandonedLedger() throws Exception {
        org.opensearch.threadpool.ThreadPool threadPool = new org.opensearch.threadpool.TestThreadPool(getTestName());
        try {
            ServerlessStoragePlugin plugin = newPluginWithPinLedgerSweep(threadPool);
            IndexSettings settings = indexSettings(true);
            String indexUuid = settings.getIndex().getUUID();
            ShardId shardId = new ShardId(settings.getIndex(), 0);
            ShardRouting primaryRouting = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);
            plugin.getEngineFactory(settings, primaryRouting);

            org.opensearch.serverless.storage.retention.PinLedgerSweepTask task = plugin.pinLedgerSweepTaskForTesting(indexUuid);
            assertNotNull(task);

            org.opensearch.common.blobstore.BlobContainer shard0Container = plugin.blobContainerForDirectoryFactory(indexUuid, 0);
            org.opensearch.serverless.storage.retention.BlobContainerPinLedgerStore ledgerStore =
                new org.opensearch.serverless.storage.retention.BlobContainerPinLedgerStore(shard0Container);
            ledgerStore.write(new org.opensearch.serverless.storage.retention.PinLedger("pin-1", "test-index", indexUuid, 1, 0));

            task.sweepNowForTesting(1000);

            assertTrue(
                "a ledger naming no live pins must actually be deleted by a real sweep pass, not merely attempted",
                ledgerStore.list().isEmpty()
            );
        } finally {
            org.opensearch.threadpool.ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private ServerlessStoragePlugin newPluginWithPinLedgerSweep(org.opensearch.threadpool.ThreadPool threadPool) throws Exception {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin(Settings.EMPTY);
        Path basePath = createTempDir();
        Settings nodeSettings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_PIN_LEDGER_SWEEP_INTERVAL_SETTING.getKey(), "1h")
            .build();
        Environment environment = TestEnvironment.newEnvironment(nodeSettings);
        plugin.createComponents(null, clusterServiceFor(plugin), threadPool, null, null, null, environment, null, null, null, null);
        return plugin;
    }

    public void testEncryptionKeyConfiguredDoesNotBreakEngineFactoryConstruction() throws Exception {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin(Settings.EMPTY);
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
        plugin.createComponents(null, clusterServiceFor(plugin), null, null, null, null, environment, null, null, null, null);

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
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin(Settings.EMPTY);
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
        plugin.createComponents(null, clusterServiceFor(plugin), null, null, null, null, environment, null, null, null, null);

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
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin(Settings.EMPTY);
        Path basePath = createTempDir();
        Settings nodeSettings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
            .build();
        Environment environment = TestEnvironment.newEnvironment(nodeSettings);
        plugin.createComponents(null, clusterServiceFor(plugin), null, null, null, null, environment, null, null, null, null);

        IndexSettings settings = indexSettings(true);
        ShardId shardId = new ShardId(settings.getIndex(), 0);
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);

        Optional<EngineFactory> factory = plugin.getEngineFactory(settings, primaryRouting);
        assertTrue(factory.isPresent());
        assertTrue(factory.get() instanceof WriterEngineFactory);
    }

    public void testWalPerShardBudgetSettingDefaultsToDisabled() {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin(Settings.EMPTY);
        Path basePath = createTempDir();
        Settings nodeSettings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
            .build();
        Environment environment = TestEnvironment.newEnvironment(nodeSettings);
        plugin.createComponents(null, clusterServiceFor(plugin), null, null, null, null, environment, null, null, null, null);
        // sharedWalChunkService is no longer built eagerly by createComponents -- it's resolved
        // lazily on first real writer-shard use (see resolveSharedWalChunkService()'s own javadoc),
        // so a writer EngineFactory must actually be requested first to trigger construction.
        triggerSharedWalChunkServiceResolution(plugin);

        assertEquals(0L, plugin.sharedWalChunkService().perShardBudgetBytesForTesting());
    }

    public void testWalPerShardBudgetSettingIsThreadedIntoTheSharedWalChunkService() {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin(Settings.EMPTY);
        Path basePath = createTempDir();
        Settings nodeSettings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_PER_SHARD_BUDGET_SETTING.getKey(), "512kb")
            .build();
        Environment environment = TestEnvironment.newEnvironment(nodeSettings);
        plugin.createComponents(null, clusterServiceFor(plugin), null, null, null, null, environment, null, null, null, null);
        triggerSharedWalChunkServiceResolution(plugin);

        assertEquals(512L * 1024, plugin.sharedWalChunkService().perShardBudgetBytesForTesting());
    }

    /** Drives a real writer EngineFactory build, the only production path that resolves sharedWalChunkService -- see that method's own javadoc. */
    private void triggerSharedWalChunkServiceResolution(ServerlessStoragePlugin plugin) {
        IndexSettings settings = indexSettings(true);
        ShardId shardId = new ShardId(settings.getIndex(), 0);
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);
        Optional<EngineFactory> factory = plugin.getEngineFactory(settings, primaryRouting);
        assertTrue("test setup must actually produce a writer EngineFactory", factory.isPresent());
    }

    public void testLazyDirectoryFileCacheIsNotConstructedByDefault() {
        ServerlessStoragePlugin plugin = newPlugin(createTempDir());
        assertNull(
            "the lazy-directory file cache must stay off by default, matching every other " + "optional-feature-off default in this plugin",
            plugin.lazyDirectoryFileCacheForDirectoryFactory()
        );
    }

    public void testLazyDirectoryCacheSizeSettingConstructsARealCache() {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin(Settings.EMPTY);
        Path basePath = createTempDir();
        Settings nodeSettings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_LAZY_DIRECTORY_CACHE_SIZE_SETTING.getKey(), "16mb")
            .build();
        Environment environment = TestEnvironment.newEnvironment(nodeSettings);
        plugin.createComponents(null, clusterServiceFor(plugin), null, null, null, null, environment, null, null, null, null);

        assertNotNull(
            "an explicit positive cache size must construct a real FileCache",
            plugin.lazyDirectoryFileCacheForDirectoryFactory()
        );
    }

    public void testReaderShardAdmissionControllerIsNotConstructedByDefault() {
        ServerlessStoragePlugin plugin = newPlugin(createTempDir());
        assertNull(
            "the admission controller must stay off by default (max_concurrent_reader_shards <= 0), "
                + "matching every other optional-feature-off default in this plugin",
            plugin.readerShardAdmissionControllerForTesting()
        );
    }

    public void testReaderShardAdmissionControllerSettingConstructsARealController() {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin(Settings.EMPTY);
        Path basePath = createTempDir();
        Settings nodeSettings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_MAX_CONCURRENT_READER_SHARDS_SETTING.getKey(), 5)
            .build();
        Environment environment = TestEnvironment.newEnvironment(nodeSettings);
        plugin.createComponents(null, clusterServiceFor(plugin), null, null, null, null, environment, null, null, null, null);

        assertNotNull(
            "an explicit positive max_concurrent_reader_shards must construct a real controller",
            plugin.readerShardAdmissionControllerForTesting()
        );
    }

    public void testWalGcSchedulerTaskIsNotConstructedByDefault() {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin(Settings.EMPTY);
        Path basePath = createTempDir();
        Settings nodeSettings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
            .build();
        Environment environment = TestEnvironment.newEnvironment(nodeSettings);
        plugin.createComponents(null, clusterServiceFor(plugin), null, null, null, null, environment, null, null, null, null);

        assertNull(
            "the WAL GC scheduler must stay off by default (non-positive interval), matching every "
                + "other optional-feature-off default in this plugin",
            plugin.walGcSchedulerTaskForTesting()
        );
    }

    // Regression test for a real bug: Plugin#close() only ever cancelled walGcSchedulerTask -- the
    // five other node-level (cluster-manager-only, one-per-node) background schedulers
    // (scaleToZeroCandidatesSchedulerTask, scaleUpCandidatesSchedulerTask,
    // dataStreamShardCountAdvisorSchedulerTask, inPlaceSplitTriggerSchedulerTask,
    // inPlaceMergeTriggerSchedulerTask) were simply left running with nothing to cancel them on
    // plugin shutdown. Exercises one of them concretely (via a real, non-test-only isCancelled()
    // check) rather than just confirming close() doesn't throw, which wouldn't have caught the bug.
    public void testCloseCancelsTheScaleToZeroCandidatesSchedulerTask() throws Exception {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin(Settings.EMPTY);
        Path basePath = createTempDir();
        Settings nodeSettings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(
                ServerlessStoragePlugin.SERVERLESS_STORAGE_SCALE_TO_ZERO_EVAL_INTERVAL_SETTING.getKey(),
                org.opensearch.common.unit.TimeValue.timeValueMinutes(5)
            )
            .build();
        Environment environment = TestEnvironment.newEnvironment(nodeSettings);
        org.opensearch.threadpool.ThreadPool threadPool = new org.opensearch.threadpool.TestThreadPool(getTestName());
        try {
            plugin.createComponents(null, clusterServiceFor(plugin), threadPool, null, null, null, environment, null, null, null, null);

            org.opensearch.serverless.storage.scaletozero.ScaleToZeroCandidatesSchedulerTask task = plugin
                .scaleToZeroCandidatesSchedulerTaskForTesting();
            assertNotNull("a positive eval interval must construct a real scheduled task", task);
            assertFalse("must be actively scheduled before close()", task.isCancelledForTesting());

            plugin.close();

            assertTrue("Plugin#close() must cancel this task, not just walGcSchedulerTask", task.isCancelledForTesting());
        } finally {
            org.opensearch.threadpool.ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    public void testNodeSelfWarmupSchedulerTaskIsNotConstructedByDefault() {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin(Settings.EMPTY);
        Path basePath = createTempDir();
        Settings nodeSettings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
        Environment environment = TestEnvironment.newEnvironment(nodeSettings);
        plugin.createComponents(null, clusterServiceFor(plugin), null, null, null, null, environment, null, null, null, null);

        assertNull(
            "the node self-warmup scheduler must stay off by default (non-positive interval), "
                + "matching every other optional-feature-off default in this plugin",
            plugin.nodeSelfWarmupSchedulerTaskForTesting()
        );
    }

    public void testCloseCancelsTheNodeSelfWarmupSchedulerTask() throws Exception {
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin(Settings.EMPTY);
        Path basePath = createTempDir();
        Settings nodeSettings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(
                ServerlessStoragePlugin.SERVERLESS_STORAGE_NODE_SELF_WARMUP_EVAL_INTERVAL_SETTING.getKey(),
                org.opensearch.common.unit.TimeValue.timeValueMinutes(5)
            )
            .build();
        Environment environment = TestEnvironment.newEnvironment(nodeSettings);
        org.opensearch.threadpool.ThreadPool threadPool = new org.opensearch.threadpool.TestThreadPool(getTestName());
        try {
            plugin.createComponents(null, clusterServiceFor(plugin), threadPool, null, null, null, environment, null, null, null, null);

            org.opensearch.serverless.storage.nodecapacity.NodeSelfWarmupSchedulerTask task = plugin
                .nodeSelfWarmupSchedulerTaskForTesting();
            assertNotNull("a positive eval interval must construct a real scheduled task", task);
            assertFalse("must be actively scheduled before close()", task.isCancelledForTesting());

            plugin.close();

            assertTrue("Plugin#close() must cancel this task too", task.isCancelledForTesting());
        } finally {
            org.opensearch.threadpool.ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
        }
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
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin(Settings.EMPTY);
        plugin.createComponents(null, clusterServiceFor(plugin), null, null, null, null, environment, null, null, null, null);

        InMemoryPlaintextBundleCache sharedCache = plugin.sharedBundleCache();
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

    public void testEveryDeclaredSettingFieldIsRegisteredInGetSettings() throws IllegalAccessException {
        // Regression guard: a new `public static final Setting<?>` field added to this class (as
        // SERVERLESS_STORAGE_WAL_PER_SHARD_BUDGET_SETTING once was) is only ever actually usable if
        // it's also added to getSettings() -- core silently ignores any setting a plugin doesn't
        // register there, so a missed registration fails silently (the setting compiles, has a
        // default, and is even read via `SETTING.get(environment.settings())`, but a real operator
        // can never actually configure it). This walks the class's own declared fields via
        // reflection so a future omission fails this test immediately, not months later.
        java.util.Set<String> declaredSettingKeys = new java.util.HashSet<>();
        for (java.lang.reflect.Field field : ServerlessStoragePlugin.class.getFields()) {
            if (field.getDeclaringClass() == ServerlessStoragePlugin.class && Setting.class.isAssignableFrom(field.getType())) {
                Setting<?> setting = (Setting<?>) field.get(null);
                declaredSettingKeys.add(setting.getKey());
            }
        }
        assertFalse("expected at least one declared Setting field to sanity-check the reflection itself", declaredSettingKeys.isEmpty());

        java.util.Set<String> registeredSettingKeys = new ServerlessStoragePlugin(Settings.EMPTY).getSettings()
            .stream()
            .map(Setting::getKey)
            .collect(java.util.stream.Collectors.toSet());

        assertEquals(
            "every public static Setting field on this class must be registered in getSettings(), "
                + "or it silently can never be configured by an operator",
            declaredSettingKeys,
            registeredSettingKeys
        );
    }

    public void testEveryActionHasAMatchingRestHandlerAndViceVersa() {
        // A transport action registered via getActions() with no corresponding REST route in
        // getRestHandlers() (or vice versa) is only reachable from Java code within the plugin --
        // exactly the state ShardCloneAction and CompactionTriggerAction were both meant to escape.
        // This doesn't inspect each handler's actual route target (RestHandler exposes no
        // machine-readable "which action does this dispatch to" surface), but the count itself is
        // still a real, cheap signal: it catches the exact mistake of adding one without the other.
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin(Settings.EMPTY);
        int actionCount = plugin.getActions().size();
        int restHandlerCount = plugin.getRestHandlers(null, null, null, null, null, null, null).size();
        assertEquals(
            "getActions() and getRestHandlers() must register the same number of entries -- "
                + "every transport action this plugin exposes must have a REST route, and vice versa",
            actionCount,
            restHandlerCount
        );
        assertTrue("expected at least one action to sanity-check this comparison itself", actionCount > 0);
    }

    public void testEveryRestHandlerDeclaresItselfAvailableUnderServerlessMode() {
        // RestHandler#apiAvailabilityScope() (rfc-serverless-opensearch.md &sect;11) defaults to
        // UNAVAILABLE precisely so new handlers must opt in consciously -- every handler this
        // plugin exposes is itself part of the serverless storage feature, so an unannotated (or
        // wrongly-UNAVAILABLE) one here would be silently unreachable the moment serverless-mode
        // enforcement is wired into RestController, exactly the kind of easy-to-miss omission this
        // annotation exists to catch before that enforcement lands.
        ServerlessStoragePlugin plugin = new ServerlessStoragePlugin(Settings.EMPTY);
        for (org.opensearch.rest.RestHandler handler : plugin.getRestHandlers(null, null, null, null, null, null, null)) {
            assertEquals(
                handler.getClass().getSimpleName() + " must declare itself AVAILABLE under serverless mode",
                org.opensearch.rest.RestHandler.ApiAvailabilityScope.AVAILABLE,
                handler.apiAvailabilityScope()
            );
        }
    }

    /**
     * S3-efficiency/data-safety regression test: the GC retention window is the sole time-based
     * margin protecting a freshly-published manifest from deletion before
     * PitrRetentionSchedulerTask's own next tick ever gets a chance to pin it -- a window shorter
     * than that reconcile cadence would defeat the margin entirely.
     */
    public void testGcRetentionWindowRejectsAValueShorterThanThePitrReconcileInterval() {
        long tooShortMillis = org.opensearch.serverless.storage.retention.PitrRetentionSchedulerTask.DEFAULT_RECONCILE_INTERVAL.millis() * 2
            - 1;
        Settings settings = Settings.builder()
            .put(
                ServerlessStoragePlugin.SERVERLESS_STORAGE_GC_RETENTION_WINDOW_SETTING.getKey(),
                org.opensearch.common.unit.TimeValue.timeValueMillis(tooShortMillis).getStringRep()
            )
            .build();
        expectThrows(
            IllegalArgumentException.class,
            () -> ServerlessStoragePlugin.SERVERLESS_STORAGE_GC_RETENTION_WINDOW_SETTING.get(settings)
        );
    }

    public void testGcRetentionWindowAcceptsTheDefaultAndValuesAtOrAboveTheMinimum() {
        assertEquals(
            org.opensearch.common.unit.TimeValue.timeValueMinutes(30),
            ServerlessStoragePlugin.SERVERLESS_STORAGE_GC_RETENTION_WINDOW_SETTING.get(Settings.EMPTY)
        );

        long exactlyMinimumMillis = org.opensearch.serverless.storage.retention.PitrRetentionSchedulerTask.DEFAULT_RECONCILE_INTERVAL
            .millis() * 2;
        Settings settings = Settings.builder()
            .put(
                ServerlessStoragePlugin.SERVERLESS_STORAGE_GC_RETENTION_WINDOW_SETTING.getKey(),
                org.opensearch.common.unit.TimeValue.timeValueMillis(exactlyMinimumMillis).getStringRep()
            )
            .build();
        assertEquals(
            org.opensearch.common.unit.TimeValue.timeValueMillis(exactlyMinimumMillis),
            ServerlessStoragePlugin.SERVERLESS_STORAGE_GC_RETENTION_WINDOW_SETTING.get(settings)
        );
    }
}
