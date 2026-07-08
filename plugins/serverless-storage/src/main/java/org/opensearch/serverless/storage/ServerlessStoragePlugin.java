/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.shard.IndexSettingProvider;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.serverless.storage.allocation.ReaderShardPlacementAllocationDecider;
import org.opensearch.serverless.storage.allocation.ServerlessStorageExistingShardsAllocator;
import org.opensearch.serverless.storage.directory.InMemoryShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileReader;
import org.opensearch.serverless.storage.format.CachingBundleFileReader;
import org.opensearch.serverless.storage.format.InMemoryPlaintextBundleCache;
import org.opensearch.serverless.storage.format.LocalDiskCachingBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.readerengine.ReaderEngineFactory;
import org.opensearch.serverless.storage.readerengine.ReaderShardAdmissionController;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.retention.PitrRetentionConfig;
import org.opensearch.serverless.storage.security.EncryptingBlobContainer;
import org.opensearch.serverless.storage.security.EncryptionKeyProvider;
import org.opensearch.serverless.storage.security.StaticEncryptionKeyProvider;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitHeadPublisher;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.serverless.storage.writerengine.WriterEngineFactory;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Entry point for the object-store-native serverless storage format: segment bundles, commit
 * manifests, WAL, GC, and the writer/reader engines described in {@code rfc-serverless-opensearch.md}.
 *
 * <p>An index only gets an object-store engine if it explicitly opts in via {@link
 * #SERVERLESS_STORAGE_ENABLED_SETTING}; every other index is untouched (returns {@link
 * Optional#empty()}, so the platform's default engine applies), matching Goal 6 of the RFC:
 * classic mode stays default and untouched.
 *
 * <p>The blob container backing an opted-in index is, for now, always a local-filesystem
 * container rooted at {@link #SERVERLESS_STORAGE_BASE_PATH_SETTING}. S3, GCS, and Azure all have
 * real, tested {@code compareAndSwapRegister} implementations in their own repository plugins
 * (see {@code S3BlobContainer}/{@code GoogleCloudStorageBlobStore}/{@code AzureBlobStore}); this
 * plugin doesn't yet construct one of those concrete containers instead of the local-filesystem
 * one, which is the remaining piece of wiring, not a correctness gap in the register primitive
 * itself. Swapping in a real repository-backed container only touches {@link #blobContainerFor};
 * nothing else in this class or the engine/factory classes it wires together is FS-specific.
 */
public class ServerlessStoragePlugin extends Plugin implements EnginePlugin, ClusterPlugin {

    public static final Setting<Boolean> SERVERLESS_STORAGE_ENABLED_SETTING = Setting.boolSetting(
        "index.serverless_storage.enabled",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<String> SERVERLESS_STORAGE_BASE_PATH_SETTING = Setting.simpleString(
        "serverless_storage.base_path",
        Setting.Property.NodeScope
    );

    /**
     * Base64-encoded raw AES key bytes (16/24/32 bytes decoded, for AES-128/192/256). A keystore
     * secret, not a plaintext setting, since it's key material -- matches how repository-s3/gcs/
     * azure hold their own credentials. Optional: if unset, blob content is stored unencrypted
     * (today's default, unchanged for every existing deployment of this plugin).
     */
    public static final Setting<SecureString> SERVERLESS_STORAGE_ENCRYPTION_KEY_SETTING = SecureSetting.secureString(
        "serverless_storage.encryption_key",
        null
    );

    /**
     * Budget for the node-shared in-memory plaintext bundle-file cache in front of every reader
     * shard's {@link LocalDiskCachingBundleStore} (one {@link InMemoryPlaintextBundleCache}
     * instance per node, not per shard -- see its javadoc for why a per-shard cache doesn't
     * compose at scale: a thousand reader shards on one node, each with its own fixed cap, has no
     * relationship to what the node can actually afford). Expressed as a percentage of heap (or an
     * absolute byte value), the same idiom {@code indices.fielddata.cache.size} uses -- percentage
     * of *heap* specifically, not of node-wide native memory the way {@code
     * indices.memory.native_index_buffer_size} is, because entries are plain heap {@code byte[]}
     * today: true off-heap storage would need the JDK Foreign Memory API, which is still a preview
     * feature on this project's JDK 21 toolchain (confirmed by compiling against it) and not
     * something to enable build-wide for one cache. A conservative default, not a tuned one --
     * unlike fielddata's long-validated 35%, this cache has no production experience behind it yet.
     */
    public static final Setting<ByteSizeValue> SERVERLESS_STORAGE_BUNDLE_CACHE_SIZE_SETTING = Setting.memorySizeSetting(
        "serverless_storage.bundle_cache.size",
        "5%",
        Setting.Property.NodeScope
    );

    /**
     * How far back point-in-time recovery must be possible (rfc-serverless-opensearch.md &sect;16
     * Phase 4.6). A non-positive value (the default) disables PITR retention entirely for every
     * index -- no {@code "pitr"} pins are ever added, matching how {@code
     * encryptionKeyProvider}/{@code PitrRetentionConfig} being absent means "this feature is off"
     * elsewhere in this plugin, not a missing configuration error.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_PITR_WINDOW_SETTING = Setting.timeSetting(
        "serverless_storage.pitr_window",
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );

    /**
     * A coarse cap on how many reader shards may be open at once on this node
     * (rfc-serverless-opensearch.md &sect;18 risk #3) -- see {@link ReaderShardAdmissionController}'s
     * javadoc for exactly what this does and does not protect against. Non-positive (the default)
     * disables it entirely, same shape as every other optional-feature-off default in this plugin.
     */
    public static final Setting<Integer> SERVERLESS_STORAGE_MAX_CONCURRENT_READER_SHARDS_SETTING = Setting.intSetting(
        "serverless_storage.max_concurrent_reader_shards",
        -1,
        Setting.Property.NodeScope
    );

    /**
     * Enables the node-level WAL service (rfc-serverless-opensearch.md &sect;6.4): every writer
     * shard's translog additionally mirrors each operation into a shared, node-scoped WAL chunk
     * stream, durable ahead of the next commit/publish. One {@link WalChunkService} instance is
     * built per node incarnation (see {@link #createComponents}) and shared by every writer shard
     * on the node -- the entire reason this is a node-level service and not a per-shard one is the
     * cross-shard group-commit batching that sharing enables (&sect;6.4's cost-sanity argument).
     * Default off: this is new wiring, without production experience behind it yet, unlike the
     * commit-publish path it sits alongside.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING = Setting.boolSetting(
        "serverless_storage.wal_mirroring.enabled",
        false,
        Setting.Property.NodeScope
    );

    private volatile Path basePath;
    private volatile Path localCacheRoot;
    private volatile EncryptionKeyProvider encryptionKeyProvider;
    private volatile String localNodeId = "unknown-node";
    private volatile InMemoryPlaintextBundleCache sharedBundleCache;
    private volatile long pitrWindowMillis = -1;
    private volatile ReaderShardAdmissionController readerShardAdmissionController;
    private volatile WalChunkService sharedWalChunkService;
    // One node-local directory instance shared by every shard on this node -- matches the target
    // design's "one node block cache" shape (&sect;9) rather than a per-shard instance, and needs
    // no I/O to construct, so it's safe to build eagerly rather than threading through createComponents.
    private final ShardDirectory shardDirectory = new InMemoryShardDirectory();

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            SERVERLESS_STORAGE_ENABLED_SETTING,
            SERVERLESS_STORAGE_BASE_PATH_SETTING,
            SERVERLESS_STORAGE_ENCRYPTION_KEY_SETTING,
            SERVERLESS_STORAGE_BUNDLE_CACHE_SIZE_SETTING,
            SERVERLESS_STORAGE_PITR_WINDOW_SETTING,
            SERVERLESS_STORAGE_MAX_CONCURRENT_READER_SHARDS_SETTING,
            SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING
        );
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        String configuredBasePath = SERVERLESS_STORAGE_BASE_PATH_SETTING.get(environment.settings());
        if (configuredBasePath.isEmpty() == false) {
            // Environment#resolveRepoFile is the same sanctioned path-resolution seam
            // repository-fs / repository-url use: it refuses to resolve anything outside the
            // node's configured allowed-paths, rather than trusting an arbitrary settings string.
            basePath = environment.resolveRepoFile(configuredBasePath);
        }
        if (nodeEnvironment != null && nodeEnvironment.nodeDataPaths().length > 0) {
            localCacheRoot = nodeEnvironment.nodeDataPaths()[0].resolve("serverless_storage_cache");
        }
        if (nodeEnvironment != null) {
            // Deliberately not ClusterService#localNode(): that reads ClusterService#state(), which
            // is both not yet available this early in node startup ("initial cluster state not set
            // yet") *and* unsafe to call later too -- getEngineFactory runs from within
            // IndicesClusterStateService's own cluster-state-applier callback (shard creation is a
            // reaction to a newly applied cluster state), so calling ClusterService#state() there,
            // even lazily, trips ClusterApplierService's own reentrancy assertion ("should not be
            // called by a cluster state applier"). NodeEnvironment#nodeId() is this node's own
            // persisted identity -- the same ID that becomes this node's DiscoveryNode#getId() once
            // cluster state exists -- available immediately, with no cluster-state dependency at all.
            localNodeId = nodeEnvironment.nodeId();
        }
        sharedBundleCache = new InMemoryPlaintextBundleCache(
            SERVERLESS_STORAGE_BUNDLE_CACHE_SIZE_SETTING.get(environment.settings()).getBytes()
        );
        pitrWindowMillis = SERVERLESS_STORAGE_PITR_WINDOW_SETTING.get(environment.settings()).millis();
        int maxConcurrentReaderShards = SERVERLESS_STORAGE_MAX_CONCURRENT_READER_SHARDS_SETTING.get(environment.settings());
        readerShardAdmissionController = maxConcurrentReaderShards > 0
            ? new ReaderShardAdmissionController(maxConcurrentReaderShards)
            : null;
        try (SecureString encryptionKey = SERVERLESS_STORAGE_ENCRYPTION_KEY_SETTING.get(environment.settings())) {
            if (encryptionKey.length() > 0) {
                byte[] rawKeyBytes = Base64.getDecoder().decode(new String(encryptionKey.getChars()));
                encryptionKeyProvider = StaticEncryptionKeyProvider.fromRawKeyBytes(rawKeyBytes);
            }
        }
        if (basePath != null && SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.get(environment.settings())) {
            try {
                // A dedicated top-level container, separate from every index/shard's own
                // <indexUuid>/<shardId>/ path -- the WAL chunk stream is node-scoped, spanning
                // every writer shard (and index) on this node, not any one shard's storage
                // (rfc-serverless-opensearch.md &sect;6.4, see WalChunkService's own javadoc).
                FsBlobStore walBlobStore = new FsBlobStore(1024 * 1024, basePath, false);
                BlobContainer walBlobContainer = walBlobStore.blobContainer(BlobPath.cleanPath().add("wal"));
                // A fresh epoch per node incarnation (see WalChunkService's own javadoc for what
                // this identifies) -- fencing during replay is a per-record primaryTerm filter, not
                // an epoch-directory one, so nothing depends on this value being stable across
                // restarts; it only needs to be unique enough that this process's chunk sequence
                // numbering never collides with a prior incarnation's.
                sharedWalChunkService = new WalChunkService(walBlobContainer, UUIDs.base64UUID());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return Collections.emptyList();
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings, ShardRouting shardRouting) {
        if (SERVERLESS_STORAGE_ENABLED_SETTING.get(indexSettings.getSettings()) == false) {
            return Optional.empty();
        }
        if (basePath == null) {
            throw new IllegalStateException(
                "index ["
                    + indexSettings.getIndex().getName()
                    + "] has serverless storage enabled but no ["
                    + SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey()
                    + "] node setting was configured (or it did not resolve to an allowed path)"
            );
        }

        try {
            String indexUuid = indexSettings.getIndex().getUUID();
            int shardIdValue = shardRouting != null ? shardRouting.shardId().getId() : 0;
            // Each shard gets its own child container (rfc-serverless-opensearch.md &sect;6.1's
            // indices/<index-uuid>/<shard>/ layout): CommitManifest#manifestName() is intentionally
            // just <term>-<generation> with no index/shard component, since it assumes the
            // container it lives in is already shard-scoped.
            BlobContainer blobContainer = blobContainerFor(basePath, indexUuid, shardIdValue);
            if (encryptionKeyProvider != null) {
                // Wrapping here, at the one seam every downstream class already depends on
                // abstractly (BlobContainer), is the entire integration -- see
                // EncryptingBlobContainer's javadoc for the ranged-read tradeoff this implies.
                blobContainer = new EncryptingBlobContainer(blobContainer, encryptionKeyProvider);
            }
            ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
            BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
            BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);

            boolean isReaderShard = shardRouting != null && shardRouting.isSearchOnly();
            if (isReaderShard) {
                BundleFileReader readPath = bundleStore;
                if (localCacheRoot != null) {
                    // The cache layer (rfc-serverless-opensearch.md &sect;9): a reader shard
                    // re-fetches the same bundle files across queries far more often than a writer
                    // re-reads its own recent writes, so caching is wired in for reader shards only.
                    // Not to be confused with the *shard-location* directory tier (metadata-plane
                    // RFC &sect;8/&sect;9/&sect;11, `shardDirectory` below) -- this one caches
                    // bytes, that one caches "which node has this shard open."
                    Path shardCacheDir = localCacheRoot.resolve(indexUuid).resolve(String.valueOf(shardIdValue));
                    // The disk tier holds ciphertext when encryption is enabled (encryptionKeyProvider
                    // non-null), so a reader node's local disk/page cache never holds plaintext at
                    // rest -- see LocalDiskCachingBundleStore's javadoc. The in-memory tier in front
                    // of it (one instance shared by every reader shard on the node, not one per
                    // shard -- see InMemoryPlaintextBundleCache's javadoc) is what keeps the
                    // actually-hot working set decrypted, so most reads never pay that decrypt cost
                    // repeatedly; only a disk-cache hit that missed this layer does.
                    BundleFileReader diskCache = new LocalDiskCachingBundleStore(bundleStore, shardCacheDir, encryptionKeyProvider);
                    readPath = new CachingBundleFileReader(sharedBundleCache, diskCache);
                }
                return Optional.of(
                    new ReaderEngineFactory(
                        shardStateStore,
                        manifestStore,
                        new ObjectStoreCommitMaterializer(readPath),
                        shardDirectory,
                        localNodeId,
                        readerShardAdmissionController
                    )
                );
            }
            ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
            PitrRetentionConfig pitrRetentionConfig = null;
            if (pitrWindowMillis > 0) {
                // Same blob container every other per-shard store here is scoped to -- a durable
                // pin lives alongside the shard's manifests/registers, not in some separate
                // namespace, matching how BlobContainerShardStateStore/BlobContainerManifestStore
                // are wired above.
                DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(blobContainer);
                pitrRetentionConfig = new PitrRetentionConfig(manifestStore, pinRegistry, pitrWindowMillis);
            }
            return Optional.of(
                new WriterEngineFactory(
                    new ObjectStoreCommitHeadPublisher(commitPublisher, shardStateStore),
                    shardDirectory,
                    localNodeId,
                    pitrRetentionConfig,
                    sharedWalChunkService,
                    // Cross-node failover materializes at most once per activation (rfc-serverless-opensearch.md
                    // &sect;7.1.2), not per-query like a reader shard -- no caching layer needed,
                    // straight to the bundle store, matching the "caching is wired in for reader
                    // shards only" note on the reader path just above.
                    new ObjectStoreCommitMaterializer(bundleStore)
                )
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static BlobContainer blobContainerFor(Path basePath, String indexUuid, int shardId) throws IOException {
        FsBlobStore blobStore = new FsBlobStore(1024 * 1024, basePath, false);
        BlobPath shardPath = BlobPath.cleanPath().add(indexUuid).add(String.valueOf(shardId));
        return blobStore.blobContainer(shardPath);
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return Collections.singletonList(new ReaderShardPlacementAllocationDecider());
    }

    /**
     * Registers {@link ServerlessStorageExistingShardsAllocator} under its own name -- see that
     * class's own javadoc for why serverless-storage indices need it instead of the default
     * gateway allocator (rfc-serverless-opensearch.md &sect;7.1.2). {@link
     * ServerlessStorageIndexSettingProvider} is what actually selects it per-index; registering it
     * here alone has no effect on any index that doesn't also opt in via that provider.
     */
    @Override
    public Map<String, ExistingShardsAllocator> getExistingShardsAllocators() {
        return Collections.singletonMap(ServerlessStorageExistingShardsAllocator.NAME, new ServerlessStorageExistingShardsAllocator());
    }

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders() {
        return Collections.singletonList(new ServerlessStorageIndexSettingProvider());
    }

    /** The node-shared bundle cache {@link #createComponents} built -- test-only visibility, not part of the plugin's contract. */
    InMemoryPlaintextBundleCache sharedBundleCacheForTesting() {
        return sharedBundleCache;
    }
}
