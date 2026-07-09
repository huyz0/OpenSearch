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
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.serverless.storage.allocation.ReaderShardPlacementAllocationDecider;
import org.opensearch.serverless.storage.allocation.ServerlessStorageExistingShardsAllocator;
import org.opensearch.serverless.storage.compaction.CompactionPolicy;
import org.opensearch.serverless.storage.compaction.CompactionRebaseExecutor;
import org.opensearch.serverless.storage.compaction.CompactionSchedulerConfig;
import org.opensearch.serverless.storage.directory.InMemoryShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileReader;
import org.opensearch.serverless.storage.format.CachingBundleFileReader;
import org.opensearch.serverless.storage.format.InMemoryPlaintextBundleCache;
import org.opensearch.serverless.storage.format.LocalDiskCachingBundleStore;
import org.opensearch.serverless.storage.gc.GcSchedulerConfig;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.readerengine.ReaderEngineFactory;
import org.opensearch.serverless.storage.readerengine.ReaderShardAdmissionController;
import org.opensearch.serverless.storage.readerengine.lazydirectory.ServerlessStorageLazyDirectoryFactory;
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
public class ServerlessStoragePlugin extends Plugin implements EnginePlugin, ClusterPlugin, IndexStorePlugin {

    /**
     * The {@code index.store.type} value that opts a reader (search-only) shard copy into a
     * {@code LazyBundleDirectory} instead of a normal local {@code FSDirectory}
     * (rfc-serverless-opensearch.md &sect;7.2/&sect;9) -- see {@link ServerlessStorageLazyDirectoryFactory}'s
     * own javadoc for how this closes the directory-swap gap that section's status note left open.
     * A writer/primary copy on the same index is completely unaffected by this store type being
     * selected -- {@link ServerlessStorageLazyDirectoryFactory} only substitutes the lazy directory
     * for a {@code ShardRouting} that {@link ShardRouting#isSearchOnly()}.
     */
    public static final String LAZY_DIRECTORY_STORE_TYPE = "serverless_storage_lazy";

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
     * The fraction of the lazy-directory block cache's byte capacity above which
     * {@link ReaderShardAdmissionController} refuses to open another reader shard on this node,
     * even if {@link #SERVERLESS_STORAGE_MAX_CONCURRENT_READER_SHARDS_SETTING}'s count cap still
     * has headroom -- see that controller's own javadoc for why this, not just a shard count, is
     * now a real, checkable budget once {@link #SERVERLESS_STORAGE_LAZY_DIRECTORY_CACHE_SIZE_SETTING}
     * configures a shared {@link FileCache}. Only takes effect when both that cache and the count
     * cap above are configured; ignored otherwise.
     */
    public static final Setting<Double> SERVERLESS_STORAGE_MAX_FILE_CACHE_USAGE_RATIO_SETTING = Setting.doubleSetting(
        "serverless_storage.reader_admission.max_file_cache_usage_ratio",
        0.9,
        Math.nextUp(0.0),
        1.0,
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

    /**
     * How often a reader shard's own background {@code CompactionSchedulerTask} evaluates whether
     * its shard is worth compacting (rfc-serverless-opensearch.md &sect;16 Phase 4.5). A reader
     * shard is this scheduler's home rather than a writer shard: a writer's own lease is always
     * held while that writer is open, so a scheduler attached to the writer itself would always see
     * its own lease as held and never do anything -- see {@code ObjectStoreReaderEngine}'s own
     * javadoc. Non-positive (the default) disables background compaction scheduling entirely, same
     * shape as every other optional-feature-off default in this plugin -- a quiescent shard's
     * segments then only shrink via {@code _forcemerge} while a writer happens to be active, exactly
     * today's (pre-this-feature) behavior.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_COMPACTION_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.compaction.interval",
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );

    /**
     * How often a reader shard's own background {@code GcSchedulerTask} sweeps for deletable
     * manifests/bundles (rfc-serverless-opensearch.md &sect;6.5). Same reader-shard-is-the-home
     * rationale as {@link #SERVERLESS_STORAGE_COMPACTION_INTERVAL_SETTING}. Non-positive (the
     * default) disables background GC entirely -- storage then only ever grows, exactly today's
     * (pre-this-feature) behavior, which is safe (nothing correctness-bearing depends on GC ever
     * running) but not sustainable to leave off indefinitely in a real deployment.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_GC_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.gc.interval",
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );

    /**
     * The sole time-based safety margin the GC sweep relies on -- see {@code GcSchedulerTask}'s own
     * javadoc for why this, not a lease-pin signal, is the real protection against deleting a
     * manifest some reader still has open. Deliberately generous by default (30 minutes): comfortably
     * longer than any legitimate reader's own manifest-generation lag, bounded by {@code
     * ObjectStoreReaderEngine}'s 5 s poll interval.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_GC_RETENTION_WINDOW_SETTING = Setting.timeSetting(
        "serverless_storage.gc.retention_window",
        TimeValue.timeValueMinutes(30),
        Setting.Property.NodeScope
    );

    /**
     * Budget for the one node-shared {@link FileCache} backing every reader shard opted into
     * {@link #LAZY_DIRECTORY_STORE_TYPE} (rfc-serverless-opensearch.md &sect;9's "one node block
     * cache" target) -- reused directly from core's own searchable-snapshots feature, not a new
     * cache built for this plugin. Zero (the default) disables the lazy directory feature entirely:
     * {@link ServerlessStorageLazyDirectoryFactory} falls back to a normal {@code FSDirectory} for
     * every shard copy when no {@link FileCache} exists, matching how every other new-and-unproven
     * feature in this plugin defaults off.
     */
    public static final Setting<ByteSizeValue> SERVERLESS_STORAGE_LAZY_DIRECTORY_CACHE_SIZE_SETTING = Setting.byteSizeSetting(
        "serverless_storage.lazy_directory.cache_size",
        ByteSizeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * Per-index opt-in into {@link #LAZY_DIRECTORY_STORE_TYPE} for that index's reader shard
     * copies, mirroring how {@link #SERVERLESS_STORAGE_ENABLED_SETTING} itself is an index-scoped
     * opt-in rather than a blanket node-wide default. Has no effect unless {@link
     * #SERVERLESS_STORAGE_LAZY_DIRECTORY_CACHE_SIZE_SETTING} is also configured on the node --
     * {@link ServerlessStorageIndexSettingProvider} is what actually translates this into {@code
     * index.store.type} at index-creation time, the same indirection already used for {@link
     * org.opensearch.serverless.storage.allocation.ServerlessStorageExistingShardsAllocator}'s own
     * selection.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_LAZY_DIRECTORY_ENABLED_SETTING = Setting.boolSetting(
        "index.serverless_storage.lazy_directory.enabled",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    private volatile Path basePath;
    private volatile Path localCacheRoot;
    private volatile ThreadPool threadPool;
    private volatile FileCache lazyDirectoryFileCache;
    private volatile EncryptionKeyProvider encryptionKeyProvider;
    private volatile String localNodeId = "unknown-node";
    private volatile InMemoryPlaintextBundleCache sharedBundleCache;
    private volatile long pitrWindowMillis = -1;
    private volatile ReaderShardAdmissionController readerShardAdmissionController;
    private volatile WalChunkService sharedWalChunkService;
    private volatile TimeValue compactionInterval;
    private volatile TimeValue gcInterval;
    private volatile long gcRetentionWindowMillis;
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
            SERVERLESS_STORAGE_MAX_FILE_CACHE_USAGE_RATIO_SETTING,
            SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING,
            SERVERLESS_STORAGE_COMPACTION_INTERVAL_SETTING,
            SERVERLESS_STORAGE_GC_INTERVAL_SETTING,
            SERVERLESS_STORAGE_GC_RETENTION_WINDOW_SETTING,
            SERVERLESS_STORAGE_LAZY_DIRECTORY_CACHE_SIZE_SETTING,
            SERVERLESS_STORAGE_LAZY_DIRECTORY_ENABLED_SETTING
        );
    }

    @Override
    public Map<String, IndexStorePlugin.DirectoryFactory> getDirectoryFactories() {
        return Map.of(LAZY_DIRECTORY_STORE_TYPE, new ServerlessStorageLazyDirectoryFactory(this));
    }

    /**
     * The shared {@link FileCache} {@link ServerlessStorageLazyDirectoryFactory} needs, or {@code
     * null} if the lazy directory feature is disabled on this node -- see {@link
     * #SERVERLESS_STORAGE_LAZY_DIRECTORY_CACHE_SIZE_SETTING}. Test-only-shaped visibility
     * (package-external, but not part of this plugin's own public contract) purely because {@link
     * ServerlessStorageLazyDirectoryFactory} lives in a different package and {@code
     * getDirectoryFactories()} above constructs it before {@link #createComponents} has populated
     * this field -- see that factory's own javadoc for why it must read this lazily instead of
     * capturing it at construction time.
     */
    public FileCache lazyDirectoryFileCacheForDirectoryFactory() {
        return lazyDirectoryFileCache;
    }

    /** See {@link #lazyDirectoryFileCacheForDirectoryFactory()}'s own javadoc for why this exists and why it's read lazily. */
    public ThreadPool threadPoolForDirectoryFactory() {
        return threadPool;
    }

    /** See {@link #lazyDirectoryFileCacheForDirectoryFactory()}'s own javadoc -- same lazy-read shape, reusing {@link #blobContainerFor}. */
    public BlobContainer blobContainerForDirectoryFactory(String indexUuid, int shardId) throws IOException {
        return resolveBlobContainer(indexUuid, shardId);
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
        this.threadPool = threadPool;
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
        TimeValue configuredCompactionInterval = SERVERLESS_STORAGE_COMPACTION_INTERVAL_SETTING.get(environment.settings());
        compactionInterval = configuredCompactionInterval.millis() > 0 ? configuredCompactionInterval : null;
        TimeValue configuredGcInterval = SERVERLESS_STORAGE_GC_INTERVAL_SETTING.get(environment.settings());
        gcInterval = configuredGcInterval.millis() > 0 ? configuredGcInterval : null;
        gcRetentionWindowMillis = SERVERLESS_STORAGE_GC_RETENTION_WINDOW_SETTING.get(environment.settings()).millis();
        long lazyDirectoryCacheSizeBytes = SERVERLESS_STORAGE_LAZY_DIRECTORY_CACHE_SIZE_SETTING.get(environment.settings()).getBytes();
        lazyDirectoryFileCache = lazyDirectoryCacheSizeBytes > 0
            ? FileCacheFactory.createConcurrentLRUFileCache(lazyDirectoryCacheSizeBytes)
            : null;
        int maxConcurrentReaderShards = SERVERLESS_STORAGE_MAX_CONCURRENT_READER_SHARDS_SETTING.get(environment.settings());
        double maxFileCacheUsageRatio = SERVERLESS_STORAGE_MAX_FILE_CACHE_USAGE_RATIO_SETTING.get(environment.settings());
        readerShardAdmissionController = maxConcurrentReaderShards > 0
            ? new ReaderShardAdmissionController(maxConcurrentReaderShards, lazyDirectoryFileCache, maxFileCacheUsageRatio)
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

    /**
     * Resolves this shard's own {@link BlobContainer} (index-UUID/shard-scoped, encryption-wrapped
     * if configured) -- shared by {@link #getEngineFactory} and {@link
     * #blobContainerForDirectoryFactory}, which both need exactly this same resolution.
     */
    private BlobContainer resolveBlobContainer(String indexUuid, int shardId) throws IOException {
        if (basePath == null) {
            throw new IllegalStateException(
                "shard ["
                    + indexUuid
                    + "]["
                    + shardId
                    + "] has serverless storage enabled but no ["
                    + SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey()
                    + "] node setting was configured (or it did not resolve to an allowed path)"
            );
        }
        // Each shard gets its own child container (rfc-serverless-opensearch.md &sect;6.1's
        // indices/<index-uuid>/<shard>/ layout): CommitManifest#manifestName() is intentionally
        // just <term>-<generation> with no index/shard component, since it assumes the
        // container it lives in is already shard-scoped.
        BlobContainer blobContainer = blobContainerFor(basePath, indexUuid, shardId);
        if (encryptionKeyProvider != null) {
            // Wrapping here, at the one seam every downstream class already depends on
            // abstractly (BlobContainer), is the entire integration -- see
            // EncryptingBlobContainer's javadoc for the ranged-read tradeoff this implies.
            blobContainer = new EncryptingBlobContainer(blobContainer, encryptionKeyProvider);
        }
        return blobContainer;
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings, ShardRouting shardRouting) {
        if (SERVERLESS_STORAGE_ENABLED_SETTING.get(indexSettings.getSettings()) == false) {
            return Optional.empty();
        }

        try {
            String indexUuid = indexSettings.getIndex().getUUID();
            int shardIdValue = shardRouting != null ? shardRouting.shardId().getId() : 0;
            BlobContainer blobContainer = resolveBlobContainer(indexUuid, shardIdValue);
            ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
            BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
            BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);
            // Shared by both roles below: the reader branch's own background CompactionSchedulerTask
            // needs one just as much as the writer branch's ordinary commit-publish path does --
            // cheap and stateless to construct once here rather than duplicating it in each branch.
            ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
            // Same blob container every other per-shard store here is scoped to -- a durable pin
            // lives alongside the shard's manifests/registers, not in some separate namespace.
            // Always constructed, not gated on PITR being enabled: a snapshot can pin a manifest
            // independent of PITR, and the reader branch's own background GcSchedulerTask needs a
            // real registry to check regardless of whether PITR retention is configured on this node.
            DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(blobContainer);

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
                CompactionSchedulerConfig compactionConfig = compactionInterval == null
                    ? null
                    : new CompactionSchedulerConfig(
                        compactionInterval,
                        manifestStore,
                        // Straight to the raw bundle store, not the (possibly cache-wrapped) readPath
                        // above -- a merge reads every input segment file exactly once, so there's no
                        // hot-rereading benefit a cache would give, matching WriterEngineFactory's own
                        // "no caching layer needed" choice for its own materializer.
                        new ObjectStoreCommitMaterializer(bundleStore),
                        commitPublisher,
                        CompactionPolicy.withDefaults(),
                        new CompactionRebaseExecutor(shardStateStore, 5)
                    );
                GcSchedulerConfig gcConfig = gcInterval == null
                    ? null
                    : new GcSchedulerConfig(
                        gcInterval,
                        gcRetentionWindowMillis,
                        manifestStore,
                        // Raw bundle store, same "no hot-rereading benefit from a cache" reasoning as
                        // the compaction config just above -- a sweep lists/deletes bundle names, it
                        // never reads their contents at all.
                        bundleStore,
                        pinRegistry
                    );
                return Optional.of(
                    new ReaderEngineFactory(
                        shardStateStore,
                        manifestStore,
                        new ObjectStoreCommitMaterializer(readPath),
                        shardDirectory,
                        localNodeId,
                        readerShardAdmissionController,
                        compactionConfig,
                        gcConfig
                    )
                );
            }
            PitrRetentionConfig pitrRetentionConfig = pitrWindowMillis > 0
                ? new PitrRetentionConfig(manifestStore, pinRegistry, pitrWindowMillis)
                : null;
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
