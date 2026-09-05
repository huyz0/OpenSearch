/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.ClaimedIndexLifecycle;
import org.opensearch.cluster.metadata.IndexCatalog;
import org.opensearch.cluster.metadata.IndexCreationStrategy;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.SupplierBackedIndexCatalog;
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
import org.opensearch.index.IndexCreationValidator;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.shard.IndexSettingProvider;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.indices.cluster.IndexResidencyPolicy;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.serverless.storage.allocation.ReaderShardPlacementAllocationDecider;
import org.opensearch.serverless.storage.allocation.ServerlessStorageExistingShardsAllocator;
import org.opensearch.serverless.storage.allocation.SuspendedShardAllocationDecider;
import org.opensearch.serverless.storage.compaction.CompactionPolicy;
import org.opensearch.serverless.storage.compaction.CompactionRebaseExecutor;
import org.opensearch.serverless.storage.compaction.CompactionSchedulerConfig;
import org.opensearch.serverless.storage.compaction.CompactionSchedulerTask;
import org.opensearch.serverless.storage.descriptor.DescriptorBackedIndexLifecycle;
import org.opensearch.serverless.storage.descriptor.ServerlessGatedIndexResidencyPolicy;
import org.opensearch.serverless.storage.descriptor.SupplierBackedIndexCreationStrategy;
import org.opensearch.serverless.storage.directory.InMemoryShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileReader;
import org.opensearch.serverless.storage.format.CachingBundleFileReader;
import org.opensearch.serverless.storage.format.InMemoryPlaintextBundleCache;
import org.opensearch.serverless.storage.format.LocalDiskCachingBundleStore;
import org.opensearch.serverless.storage.gc.BlobGcCandidateLog;
import org.opensearch.serverless.storage.gc.GcCandidateTailer;
import org.opensearch.serverless.storage.gc.GcSchedulerConfig;
import org.opensearch.serverless.storage.gc.GcSchedulerTask;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.readerengine.ReaderEngineFactory;
import org.opensearch.serverless.storage.readerengine.ReaderShardAdmissionController;
import org.opensearch.serverless.storage.readerengine.lazydirectory.ServerlessStorageLazyDirectoryFactory;
import org.opensearch.serverless.storage.rest.ServerlessRestGate;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinLedgerSweepTask;
import org.opensearch.serverless.storage.retention.PitrRetentionConfig;
import org.opensearch.serverless.storage.security.EncryptingBlobContainer;
import org.opensearch.serverless.storage.security.EncryptionKeyProvider;
import org.opensearch.serverless.storage.security.ObjectStoreRequestCounter;
import org.opensearch.serverless.storage.security.RequestCountingBlobContainer;
import org.opensearch.serverless.storage.security.StaticEncryptionKeyProvider;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitHeadPublisher;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.serverless.storage.writerengine.WriterEngineFactory;
import org.opensearch.serverless.storage.writerengine.WriterPublicationNotifier;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
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
 * <p>The blob container backing an opted-in index defaults to a local-filesystem container rooted
 * at {@link #SERVERLESS_STORAGE_BASE_PATH_SETTING}, but {@link #SERVERLESS_STORAGE_REPOSITORY_SETTING}
 * can instead name an already-registered snapshot repository (S3, GCS, and Azure all have real,
 * tested {@code compareAndSwapRegister} implementations in their own repository plugins -- see
 * {@code S3BlobContainer}/{@code GoogleCloudStorageBlobStore}/{@code AzureBlobStore}) whose {@code
 * BlobStore} this plugin resolves shard containers through instead, via the standard {@code
 * BlobStoreRepository} seam every repository plugin already exposes. Swapping between the two only
 * ever touches {@link #blobContainerFor}; nothing else in this class or the engine/factory classes
 * it wires together is storage-backend-specific.
 */
public class ServerlessStoragePlugin extends Plugin implements EnginePlugin, ClusterPlugin, IndexStorePlugin, ActionPlugin {

    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(
        ServerlessStoragePlugin.class
    );

    /**
     * Node settings, held because a few plugin hooks are called before {@link #createComponents} and are
     * not handed settings of their own. {@link #getRestHandlerWrapper} is the case that forced this: core
     * builds the wrapper inside {@code ActionModule}'s constructor and passes only a thread context.
     *
     * <p>This is the sole public constructor on purpose. {@code PluginsService.loadPlugin} refuses any
     * plugin class with more than one public constructor, so keeping a no-arg overload alongside this one
     * would load fine in unit tests, which call {@code new} directly, and fail every real node.
     */
    private final Settings settings;

    /** Creates the plugin; all real wiring happens in {@link #createComponents} once node services are available. */
    public ServerlessStoragePlugin(Settings settings) {
        this.settings = settings;
    }

    /**
     * Constructed eagerly (not in {@link #createComponents}) because {@link #getActionFilters()} is
     * called before {@code createComponents} runs -- see the filter's own javadoc for why it takes
     * its {@code ClusterService} via a late setter instead of its constructor.
     */
    private final org.opensearch.serverless.storage.scaletozero.ShardReactivationActionFilter shardReactivationActionFilter =
        new org.opensearch.serverless.storage.scaletozero.ShardReactivationActionFilter();

    /** Constructed eagerly for the same reason as {@link #shardReactivationActionFilter}. */
    private final org.opensearch.serverless.storage.resharding.WritePartitionRoutingActionFilter writePartitionRoutingActionFilter =
        new org.opensearch.serverless.storage.resharding.WritePartitionRoutingActionFilter();

    /** Constructed eagerly for the same reason as {@link #shardReactivationActionFilter}. */
    private final org.opensearch.serverless.storage.resharding.AffinityForwardingActionFilter affinityForwardingActionFilter =
        new org.opensearch.serverless.storage.resharding.AffinityForwardingActionFilter();

    /**
     * Constructed eagerly for the same reason as {@link #serverlessStorageExistingShardsAllocator}:
     * {@link #getAdditionalIndexSettingProviders()} is called before {@link #createComponents} runs.
     */
    private final ServerlessStorageIndexSettingProvider serverlessStorageIndexSettingProvider = new ServerlessStorageIndexSettingProvider();

    /** Shared with {@link #serverlessStorageIndexSettingProvider} once available; populated by {@link #dataStreamShardCountAdvisorSchedulerTask}. */
    private final org.opensearch.serverless.storage.resharding.DataStreamShardCountAdvisorCache dataStreamShardCountAdvisorCache =
        new org.opensearch.serverless.storage.resharding.DataStreamShardCountAdvisorCache();

    /**
     * Constructed eagerly for the same reason as {@link #shardReactivationActionFilter}: {@link
     * #getExistingShardsAllocators()} is called before {@link #createComponents} runs, and this is
     * the instance returned from there, so the one {@link #createComponents} later calls {@code
     * setDependencies} on is the exact same one core holds onto.
     */
    private final org.opensearch.serverless.storage.allocation.ServerlessStorageExistingShardsAllocator serverlessStorageExistingShardsAllocator =
        new org.opensearch.serverless.storage.allocation.ServerlessStorageExistingShardsAllocator();

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

    /** Per-index opt-in switch for serverless storage; final once set, since switching modes on a live index is unsupported. */
    public static final Setting<Boolean> SERVERLESS_STORAGE_ENABLED_SETTING = Setting.boolSetting(
        "index.serverless_storage.enabled",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /** Node-level root path of the local-filesystem blob container backing every opted-in index on this node. */
    public static final Setting<String> SERVERLESS_STORAGE_BASE_PATH_SETTING = Setting.simpleString(
        "serverless_storage.base_path",
        Setting.Property.NodeScope
    );

    /**
     * The name of an already-registered snapshot repository (via the standard {@code _snapshot}
     * API -- {@code repository-s3}/{@code repository-gcs}/{@code repository-azure} or any other
     * {@code BlobStoreRepository} implementation) whose underlying {@code BlobStore} this shard's
     * own manifest/bundle {@link BlobContainer} should be built from, instead of the local
     * filesystem {@link #SERVERLESS_STORAGE_BASE_PATH_SETTING} otherwise uses. This is the "one
     * remaining piece of wiring" this class's own top-of-file javadoc used to describe: S3, GCS,
     * and Azure each already have a real, tested {@code compareAndSwapRegister} implementation in
     * their own repository plugin -- {@code BlobStoreRepository#blobStore()} is the seam that
     * reaches whichever one the operator registered, without this plugin ever needing to depend on
     * {@code repository-s3}/{@code repository-gcs}/{@code repository-azure} directly or construct
     * a concrete client of its own.
     *
     * <p>Empty (the default) preserves every existing deployment's behavior unchanged: the local
     * filesystem container {@link #SERVERLESS_STORAGE_BASE_PATH_SETTING} resolves. Deliberately
     * scoped to only this shard's own regular container -- the shared/dedicated WAL containers
     * ({@link #SERVERLESS_STORAGE_WAL_DEDICATED_STREAM_SETTING} and the node-shared {@code wal/}
     * container {@code createComponents} builds) remain local-filesystem-only for now, a natural
     * follow-up once this seam is proven rather than widening the blast radius of one change.
     */
    public static final Setting<String> SERVERLESS_STORAGE_REPOSITORY_SETTING = Setting.simpleString(
        "serverless_storage.repository",
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
     * Refuses to read a block-encrypted blob written before per-block associated data existed
     * ({@code BlockLayout} format version 1) instead of reading it unauthenticated.
     *
     * <p>Version 1 blocks are bare {@code IV||ciphertext||tag} envelopes under one key, with the
     * header covered by nothing, so they can be swapped between blobs, reordered, or truncated and
     * still decrypt cleanly -- see {@link
     * org.opensearch.serverless.storage.security.EncryptingBlobContainer}'s javadoc for the full
     * account and the migration. Version 2 blocks bind each block to its index, shard, blob name and
     * offset, which is what makes those attacks fail.
     *
     * <p><b>Default {@code false}, and the default is the whole design of this setting.</b> An
     * upgrade must not turn existing data into an unreadable shard, and this plugin has no way to
     * know whether a given deployment still holds version 1 objects -- only the operator who ran
     * compaction and waited out GC knows that. So the node keeps reading version 1 until an operator
     * asserts otherwise by setting this. It is deliberately not dynamic: "are all my old objects
     * gone" is a claim about durable state, and flipping it at runtime on a hunch would fail live
     * reads rather than a restart.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_REQUIRE_AUTHENTICATED_BLOCKS_SETTING = Setting.boolSetting(
        "serverless_storage.encryption.require_authenticated_blocks",
        false,
        Setting.Property.NodeScope
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
     * How many gated indices one wildcard may expand to before the request is refused.
     *
     * <p>T28's cap, and it exists to bound fan-out rather than resolution. Resolving names is cheap: S24
     * measured about 33 ms per thousand. What an expansion actually decides is how many <em>sleeping</em>
     * shards one request wakes, since with index per tenant most tenant indices are asleep, and T20 and T21
     * measured 118 KB and 3.06 file descriptors per awake shard.
     *
     * <p>Dynamic, because the right value depends on how tenants are grouped and an operator who guesses
     * wrong should not need a restart to correct it. Zero refuses every wildcard over gated indices, which
     * is the narrowest form of the contract rather than a separate design.
     *
     * <p>See {@link org.opensearch.serverless.storage.descriptor.DescriptorGate#DEFAULT_WILDCARD_EXPANSION_LIMIT}
     * for why the default is a hundred.
     */
    /**
     * Whether this node installs the descriptor plane at all.
     *
     * <p>Node-scoped, and it has to be, which is the defect it exists to fix. The two decisions below used
     * to read {@link #SERVERLESS_STORAGE_ENABLED_SETTING}, which is {@code IndexScope}, out of node
     * settings. OpenSearch rejects index-scoped settings in node settings outright, so that expression
     * could never be true and {@code DescriptorGate.install} and the suspension registry were never
     * installed by the plugin in any real node.
     *
     * <p>Nothing caught it because every gated test calls {@code DescriptorGate.install} by hand inside the
     * test method, so the plugin's own wiring was never the thing under test. A whole subsystem was
     * correct, tested, and unreachable in production, which is the same shape as the six components the
     * caller count turned up, at a larger scale.
     *
     * <p>The index-scoped setting keeps its job of marking an individual index gated. This one decides
     * whether the machinery that serves such an index is present on the node.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_NODE_ENABLED_SETTING = Setting.boolSetting(
        "serverless_storage.enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * Whether this node refuses REST handlers that have not declared themselves available under serverless
     * mode, per each handler's {@link org.opensearch.rest.RestHandler#apiAvailabilityScope()}.
     *
     * <p>Off by default, and enforcement lives entirely in {@link ServerlessRestGate}, which this plugin
     * hands to core through the pre-existing {@code ActionPlugin.getRestHandlerWrapper} hook. Core has no
     * serverless-mode setting and no enforcement branch of its own; it carries the scope declaration and
     * nothing reads it there. A node without this plugin therefore serves every API exactly as before,
     * which is the point.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_REST_GATING_ENABLED_SETTING = Setting.boolSetting(
        "serverless_storage.rest_gating.enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * How many primary shards {@code .opensearch-index-mappings} is created with.
     *
     * <p>The index holds one small document per gated index, so this is not about bytes. What it is about is
     * unmeasured: every gated creation declaring a mapping writes here, and round 004 measured this store at
     * least about 80% of what such a creation costs, but nothing has shown that the shard count is what
     * governs that. A single-document write goes to one shard whatever the count is. Five is the number this
     * index was hardcoded to; naming it is what lets the question be answered rather than argued.
     *
     * <p>Raising it is not free on the read side: {@code IndexBackedMappingStatsAggregator} runs a nested
     * aggregation across every shard of this index, and with one replica the cluster carries 2n shards for
     * it, on a system whose point is not paying for idle shards.
     *
     * <p><b>Read once, at node startup.</b> Not when the index is created, which is the case that bites: a
     * node started after the index exists reads a value that can never apply, because an index's shard count
     * is fixed at creation. The store logs when it finds the index already there.
     */
    public static final Setting<Integer> SERVERLESS_STORAGE_MAPPING_INDEX_SHARDS_SETTING = Setting.intSetting(
        "serverless_storage.mapping_index.shards",
        org.opensearch.serverless.storage.descriptor.IndexBackedMappingStore.DEFAULT_SHARDS,
        1,
        // Bounded so a typo fails the node at startup rather than the first gated creation that declares a
        // mapping, where it would surface as an error about the user's index. 1024 is what core caps an
        // index at by default, via the opensearch.index.max_number_of_shards property.
        1024,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> SERVERLESS_STORAGE_WILDCARD_MAX_EXPANDED_INDICES_SETTING = Setting.intSetting(
        "serverless_storage.wildcard.max_expanded_indices",
        org.opensearch.serverless.storage.descriptor.DescriptorGate.DEFAULT_WILDCARD_EXPANSION_LIMIT,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
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
     * A coarse cap on how many compaction/partition-rewrite ticks (each a real Lucene merge,
     * materializing segments and doing sustained CPU/disk work) may run concurrently across every
     * shard on this node -- see {@code RewriteAdmissionController}'s javadoc. Every shard's own
     * scheduled tick is otherwise independent, so nothing else stops many shards from deciding to
     * compact/rewrite at once. Non-positive (the default) disables it entirely, same shape as every
     * other optional-feature-off default in this plugin.
     */
    public static final Setting<Integer> SERVERLESS_STORAGE_MAX_CONCURRENT_REWRITES_SETTING = Setting.intSetting(
        "serverless_storage.max_concurrent_rewrites",
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
     *
     * <p><b>Default flipped to {@code true}</b>
     *
     * <p>It defaulted to {@code false}, on the reasoning that this was new wiring without production
     * experience behind it. That reasoning weighed the risk of the new path against nothing, when
     * what it should have weighed it against is what the old path actually does: with mirroring off,
     * {@code walChunkService} is null, {@code ObjectStoreWriterEngine#createTranslogManager} falls
     * back to core's {@code LocalTranslog}, {@code currentWalPosition()} returns null and
     * {@code engineRecoveryOperations()} returns empty. Durability against node loss is then
     * <em>only</em> the last published manifest, and every write acknowledged since it is gone --
     * silently, with no error anywhere -- on a shard that by design has zero writer replicas to
     * recover from.
     *
     * <p>That makes &sect;2 goal 1 ("the object store is the sole durable home of segments and
     * write-ahead data; local disk is strictly a cache") false in the shipped default, and it breaks
     * &sect;13's stated contract that "writes degrade to rejection, never to silent un-durability".
     * An acknowledged write that dies with the node is the one failure this whole design exists to
     * remove, and it was the default.
     *
     * <p><b>This must stay paired with {@link #SERVERLESS_STORAGE_WAL_FLUSH_BATCHING_ENABLED_SETTING},
     * which also defaults to {@code true}.</b> They are not independent: mirroring on with batching
     * off makes every indexing thread pay a synchronous PUT per operation -- roughly one object-store
     * request per document, which inverts &sect;6.4's own cost argument for choosing a node-level WAL
     * in the first place. Turning one on without the other is the configuration nobody wants; if you
     * disable batching, consider whether you meant to disable mirroring too.
     *
     * <p><b>This also required flipping {@link #SERVERLESS_STORAGE_WAL_GC_INTERVAL_SETTING}</b>, from
     * disabled to one minute. WAL chunks are reclaimed by nothing else, so mirroring on with WAL GC
     * off would have traded a data-loss bug for an unbounded-storage bug and shipped both defaults in
     * the same release. That setting's javadoc carries the argument for why its cadence is a cost
     * choice rather than a safety one -- what is deletable is fixed by published state, so sweeping
     * more or less often changes only how much already-dead garbage is lying around.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING = Setting.boolSetting(
        "serverless_storage.wal_mirroring.enabled",
        true,
        Setting.Property.NodeScope
    );

    /**
     * Refuses to open a serverless writer shard that has no WAL service behind it, instead of
     * opening one whose unflushed operations live only on local disk.
     *
     * <p><b>The gap this exists for</b>
     *
     * <p>rfc-serverless-opensearch.md &sect;2 goal 1 is that the object store is the <em>sole</em>
     * durable home of segments and write-ahead data, and that local disk is strictly a cache. With
     * {@link #SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING} off -- the default --
     * {@code ObjectStoreWriterEngine#createTranslogManager} falls back to core's ordinary
     * {@code LocalTranslog}, so an opted-in index keeps its local translog as the only durability
     * for anything not yet flushed. Kill the node and every unflushed document is gone: exactly the
     * classic failure mode this design exists to remove, present by default, in the configuration
     * an operator gets by following the README.
     *
     * <p>What makes that worse than an unfinished feature is that it is <b>silent</b>. Nothing in
     * the shard's state, the index's settings, or any log line says "this index is not actually
     * durable in the object store." The index looks serverless, publishes to the object store, and
     * recovers from a manifest -- all correct -- and the one property it is missing is the one
     * nobody can observe until a node dies.
     *
     * <p>Setting this to {@code true} turns that into a shard that refuses to open, naming the
     * setting that would fix it. It costs an outage on a misconfigured cluster and buys the
     * guarantee the RFC claims.
     *
     * <p><b>What this is for now that mirroring is on by default</b>
     *
     * <p>{@link #SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING} now defaults to {@code true}, so
     * the gap this setting was written to expose is closed in the default configuration. It is kept,
     * and it is still worth having, because "mirroring is enabled" and "this shard actually got a
     * WAL service" are not the same statement: a shard can still open with none if an operator
     * turned mirroring off deliberately, or if the shared WAL container failed to resolve on this
     * node. In both cases the shard opens and quietly stops being durable, and this setting is what
     * turns that into a refusal to open.
     *
     * <p>Default {@code false} because the ordinary case is now handled by the default above, and a
     * node that cannot resolve its WAL container has a configuration problem better surfaced as a
     * loud warning plus an operator's explicit choice than as a mandatory shard failure.
     *
     * <p>Independent of this setting, a serverless writer shard opening without a WAL service logs a
     * warning naming the index and shard -- see {@code getEngineFactory}. Silence was the real
     * defect, and it stays fixed regardless of which way either default goes.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_WAL_MIRRORING_REQUIRED_SETTING = Setting.boolSetting(
        "serverless_storage.wal_mirroring.required",
        false,
        Setting.Property.NodeScope
    );

    /**
     * How often the node-level background {@code CompactionSchedulerTask} evaluates whether a shard
     * is worth compacting (rfc-serverless-opensearch.md &sect;16 Phase 4.5).
     *
     * <p><b>This scheduler used to live on the reader engine, and that made this setting a no-op for
     * most indices.</b> The original argument for the reader-shard home was that a writer always
     * holds its own lease, so a writer-hosted scheduler would see the lease held and never act. That
     * argument expired when the lease gate was removed from {@code maybeCompact}, and what remained
     * was a scheduler reachable only through a reader engine -- which exists only when the index has
     * {@code index.number_of_search_replicas > 0}, which requires {@code remote_store.enabled}. The
     * ordinary serverless index has neither, so setting this interval on such a cluster changed
     * nothing at all. It is now started per shard from {@code ServerlessStoragePlugin} itself, on
     * the node holding that shard's writer primary, which every assigned shard has.
     *
     * <p>Non-positive (the default) disables background compaction scheduling entirely, same
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
     * How often a split-target reader shard's own background {@code PartitionRewriteSchedulerTask}
     * attempts a physical partition rewrite (rfc-serverless-opensearch.md &sect;16 Phase 5). Still
     * reader-hosted, unlike compaction and GC: a partition rewrite only ever applies to a split
     * target, and that scheduler was not part of the move -- so on an index with no search replicas
     * it remains reachable only through the on-demand {@code ShardPartitionRewriteAction} trigger.
     * Non-positive (the default) disables the background scheduler entirely -- a split target then
     * only ever gets physically rewritten via the on-demand {@code ShardPartitionRewriteAction}
     * trigger, exactly this feature's original, narrower scope before this scheduler existed.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_PARTITION_REWRITE_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.partition_rewrite.interval",
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );

    /**
     * How often the node-level background {@code GcSchedulerTask} sweeps for deletable
     * manifests/bundles (rfc-serverless-opensearch.md &sect;6.5). Started per shard on the node
     * holding that shard's writer primary -- see
     * {@link #SERVERLESS_STORAGE_COMPACTION_INTERVAL_SETTING} for why it is no longer the reader
     * engine's job, and for the period in which setting this interval reclaimed nothing on an index
     * with no search replicas. Non-positive (the
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
     * Reclaims a deleted index's object-store bytes when its shards are removed, instead of leaving
     * them in the bucket forever.
     *
     * <p><b>What is broken without it</b>
     *
     * <p>Deleting a serverless index frees <b>nothing</b>. GC is per shard and runs from a scheduler
     * attached to a live shard, so once the index is gone there is nothing left that could ever
     * sweep its prefix: every manifest, bundle and register it ever wrote stays in the bucket
     * permanently. That is not a slow leak, it is 100% of a deleted index's storage, and the
     * index-per-tenant fleet this design exists to enable is precisely the workload that creates and
     * deletes indices continuously.
     *
     * <p><b>Why the default is {@code false}</b>
     *
     * <p>This is the first code path in the plugin that deletes a whole shard prefix in one call,
     * reached from an index-deletion callback where the operator's intent is already irreversible.
     * The safety check -- {@code DeletedShardReclaimer} refuses whenever a live pin names the shard,
     * and a clone pins every lineage hop, a snapshot pins what it names, PITR pins its window -- is
     * sound, but a bug in it would not lose the deleted index's bytes (those were meant to go), it
     * would lose whatever the check got wrong about, and silently. One release behind a flag, in
     * deployments that care more about the bill than the blast radius, is cheap insurance before
     * flipping the default.
     *
     * <p><b>This default should be flipped</b>, and leaving it off forever is not the safe option --
     * it is the option where storage grows without bound in exactly the deployment shape the RFC
     * describes. Treat it as a one-release soak, not a permanent opt-in.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_RECLAIM_ON_INDEX_DELETE_SETTING = Setting.boolSetting(
        "serverless_storage.reclaim_on_index_delete",
        false,
        Setting.Property.NodeScope
    );

    /**
     * The sole time-based safety margin the GC sweep relies on -- see {@code GcSchedulerTask}'s own
     * javadoc for why this, not a lease-pin signal, is the real protection against deleting a
     * manifest some reader still has open. Deliberately generous by default (30 minutes): comfortably
     * longer than any legitimate reader's own manifest-generation lag, bounded by {@code
     * ObjectStoreReaderEngine}'s 5 s poll interval.
     *
     * <p>Also the sole time-based margin protecting a freshly-published manifest from GC deletion
     * before {@code PitrRetentionSchedulerTask}'s own next tick ever gets a chance to pin it -- see
     * that task's {@code DEFAULT_RECONCILE_INTERVAL} javadoc. The minimum below (double that
     * interval) keeps an operator from configuring this window shorter than one full reconcile
     * cycle, which would defeat that margin for PITR the same way an unbounded window would defeat
     * it for a slow reader.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_GC_RETENTION_WINDOW_SETTING = Setting.timeSetting(
        "serverless_storage.gc.retention_window",
        TimeValue.timeValueMinutes(30),
        TimeValue.timeValueMillis(
            org.opensearch.serverless.storage.retention.PitrRetentionSchedulerTask.DEFAULT_RECONCILE_INTERVAL.millis() * 2
        ),
        Setting.Property.NodeScope
    );

    /**
     * How often the fleet-wide {@code GcCandidateTailer} pass runs, on the elected cluster manager alone --
     * see {@code GcCandidate}'s own class javadoc for why this exists at all: it is what lets a warm shard
     * that has not superseded anything since the last pass cost nothing, instead of paying {@code
     * GcSchedulerTask}'s two guaranteed {@code listBlobsByPrefix} calls every tick regardless of whether
     * anything changed. {@code GcSchedulerTask} itself is untouched and keeps running per warm reader shard
     * as the backstop -- this is a cheaper, faster-to-notice front door onto the same eventual outcome, not
     * a replacement for it.
     *
     * <p>Non-positive (the default) disables the tailer entirely, matching {@link
     * #SERVERLESS_STORAGE_GC_INTERVAL_SETTING}'s own off-by-default shape: nothing correctness-bearing
     * depends on this running, since {@code GcSchedulerTask} reaches the same manifests eventually on its
     * own regardless of whether this is on.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_GC_CANDIDATE_TAIL_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.gc.candidate_tail_interval",
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );

    /**
     * How far back one {@code GcCandidateTailer} pass looks, and how long {@code BlobGcCandidateLog}
     * retains an entry before its bucket is eligible for pruning.
     *
     * <p>Deliberately a separate knob from {@link #SERVERLESS_STORAGE_GC_RETENTION_WINDOW_SETTING} rather
     * than derived from it -- see {@code GcCandidateTailer}'s own constructor javadoc for exactly why: if a
     * pass only ever looked back one retention window, any tailer downtime longer than that (a
     * cluster-manager failover, a rolling restart) would silently and permanently age a genuinely
     * still-pending candidate out of every future pass' reach. The default here (2 hours) is chosen the
     * same way the retention window's own default is -- comfortably longer than realistic downtime, not a
     * tuned value -- and the constructor enforces this can never be configured shorter than the retention
     * window itself, which would defeat the margin entirely rather than merely shrink it.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_GC_CANDIDATE_LOOKBACK_SETTING = Setting.timeSetting(
        "serverless_storage.gc.candidate_lookback",
        TimeValue.timeValueHours(2),
        Setting.Property.NodeScope
    );

    /**
     * Round 006 item 6: how often this node sweeps a pin ledger it has a resolver for, dropping it
     * once none of the pins it names are still live -- {@code PinLedgerSweepTask}'s own javadoc for
     * why coverage is per-index (bounded to indices whose shard 0 has opened on this node) rather
     * than fleet-wide. Off by default, matching every other GC-adjacent setting's own shape: nothing
     * correctness-bearing depends on this running, an abandoned ledger is a bounded storage leak
     * rather than a risk of losing anything, and {@code SnapshotReleaseAction} remains the only way a
     * pin itself is ever released.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_PIN_LEDGER_SWEEP_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.retention.pin_ledger_sweep_interval",
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );

    /**
     * How old a still-live pin ledger has to be before a sweep pass logs it as worth an operator's
     * attention. See {@code PinLedgerSweepTask#DEFAULT_ABANDONED_AFTER_MILLIS}'s own javadoc for why
     * two hours -- an order of magnitude past {@code TransportIndexSnapshotPinAction}'s own ten-minute
     * unconfirmed-pin TTL, so a slow-but-healthy fan-out across a large shard count is never mistaken
     * for an abandoned one.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_PIN_LEDGER_ABANDONED_AFTER_SETTING = Setting.timeSetting(
        "serverless_storage.retention.pin_ledger_abandoned_after",
        TimeValue.timeValueHours(2),
        Setting.Property.NodeScope
    );

    /**
     * How often the node-level {@code WalGcSchedulerTask} sweeps for WAL chunks safe to delete
     * (rfc-serverless-opensearch.md &sect;6.4's own status note on this gap). Unlike {@link
     * #SERVERLESS_STORAGE_GC_INTERVAL_SETTING}'s sweep, this one needs no separate time-based
     * retention window setting -- see that task's own javadoc for why the minimum covered {@code
     * WalPosition} across every {@code WalShardRegistry}-known shard is already an airtight bound
     * on its own. Non-positive disables it, and it only takes effect when WAL mirroring itself is
     * also enabled -- there is nothing to sweep otherwise.
     *
     * <p><b>Default changed from disabled to one minute</b>
     *
     * <p>It defaulted to {@code -1}, which was harmless for exactly as long as nothing wrote WAL
     * chunks. {@link #SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING} now defaults to {@code true},
     * so every writer shard writes them continuously and, at the old default, nothing ever reclaimed
     * one. Turning durability on while leaving its garbage collector off would have traded a
     * data-loss bug for an unbounded-storage bug, and shipped both defaults in the same release.
     *
     * <p><b>Why cadence is a cost knob and not a safety knob.</b> What is deletable here is defined
     * by published state, not by elapsed time: a shard replays strictly forward from the {@code
     * WalPosition} its own last published manifest covers, so a sequence is needed by a shard if and
     * only if it is above that shard's offset, and the minimum offset across every registered shard
     * is a bound at or below which no registered shard can ever ask to replay again. Sweeping more
     * often does not lower that bound and sweeping less often does not raise it. Cadence changes only
     * how much already-deletable garbage is lying around -- which is why, unlike {@code
     * GcSchedulerTask}, this sweep needs no retention window at all.
     *
     * <p><b>Why one minute.</b> Accumulation scales with {@code cadence x chunk rate x node count}:
     * at the 200 ms flush interval that is roughly 300 chunks per node per sweep at one minute,
     * 1,500 at five minutes, 18,000 at an hour. Sweep cost is {@code O(registered shards)} per tick
     * and <em>independent of cadence</em>, so it is the term that constrains how often this can run
     * -- and in steady state on an idle cluster a tick is a handful of cheap head reads and nothing
     * else. It runs on the elected cluster-manager only, and the interval is jittered per instance,
     * so a one-minute cadence is affordable.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_WAL_GC_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.wal_gc.interval",
        TimeValue.timeValueMinutes(1),
        Setting.Property.NodeScope
    );

    /**
     * The default idle-time threshold {@code ScaleToZeroCandidatesAction} uses to decide a writer
     * shard is idle enough to matter (rfc-serverless-opensearch.md &sect;7.3/&sect;10's still-open
     * "policy consumer" gap for the {@code NodeIdleShardsAction}/{@code NodeManifestLagAction}
     * signals) -- a caller can override this per-request, this is only the default when they don't.
     * Deliberately conservative (10 minutes): long enough that ordinary bursty-but-live traffic
     * never gets flagged, short enough to be a useful signal for a real controller once one exists.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_SCALE_TO_ZERO_IDLE_THRESHOLD_SETTING = Setting.timeSetting(
        "serverless_storage.scale_to_zero.idle_threshold",
        TimeValue.timeValueMinutes(10),
        Setting.Property.NodeScope
    );

    /**
     * The default manifest-generation-lag threshold {@code ScaleToZeroCandidatesAction} uses to
     * decide every observed reader copy of a shard has caught up with the writer's last published
     * manifest -- see that action's own javadoc for why a candidate additionally requires this,
     * not idle time alone: suspending a writer whose readers are still catching up would leave
     * those readers permanently stale with no new manifest ever coming. Zero (the default) means
     * "reader must be exactly caught up," matching {@code ObjectStoreReaderEngine#manifestGenerationLag()}'s
     * own "0 once caught up" contract.
     */
    public static final Setting<Long> SERVERLESS_STORAGE_SCALE_TO_ZERO_LAG_THRESHOLD_SETTING = Setting.longSetting(
        "serverless_storage.scale_to_zero.lag_threshold",
        0L,
        0L,
        Setting.Property.NodeScope
    );

    /**
     * How often {@code ScaleToZeroCandidatesSchedulerTask} re-evaluates {@code
     * ScaleToZeroCandidatesAction} in the background, turning it from an on-demand-only REST/transport
     * surface into a live, continuously refreshed signal -- same "background schedule mirrors an
     * on-demand trigger" shape {@link #SERVERLESS_STORAGE_COMPACTION_INTERVAL_SETTING}/{@code
     * CompactionTriggerAction} already established. Non-positive (the default) disables the
     * scheduled evaluation entirely; the on-demand REST/transport action is unaffected either way.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_SCALE_TO_ZERO_EVAL_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.scale_to_zero.eval_interval",
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );

    /**
     * Deliberately separate from {@link #SERVERLESS_STORAGE_SCALE_TO_ZERO_EVAL_INTERVAL_SETTING}
     * and defaulting to {@code false}: turning on the scheduled evaluation alone must stay purely
     * observational (as it always has -- {@code ScaleToZeroCandidatesSchedulerTask#latestCandidates()}
     * remains read-only either way), never a silent trigger for real suspension the first time an
     * operator enables the eval interval. An operator (or an eventual real policy controller) opts
     * into the actual "do the work" half by setting both this <em>and</em> a positive eval interval.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_SCALE_TO_ZERO_SUSPEND_ENABLED_SETTING = Setting.boolSetting(
        "serverless_storage.scale_to_zero.suspend_enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * The maximum time {@code ShardReactivationActionFilter} holds a search request against a
     * suspended reader shard while waiting for reactivation, before proceeding anyway (rfc-serverless-opensearch.md
     * &sect;7.3) -- see that class's own javadoc for why search (unlike write/get) needs this filter
     * to do its own bounded wait rather than relying on a core-provided retry.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_SCALE_TO_ZERO_SEARCH_REACTIVATION_WAIT_SETTING = Setting.timeSetting(
        "serverless_storage.scale_to_zero.search_reactivation_wait",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    /**
     * The hysteresis guard {@code ShardSuspensionCoordinator} enforces (rfc-serverless-opensearch.md
     * &sect;16 Phase 4's "balancer hysteresis" milestone item): a shard reactivated more recently
     * than this may not be suspended again, even if it otherwise qualifies as a candidate --
     * without it, a shard idling just past the idle threshold, getting a single request, and
     * immediately idling again would suspend and reactivate on every single evaluation tick.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_SCALE_TO_ZERO_COOLDOWN_SETTING = Setting.timeSetting(
        "serverless_storage.scale_to_zero.cooldown",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope
    );

    /**
     * Whether a fully cold index loses its {@code IndexRoutingTable} entry entirely, rather than
     * keeping it with every shard {@code UNASSIGNED} for {@code SuspendedShardAllocationDecider} to
     * keep rejecting.
     *
     * <p>This is what makes a quiescent tenant genuinely free to the allocator, and the size of the
     * effect is measured rather than assumed: {@code ColdIndexRerouteCostSpikeTests} puts today's
     * held-down shape at roughly 15 ms of steady-state reroute per 1,000 cold indices, growing
     * linearly, against a flat ~9 ms when the entry is absent -- 154-171 ms versus 9 ms at 10,000
     * cold indices, paid on the cluster-manager on every cluster state change.
     *
     * <p>Off by default because it changes the scale-to-zero lifecycle rather than tuning it. Note
     * the asymmetry with {@code TransportReactivateShardsAction}, which recreates a missing entry
     * unconditionally: turning this off must not strand indices pruned while it was on.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_SCALE_TO_ZERO_PRUNE_ROUTING_ENTRY_SETTING = Setting.boolSetting(
        "serverless_storage.scale_to_zero.prune_routing_entry",
        false,
        Setting.Property.NodeScope
    );

    /**
     * How long a reader shard's recorded cache-locality affinity (rfc-serverless-opensearch.md
     * &sect;10, {@code ReaderCacheAffinityMetadata}) stays honorable after being recorded. {@code
     * TimeValue.MINUS_ONE} (this setting's default) disables cache-locality preference entirely,
     * restoring {@code ServerlessStorageExistingShardsAllocator}'s original "first decider-approved
     * node" behavior -- the same "explicit opt-in, off by default" shape {@code
     * SERVERLESS_STORAGE_GC_INTERVAL_SETTING} uses.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_READER_CACHE_AFFINITY_TTL_SETTING = Setting.timeSetting(
        "serverless_storage.reader_cache_affinity.ttl",
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );

    /**
     * The default queries-per-minute threshold {@code ScaleUpCandidatesAction} uses to decide a
     * reader shard is busy enough to be worth expanding (see the RFC's scale-up autoscaling
     * subsection) -- a caller can override this per-request, this is only the default when they
     * don't. Deliberately just a per-tick threshold check, no sustained-duration tracking yet
     * (unlike scale-to-zero's idle threshold, which only ever needs a single "how long since,"
     * this would need multiple consecutive over-threshold ticks to avoid reacting to one noisy
     * evaluation -- out of scope for this first increment, see {@code ScaleUpCandidatesSchedulerTask}'s own javadoc).
     */
    public static final Setting<Long> SERVERLESS_STORAGE_SCALE_UP_QPM_THRESHOLD_SETTING = Setting.longSetting(
        "serverless_storage.scale_up.qpm_threshold",
        600L,
        0L,
        Setting.Property.NodeScope
    );

    /**
     * The maximum {@code index.number_of_search_replicas} {@code ScaleUpCandidatesAction} will
     * ever flag a shard as a scale-up candidate past -- an index already at this cap is never a
     * candidate, no matter how busy, until an operator raises this setting or lowers traffic.
     */
    public static final Setting<Integer> SERVERLESS_STORAGE_SCALE_UP_MAX_SEARCH_REPLICAS_SETTING = Setting.intSetting(
        "serverless_storage.scale_up.max_search_replicas",
        5,
        0,
        Setting.Property.NodeScope
    );

    /**
     * How often {@code NodeCapacitySignalService} re-evaluates the cluster-wide node-autoscaling
     * signal (docs-site/src/content/docs/design/node-autoscaling.md, "The aggregation task") --
     * same "background schedule, non-positive disables it" shape every other eval-interval setting
     * in this plugin uses. Requires {@link #SERVERLESS_STORAGE_SCALE_TO_ZERO_EVAL_INTERVAL_SETTING}
     * to also be positive, since the signal reuses that task's idle-candidate cache rather than
     * re-deriving it.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_NODE_CAPACITY_EVAL_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.node_capacity.eval_interval",
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );

    /**
     * How many consecutive fully-idle evaluation ticks a node must have before it appears in {@code
     * RoleCapacitySignal#drainCandidates()} -- the in-cluster half of the asymmetric-window rule
     * (docs-site/src/content/docs/design/node-autoscaling.md, "Asymmetric reaction windows"); an
     * external control plane may apply its own additional window on top.
     */
    public static final Setting<Integer> SERVERLESS_STORAGE_NODE_CAPACITY_DRAIN_REQUIRED_CONSECUTIVE_TICKS_SETTING = Setting.intSetting(
        "serverless_storage.node_capacity.drain_required_consecutive_ticks",
        10,
        1,
        Setting.Property.NodeScope
    );

    /**
     * Comma-joined node names currently marked warming (node autoscaling design doc, "Warming up a
     * node before it serves") -- see {@link
     * org.opensearch.serverless.storage.nodecapacity.NodeWarmupCoordinator}. A transient cluster
     * setting, mirroring how core's own {@code cluster.routing.allocation.exclude._name} is
     * registered ({@code Property.Dynamic, Property.NodeScope}), so a {@code
     * ClusterUpdateSettingsRequest} against it validates and survives cluster-manager failover for
     * free.
     */
    public static final Setting<String> SERVERLESS_STORAGE_NODE_WARMUP_NAMES_SETTING = Setting.simpleString(
        org.opensearch.serverless.storage.nodecapacity.NodeWarmupCoordinator.WARMING_NAMES_SETTING_KEY,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * How often {@code NodeSelfWarmupSchedulerTask} checks whether this node still needs to
     * self-mark as warming (docs-site/src/content/docs/design/node-autoscaling.md, "Warming up a
     * node before it serves") -- same "background schedule, non-positive disables it" shape as
     * every other eval-interval setting in this plugin. Runs on every reader-role node, not just
     * the cluster-manager, since self-marking is inherently local-node work.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_NODE_SELF_WARMUP_EVAL_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.node_warmup.self_mark_eval_interval",
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );

    /**
     * How long after a successful self-mark {@code NodeSelfWarmupSchedulerTask} automatically clears
     * it again. {@link TimeValue#ZERO} (the default) means never auto-clear -- the node stays
     * warming until an external caller (the control plane, or an operator) clears it, matching the
     * fully-external flow this task's self-marking only narrows the race window on, not replaces.
     * Only worth setting for a deployment with no readiness check of its own; a control plane with a
     * real one should leave this disabled and clear explicitly once its own check passes.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_NODE_SELF_WARMUP_AUTO_CLEAR_DELAY_SETTING = Setting.timeSetting(
        "serverless_storage.node_warmup.self_mark_auto_clear_delay",
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * The default writes-per-minute threshold {@code ShardSplitCandidatesAction} uses to decide a
     * writer shard's sustained write rate is high enough to be worth an operator's attention as a
     * possible split target -- a caller can override this per-request, this is only the default
     * when they don't. Illustrative, not tuned against any real workload: {@code
     * ObjectStoreWriterEngine#writesPerMinute()} itself was added deliberately unconsumed (rfc-serverless-opensearch.md's
     * resharding/autoscaling material), and this action is the first, deliberately read-only,
     * consumer of that signal -- no auto-split trigger exists to calibrate this value against, see
     * {@code org.opensearch.serverless.storage.resharding.action.ShardSplitCandidateEntry}'s own
     * javadoc for why.
     */
    public static final Setting<Long> SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_WPM_THRESHOLD_SETTING = Setting.longSetting(
        "serverless_storage.resharding.split_candidate_writes_per_minute_threshold",
        10_000L,
        0L,
        Setting.Property.NodeScope
    );

    /**
     * The split-for-size counterpart to {@link #SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_WPM_THRESHOLD_SETTING}
     * (dynamic-partitioning-plan.md Phase 1 item 1.1): the shard size, in bytes (from the writer
     * engine's own latest published manifest, via {@code ManifestSegmentMetrics#totalBytes}), a
     * writer copy must exceed to count as a split candidate. Same "illustrative default, untuned
     * against a real workload" caveat as the write-rate threshold -- 10 GiB is a round, plausible
     * DynamoDB-style split-for-size number, not a measured one.
     */
    public static final Setting<Long> SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_SIZE_THRESHOLD_BYTES_SETTING = Setting.longSetting(
        "serverless_storage.resharding.split_candidate_size_threshold_bytes",
        10L * 1024 * 1024 * 1024,
        0L,
        Setting.Property.NodeScope
    );

    /**
     * How often {@code InPlaceSplitTriggerSchedulerTask} re-evaluates {@code
     * ShardSplitCandidatesAction} in the background -- dynamic-partitioning-plan.md Phase 1 item
     * 1.2, same "background schedule mirrors an on-demand trigger, off by default" shape as {@link
     * #SERVERLESS_STORAGE_SCALE_UP_EVAL_INTERVAL_SETTING}. Non-positive (the default) disables the
     * scheduled evaluation entirely; the on-demand REST/transport action is unaffected either way.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_EVAL_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.resharding.auto_split.eval_interval",
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );

    /**
     * Deliberately separate from {@link #SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_EVAL_INTERVAL_SETTING}
     * and defaulting to {@code false}, same two-gate reasoning as {@link
     * #SERVERLESS_STORAGE_SCALE_UP_ENABLED_SETTING}: turning on the scheduled evaluation alone must
     * stay purely observational, never a silent trigger for a real, irreversible-in-this-increment
     * in-place split the first time an operator enables the eval interval.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_ENABLED_SETTING = Setting.boolSetting(
        "serverless_storage.resharding.auto_split.enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * How many consecutive over-threshold evaluation ticks a shard must be flagged a candidate on,
     * back to back, before {@code InPlaceSplitTriggerCoordinator} actually splits it -- same
     * "avoid reacting to one noisy evaluation" reasoning as {@link
     * #SERVERLESS_STORAGE_SCALE_UP_REQUIRED_CONSECUTIVE_TICKS_SETTING}, defaulted to a slightly
     * higher value than that setting's own default: an in-place split is a one-way, harder-to-undo
     * action (no in-place merge exists yet in this plugin, per dynamic-partitioning-plan.md's Phase
     * 2) than a reader replica-count bump, so this warrants a longer sustained-signal requirement
     * before acting. A value of 1 restores the original single-tick behavior.
     */
    public static final Setting<Integer> SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_REQUIRED_CONSECUTIVE_TICKS_SETTING = Setting.intSetting(
        "serverless_storage.resharding.auto_split.required_consecutive_ticks",
        5,
        1,
        Setting.Property.NodeScope
    );

    /**
     * The illustrative per-tick rate-limit cap on how many distinct shards {@code
     * InPlaceSplitTriggerCoordinator} will actually split in one evaluation -- same "protect
     * against a stampede by default" reasoning as {@link
     * #SERVERLESS_STORAGE_SCALE_UP_MAX_EXPANSIONS_PER_TICK_SETTING}, deliberately a smaller default
     * given a split is a heavier, less reversible operation than a reader replica-count bump. A
     * non-positive value restores the original unbounded behavior.
     */
    public static final Setting<Integer> SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_MAX_SPLITS_PER_TICK_SETTING = Setting.intSetting(
        "serverless_storage.resharding.auto_split.max_splits_per_tick",
        2,
        Setting.Property.NodeScope
    );

    /**
     * The combined-writes-per-minute ceiling a committed split's full sibling pair must stay at or
     * below to count as an in-place *merge* candidate -- dynamic-partitioning-plan.md Phase 2 item
     * 2.3, the mirror image of {@link #SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_WPM_THRESHOLD_SETTING}'s
     * split-for-heat trigger. {@code InPlaceMergeTriggerCoordinator} sums both siblings' own
     * writes-per-minute (the same per-shard signal {@code ShardSplitCandidatesAction} already
     * surfaces) and merges the pair back only when that sum, sustained, is at or below this.
     *
     * <p><b>Deliberately far below the split threshold, not equal to it.</b> Defaulted to {@code
     * 2_000}/min -- one fifth of the {@code 10_000}/min per-shard split trigger -- so a pair that was
     * just split for heat cannot immediately qualify to be merged straight back: right after a split
     * each child carries roughly half the parent's former (>10_000/min) rate, a combined sum still an
     * order of magnitude above this ceiling. Only once the pair's real, sustained combined write rate
     * has fallen to a small fraction of what triggered the split does merging back make sense. This
     * margin, together with the sustained-tick hysteresis and the split-commit-time cool-down (see
     * {@link #SERVERLESS_STORAGE_RESHARDING_AUTO_MERGE_MIN_COOLDOWN_SETTING}), is this feature's
     * anti-flap defense -- see {@code InPlaceMergeTriggerCoordinator}'s own javadoc for all three.
     */
    public static final Setting<Long> SERVERLESS_STORAGE_RESHARDING_MERGE_CANDIDATE_COMBINED_WPM_THRESHOLD_SETTING = Setting.longSetting(
        "serverless_storage.resharding.merge_candidate_combined_writes_per_minute_threshold",
        2_000L,
        0L,
        Setting.Property.NodeScope
    );

    /**
     * The merge-for-size counterpart to {@link #SERVERLESS_STORAGE_RESHARDING_MERGE_CANDIDATE_COMBINED_WPM_THRESHOLD_SETTING}:
     * the combined size, in bytes, a committed split's full sibling pair must stay at or below to
     * count as a merge candidate. Same "comfortably below the split threshold, with real margin"
     * reasoning -- defaulted to {@code 4 GiB}, well under the {@code 10 GiB} per-shard split-for-size
     * trigger, and well under the {@code >10 GiB} a pair carries immediately after being split for
     * size. A pair only becomes a merge candidate once enough data has been deleted for the two
     * siblings *together* to fit comfortably back inside one shard.
     */
    public static final Setting<Long> SERVERLESS_STORAGE_RESHARDING_MERGE_CANDIDATE_COMBINED_SIZE_THRESHOLD_BYTES_SETTING = Setting
        .longSetting(
            "serverless_storage.resharding.merge_candidate_combined_size_threshold_bytes",
            4L * 1024 * 1024 * 1024,
            0L,
            Setting.Property.NodeScope
        );

    /**
     * How often {@code InPlaceMergeTriggerSchedulerTask} re-evaluates merge candidates in the
     * background -- the merge-side counterpart to {@link #SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_EVAL_INTERVAL_SETTING},
     * its own independent setting and its own scheduler-task instance. Non-positive (the default)
     * disables the scheduled evaluation entirely.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_RESHARDING_AUTO_MERGE_EVAL_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.resharding.auto_merge.eval_interval",
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );

    /**
     * Independent enable gate for automatic in-place merges, defaulting to {@code false} -- same
     * two-gate reasoning as {@link #SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_ENABLED_SETTING}, and
     * held to it especially firmly here: an automatic merge reverses a committed split and revives a
     * fresh parent primary that must recover, so turning on the merge eval interval alone must stay
     * purely observational, never a silent trigger. Operators should read {@code
     * InPlaceMergeTriggerCoordinator}'s known-limitation note (no split-commit-time cool-down exists
     * yet) before enabling this.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_RESHARDING_AUTO_MERGE_ENABLED_SETTING = Setting.boolSetting(
        "serverless_storage.resharding.auto_merge.enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * How many consecutive quiet evaluation ticks a committed sibling pair must be flagged a merge
     * candidate on, back to back, before {@code InPlaceMergeTriggerCoordinator} actually merges it --
     * the merge-side counterpart to {@link #SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_REQUIRED_CONSECUTIVE_TICKS_SETTING},
     * defaulted deliberately *higher* than split's own default (10 vs 5). Merge is the "give back"
     * side of resharding: being too eager to merge a just-split, momentarily-quiet pair risks a
     * split/merge/split flap, so it warrants a longer sustained-signal requirement than split does
     * before acting. This sustained-duration requirement, together with the deliberately-low combined
     * thresholds above, is the anti-flap defense standing in for the split-commit-time cool-down this
     * increment cannot yet build (see {@code InPlaceMergeTriggerCoordinator}'s javadoc). A value of 1
     * restores single-tick behavior.
     */
    public static final Setting<Integer> SERVERLESS_STORAGE_RESHARDING_AUTO_MERGE_REQUIRED_CONSECUTIVE_TICKS_SETTING = Setting.intSetting(
        "serverless_storage.resharding.auto_merge.required_consecutive_ticks",
        10,
        1,
        Setting.Property.NodeScope
    );

    /**
     * The illustrative per-tick cap on how many distinct sibling pairs {@code
     * InPlaceMergeTriggerCoordinator} will actually merge in one evaluation -- the merge-side
     * counterpart to {@link #SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_MAX_SPLITS_PER_TICK_SETTING},
     * defaulted to 1 (even smaller than split's 2): a merge revives a fresh parent primary that must
     * recover, a heavier operation than a split, so the default drip-feeds merges one pair per tick.
     * A non-positive value restores unbounded behavior.
     */
    public static final Setting<Integer> SERVERLESS_STORAGE_RESHARDING_AUTO_MERGE_MAX_MERGES_PER_TICK_SETTING = Setting.intSetting(
        "serverless_storage.resharding.auto_merge.max_merges_per_tick",
        1,
        Setting.Property.NodeScope
    );

    /**
     * The minimum time that must elapse after a split commits before {@code
     * InPlaceMergeTriggerCoordinator} will even consider folding that pair back -- the strongest of the
     * three anti-flap guards (see that class's javadoc), and the one this increment adds now that a real
     * split-commit timestamp exists in cluster state ({@link
     * org.opensearch.cluster.metadata.SplitShardsMetadata#getSplitCommitTimestamp(int)}). Unlike the
     * combined-signal thresholds and the sustained-tick requirement, this is a hard time floor: a pair
     * inside its cool-down is not a merge candidate at all, no matter how quiet it looks, so a split
     * cannot be followed immediately by a merge even if the write rate dips right after the split.
     *
     * <p>Defaulted to {@code 30m}: long enough for post-split write patterns to stabilize (well beyond a
     * few evaluation ticks at the plugin's usual sub-minute-to-minutes eval intervals), yet short enough
     * that a genuinely-and-durably-idle pair is still reclaimed within an operational window rather than
     * pinned indefinitely. A non-positive value disables the gate, leaving only the signal-margin and
     * sustained-tick guards; a split whose commit predates the timestamp field fails open (no floor).
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_RESHARDING_AUTO_MERGE_MIN_COOLDOWN_SETTING = Setting.timeSetting(
        "serverless_storage.resharding.auto_merge.min_cooldown",
        TimeValue.timeValueMinutes(30),
        Setting.Property.NodeScope
    );

    /**
     * How often {@code ScaleUpCandidatesSchedulerTask} re-evaluates {@code ScaleUpCandidatesAction}
     * in the background -- same "background schedule mirrors an on-demand trigger" shape as {@link
     * #SERVERLESS_STORAGE_SCALE_TO_ZERO_EVAL_INTERVAL_SETTING}, but deliberately its own setting
     * and its own scheduler task instance (see {@code ScaleUpCandidatesSchedulerTask}'s own
     * javadoc for why scale-up and scale-to-zero stay on independent schedules). Non-positive
     * (the default, same as {@link #SERVERLESS_STORAGE_SCALE_TO_ZERO_EVAL_INTERVAL_SETTING}'s own
     * default) disables the scheduled evaluation entirely -- an operator opts in explicitly by
     * setting this to a positive value, same "off unless asked for" default every scheduled task
     * in this plugin uses; the on-demand REST/transport action is unaffected either way.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_SCALE_UP_EVAL_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.scale_up.eval_interval",
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );

    /**
     * How often {@code DataStreamShardCountAdvisorSchedulerTask} re-evaluates {@code
     * ShardSplitCandidatesAction} to refresh its data-stream next-generation shard-count
     * recommendations -- same "background schedule mirrors an on-demand trigger, off by default"
     * shape as {@link #SERVERLESS_STORAGE_SCALE_UP_EVAL_INTERVAL_SETTING}. Non-positive (the
     * default) disables the scheduled evaluation entirely; {@code
     * ServerlessStorageIndexSettingProvider} simply never has a recommendation to inject, the same
     * safe "no cache entry -> no opinion" behavior as an evaluation that just hasn't run yet.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_SHARD_COUNT_ADVISOR_EVAL_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.resharding.shard_count_advisor.eval_interval",
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );

    /**
     * Deliberately separate from {@link #SERVERLESS_STORAGE_SCALE_UP_EVAL_INTERVAL_SETTING} and
     * defaulting to {@code false}, same reasoning as {@link #SERVERLESS_STORAGE_SCALE_TO_ZERO_SUSPEND_ENABLED_SETTING}:
     * turning on the scheduled evaluation alone must stay purely observational, never a silent
     * trigger for real index expansion the first time an operator enables the eval interval.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_SCALE_UP_ENABLED_SETTING = Setting.boolSetting(
        "serverless_storage.scale_up.enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * How many consecutive over-threshold evaluation ticks a shard must be flagged a candidate on,
     * back to back, before {@code ReaderReplicaExpansionCoordinator} actually expands its index --
     * closes {@link #SERVERLESS_STORAGE_SCALE_UP_QPM_THRESHOLD_SETTING}'s own javadoc note that this
     * "would need multiple consecutive over-threshold ticks to avoid reacting to one noisy
     * evaluation." Defaulted <em>on</em> at a small positive value (unlike most optional-feature
     * settings in this plugin, which default off) for the same reason {@link
     * #SERVERLESS_STORAGE_SCALE_TO_ZERO_COOLDOWN_SETTING} defaults on: without it, a single noisy
     * tick could trigger a real index expansion the moment {@link
     * #SERVERLESS_STORAGE_SCALE_UP_ENABLED_SETTING} is turned on, exactly the flapping risk this
     * setting exists to prevent. A value of 1 restores the original single-tick behavior.
     */
    public static final Setting<Integer> SERVERLESS_STORAGE_SCALE_UP_REQUIRED_CONSECUTIVE_TICKS_SETTING = Setting.intSetting(
        "serverless_storage.scale_up.required_consecutive_ticks",
        2,
        1,
        Setting.Property.NodeScope
    );

    /**
     * The illustrative per-tick cost-model cap this section's own "a real cost model remains
     * future work" note was closed with -- see {@code ReaderReplicaExpansionCoordinator}'s own
     * "Illustrative per-tick expansion budget" javadoc for why this is a call-rate limit, not a
     * dollar-cost model. Deliberately defaulted <em>on</em> at a small positive value, the same
     * "protect against a stampede by default" reasoning {@link
     * #SERVERLESS_STORAGE_SCALE_UP_REQUIRED_CONSECUTIVE_TICKS_SETTING} already uses. A
     * non-positive value restores the original unbounded behavior.
     */
    public static final Setting<Integer> SERVERLESS_STORAGE_SCALE_UP_MAX_EXPANSIONS_PER_TICK_SETTING = Setting.intSetting(
        "serverless_storage.scale_up.max_expansions_per_tick",
        10,
        Setting.Property.NodeScope
    );

    /**
     * A configured per-node reader shard capacity, used only to derive a headroom estimate for
     * {@link org.opensearch.serverless.storage.scaleup.ReaderReplicaExpansionCoordinator}'s
     * per-tick expansion budget (node autoscaling design doc part 4, "per-index scale-up fairness")
     * -- {@code (reader node count &times; this value) - currently assigned reader shards}. Not a
     * hard placement limit anywhere else in this plugin; purely a scale-up-budget input. Disabled
     * (the default) means no headroom estimate is computed and expansion is bounded only by {@link
     * #SERVERLESS_STORAGE_SCALE_UP_MAX_EXPANSIONS_PER_TICK_SETTING}, matching every other
     * optional-feature-off default in this plugin.
     */
    public static final Setting<Integer> SERVERLESS_STORAGE_SCALE_UP_MAX_SHARDS_PER_READER_NODE_SETTING = Setting.intSetting(
        "serverless_storage.scale_up.max_shards_per_reader_node",
        0,
        Setting.Property.NodeScope
    );

    /**
     * Per-shard fairness budget for the one node-shared {@code WalChunkService}
     * (rfc-serverless-opensearch.md &sect;18 risk #4, "WAL multiplexing fairness") -- a shard whose
     * own buffered payload bytes since its last flush cross this budget is immediately siphoned
     * into its own dedicated chunk, so one noisy shard sharing the buffer can never delay acks for
     * every other shard indefinitely. Non-positive (the default) disables the budget entirely,
     * matching how every other optional-feature-off default in this plugin is expressed -- see
     * {@code WalChunkService}'s own javadoc for the full mechanism.
     */
    public static final Setting<ByteSizeValue> SERVERLESS_STORAGE_WAL_PER_SHARD_BUDGET_SETTING = Setting.byteSizeSetting(
        "serverless_storage.wal_per_shard_budget",
        ByteSizeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * Turns WAL mirroring's per-operation object-store PUT into a real group-commit batch
     * (rfc-serverless-opensearch.md &sect;6.4's cost-sanity argument). When {@code false} (the
     * default), every writer shard's {@code WalMirroringTranslog#add} keeps flushing its record to
     * the object store synchronously before returning -- roughly one PUT per document, the reason
     * {@link #SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING} itself defaults off. When {@code
     * true}, records from every shard on the node are instead enqueued onto one shared {@code
     * WalBatchingProcessor} (a {@link org.opensearch.common.util.concurrent.BufferedAsyncIOProcessor},
     * the same group-commit primitive core's own {@code RemoteFsTranslog} uses) that drains them
     * into one chunk per {@link #SERVERLESS_STORAGE_WAL_FLUSH_INTERVAL_SETTING} tick, and whether a
     * client's write waits for that upload is governed by the standard, already-dynamic {@code
     * index.translog.durability} ({@code REQUEST} waits, {@code ASYNC} does not) -- no plugin-specific
     * "wait or not" knob is added. Default off: this reshapes the durability-critical write path and
     * has no production load-testing behind it yet.
     *
     * <p><b>Default flipped to {@code true}.</b> rfc-serverless-opensearch.md &sect;5 principle 2 is
     * "batch writes, stream reads", and with this off the write half of that principle was not
     * merely unmet but inverted: {@code WalMirroringTranslog} flushed one WAL chunk per operation --
     * its own comment says "roughly one PUT per document" -- which is the exact object-store request
     * pattern &sect;18 risk 1 exists to avoid, at a cost an operator pays per document indexed.
     *
     * <p>The reason to leave it off was load testing that has not happened. That reason does not
     * survive contact with what the alternative costs: with mirroring on and batching off,
     * {@code WalMirroringTranslog} flushes one WAL chunk <em>per operation</em> -- its own comment
     * says "roughly one PUT per document" -- which is orders of magnitude over &sect;6.4's own
     * request budget and inverts the cost argument that justified a node-level WAL rather than a
     * per-shard one. Batching is the design; it was never meant to be the exception.
     *
     * <p>The durability contract is unchanged ({@code index.translog.durability=REQUEST} still waits
     * for the upload; batching changes what is uploaded together, not what a client is told is
     * durable), and the 200 ms buffer interval is already inside the RFC's own stated 100--250 ms
     * target. The remaining risk is a latency change under a load shape nobody has measured, bounded
     * by a node setting anyone can turn back off.
     *
     * <p><b>Paired with {@link #SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING}</b>, which now also
     * defaults to {@code true}. These two land together deliberately -- see that setting's javadoc
     * for why turning mirroring on without batching is the one combination nobody wants.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_WAL_FLUSH_BATCHING_ENABLED_SETTING = Setting.boolSetting(
        "serverless_storage.wal_flush.batching.enabled",
        true,
        Setting.Property.NodeScope
    );

    /**
     * The group-commit buffer interval fed to the shared {@code WalBatchingProcessor} when {@link
     * #SERVERLESS_STORAGE_WAL_FLUSH_BATCHING_ENABLED_SETTING} is on -- the maximum time a record
     * waits in the queue before the next drain writes it (and every other record enqueued in the
     * same window) into one chunk. Ignored entirely when batching is off. Defaults to {@code 200ms},
     * the middle of rfc-serverless-opensearch.md's 100--250ms target, mirroring how {@code
     * index.remote_store.translog.buffer_interval} feeds core's own {@link
     * org.opensearch.common.util.concurrent.BufferedAsyncIOProcessor} usage.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_WAL_FLUSH_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.wal_flush.interval",
        TimeValue.timeValueMillis(200),
        Setting.Property.NodeScope
    );

    /**
     * The bounded capacity of the shared {@code WalBatchingProcessor}'s queue when {@link
     * #SERVERLESS_STORAGE_WAL_FLUSH_BATCHING_ENABLED_SETTING} is on -- the batching layer's own
     * backpressure: once full, an enqueuing shard's thread blocks until the next drain frees space,
     * the same {@link java.util.concurrent.ArrayBlockingQueue} bound core already relies on for
     * local/remote translog sync backpressure. Ignored entirely when batching is off.
     */
    public static final Setting<Integer> SERVERLESS_STORAGE_WAL_FLUSH_QUEUE_CAPACITY_SETTING = Setting.intSetting(
        "serverless_storage.wal_flush.queue_capacity",
        10000,
        1,
        Setting.Property.NodeScope
    );

    /**
     * The cumulative buffered-payload size at which the shared {@code WalBatchingProcessor} drains
     * immediately instead of waiting for the next {@link #SERVERLESS_STORAGE_WAL_FLUSH_INTERVAL_SETTING}
     * tick, when {@link #SERVERLESS_STORAGE_WAL_FLUSH_BATCHING_ENABLED_SETTING} is on. Complements the
     * interval trigger for a write burst large enough that waiting out the rest of the interval would
     * buffer an outsized batch in memory for no durability benefit. {@link ByteSizeValue#ZERO} (the
     * default) disables this trigger, leaving batching purely interval-driven -- the byte threshold is
     * an additive early-flush trigger, not a replacement for the interval, and the interval still fires
     * on its own schedule regardless of this setting. Ignored entirely when batching is off.
     */
    public static final Setting<ByteSizeValue> SERVERLESS_STORAGE_WAL_FLUSH_BYTE_THRESHOLD_SETTING = Setting.byteSizeSetting(
        "serverless_storage.wal_flush.byte_threshold",
        ByteSizeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * The WAL upload backlog size (bytes enqueued or mid-write, not yet confirmed durably written)
     * above which new writes on the batching path are rejected with backpressure instead of being
     * enqueued, when {@link #SERVERLESS_STORAGE_WAL_FLUSH_BATCHING_ENABLED_SETTING} is on --
     * rfc-serverless-opensearch.md &sect;6.4's "the WAL buffer is bounded; when upload backlog crosses
     * a threshold the node rejects new indexing with 429." {@code WalMirroringTranslog#add} throws an
     * {@code org.opensearch.core.concurrency.OpenSearchRejectedExecutionException} when the backlog is
     * over this threshold, the same exception type and rejection path {@code IndexingPressure} already
     * uses -- so it is recognized by the same downstream backpressure handling (see {@code
     * ReplicationOperation}) rather than needing plugin-specific 429 wiring. {@link ByteSizeValue#ZERO}
     * disables rejection: the queue's own bounded {@link
     * #SERVERLESS_STORAGE_WAL_FLUSH_QUEUE_CAPACITY_SETTING} capacity remains the only backstop, which
     * blocks the calling thread rather than rejecting it cleanly. Ignored entirely when batching is
     * off.
     *
     * <p><b>Default changed from {@link ByteSizeValue#ZERO} to 512 MB</b>, because zero made
     * &sect;13's claim -- "buffered mode keeps a bounded window then rejects" -- false by
     * construction: there was no bound and nothing ever rejected. The failure it left open is the
     * bad one. Under sustained object-store slowness the backlog grows on the heap, and because the
     * only backstop was a queue that <em>blocks</em> rather than rejects, the node degrades into
     * stalled indexing threads and heap pressure instead of returning the 429 that tells a client
     * to back off. 512 MB is chosen to be far above any legitimate steady-state backlog (at the
     * 200 ms flush interval it is roughly 2.5 GB/s of sustained ingest before the bound is even
     * approached) so it is a genuine circuit breaker rather than a throughput limit, and it is a
     * node setting an operator can raise or zero out.
     */
    public static final Setting<ByteSizeValue> SERVERLESS_STORAGE_WAL_FLUSH_BACKLOG_REJECT_THRESHOLD_SETTING = Setting.byteSizeSetting(
        "serverless_storage.wal_flush.backlog_reject_threshold",
        new ByteSizeValue(512, org.opensearch.core.common.unit.ByteSizeUnit.MB),
        Setting.Property.NodeScope
    );

    /**
     * Minimum real time between two refresh-triggered publish attempts on any one writer shard
     * (rfc-serverless-opensearch.md &sect;8: "a per-index publication rate limit protect[s] against
     * that footgun" of a caller hammering {@code _refresh}, now that {@code api}/{@code
     * schedule}-sourced refreshes flush and publish rather than just reopening the local reader --
     * see {@code ObjectStoreWriterEngine#maybePublishOnRefresh}). Zero (non-positive) disables this
     * limiter entirely, matching how every other optional-feature-off default in this plugin is
     * expressed -- but even disabled, an unchanged index still never publishes an identical
     * manifest on every {@code schedule} tick, since the underlying flush this triggers is
     * non-forcing.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_PUBLICATION_RATE_LIMIT_SETTING = Setting.timeSetting(
        "serverless_storage.publication_rate_limit",
        TimeValue.ZERO,
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
     * Per-shard byte budget for {@code LocalDiskCachingBundleStore}'s own on-disk cache
     * (rfc-serverless-opensearch.md &sect;9.2's admission control). Zero (the default) leaves that
     * cache exactly as unbounded as it always was -- every existing deployment is untouched.
     * <b>A positive value here is a first-pass guess, not a default tuned against real workload
     * data</b> -- see that class's own javadoc for the LRU-by-mtime eviction mechanism this
     * actually configures; an operator with real numbers should override it, not treat this
     * setting's own default as authoritative.
     */
    public static final Setting<ByteSizeValue> SERVERLESS_STORAGE_LOCAL_CACHE_MAX_BYTES_PER_SHARD_SETTING = Setting.byteSizeSetting(
        "serverless_storage.local_cache.max_bytes_per_shard",
        ByteSizeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * Node-wide byte budget for the whole {@code <data path>/serverless_storage_cache} tree, across
     * every reader shard on this node -- see {@link
     * org.opensearch.serverless.storage.format.DiskCacheSpaceGovernor} for the sweep it configures.
     *
     * <p>This exists because {@link #SERVERLESS_STORAGE_LOCAL_CACHE_MAX_BYTES_PER_SHARD_SETTING} is
     * the wrong shape to protect a disk: a per-shard budget bounds a shard, and what fills the disk
     * is N shards times that budget, with N decided by allocation rather than by the operator who
     * set it. Both settings stay independent -- the per-shard one remains an optional extra cap on
     * any single shard's share.
     *
     * <p>Unlike every other optional feature in this plugin, unset here does <b>not</b> mean off.
     * A local disk cache is created for every reader shard on any node with a data path, so "unset"
     * previously meant an untuned deployment cached to local disk without limit. Unset therefore
     * derives a budget from the filesystem actually holding the cache root (10% of its total space,
     * floored at 64MB) in {@code createComponents}; an explicitly configured value always wins, and
     * an explicit {@code 0} still means unbounded, so an operator can deliberately opt out.
     */
    public static final Setting<ByteSizeValue> SERVERLESS_STORAGE_LOCAL_CACHE_MAX_BYTES_SETTING = Setting.byteSizeSetting(
        "serverless_storage.local_cache.max_bytes",
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

    /**
     * Per-index opt-in into a WAL stream never physically co-resident (never sharing a chunk blob)
     * with any other index's WAL bytes (rfc-serverless-opensearch.md &sect;12's "dedicated WAL
     * streams" bullet: "indices with hard co-residency prohibitions (regulatory) can opt into
     * dedicated WAL streams at higher request cost -- an explicit, per-index trade"). Stronger than
     * the per-record envelope encryption every WAL record already gets regardless of this setting
     * (&sect;12 bullet 1) -- that makes a compromised chunk yield nothing without this index's own
     * key; this setting is for a compliance requirement that bytes never land in the same object at
     * all, independent of whether they're encrypted. Has no effect when WAL mirroring itself is off
     * ({@link #SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING} is the on/off switch this setting
     * only refines, matching how {@link #SERVERLESS_STORAGE_LAZY_DIRECTORY_ENABLED_SETTING} only
     * refines an already-node-enabled feature). Default {@code false}: the "higher request cost"
     * the RFC's own bullet names (this index's chunks group-commit with nobody, so batching
     * efficiency drops to whatever this one index's own write rate produces) is a real, deliberate
     * per-index trade an operator opts into, not something every index should pay by default.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_WAL_DEDICATED_STREAM_SETTING = Setting.boolSetting(
        "index.serverless_storage.wal.dedicated_stream",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    private volatile Path basePath;
    // Empty means "use the local-filesystem basePath above" -- see SERVERLESS_STORAGE_REPOSITORY_SETTING's
    // own javadoc for why a registered repository's BlobStore is resolved lazily via this name
    // (repositoriesServiceSupplier) rather than eagerly here: RepositoriesService may not have the
    // named repository registered yet this early in node startup, and a repository can be
    // deleted/recreated later in the node's lifetime, so nothing about it is safe to cache once at
    // createComponents time.
    private volatile String repositoryName;
    private volatile Supplier<RepositoriesService> repositoriesServiceSupplier;
    private volatile Path localCacheRoot;

    /**
     * Where compaction stages its merge. A sibling of the cache root and resolved the same way, because a
     * merge writes a whole shard's worth of segments and the platform temp directory is routinely a small
     * tmpfs that a large shard fills.
     */
    private volatile Path compactionMergeWorkRoot;

    /**
     * Where compaction stages its merge, for the operator-triggered path as well as the scheduled one.
     *
     * @return the merge work root on this node's data path
     */
    public Path compactionMergeWorkRootForTrigger() {
        return compactionMergeWorkRoot();
    }

    private Path compactionMergeWorkRoot() {
        Path root = compactionMergeWorkRoot;
        if (root == null) {
            throw new IllegalStateException("compaction was configured before the node environment was resolved");
        }
        return root;
    }

    // Resolved once in createComponents, same "read the NodeScope setting where Environment is
    // actually available" reasoning as every other field in this group -- getEngineFactory reads
    // this to configure each reader shard's own LocalDiskCachingBundleStore eviction budget.
    private volatile long localCacheMaxBytesPerShard;
    /**
     * The one node-wide bound over {@link #localCacheRoot}, shared by every reader shard's store on
     * this node -- {@code null} when there is no cache root at all, or when the budget resolved to
     * "unbounded" (an explicit zero, or a filesystem this node could not query). Resolved once in
     * {@code createComponents}, same as every other field in this group.
     */
    private volatile org.opensearch.serverless.storage.format.DiskCacheSpaceGovernor localCacheSpaceGovernor;
    private volatile ThreadPool threadPool;
    private volatile FileCache lazyDirectoryFileCache;
    private volatile EncryptionKeyProvider encryptionKeyProvider;

    /**
     * Resolved once alongside {@link #encryptionKeyProvider}: see
     * {@link #SERVERLESS_STORAGE_REQUIRE_AUTHENTICATED_BLOCKS_SETTING}. Read on every
     * {@code resolveBlobContainer} call, so it is a field rather than a settings lookup per shard.
     */
    private volatile boolean requireAuthenticatedBlocks;
    private volatile String localNodeId = "unknown-node";
    private volatile InMemoryPlaintextBundleCache sharedBundleCache;
    /**
     * Constructed once in {@code createComponents}, read lazily by the node-wide {@link
     * org.opensearch.serverless.storage.writerengine.ObjectStoreShardRecoveryStrategy} registered in
     * {@link #getShardRecoveryStrategies()} (which runs before this field is populated) for the
     * engine-native snapshot restore path, and separately registered under {@link
     * org.opensearch.serverless.storage.writerengine.EngineNativeSnapshotSupport#ENGINE_ID} in {@link
     * org.opensearch.index.engine.EngineNativeSnapshotReleasers} for the release path -- see that
     * class's own javadoc for why one shared instance backs both.
     */
    private volatile org.opensearch.serverless.storage.writerengine.EngineNativeSnapshotSupport engineNativeSnapshotSupport;
    private volatile long pitrWindowMillis = -1;
    /**
     * Round 006 item 6. {@code null} when off (the default), matching {@code compactionInterval}'s
     * own "resolved once, null means disabled" shape just below.
     */
    private volatile TimeValue pinLedgerSweepInterval;
    private volatile long pinLedgerAbandonedAfterMillis;
    /**
     * Dedups {@code PinLedgerSweepTask} by index uuid, since {@code getEngineFactory} runs once per
     * shard-0 open and a relocation or restart-in-place must not start a second task racing the
     * first -- see that task's own javadoc for why it is left running rather than torn down when
     * shard 0 later moves off this node. Cleared, cancelling every entry, when this plugin's own
     * node closes.
     */
    private final java.util.concurrent.ConcurrentMap<String, PinLedgerSweepTask> pinLedgerSweepTasks =
        new java.util.concurrent.ConcurrentHashMap<>();
    /**
     * Guards {@link #pinLedgerSweepTasks} against the shutdown race round 2 of the bug hunt found:
     * {@code close()}'s {@code forEach(close).then(clear())} is two separate steps against a plain
     * {@code ConcurrentHashMap}, so a shard-0 open racing shutdown could insert a freshly-scheduled
     * task after the drain and have {@code clear()} silently discard the map entry without ever
     * cancelling it. Every mutator of {@link #pinLedgerSweepTasks} -- {@link #getEngineFactory}'s
     * check-then-insert, {@code afterIndexRemoved}'s check-then-remove, and {@link #close()}'s
     * set-then-drain -- synchronizes on this lock so none of the three sequences can interleave with
     * another; a flag alone (checked, then acted on, as two separate steps) would only narrow each
     * window, not close it.
     */
    private final Object pinLedgerSweepTasksLock = new Object();
    // Not volatile: every read and write of this flag already happens inside a block synchronized on
    // pinLedgerSweepTasksLock (see that field's own javadoc), which already gives the same
    // happens-before/visibility guarantee volatile would -- adding it here would only invite a future
    // access outside the lock that reads it as "already safe," reopening the exact race the lock
    // exists to close. Found by round 3 of the bug hunt for being redundant, not for being wrong.
    private boolean pinLedgerSweepTasksClosed = false;

    /**
     * The node-level compaction and GC schedulers, keyed by {@code <indexUuid>/<shardId>}.
     *
     * <p>These used to hang off {@code ObjectStoreReaderEngine}, which meant they existed only for
     * indices that had search replicas -- and a search replica requires {@code remote_store.enabled},
     * so the ordinary serverless index (one writer shard, no search replicas) had neither scheduler
     * anywhere in the cluster and its object-store footprint grew forever. They now hang off the
     * plugin instead, started for the shard's writer primary, which is the one copy that exists for
     * every assigned shard exactly once.
     *
     * <p>Keyed per shard rather than per index because both tasks are per-shard: a four-shard index
     * needs four sweeps over four different containers, and its shards may be on four nodes.
     * Deduped by that key for the same reason {@link #pinLedgerSweepTasks} is deduped by index uuid
     * -- {@code getEngineFactory} runs again on a restart-in-place or a relocation back, and two
     * live schedulers for one shard would just do everything twice.
     */
    private final java.util.concurrent.ConcurrentMap<String, ShardMaintenanceTasks> shardMaintenanceTasks =
        new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Guards {@link #shardMaintenanceTasks} exactly as {@link #pinLedgerSweepTasksLock} guards its
     * own map, and for the identical reason: {@link #close()} drains in two non-atomic steps, so a
     * shard opening concurrently with node shutdown could otherwise insert a freshly-scheduled task
     * that {@code clear()} then discards without ever cancelling, leaving a live scheduler on a
     * closing node.
     */
    private final Object shardMaintenanceTasksLock = new Object();

    // Not volatile, for the same reason pinLedgerSweepTasksClosed is not: every access is already
    // inside a block synchronized on shardMaintenanceTasksLock.
    private boolean shardMaintenanceTasksClosed = false;

    /**
     * One shard's pair of background maintenance schedulers, held together so the map has one entry
     * per shard rather than two parallel maps that could disagree about which shards are covered.
     * Either half may be {@code null} when its interval is unset -- the pair is still worth holding,
     * because "this shard is registered, with GC on and compaction off" is a real state.
     */
    private record ShardMaintenanceTasks(CompactionSchedulerTask compactionTask, GcSchedulerTask gcTask) implements java.io.Closeable {
        @Override
        public void close() {
            if (compactionTask != null) {
                compactionTask.close();
            }
            if (gcTask != null) {
                gcTask.close();
            }
        }
    }

    private volatile ReaderShardAdmissionController readerShardAdmissionController;
    // One shared instance node-wide, threaded into every shard's CompactionSchedulerConfig/
    // PartitionRewriteSchedulerConfig -- see RewriteAdmissionController's own javadoc for why these
    // two task types must share one cap, not one each.
    private volatile org.opensearch.serverless.storage.scheduling.RewriteAdmissionController rewriteAdmissionController;
    private volatile WalChunkService sharedWalChunkService;
    // Resolved once in createComponents (same "read the NodeScope setting where Environment is
    // actually available" reasoning as every other field in this group), consumed lazily by
    // resolveSharedWalChunkService() -- see that method's own javadoc for why sharedWalChunkService
    // itself is no longer built eagerly here.
    private volatile boolean walMirroringEnabled;
    private volatile long walPerShardBudgetBytes;
    // WAL group-commit batching config, read in createComponents (same "read the NodeScope setting
    // where Environment is available" reasoning as walPerShardBudgetBytes above) and consumed lazily
    // by resolveSharedWalChunkService() when it builds the shared WalBatchingProcessor.
    private volatile boolean walFlushBatchingEnabled;
    private volatile TimeValue walFlushInterval = TimeValue.timeValueMillis(200);
    private volatile int walFlushQueueCapacity = 10000;
    private volatile long walFlushByteThreshold = 0;
    private volatile long walFlushBacklogRejectThreshold = 0;
    // The one node-shared group-commit processor, built and attached to sharedWalChunkService in
    // resolveSharedWalChunkService() only when batching is enabled; null otherwise (and never
    // attached to a dedicated-WAL-stream shard's own service, which keeps the legacy synchronous
    // path). Shutdown-drain concerns are covered by index.translog.durability=REQUEST already having
    // waited before ack -- see close()'s javadoc.
    private volatile org.opensearch.serverless.storage.wal.WalBatchingProcessor sharedWalBatchingProcessor;
    // Guards resolveSharedWalChunkService()'s double-checked-locking build -- a plain Object, not
    // `this`, so a caller synchronizing on the plugin instance for an unrelated reason can never
    // accidentally contend with (or deadlock against) this specific lazy-init path.
    private final Object walChunkServiceLock = new Object();
    private volatile org.opensearch.serverless.storage.wal.WalGcSchedulerTask walGcSchedulerTask;
    private volatile TimeValue compactionInterval;
    private volatile TimeValue partitionRewriteInterval;
    private volatile TimeValue gcInterval;
    // Reused by getEngineFactory to schedule a dedicated-WAL-stream shard's own sweep at the same
    // configured cadence as the shared WAL container's node-level WalGcSchedulerTask -- non-positive
    // (the default) disables both the shared sweep (existing behavior) and any dedicated one.
    private volatile TimeValue walGcInterval;
    private volatile long gcRetentionWindowMillis;

    /**
     * Resolved once in {@code createComponents}: see
     * {@link #SERVERLESS_STORAGE_RECLAIM_ON_INDEX_DELETE_SETTING}. Read from an index-deletion
     * callback, so it is a field rather than a settings lookup per deleted shard.
     */
    private volatile boolean reclaimOnIndexDelete;
    // Non-null iff GcCandidateTailer is configured on -- read by getEngineFactory to decide whether a
    // writer's ObjectStoreCommitHeadPublisher gets a BlobGcCandidateLog to append to at all. See
    // GcCandidate's own javadoc for why a null log (the default) is a fully supported, zero-cost
    // configuration: GcSchedulerTask's own per-shard sweep reaches the same manifests regardless.
    private volatile TimeValue gcCandidateTailInterval;
    private volatile long gcCandidateLookbackMillis;
    // Resolved once in createComponents, same "read the NodeScope setting where Environment is
    // actually available" reasoning as every other field in this group -- getEngineFactory reads
    // this to configure each produced WriterEngineFactory's own rate limiter.
    private volatile long publicationRateLimitMillis;
    // Resolved once in createComponents, needed by writerPublicationNotifierForWriterEngine()
    // below to build a WriterPublicationNotifier.
    private volatile Settings nodeSettings;
    // Resolved once in createComponents -- createComponents itself has no TransportService
    // parameter, so this stays null until TransportPollNowAction's own constructor sets it (see
    // that class's javadoc for why it's the seam that captures this instead).
    private volatile ClusterService clusterService;
    private volatile TransportService transportService;
    // Same seam and same reason as transportService immediately above: createComponents has no
    // IndicesClusterStateService parameter either, so this stays null until TransportPollNowAction's
    // constructor sets it. ReaderShardPreWarmCoordinator (D1) is the consumer.
    private volatile org.opensearch.indices.cluster.IndicesClusterStateService indicesClusterStateService;
    // Resolved once in createComponents, same "read the NodeScope setting where Environment is
    // actually available" reasoning as every other field in this group -- TransportScaleToZeroCandidatesAction
    // reads these as its per-request defaults, overridable per ScaleToZeroCandidatesRequest.
    private volatile long scaleToZeroIdleThresholdMillis = SERVERLESS_STORAGE_SCALE_TO_ZERO_IDLE_THRESHOLD_SETTING.getDefault(
        Settings.EMPTY
    ).millis();
    private volatile long scaleToZeroLagThreshold = SERVERLESS_STORAGE_SCALE_TO_ZERO_LAG_THRESHOLD_SETTING.getDefault(Settings.EMPTY);
    private volatile org.opensearch.serverless.storage.scaletozero.ScaleToZeroCandidatesSchedulerTask scaleToZeroCandidatesSchedulerTask;

    /** Sleeping shards of gated indices, read by placement and written by the suspension coordinator. */
    private volatile org.opensearch.serverless.storage.scaletozero.GatedShardSuspensionRegistry gatedShardSuspensions;
    private volatile org.opensearch.serverless.storage.nodecapacity.NodeCapacitySignalService nodeCapacitySignalService;
    private volatile org.opensearch.serverless.storage.nodecapacity.NodeSelfWarmupSchedulerTask nodeSelfWarmupSchedulerTask;
    // Same "resolved once in createComponents" reasoning as the scale-to-zero thresholds above --
    // TransportScaleUpCandidatesAction reads these as its per-request defaults, overridable per
    // ScaleUpCandidatesRequest.
    private volatile long scaleUpQpmThreshold = SERVERLESS_STORAGE_SCALE_UP_QPM_THRESHOLD_SETTING.getDefault(Settings.EMPTY);
    private volatile int scaleUpMaxSearchReplicas = SERVERLESS_STORAGE_SCALE_UP_MAX_SEARCH_REPLICAS_SETTING.getDefault(Settings.EMPTY);
    private volatile long reshardingSplitCandidateWritesPerMinuteThreshold =
        SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_WPM_THRESHOLD_SETTING.getDefault(Settings.EMPTY);
    private volatile long reshardingSplitCandidateSizeThresholdBytes =
        SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_SIZE_THRESHOLD_BYTES_SETTING.getDefault(Settings.EMPTY);
    private volatile org.opensearch.serverless.storage.scaleup.ScaleUpCandidatesSchedulerTask scaleUpCandidatesSchedulerTask;
    private volatile org.opensearch.serverless.storage.resharding.DataStreamShardCountAdvisorSchedulerTask dataStreamShardCountAdvisorSchedulerTask;
    private volatile org.opensearch.serverless.storage.resharding.InPlaceSplitTriggerSchedulerTask inPlaceSplitTriggerSchedulerTask;
    private volatile org.opensearch.serverless.storage.resharding.InPlaceMergeTriggerSchedulerTask inPlaceMergeTriggerSchedulerTask;
    // One node-local directory instance shared by every shard on this node -- matches the target
    // design's "one node block cache" shape (&sect;9) rather than a per-shard instance, and needs
    // no I/O to construct, so it's safe to build eagerly rather than threading through createComponents.
    private final ShardDirectory shardDirectory = new InMemoryShardDirectory();
    // Same "no I/O, safe to build eagerly" reasoning as shardDirectory above -- one node-shared
    // registry every writer shard's engine on this node registers itself into (rfc-serverless-opensearch.md
    // &sect;16 Phase 4's idle-activity signal, &sect;15's "autoscaling signal emitters" bullet).
    private final org.opensearch.serverless.storage.writerengine.ShardActivityRegistry shardActivityRegistry =
        new org.opensearch.serverless.storage.writerengine.ShardActivityRegistry();
    // Reader-tier counterpart to shardActivityRegistry above, same "no I/O, safe to build eagerly"
    // reasoning -- every reader shard's engine on this node registers itself into this one for
    // manifest-generation-lag lookups (&sect;10's "search tier: manifest-generation lag" hook).
    private final org.opensearch.serverless.storage.readerengine.ReaderShardActivityRegistry readerShardActivityRegistry =
        new org.opensearch.serverless.storage.readerengine.ReaderShardActivityRegistry();
    // Every reader shard's local disk cache on this node registers into this one, closing
    // &sect;9's "cache hit-rate and cold-read latency are first-class metrics" gap the same way
    // readerShardActivityRegistry above closed the manifest-generation-lag one.
    private final org.opensearch.serverless.storage.format.CacheStatsRegistry cacheStatsRegistry =
        new org.opensearch.serverless.storage.format.CacheStatsRegistry();
    // §18 risk #1's own mitigation ("publish request-count metrics from day one"): every real
    // container this plugin resolves gets wrapped in RequestCountingBlobContainer against this one
    // shared instance -- see resolveContainer and the shared WAL container's own construction below.
    private final ObjectStoreRequestCounter requestCounter = new ObjectStoreRequestCounter();

    /**
     * Whether this node computes shard placement instead of reading it from the published routing table.
     *
     * <p>Off by default. C11 has not yet re-measured the ceiling with it on, so nothing has verified the
     * claim the change exists to make.
     */
    public static final Setting<Boolean> COMPUTED_PLACEMENT_ENABLED_SETTING = Setting.boolSetting(
        "serverless_storage.computed_placement.enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * Plan item B4-B6 (plan-100m-index-implementation.md, Area B): whether
     * {@link org.opensearch.serverless.storage.resharding.AffinityForwardingActionFilter} forwards a
     * multi-index/wildcard/{@code _bulk}/{@code _msearch} request to its shared affinity coordinator.
     *
     * <p>Off by default -- see that class's own javadoc for the real, measured-nowhere-yet cost: every
     * request the filter is asked to look at pays one {@code GENERIC} thread-pool hop, even when it
     * never ends up forwarding, since determining "is this index gated" cannot safely run on the
     * calling transport thread.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_AFFINITY_FORWARDING_ENABLED_SETTING = Setting.boolSetting(
        "serverless_storage.affinity_forwarding.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * B6: what happens when the resolved affine coordinator can't be reached. Defaults strict --
     * fail rather than silently execute locally -- because a fallback that breaks affinity does so
     * exactly when the system is least able to absorb the cache-locality loss (the plan's own
     * reasoning, not invented here).
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_AFFINITY_FORWARDING_STRICT_SETTING = Setting.boolSetting(
        "serverless_storage.affinity_forwarding.strict",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Plan item D1 (plan-100m-index-implementation.md, Area D): whether
     * {@link org.opensearch.serverless.storage.placement.ReaderShardPreWarmCoordinator} proactively
     * polls a newly rendezvous-eligible node for a shard it just became a candidate for, ahead of any
     * real request landing there. Off by default -- the real cost of this against a real object store
     * has not been measured, same discipline as {@link #SERVERLESS_STORAGE_AFFINITY_FORWARDING_ENABLED_SETTING}.
     */
    public static final Setting<Boolean> SERVERLESS_STORAGE_READER_PRE_WARM_ENABLED_SETTING = Setting.boolSetting(
        "serverless_storage.reader_pre_warm.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The most {@code PollNowRequest}s one cluster-state event dispatches -- see that coordinator's
     * own "per-invocation budget" javadoc for why this exists at all. Illustrative, untuned against a
     * real workload, same caveat every other per-tick budget in this plugin already carries.
     */
    public static final Setting<Integer> SERVERLESS_STORAGE_READER_PRE_WARM_MAX_PER_EVENT_SETTING = Setting.intSetting(
        "serverless_storage.reader_pre_warm.max_per_event",
        50,
        0,
        Setting.Property.NodeScope
    );

    /**
     * How often a node reads the descriptor change log to learn what other nodes wrote.
     *
     * <p>This is the staleness bound for cross-node visibility of a descriptor write: how long another
     * node may keep serving a cached descriptor for an index that was changed or deleted elsewhere.
     *
     * <p>Five seconds because the cost of a pass is one listing over recent buckets and the cost of being
     * late is a node holding a stale descriptor. Shorter would spend requests on an idle cluster; much
     * longer and a delete on one node takes visibly long to close the shard on another.
     */
    /**
     * How long a node may serve a cached descriptor before re-reading it from the object store.
     *
     * <p>This is the object-store request floor. A node serving T active tenants issues T reads per window
     * whatever the request rate above it, so a one second window means T reads per second per node forever.
     * The cache's own default was one second, chosen when descriptors lived in a local index where a read
     * cost microseconds; against a 20 to 40 ms GET it is wrong by two orders of magnitude, which
     * {@code DescriptorCache}'s javadoc says outright.
     *
     * <p>A minute is safe because this is not what makes a change visible. A descriptor written on another
     * node is invalidated here by the change log tailer within
     * {@link #DESCRIPTOR_CHANGE_TAIL_INTERVAL_SETTING}, so this window only bounds how long a <em>lost</em>
     * change log entry can go unnoticed. Shortening it buys nothing that the tailer does not already
     * provide, and costs a read per tenant per window.
     */
    /**
     * How long descriptor change log buckets are kept before being deleted.
     *
     * <p>The log had no retention at all and grew with every descriptor write, forever. Nothing reads it
     * beyond the tailer's own poll interval: a tailer starts at the bucket its node started in and revisits
     * only that one, so a bucket older than a few intervals has no reader.
     *
     * <p>An hour rather than minutes because the only thing retention buys is slack for a tailer that has
     * been stalled, and an hour is far longer than any pass takes. Longer costs storage for nothing, since
     * a node behind by more than this has an empty cache anyway and does not need the history.
     */
    /**
     * How long a tombstone is kept before it can be reclaimed.
     *
     * <p>A tombstone records that an index was deleted, so that a node partitioned through the delete drops
     * its local shard data on rejoin instead of keeping it. It is not what stops a deleted index being
     * served, since a gated shard only opens through a successful descriptor lookup, so the window this has
     * to cover is the longest a node can be partitioned and still come back carrying that data.
     *
     * <p>Seven days is deliberately generous. Reclaiming late costs storage; reclaiming early costs a node
     * the record it needed, and the two are not worth trading evenly.
     */
    public static final Setting<TimeValue> TOMBSTONE_RETENTION_SETTING = Setting.timeSetting(
        "serverless_storage.descriptor.tombstone_retention",
        TimeValue.timeValueDays(7),
        TimeValue.timeValueMinutes(1),
        Setting.Property.NodeScope
    );

    /**
     * How often tombstones past the retention window are reclaimed, or zero to leave it to the object store.
     *
     * <p>Zero by default, because an object-store lifecycle rule over the tombstone prefix does the same job
     * for no requests at all and is the better answer wherever it exists. This scrubber is for stores with no
     * such facility, and for a deployment where the rule was never configured; it costs one listing plus a
     * read per tombstone examined per pass, which is affordable at a modest population and is not how anyone
     * should reclaim a hundred million of them.
     */
    public static final Setting<TimeValue> TOMBSTONE_SCRUB_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.descriptor.tombstone_scrub_interval",
        TimeValue.ZERO,
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * Phase E2 of {@code core-pluggability-refactor-plan.md}: how often a node checks whether the gated
     * indices it opened on demand still exist. Zero disables the sweep. Backs {@link
     * org.opensearch.serverless.storage.descriptor.ServerlessGatedIndexResidencyPolicy#sweepInterval()} --
     * declared here rather than on that class because every {@code Setting} this plugin owns lives here,
     * enforced by {@code testEveryDeclaredSettingFieldIsRegisteredInGetSettings}. Same key this setting had
     * when it was a hardcoded {@code Setting} field on core's own {@code IndicesClusterStateService}.
     */
    public static final Setting<TimeValue> GATED_SHARD_SWEEP_INTERVAL_SETTING = Setting.timeSetting(
        "indices.gated.deleted_shard_sweep_interval",
        TimeValue.timeValueSeconds(60),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * Phase E2 of {@code core-pluggability-refactor-plan.md}: how long a gated index this node opened on
     * demand may sit untouched before it is closed again. Zero disables idle eviction. Backs {@link
     * org.opensearch.serverless.storage.descriptor.ServerlessGatedIndexResidencyPolicy#idleEvictionAfter()}
     * -- see {@link #GATED_SHARD_SWEEP_INTERVAL_SETTING}'s own javadoc for why this is declared here.
     *
     * <p>Thirty minutes rather than something shorter because the cost of being wrong is asymmetric.
     * Evicting a shard about to be used again costs one cold start -- a descriptor read and a shard open.
     * Not evicting costs heap that is never given back.
     */
    public static final Setting<TimeValue> GATED_SHARD_IDLE_EVICTION_SETTING = Setting.timeSetting(
        "indices.gated.idle_eviction_after",
        TimeValue.timeValueMinutes(30),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * Phase E2 of {@code core-pluggability-refactor-plan.md}: the most gated indices this node will hold
     * open at once, or zero to derive one from the heap using this plugin's own measured per-index cost
     * ({@code GatedResidencySoakIT} measured 150,888 bytes per open gated index). Backs {@link
     * org.opensearch.serverless.storage.descriptor.ServerlessGatedIndexResidencyPolicy#maxOpen()} -- see
     * {@link #GATED_SHARD_SWEEP_INTERVAL_SETTING}'s own javadoc for why this is declared here.
     */
    public static final Setting<Integer> GATED_MAX_OPEN_SETTING = Setting.intSetting(
        "indices.gated.max_open",
        0,
        0,
        Setting.Property.NodeScope
    );

    /**
     * How long a member node must be continuously absent before computed-placement membership will
     * decommission it -- see {@code ComputedPlacementMembershipService}, which aliases this and applies it
     * alongside its observation count. Declared here for the same reason
     * {@link #GATED_SHARD_SWEEP_INTERVAL_SETTING} is: a setting this plugin does not both declare here and
     * register in {@link #getSettings()} is one an operator can never configure.
     */
    public static final Setting<TimeValue> PLACEMENT_DECOMMISSION_ABSENCE_SETTING = Setting.timeSetting(
        "serverless_storage.placement.decommission_after_absence",
        TimeValue.timeValueMinutes(15),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> DESCRIPTOR_CHANGE_LOG_RETENTION_SETTING = Setting.timeSetting(
        "serverless_storage.descriptor.change_log_retention",
        TimeValue.timeValueHours(1),
        TimeValue.timeValueMinutes(2),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> DESCRIPTOR_CACHE_FRESHNESS_SETTING = Setting.timeSetting(
        "serverless_storage.descriptor.cache_freshness",
        TimeValue.timeValueSeconds(60),
        TimeValue.timeValueMillis(100),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> DESCRIPTOR_CHANGE_TAIL_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.descriptor.change_tail_interval",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueMillis(100),
        Setting.Property.NodeScope
    );

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            GATED_SHARD_SWEEP_INTERVAL_SETTING,
            GATED_SHARD_IDLE_EVICTION_SETTING,
            GATED_MAX_OPEN_SETTING,
            DESCRIPTOR_CHANGE_TAIL_INTERVAL_SETTING,
            DESCRIPTOR_CACHE_FRESHNESS_SETTING,
            DESCRIPTOR_CHANGE_LOG_RETENTION_SETTING,
            SERVERLESS_STORAGE_AFFINITY_FORWARDING_ENABLED_SETTING,
            SERVERLESS_STORAGE_AFFINITY_FORWARDING_STRICT_SETTING,
            SERVERLESS_STORAGE_READER_PRE_WARM_ENABLED_SETTING,
            SERVERLESS_STORAGE_READER_PRE_WARM_MAX_PER_EVENT_SETTING,
            TOMBSTONE_RETENTION_SETTING,
            TOMBSTONE_SCRUB_INTERVAL_SETTING,
            COMPUTED_PLACEMENT_ENABLED_SETTING,
            // How long a placement member must be gone before it is decommissioned -- see the setting's
            // own javadoc for why an observation count alone was not a delay at all.
            PLACEMENT_DECOMMISSION_ABSENCE_SETTING,
            SERVERLESS_STORAGE_ENABLED_SETTING,
            SERVERLESS_STORAGE_BASE_PATH_SETTING,
            SERVERLESS_STORAGE_ENCRYPTION_KEY_SETTING,
            SERVERLESS_STORAGE_REQUIRE_AUTHENTICATED_BLOCKS_SETTING,
            SERVERLESS_STORAGE_BUNDLE_CACHE_SIZE_SETTING,
            SERVERLESS_STORAGE_PITR_WINDOW_SETTING,
            SERVERLESS_STORAGE_MAX_CONCURRENT_READER_SHARDS_SETTING,
            SERVERLESS_STORAGE_WILDCARD_MAX_EXPANDED_INDICES_SETTING,
            SERVERLESS_STORAGE_MAPPING_INDEX_SHARDS_SETTING,
            SERVERLESS_STORAGE_NODE_ENABLED_SETTING,
            SERVERLESS_STORAGE_REST_GATING_ENABLED_SETTING,
            SERVERLESS_STORAGE_MAX_FILE_CACHE_USAGE_RATIO_SETTING,
            SERVERLESS_STORAGE_MAX_CONCURRENT_REWRITES_SETTING,
            SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING,
            SERVERLESS_STORAGE_WAL_MIRRORING_REQUIRED_SETTING,
            SERVERLESS_STORAGE_COMPACTION_INTERVAL_SETTING,
            SERVERLESS_STORAGE_PARTITION_REWRITE_INTERVAL_SETTING,
            SERVERLESS_STORAGE_GC_INTERVAL_SETTING,
            SERVERLESS_STORAGE_RECLAIM_ON_INDEX_DELETE_SETTING,
            SERVERLESS_STORAGE_GC_RETENTION_WINDOW_SETTING,
            SERVERLESS_STORAGE_GC_CANDIDATE_TAIL_INTERVAL_SETTING,
            SERVERLESS_STORAGE_GC_CANDIDATE_LOOKBACK_SETTING,
            SERVERLESS_STORAGE_PIN_LEDGER_SWEEP_INTERVAL_SETTING,
            SERVERLESS_STORAGE_PIN_LEDGER_ABANDONED_AFTER_SETTING,
            SERVERLESS_STORAGE_WAL_GC_INTERVAL_SETTING,
            SERVERLESS_STORAGE_WAL_PER_SHARD_BUDGET_SETTING,
            SERVERLESS_STORAGE_WAL_FLUSH_BATCHING_ENABLED_SETTING,
            SERVERLESS_STORAGE_WAL_FLUSH_INTERVAL_SETTING,
            SERVERLESS_STORAGE_WAL_FLUSH_QUEUE_CAPACITY_SETTING,
            SERVERLESS_STORAGE_WAL_FLUSH_BYTE_THRESHOLD_SETTING,
            SERVERLESS_STORAGE_WAL_FLUSH_BACKLOG_REJECT_THRESHOLD_SETTING,
            SERVERLESS_STORAGE_PUBLICATION_RATE_LIMIT_SETTING,
            SERVERLESS_STORAGE_LAZY_DIRECTORY_CACHE_SIZE_SETTING,
            SERVERLESS_STORAGE_LOCAL_CACHE_MAX_BYTES_PER_SHARD_SETTING,
            SERVERLESS_STORAGE_LOCAL_CACHE_MAX_BYTES_SETTING,
            SERVERLESS_STORAGE_LAZY_DIRECTORY_ENABLED_SETTING,
            SERVERLESS_STORAGE_WAL_DEDICATED_STREAM_SETTING,
            SERVERLESS_STORAGE_SCALE_TO_ZERO_IDLE_THRESHOLD_SETTING,
            SERVERLESS_STORAGE_SCALE_TO_ZERO_LAG_THRESHOLD_SETTING,
            SERVERLESS_STORAGE_NODE_CAPACITY_EVAL_INTERVAL_SETTING,
            SERVERLESS_STORAGE_NODE_CAPACITY_DRAIN_REQUIRED_CONSECUTIVE_TICKS_SETTING,
            SERVERLESS_STORAGE_NODE_WARMUP_NAMES_SETTING,
            SERVERLESS_STORAGE_NODE_SELF_WARMUP_EVAL_INTERVAL_SETTING,
            SERVERLESS_STORAGE_NODE_SELF_WARMUP_AUTO_CLEAR_DELAY_SETTING,
            SERVERLESS_STORAGE_SCALE_TO_ZERO_EVAL_INTERVAL_SETTING,
            SERVERLESS_STORAGE_SCALE_TO_ZERO_SUSPEND_ENABLED_SETTING,
            SERVERLESS_STORAGE_SCALE_TO_ZERO_SEARCH_REACTIVATION_WAIT_SETTING,
            SERVERLESS_STORAGE_SCALE_TO_ZERO_COOLDOWN_SETTING,
            SERVERLESS_STORAGE_SCALE_TO_ZERO_PRUNE_ROUTING_ENTRY_SETTING,
            SERVERLESS_STORAGE_READER_CACHE_AFFINITY_TTL_SETTING,
            SERVERLESS_STORAGE_SCALE_UP_QPM_THRESHOLD_SETTING,
            SERVERLESS_STORAGE_SCALE_UP_MAX_SEARCH_REPLICAS_SETTING,
            SERVERLESS_STORAGE_SCALE_UP_EVAL_INTERVAL_SETTING,
            SERVERLESS_STORAGE_SHARD_COUNT_ADVISOR_EVAL_INTERVAL_SETTING,
            SERVERLESS_STORAGE_SCALE_UP_ENABLED_SETTING,
            SERVERLESS_STORAGE_SCALE_UP_REQUIRED_CONSECUTIVE_TICKS_SETTING,
            SERVERLESS_STORAGE_SCALE_UP_MAX_EXPANSIONS_PER_TICK_SETTING,
            SERVERLESS_STORAGE_SCALE_UP_MAX_SHARDS_PER_READER_NODE_SETTING,
            SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_WPM_THRESHOLD_SETTING,
            SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_SIZE_THRESHOLD_BYTES_SETTING,
            SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_EVAL_INTERVAL_SETTING,
            SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_ENABLED_SETTING,
            SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_REQUIRED_CONSECUTIVE_TICKS_SETTING,
            SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_MAX_SPLITS_PER_TICK_SETTING,
            SERVERLESS_STORAGE_RESHARDING_MERGE_CANDIDATE_COMBINED_WPM_THRESHOLD_SETTING,
            SERVERLESS_STORAGE_RESHARDING_MERGE_CANDIDATE_COMBINED_SIZE_THRESHOLD_BYTES_SETTING,
            SERVERLESS_STORAGE_RESHARDING_AUTO_MERGE_EVAL_INTERVAL_SETTING,
            SERVERLESS_STORAGE_RESHARDING_AUTO_MERGE_ENABLED_SETTING,
            SERVERLESS_STORAGE_RESHARDING_AUTO_MERGE_REQUIRED_CONSECUTIVE_TICKS_SETTING,
            SERVERLESS_STORAGE_RESHARDING_AUTO_MERGE_MAX_MERGES_PER_TICK_SETTING,
            SERVERLESS_STORAGE_RESHARDING_AUTO_MERGE_MIN_COOLDOWN_SETTING,
            SERVERLESS_STORAGE_REPOSITORY_SETTING
        );
    }

    @Override
    public Map<String, IndexStorePlugin.DirectoryFactory> getDirectoryFactories() {
        return Map.of(LAZY_DIRECTORY_STORE_TYPE, new ServerlessStorageLazyDirectoryFactory(this));
    }

    /**
     * Registers this plugin's {@link org.opensearch.serverless.storage.writerengine.ObjectStoreShardRecoveryStrategy}
     * node-wide; {@link ServerlessStorageIndexSettingProvider} is what actually selects it, per index, for every
     * serverless-storage index. Every index that does not select it keeps core's own {@code local-lucene}
     * strategy, so installing this plugin changes nothing for an ordinary index on the same node.
     *
     * <p>Constructed here even though {@link #createComponents} has not run yet, exactly like {@link
     * #getDirectoryFactories()} above: both arguments are lazy reads of this plugin instance rather than
     * captured values -- see {@link #lazyDirectoryFileCacheForDirectoryFactory()}'s own javadoc for the
     * pattern and why it is necessary here.
     */
    @Override
    public Map<String, org.opensearch.index.shard.ShardRecoveryStrategy> getShardRecoveryStrategies() {
        return Map.of(
            org.opensearch.serverless.storage.writerengine.ObjectStoreShardRecoveryStrategy.NAME,
            new org.opensearch.serverless.storage.writerengine.ObjectStoreShardRecoveryStrategy(
                this::blobContainerForDirectoryFactory,
                () -> engineNativeSnapshotSupport
            )
        );
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

    /**
     * See {@link #lazyDirectoryFileCacheForDirectoryFactory()}'s own javadoc -- same lazy-read shape, reusing {@link #blobContainerFor}.
     *
     * @param indexUuid the UUID of the index the shard belongs to.
     * @param shardId the shard's numeric id within the index.
     */
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
        this.nodeSettings = environment.settings();
        this.clusterService = clusterService;
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.repositoryName = SERVERLESS_STORAGE_REPOSITORY_SETTING.get(environment.settings());
        this.scaleToZeroIdleThresholdMillis = SERVERLESS_STORAGE_SCALE_TO_ZERO_IDLE_THRESHOLD_SETTING.get(environment.settings()).millis();
        this.scaleToZeroLagThreshold = SERVERLESS_STORAGE_SCALE_TO_ZERO_LAG_THRESHOLD_SETTING.get(environment.settings());
        shardReactivationActionFilter.setDependencies(
            clusterService,
            client,
            threadPool,
            SERVERLESS_STORAGE_SCALE_TO_ZERO_SEARCH_REACTIVATION_WAIT_SETTING.get(environment.settings())
        );
        writePartitionRoutingActionFilter.setDependencies(clusterService, threadPool);
        affinityForwardingActionFilter.setDependencies(
            clusterService,
            threadPool,
            indexNameExpressionResolver,
            this::getTransportService,
            clusterService.getClusterSettings(),
            SERVERLESS_STORAGE_AFFINITY_FORWARDING_ENABLED_SETTING,
            SERVERLESS_STORAGE_AFFINITY_FORWARDING_STRICT_SETTING,
            SERVERLESS_STORAGE_AFFINITY_FORWARDING_ENABLED_SETTING.get(environment.settings()),
            SERVERLESS_STORAGE_AFFINITY_FORWARDING_STRICT_SETTING.get(environment.settings())
        );
        // D1: constructed and registered here, not eagerly, since addStateApplier only needs to run
        // once createComponents has a real ClusterService -- unlike the ActionFilters above, nothing
        // calls this coordinator before createComponents runs.
        org.opensearch.serverless.storage.placement.ReaderShardPreWarmCoordinator readerShardPreWarmCoordinator =
            new org.opensearch.serverless.storage.placement.ReaderShardPreWarmCoordinator(
                this::getTransportService,
                this::onDemandOpenGatedIndices,
                SERVERLESS_STORAGE_READER_PRE_WARM_MAX_PER_EVENT_SETTING.get(environment.settings()),
                SERVERLESS_STORAGE_READER_PRE_WARM_ENABLED_SETTING.get(environment.settings())
            );
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(SERVERLESS_STORAGE_READER_PRE_WARM_ENABLED_SETTING, readerShardPreWarmCoordinator::setEnabled);
        clusterService.addStateApplier(readerShardPreWarmCoordinator);
        // Same registration shape as the coordinator above: this applier used to be constructed by
        // core's IndicesClusterStateService, but everything it does (descriptor prefetch on fleet
        // expansion, filtered to this node's affinity share) only means something with this plugin
        // installed, so this plugin owns it. Its gated-index supplier tolerates running before
        // setIndicesClusterStateService by answering empty, same as onDemandOpenGatedIndices().
        clusterService.addStateApplier(
            new org.opensearch.serverless.storage.placement.GatedIndexPrewarmer(this::onDemandOpenGatedIndexNames)
        );
        // Placement membership maintenance, relocated here from core's Node.java when the service moved
        // into this plugin. Same registration shape Node used: membership is written by whichever node is
        // elected, so every cluster-manager-eligible node carries the maintainer and the service itself
        // checks election. It stays inert until a placement supplier is installed, so a cluster running
        // this plugin with the feature off never acquires the metadata.
        if (org.opensearch.cluster.node.DiscoveryNode.isClusterManagerNode(environment.settings())) {
            clusterService.addListener(new org.opensearch.serverless.storage.placement.ComputedPlacementMembershipService(clusterService));
        }
        serverlessStorageIndexSettingProvider.setDependencies(dataStreamShardCountAdvisorCache);
        serverlessStorageExistingShardsAllocator.setDependencies(
            clusterService,
            SERVERLESS_STORAGE_READER_CACHE_AFFINITY_TTL_SETTING.get(environment.settings()).millis()
        );
        // Where a gated index's sleeping shards are recorded. It has to be installed as well as
        // constructed: placement reads it to decide which shards not to place, which is the only definition
        // of asleep available to an index whose routing is computed rather than allocated (H9c).
        this.gatedShardSuspensions = new org.opensearch.serverless.storage.scaletozero.GatedShardSuspensionRegistry();
        if (SERVERLESS_STORAGE_NODE_ENABLED_SETTING.get(environment.settings())) {
            this.gatedShardSuspensions.install();
        }

        TimeValue scaleToZeroEvalInterval = SERVERLESS_STORAGE_SCALE_TO_ZERO_EVAL_INTERVAL_SETTING.get(environment.settings());
        // Node-capacity evaluation is nested inside the scale-to-zero interval check below, and that
        // nesting is not incidental: NodeCapacitySignalService reads per-shard idleness from
        // ScaleToZeroCandidatesSchedulerTask#latestCandidates(), so without that task there is
        // nothing for it to compute a signal from.
        //
        // What made this worth failing on rather than tolerating is how it failed. Configuring
        // node_capacity.eval_interval while leaving scale_to_zero.eval_interval at its -1 default
        // silently built no service at all, and TransportNodeCapacityAction then answered
        // NodeCapacitySignal.empty() with HTTP 200 -- so a control plane polling for scale-down
        // candidates read a perfectly healthy, entirely unloaded fleet and acted on it. An empty
        // answer and "this subsystem was never started" are indistinguishable over that API, which
        // makes the misconfiguration invisible precisely to the automation that depends on it.
        //
        // Un-nesting was the other option and is worse: it would produce a service that starts,
        // answers 200, and still has no idleness data to report -- the same lie, one layer down.
        // Refusing at startup names both settings to the operator who set one of them.
        TimeValue nodeCapacityEvalIntervalForValidation = SERVERLESS_STORAGE_NODE_CAPACITY_EVAL_INTERVAL_SETTING.get(
            environment.settings()
        );
        if (nodeCapacityEvalIntervalForValidation.millis() > 0 && scaleToZeroEvalInterval.millis() <= 0) {
            throw new IllegalArgumentException(
                "["
                    + SERVERLESS_STORAGE_NODE_CAPACITY_EVAL_INTERVAL_SETTING.getKey()
                    + "] is set but ["
                    + SERVERLESS_STORAGE_SCALE_TO_ZERO_EVAL_INTERVAL_SETTING.getKey()
                    + "] is not: node-capacity signals are derived from the scale-to-zero candidate "
                    + "evaluation, so with that evaluation off this node would report an empty capacity "
                    + "signal indistinguishable from a genuinely idle fleet. Set ["
                    + SERVERLESS_STORAGE_SCALE_TO_ZERO_EVAL_INTERVAL_SETTING.getKey()
                    + "] as well, or unset ["
                    + SERVERLESS_STORAGE_NODE_CAPACITY_EVAL_INTERVAL_SETTING.getKey()
                    + "]"
            );
        }
        if (scaleToZeroEvalInterval.millis() > 0) {
            boolean suspendEnabled = SERVERLESS_STORAGE_SCALE_TO_ZERO_SUSPEND_ENABLED_SETTING.get(environment.settings());
            // Suspending a writer shard can lose a write that was acknowledged between the suspend
            // marker committing and the shard being evicted. ShardSuspensionCoordinator narrows that
            // window as far as it can from the coordinator side (it re-reads the marker before
            // cancelling, so a reactivation that already landed aborts the eviction) and its javadoc
            // is explicit that this narrows rather than closes it. Closing it properly needs a fence
            // installed in the same cluster-state update as the marker and released only once the
            // engine reports its final publish landed -- a coordinator-to-engine channel that does
            // not exist.
            //
            // WAL mirroring makes the remaining window harmless rather than merely narrow: an
            // acknowledged write is already durable in the object store before the shard stops, so
            // losing the race costs a reactivation and a replay, not data. With mirroring off, the
            // same race costs the write, silently, on a shard that by design has no writer replica.
            //
            // This guard is cheap now in a way it would not have been before: wal_mirroring.enabled
            // defaults to true, so it can only fire for an operator who enabled suspension AND
            // deliberately turned mirroring off -- two explicit choices whose combination is the one
            // configuration where scale-to-zero silently drops acknowledged writes. Refusing at
            // startup names both.
            // The setting, not the walMirroringEnabled field: that field is assigned further down in
            // createComponents, so reading it here would see its default false and fire this guard
            // for every deployment that enabled suspension, regardless of the real configuration.
            if (suspendEnabled && SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.get(environment.settings()) == false) {
                throw new IllegalArgumentException(
                    "["
                        + SERVERLESS_STORAGE_SCALE_TO_ZERO_SUSPEND_ENABLED_SETTING.getKey()
                        + "] is enabled but ["
                        + SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey()
                        + "] is disabled: suspending a writer shard can lose a write acknowledged just before "
                        + "the shard stops, and without WAL mirroring that write is not durable anywhere else. "
                        + "Enable ["
                        + SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey()
                        + "] (its default), or disable ["
                        + SERVERLESS_STORAGE_SCALE_TO_ZERO_SUSPEND_ENABLED_SETTING.getKey()
                        + "] to keep evaluation observational"
                );
            }
            long cooldownMillis = SERVERLESS_STORAGE_SCALE_TO_ZERO_COOLDOWN_SETTING.get(environment.settings()).millis();
            this.scaleToZeroCandidatesSchedulerTask = new org.opensearch.serverless.storage.scaletozero.ScaleToZeroCandidatesSchedulerTask(
                threadPool,
                scaleToZeroEvalInterval,
                client,
                clusterService,
                suspendEnabled
                    ? new org.opensearch.serverless.storage.scaletozero.ShardSuspensionCoordinator(
                        clusterService,
                        client,
                        cooldownMillis,
                        SERVERLESS_STORAGE_SCALE_TO_ZERO_PRUNE_ROUTING_ENTRY_SETTING.get(environment.settings()),
                        // H9a measured that without this a gated index can never sleep: the suspend task is
                        // submitted, finds no IndexMetadata to rewrite, and returns the state unchanged. The
                        // four-argument constructor above is that behaviour, and scaling to zero is what the
                        // serverless design exists for, so an index that cannot do it is worse than one that
                        // fails loudly.
                        gatedShardSuspensions
                    )
                    : null
            );
            TimeValue nodeCapacityEvalInterval = SERVERLESS_STORAGE_NODE_CAPACITY_EVAL_INTERVAL_SETTING.get(environment.settings());
            if (nodeCapacityEvalInterval.millis() > 0) {
                this.nodeCapacitySignalService = new org.opensearch.serverless.storage.nodecapacity.NodeCapacitySignalService(
                    threadPool,
                    nodeCapacityEvalInterval,
                    clusterService,
                    this.scaleToZeroCandidatesSchedulerTask,
                    SERVERLESS_STORAGE_READER_CACHE_AFFINITY_TTL_SETTING.get(environment.settings()).millis(),
                    SERVERLESS_STORAGE_NODE_CAPACITY_DRAIN_REQUIRED_CONSECUTIVE_TICKS_SETTING.get(environment.settings())
                );
            }
        }
        TimeValue selfWarmupEvalInterval = SERVERLESS_STORAGE_NODE_SELF_WARMUP_EVAL_INTERVAL_SETTING.get(environment.settings());
        if (selfWarmupEvalInterval.millis() > 0) {
            this.nodeSelfWarmupSchedulerTask = new org.opensearch.serverless.storage.nodecapacity.NodeSelfWarmupSchedulerTask(
                threadPool,
                selfWarmupEvalInterval,
                clusterService,
                client,
                SERVERLESS_STORAGE_NODE_SELF_WARMUP_AUTO_CLEAR_DELAY_SETTING.get(environment.settings())
            );
        }
        this.scaleUpQpmThreshold = SERVERLESS_STORAGE_SCALE_UP_QPM_THRESHOLD_SETTING.get(environment.settings());
        this.scaleUpMaxSearchReplicas = SERVERLESS_STORAGE_SCALE_UP_MAX_SEARCH_REPLICAS_SETTING.get(environment.settings());
        this.reshardingSplitCandidateWritesPerMinuteThreshold = SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_WPM_THRESHOLD_SETTING.get(
            environment.settings()
        );
        this.reshardingSplitCandidateSizeThresholdBytes = SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_SIZE_THRESHOLD_BYTES_SETTING.get(
            environment.settings()
        );
        TimeValue scaleUpEvalInterval = SERVERLESS_STORAGE_SCALE_UP_EVAL_INTERVAL_SETTING.get(environment.settings());
        if (scaleUpEvalInterval.millis() > 0) {
            boolean scaleUpEnabled = SERVERLESS_STORAGE_SCALE_UP_ENABLED_SETTING.get(environment.settings());
            int maxShardsPerReaderNode = SERVERLESS_STORAGE_SCALE_UP_MAX_SHARDS_PER_READER_NODE_SETTING.get(environment.settings());
            this.scaleUpCandidatesSchedulerTask = new org.opensearch.serverless.storage.scaleup.ScaleUpCandidatesSchedulerTask(
                threadPool,
                scaleUpEvalInterval,
                client,
                clusterService,
                scaleUpEnabled
                    ? new org.opensearch.serverless.storage.scaleup.ReaderReplicaExpansionCoordinator(
                        client,
                        scaleUpMaxSearchReplicas,
                        SERVERLESS_STORAGE_SCALE_UP_REQUIRED_CONSECUTIVE_TICKS_SETTING.get(environment.settings()),
                        SERVERLESS_STORAGE_SCALE_UP_MAX_EXPANSIONS_PER_TICK_SETTING.get(environment.settings()),
                        // Degrades safely to "never saturated" when node capacity signaling is off
                        // (nodeCapacitySignalService null) or hasn't evaluated yet (empty signal,
                        // unassignedShardCount() == 0) -- see ReaderReplicaExpansionCoordinator's own
                        // "Ceiling-aware" javadoc for why a fresh service must never block expansion.
                        () -> this.nodeCapacitySignalService != null
                            && this.nodeCapacitySignalService.latestSignal().reader().unassignedShardCount() > 0,
                        // Headroom-aware budget refinement -- disabled (Integer.MAX_VALUE, no
                        // additional constraint) unless both the per-node capacity setting is
                        // configured and the signal service has evaluated at least once, matching
                        // the same safe-degrade convention as readerCapacitySaturated above.
                        () -> {
                            if (maxShardsPerReaderNode <= 0 || this.nodeCapacitySignalService == null) {
                                return Integer.MAX_VALUE;
                            }
                            org.opensearch.serverless.storage.nodecapacity.RoleCapacitySignal reader = this.nodeCapacitySignalService
                                .latestSignal()
                                .reader();
                            // long arithmetic, not int: nodeCount() * maxShardsPerReaderNode can
                            // overflow Integer for a pathologically large configured capacity,
                            // which Math.max(0, ...) on an already-wrapped-negative int would then
                            // misreport as zero headroom -- falsely blocking all scale-up instead
                            // of reporting the (very large but real) capacity.
                            long headroom = (long) reader.nodeCount() * maxShardsPerReaderNode - reader.totalAssignedShardCount();
                            return (int) Math.max(0, Math.min(Integer.MAX_VALUE, headroom));
                        }
                    )
                    : null
            );
        }
        TimeValue shardCountAdvisorEvalInterval = SERVERLESS_STORAGE_SHARD_COUNT_ADVISOR_EVAL_INTERVAL_SETTING.get(environment.settings());
        if (shardCountAdvisorEvalInterval.millis() > 0) {
            this.dataStreamShardCountAdvisorSchedulerTask =
                new org.opensearch.serverless.storage.resharding.DataStreamShardCountAdvisorSchedulerTask(
                    threadPool,
                    shardCountAdvisorEvalInterval,
                    client,
                    clusterService,
                    dataStreamShardCountAdvisorCache
                );
        }
        TimeValue autoSplitEvalInterval = SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_EVAL_INTERVAL_SETTING.get(environment.settings());
        if (autoSplitEvalInterval.millis() > 0) {
            boolean autoSplitEnabled = SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_ENABLED_SETTING.get(environment.settings());
            this.inPlaceSplitTriggerSchedulerTask = new org.opensearch.serverless.storage.resharding.InPlaceSplitTriggerSchedulerTask(
                threadPool,
                autoSplitEvalInterval,
                client,
                clusterService,
                autoSplitEnabled
                    ? new org.opensearch.serverless.storage.resharding.InPlaceSplitTriggerCoordinator(
                        client,
                        SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_REQUIRED_CONSECUTIVE_TICKS_SETTING.get(environment.settings()),
                        SERVERLESS_STORAGE_RESHARDING_AUTO_SPLIT_MAX_SPLITS_PER_TICK_SETTING.get(environment.settings())
                    )
                    : null
            );
        }
        TimeValue autoMergeEvalInterval = SERVERLESS_STORAGE_RESHARDING_AUTO_MERGE_EVAL_INTERVAL_SETTING.get(environment.settings());
        if (autoMergeEvalInterval.millis() > 0) {
            boolean autoMergeEnabled = SERVERLESS_STORAGE_RESHARDING_AUTO_MERGE_ENABLED_SETTING.get(environment.settings());
            this.inPlaceMergeTriggerSchedulerTask = new org.opensearch.serverless.storage.resharding.InPlaceMergeTriggerSchedulerTask(
                threadPool,
                autoMergeEvalInterval,
                client,
                clusterService,
                autoMergeEnabled
                    ? new org.opensearch.serverless.storage.resharding.InPlaceMergeTriggerCoordinator(
                        client,
                        SERVERLESS_STORAGE_RESHARDING_AUTO_MERGE_REQUIRED_CONSECUTIVE_TICKS_SETTING.get(environment.settings()),
                        SERVERLESS_STORAGE_RESHARDING_AUTO_MERGE_MAX_MERGES_PER_TICK_SETTING.get(environment.settings()),
                        SERVERLESS_STORAGE_RESHARDING_MERGE_CANDIDATE_COMBINED_WPM_THRESHOLD_SETTING.get(environment.settings()),
                        SERVERLESS_STORAGE_RESHARDING_MERGE_CANDIDATE_COMBINED_SIZE_THRESHOLD_BYTES_SETTING.get(environment.settings()),
                        SERVERLESS_STORAGE_RESHARDING_AUTO_MERGE_MIN_COOLDOWN_SETTING.get(environment.settings()).millis()
                    )
                    : null
            );
        }
        String configuredBasePath = SERVERLESS_STORAGE_BASE_PATH_SETTING.get(environment.settings());
        if (configuredBasePath.isEmpty() == false) {
            // Environment#resolveRepoFile is the same sanctioned path-resolution seam
            // repository-fs / repository-url use: it refuses to resolve anything outside the
            // node's configured allowed-paths, rather than trusting an arbitrary settings string.
            basePath = environment.resolveRepoFile(configuredBasePath);
        }
        if (nodeEnvironment != null && nodeEnvironment.nodeDataPaths().length > 0) {
            localCacheRoot = nodeEnvironment.nodeDataPaths()[0].resolve("serverless_storage_cache");
            compactionMergeWorkRoot = nodeEnvironment.nodeDataPaths()[0].resolve("serverless_compaction_work");
        }
        localCacheMaxBytesPerShard = SERVERLESS_STORAGE_LOCAL_CACHE_MAX_BYTES_PER_SHARD_SETTING.get(environment.settings()).getBytes();
        if (localCacheRoot != null) {
            // Setting#exists, not "is the value zero": those mean opposite things here. Unset means
            // "derive something sane, because the alternative is an unbounded cache on every
            // untuned node"; an explicit zero is an operator deliberately asking for unbounded.
            long nodeWideCacheBudgetBytes = resolveNodeWideLocalCacheBudgetBytes(environment.settings(), localCacheRoot);
            localCacheSpaceGovernor = nodeWideCacheBudgetBytes > 0
                ? new org.opensearch.serverless.storage.format.DiskCacheSpaceGovernor(localCacheRoot, nodeWideCacheBudgetBytes)
                : null;
            // Orphans from an index deleted while this node was down: no shard- or index-level
            // lifecycle callback ever fires for those, so the only moment this node can tell a
            // cached uuid apart from a deleted one is the first cluster state it actually trusts.
            registerOrphanedCacheSweep();
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
        TimeValue configuredPinLedgerSweepInterval = SERVERLESS_STORAGE_PIN_LEDGER_SWEEP_INTERVAL_SETTING.get(environment.settings());
        pinLedgerSweepInterval = configuredPinLedgerSweepInterval.millis() > 0 ? configuredPinLedgerSweepInterval : null;
        pinLedgerAbandonedAfterMillis = SERVERLESS_STORAGE_PIN_LEDGER_ABANDONED_AFTER_SETTING.get(environment.settings()).millis();
        TimeValue configuredCompactionInterval = SERVERLESS_STORAGE_COMPACTION_INTERVAL_SETTING.get(environment.settings());
        compactionInterval = configuredCompactionInterval.millis() > 0 ? configuredCompactionInterval : null;
        TimeValue configuredPartitionRewriteInterval = SERVERLESS_STORAGE_PARTITION_REWRITE_INTERVAL_SETTING.get(environment.settings());
        partitionRewriteInterval = configuredPartitionRewriteInterval.millis() > 0 ? configuredPartitionRewriteInterval : null;
        TimeValue configuredGcInterval = SERVERLESS_STORAGE_GC_INTERVAL_SETTING.get(environment.settings());
        gcInterval = configuredGcInterval.millis() > 0 ? configuredGcInterval : null;
        gcRetentionWindowMillis = SERVERLESS_STORAGE_GC_RETENTION_WINDOW_SETTING.get(environment.settings()).millis();
        reclaimOnIndexDelete = SERVERLESS_STORAGE_RECLAIM_ON_INDEX_DELETE_SETTING.get(environment.settings());
        TimeValue configuredGcCandidateTailInterval = SERVERLESS_STORAGE_GC_CANDIDATE_TAIL_INTERVAL_SETTING.get(environment.settings());
        gcCandidateTailInterval = configuredGcCandidateTailInterval.millis() > 0 ? configuredGcCandidateTailInterval : null;
        gcCandidateLookbackMillis = SERVERLESS_STORAGE_GC_CANDIDATE_LOOKBACK_SETTING.get(environment.settings()).millis();
        publicationRateLimitMillis = SERVERLESS_STORAGE_PUBLICATION_RATE_LIMIT_SETTING.get(environment.settings()).millis();
        long lazyDirectoryCacheSizeBytes = SERVERLESS_STORAGE_LAZY_DIRECTORY_CACHE_SIZE_SETTING.get(environment.settings()).getBytes();
        lazyDirectoryFileCache = lazyDirectoryCacheSizeBytes > 0
            ? FileCacheFactory.createConcurrentLRUFileCache(lazyDirectoryCacheSizeBytes)
            : null;
        int maxConcurrentReaderShards = SERVERLESS_STORAGE_MAX_CONCURRENT_READER_SHARDS_SETTING.get(environment.settings());
        double maxFileCacheUsageRatio = SERVERLESS_STORAGE_MAX_FILE_CACHE_USAGE_RATIO_SETTING.get(environment.settings());
        readerShardAdmissionController = maxConcurrentReaderShards > 0
            ? new ReaderShardAdmissionController(maxConcurrentReaderShards, lazyDirectoryFileCache, maxFileCacheUsageRatio)
            : null;
        int maxConcurrentRewrites = SERVERLESS_STORAGE_MAX_CONCURRENT_REWRITES_SETTING.get(environment.settings());
        rewriteAdmissionController = maxConcurrentRewrites > 0
            ? new org.opensearch.serverless.storage.scheduling.RewriteAdmissionController(maxConcurrentRewrites)
            : null;
        requireAuthenticatedBlocks = SERVERLESS_STORAGE_REQUIRE_AUTHENTICATED_BLOCKS_SETTING.get(environment.settings());
        // The master key never becomes a String on this path, and every intermediate copy is zeroed.
        //
        // It used to read `Base64.getDecoder().decode(new String(encryptionKey.getChars()))`. The
        // SecureString itself was closed correctly by the try-with-resources, which made the leak
        // easy to miss: the leak was the `new String(...)`. A String is immutable, so nothing can
        // clear it; the base64 encoding of the node's master key therefore sat on the heap until a
        // garbage collection that may never be observed, and any heap dump, OOM .hprof, or core
        // dump taken in between contains it in full. Base64 is not obfuscation -- it is the exact
        // key, one decode away, and it is trivially recognisable in a dump.
        //
        // The replacement encodes the char[] straight to UTF-8 bytes and decodes those, so the
        // longest-lived copy is a byte[] we control and wipe. rawKeyBytes is wiped too, immediately
        // after SecretKeySpec has taken its own defensive copy inside fromRawKeyBytes.
        //
        // This is best-effort, and worth being honest about the limit: the JVM may have moved any
        // of these arrays during a GC before we overwrite them, leaving an unreachable copy behind.
        // Wiping shrinks the window from "forever" to "until the next collection of that region",
        // which is the most any Java process can do without off-heap key storage.
        try (SecureString encryptionKey = SERVERLESS_STORAGE_ENCRYPTION_KEY_SETTING.get(environment.settings())) {
            if (encryptionKey.length() > 0) {
                byte[] base64Bytes = null;
                byte[] rawKeyBytes = null;
                java.nio.ByteBuffer encoded = null;
                try {
                    encoded = java.nio.charset.StandardCharsets.UTF_8.encode(java.nio.CharBuffer.wrap(encryptionKey.getChars()));
                    base64Bytes = new byte[encoded.remaining()];
                    encoded.get(base64Bytes);
                    try {
                        rawKeyBytes = Base64.getDecoder().decode(base64Bytes);
                    } catch (IllegalArgumentException e) {
                        // Fail node start with the setting's name in the message. Previously a
                        // malformed value threw the JDK's own "Illegal base64 character" out of
                        // createComponents with no indication of which of this plugin's 80-odd
                        // settings produced it.
                        throw new IllegalArgumentException(
                            "["
                                + SERVERLESS_STORAGE_ENCRYPTION_KEY_SETTING.getKey()
                                + "] is not valid base64; it must hold the base64 encoding of 16, 24 or 32 raw AES key bytes",
                            e
                        );
                    }
                    // Throws on a wrong key length -- see StaticEncryptionKeyProvider#fromRawKeyBytes
                    // for why that check belongs at startup rather than at the first write.
                    encryptionKeyProvider = StaticEncryptionKeyProvider.fromRawKeyBytes(rawKeyBytes);
                } finally {
                    if (encoded != null && encoded.hasArray()) {
                        java.util.Arrays.fill(encoded.array(), (byte) 0);
                    }
                    if (base64Bytes != null) {
                        java.util.Arrays.fill(base64Bytes, (byte) 0);
                    }
                    if (rawKeyBytes != null) {
                        java.util.Arrays.fill(rawKeyBytes, (byte) 0);
                    }
                }
            }
        }
        // Only the config is resolved here (same "read the NodeScope setting where Environment is
        // actually available" reasoning as every other field in this group); the shared WAL
        // container and sharedWalChunkService itself are no longer built eagerly -- see
        // resolveSharedWalChunkService()'s own javadoc for why.
        walMirroringEnabled = SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.get(environment.settings());
        if (walMirroringEnabled) {
            // Caught here, at component construction, rather than where it actually bites.
            //
            // The WAL path needs a real ThreadPool: the group-commit processor takes its
            // ThreadContext and schedules on it, and WalGcSchedulerTask schedules on it too. Without
            // one, nothing failed until the first writer shard opened, and then it failed as a raw
            // NullPointerException inside resolveSharedWalChunkService -- several frames below
            // getEngineFactory, naming a field rather than a cause, on a code path whose connection
            // to "this plugin was constructed without a thread pool" is not visible from the stack.
            //
            // A real node always passes one, so reaching this is a construction error and not a
            // configuration error: the only callers who can are tests that build the plugin
            // partially. That is exactly why it should throw here and say so, instead of being
            // tolerated -- the tolerant version is a plugin that comes up with WAL mirroring
            // "enabled" and silently no WAL, which is the failure mode this whole area of the code
            // has been removing rather than adding.
            if (threadPool == null) {
                throw new IllegalStateException(
                    "["
                        + SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey()
                        + "] is enabled but createComponents was called with no ThreadPool: the shared WAL "
                        + "service cannot batch, schedule its GC sweep, or propagate a thread context without "
                        + "one. A real node always supplies a ThreadPool here, so this is a partially-constructed "
                        + "plugin rather than a misconfiguration -- supply one, or disable ["
                        + SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey()
                        + "]"
                );
            }
            walPerShardBudgetBytes = SERVERLESS_STORAGE_WAL_PER_SHARD_BUDGET_SETTING.get(environment.settings()).getBytes();
            walGcInterval = SERVERLESS_STORAGE_WAL_GC_INTERVAL_SETTING.get(environment.settings());
            walFlushBatchingEnabled = SERVERLESS_STORAGE_WAL_FLUSH_BATCHING_ENABLED_SETTING.get(environment.settings());
            walFlushInterval = SERVERLESS_STORAGE_WAL_FLUSH_INTERVAL_SETTING.get(environment.settings());
            walFlushQueueCapacity = SERVERLESS_STORAGE_WAL_FLUSH_QUEUE_CAPACITY_SETTING.get(environment.settings());
            walFlushByteThreshold = SERVERLESS_STORAGE_WAL_FLUSH_BYTE_THRESHOLD_SETTING.get(environment.settings()).getBytes();
            walFlushBacklogRejectThreshold = SERVERLESS_STORAGE_WAL_FLUSH_BACKLOG_REJECT_THRESHOLD_SETTING.get(environment.settings())
                .getBytes();
        }
        // Engine-native snapshot restore/release (docs-site design/snapshot-restore-proposal.md):
        // one shared instance, built once here rather than per-shard, since restore runs off the
        // node-wide ObjectStoreShardRecoveryStrategy and release is registered node-wide below --
        // see its own class javadoc for why one instance backs both. blobContainerForDirectoryFactory's signature already matches
        // ShardCloner.ContainerResolver's exactly (String indexUuid, int shardId -> BlobContainer
        // throws IOException), so the method reference needs no adapting; wrapped delete-denied,
        // the same least-privilege scoping TransportShardCloneAction's own cross-index resolution
        // already uses.
        engineNativeSnapshotSupport = new org.opensearch.serverless.storage.writerengine.EngineNativeSnapshotSupport(
            (indexUuid, shardId) -> new org.opensearch.serverless.storage.security.RestrictingBlobContainer(
                blobContainerForDirectoryFactory(indexUuid, shardId),
                false
            )
        );
        org.opensearch.index.engine.EngineNativeSnapshotReleasers.register(
            org.opensearch.serverless.storage.writerengine.EngineNativeSnapshotSupport.ENGINE_ID,
            engineNativeSnapshotSupport
        );
        // This plugin instance itself, so TransportShardCloneAction (the only consumer) can be
        // constructor-injected with it and reach blobContainerForDirectoryFactory -- the same
        // resolution ServerlessStorageLazyDirectoryFactory already depends on, just handed to a
        // different consumer via a different injection path (Guice component vs. direct reference).
        // Computed placement. Installing the supplier is the whole of the wiring: a serverless index
        // publishes no routing entry and each node fills the gap locally, so there is nothing to
        // subscribe to and nothing to keep in sync.
        org.opensearch.serverless.storage.placement.ComputedPlacementGate.install(
            COMPUTED_PLACEMENT_ENABLED_SETTING.get(environment.settings())
        );

        // Descriptor resolution. H2e built the seam, H8a taught the resolver to consult it and H17 added
        // pagination, and until now nothing registered a supplier, so the fallback returned null on every
        // node and a gated index could not be named by any request. Installing is the whole of the wiring
        // for the same reason computed placement's is: the registries are static and each node answers
        // locally.
        // T28's wildcard cap, applied before the gate is installed so no expansion can run against the
        // default when the operator configured something else, and kept current afterwards. Registering the
        // update consumer is what makes the setting dynamic rather than merely declared as such.
        org.opensearch.serverless.storage.descriptor.DescriptorGate.setWildcardExpansionLimit(
            SERVERLESS_STORAGE_WILDCARD_MAX_EXPANDED_INDICES_SETTING.get(environment.settings())
        );
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                SERVERLESS_STORAGE_WILDCARD_MAX_EXPANDED_INDICES_SETTING,
                org.opensearch.serverless.storage.descriptor.DescriptorGate::setWildcardExpansionLimit
            );
        // Both halves are the object store. There is no longer a choice to make: the point half is a
        // conditional write and a read of one key, and the prefix half is one bounded listing over the same
        // descriptors/ prefix, so nothing a request touches lives in an index any more. That removes the
        // dual write too -- the two halves address one keyspace, so there is no second copy to keep in step.
        //
        // A configured object store is therefore a hard requirement of this plugin rather than an option,
        // the same posture remote-backed storage already takes. Failing at startup is the point: quietly
        // serving descriptors from somewhere else would be a durability guarantee nobody would notice was
        // missing.
        // Only built when this node is actually running serverless storage. Resolving the container is
        // I/O against a configured repository, and a node that merely has the plugin on its classpath has
        // not asked for one -- doing it unconditionally would fail startup for an ordinary cluster, which
        // is precisely the R1 breakage the enabled flag exists to prevent.
        org.opensearch.serverless.storage.descriptor.DescriptorBackend descriptorBackend = null;
        if (SERVERLESS_STORAGE_NODE_ENABLED_SETTING.get(environment.settings())) {
            final org.opensearch.serverless.storage.descriptor.DescriptorEnumerator descriptorPrefixes;
            try {
                descriptorBackend = new org.opensearch.serverless.storage.descriptor.BlobDescriptorBackend(
                    resolveContainerForDescriptors(
                        BlobPath.cleanPath().add("descriptors-root"),
                        "no container could be resolved for descriptors; set ["
                            + SERVERLESS_STORAGE_REPOSITORY_SETTING.getKey()
                            + "] or ["
                            + SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey()
                            + "]"
                    ),
                    // GENERIC, because the descriptor hooks are registered on the cluster state thread and
                    // must not do I/O there. Which pool is the caller's decision precisely because getting
                    // it wrong hangs a node rather than slowing one down.
                    threadPool.executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC),
                    DESCRIPTOR_CACHE_FRESHNESS_SETTING.get(environment.settings()).nanos()
                );
                descriptorPrefixes = new org.opensearch.serverless.storage.descriptor.DescriptorEnumerator(path -> {
                    try {
                        return resolveContainerForDescriptors(path, "no blob store for wildcard expansion");
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }, BlobPath.cleanPath().add("descriptors-root"));
            } catch (java.io.IOException e) {
                throw new IllegalStateException("could not resolve the descriptor container", e);
            }
            // Watches for the mapping index being deleted under a running cluster, which is the only thing
            // that tells a missing mapping apart from a lost one. Registered before the gate, so no read
            // can happen through a store whose signal is not yet listening.
            org.opensearch.serverless.storage.descriptor.MappingIndexWatcher mappingIndexWatcher =
                new org.opensearch.serverless.storage.descriptor.MappingIndexWatcher();
            clusterService.addListener(mappingIndexWatcher);
            // The mapping store is two things composed, and the composition is spelled out here because
            // getting it wrong is invisible. The descriptor owns the mapping (T58). The mapping index is a
            // write-behind projection whose only reader is IndexBackedMappingStatsAggregator below, which is
            // how the gated population's field type counts stay one search rather than a walk over every
            // descriptor. Registering the descriptor store alone -- which is what T58 left behind -- leaves
            // that aggregator reading an index with no writer, and cluster stats silently omits every gated
            // index.
            final org.opensearch.serverless.storage.descriptor.DescriptorBackend descriptorsForMappings = descriptorBackend;
            org.opensearch.serverless.storage.descriptor.DescriptorGate.install(
                descriptorBackend,
                descriptorPrefixes,
                new org.opensearch.serverless.storage.descriptor.StatsProjectingMappingStore(
                    new org.opensearch.serverless.storage.descriptor.DescriptorBackedMappingStore(() -> descriptorsForMappings, null),
                    new org.opensearch.serverless.storage.descriptor.IndexBackedMappingStore(
                        client,
                        SERVERLESS_STORAGE_MAPPING_INDEX_SHARDS_SETTING.get(environment.settings()),
                        mappingIndexWatcher
                    ),
                    // GENERIC for the same reason the descriptor backend uses it: a projection write is an
                    // indexing request, and the mapping write that triggers it can be running anywhere,
                    // including a thread that must not do I/O.
                    threadPool.executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC)
                ),
                new org.opensearch.serverless.storage.descriptor.IndexBackedMappingStatsAggregator(client),
                new org.opensearch.serverless.storage.descriptor.StoreBackedFieldRefresher(),
                true
            );
        }

        // G2. The change feed, both ends of it.
        //
        // Neither end had a production caller. DescriptorGate.setChangeFeed was referenced only by its own
        // uninstall, so no node ever appended a change and no node ever read one. That left a gated index
        // created on one node invisible to every other node's name index until a rebuild, and a descriptor
        // changed elsewhere visible here only once the cache's freshness window expired.
        //
        // The appender is registered before the tailer starts, so the tailer never reads a log nothing
        // writes, which is the configuration that looks exactly like a quiet cluster.
        if (SERVERLESS_STORAGE_NODE_ENABLED_SETTING.get(environment.settings())) {
            try {
                org.opensearch.serverless.storage.descriptor.BlobDescriptorChangeLog changeLog =
                    new org.opensearch.serverless.storage.descriptor.BlobDescriptorChangeLog(path -> {
                        try {
                            return resolveContainerForDescriptors(path, "no blob store for the descriptor change log");
                        } catch (IOException e) {
                            throw new java.io.UncheckedIOException(e);
                        }
                    }, BlobPath.cleanPath().add("descriptors-root"));
                org.opensearch.serverless.storage.descriptor.DescriptorGate.setChangeFeed(changeLog);

                org.opensearch.serverless.storage.descriptor.DescriptorChangeTailer tailer =
                    new org.opensearch.serverless.storage.descriptor.DescriptorChangeTailer(changeLog, descriptorBackend);
                // On GENERIC because a pass is object-store I/O. Every node tails, including the cluster
                // manager: a gated index has no cluster state entry, so there is no node that learns about
                // one by any other route.
                threadPool.scheduleWithFixedDelay(
                    tailer::tailOnce,
                    DESCRIPTOR_CHANGE_TAIL_INTERVAL_SETTING.get(environment.settings()),
                    ThreadPool.Names.GENERIC
                );

                // Pruning, on the elected cluster manager alone. Every node could safely run this, since
                // deleting an already-deleted bucket is not an error, but every node running it would pay
                // the listing N times over for one bucket's worth of work.
                final org.opensearch.serverless.storage.descriptor.BlobDescriptorChangeLog pruneTarget = changeLog;
                final TimeValue changeLogRetention = DESCRIPTOR_CHANGE_LOG_RETENTION_SETTING.get(environment.settings());
                threadPool.scheduleWithFixedDelay(() -> {
                    if (clusterService.state().nodes().isLocalNodeElectedClusterManager()) {
                        pruneTarget.pruneOlderThan(changeLogRetention.millis());
                    }
                }, changeLogRetention, ThreadPool.Names.GENERIC);

                // Tombstone reclamation, off unless asked for, and on the elected cluster manager alone for
                // the same reason as pruning: a pass is one listing, and every node doing it pays that N
                // times for one pass worth of work.
                final TimeValue scrubInterval = TOMBSTONE_SCRUB_INTERVAL_SETTING.get(environment.settings());
                if (scrubInterval.equals(TimeValue.ZERO) == false) {
                    final long tombstoneRetention = TOMBSTONE_RETENTION_SETTING.get(environment.settings()).millis();
                    final org.opensearch.serverless.storage.descriptor.TombstoneScrubber scrubber =
                        new org.opensearch.serverless.storage.descriptor.TombstoneScrubber(path -> {
                            try {
                                return resolveContainerForDescriptors(path, "no blob store for tombstone reclamation");
                            } catch (IOException e) {
                                throw new java.io.UncheckedIOException(e);
                            }
                        }, BlobPath.cleanPath().add("descriptors-root"), System::currentTimeMillis);
                    threadPool.scheduleWithFixedDelay(() -> {
                        if (clusterService.state().nodes().isLocalNodeElectedClusterManager()) {
                            scrubber.scrubOnce(tombstoneRetention);
                        }
                    }, scrubInterval, ThreadPool.Names.GENERIC);
                }

                // GcCandidateTailer, off unless asked for, and on the elected cluster manager alone -- same
                // reasoning as pruning and tombstone reclamation just above: one pass here is one bounded
                // listing over the candidate log, not a per-shard operation, so every node running it would
                // pay for the same bounded work N times over rather than sharing it. See GcCandidate's own
                // class javadoc for what this buys over GcSchedulerTask's unconditional per-shard sweep,
                // which keeps running underneath this unchanged either way.
                if (gcCandidateTailInterval != null) {
                    BlobGcCandidateLog gcCandidateLog = gcCandidateLogOrNull();
                    GcCandidateTailer gcCandidateTailer = new GcCandidateTailer(
                        gcCandidateLog,
                        this::blobContainerForDirectoryFactory,
                        gcRetentionWindowMillis,
                        gcCandidateLookbackMillis
                    );
                    threadPool.scheduleWithFixedDelay(() -> {
                        if (clusterService.state().nodes().isLocalNodeElectedClusterManager()) {
                            gcCandidateTailer.tailOnce();
                        }
                    }, gcCandidateTailInterval, ThreadPool.Names.GENERIC);

                    // Same backstop reasoning as the descriptor log's own pruning: a healthy candidate is
                    // always resolved by the tailer itself long before this would ever reach it, so this is
                    // hygiene for the abandoned case (a permanently pinned generation whose release does not
                    // currently re-trigger anything here), not the normal retirement path.
                    threadPool.scheduleWithFixedDelay(() -> {
                        if (clusterService.state().nodes().isLocalNodeElectedClusterManager()) {
                            gcCandidateLog.pruneOlderThan(gcCandidateLookbackMillis);
                        }
                    }, TimeValue.timeValueMillis(gcCandidateLookbackMillis), ThreadPool.Names.GENERIC);
                }
            } catch (Exception e) {
                // A cluster with no object store configured still publishes descriptors and still resolves
                // them; it simply has no cross-node feed, which is the state it was in before this existed.
                // Also covers GcCandidateTailer's own setup just above, which shares this try block: no
                // object store configured means no bounded location for that log either, and GcSchedulerTask
                // remains fully able to reach every manifest on its own regardless.
                logger.warn("no descriptor change feed; other nodes will learn of changes only by rebuild", e);
            }
        }

        return java.util.List.of(this);
    }

    /**
     * Builds a {@link BundleFileReader} that reads this shard's own bundles from {@code ownContainer}
     * first, falling back to each ancestor container in {@code lineageChain} (see {@link
     * #getEngineFactory}'s own resolution of that chain) in order -- the same {@code
     * FallbackBundleFileReader.chain(...)} shape {@code TransportShardShrinkAction.resolveShrinkSource}
     * already uses, generalized so every read path built in {@link #getEngineFactory} can share it
     * rather than only the shrink path having it. {@code ownContainer} need not be {@code
     * lineageChain.get(0)} itself -- callers pass whichever permission-scoped wrapper of this shard's
     * own container that specific read path already uses (e.g. the reader query-serving path's
     * further-restricted {@code readOnlyContainer}), while ancestor containers are always resolved
     * unrestricted, since a {@link BundleFileReader} only ever reads.
     *
     * @param ownContainer this shard's own (possibly permission-wrapped) container, tried first.
     * @param lineageChain this shard's full clone-lineage chain; only entries from index 1 onward
     *                     (the ancestors) are used as fallbacks -- entry 0 is never consulted since
     *                     {@code ownContainer} already covers this shard's own data.
     */
    private static BundleFileReader chainedBundleReadPath(BlobContainer ownContainer, List<BlobContainer> lineageChain) {
        BundleFileReader ownReadPath = new BlobContainerBundleStore(ownContainer);
        if (lineageChain.size() <= 1) {
            return ownReadPath;
        }
        List<BundleFileReader> readers = new java.util.ArrayList<>(lineageChain.size());
        readers.add(ownReadPath);
        for (int i = 1; i < lineageChain.size(); i++) {
            readers.add(new BlobContainerBundleStore(lineageChain.get(i)));
        }
        return org.opensearch.serverless.storage.clone.FallbackBundleFileReader.chain(readers);
    }

    /** The {@link #shardMaintenanceTasks} key for one shard. */
    private static String shardMaintenanceKey(String indexUuid, int shardId) {
        return indexUuid + "/" + shardId;
    }

    /**
     * Starts this shard's background compaction and GC schedulers on this node, unless it already
     * has them.
     *
     * <p>Idempotent by key, which is what makes it safe to call from {@link #getEngineFactory}: that
     * method runs again whenever the shard reopens -- a restart in place, a relocation back to this
     * node, a failed-then-retried allocation -- and a second live scheduler for one shard would
     * double its object-store request rate for no benefit.
     *
     * <p>Does nothing at all when both intervals are unset, which is still the default. This method
     * makes the schedulers <em>reachable</em>; it does not turn them on. An operator who has never
     * set {@code serverless_storage.gc.interval} still gets no GC -- the difference is that an
     * operator who <em>has</em> set it now gets it for every serverless index, rather than only for
     * indices that happen to have search replicas.
     *
     * @param indexUuid the shard's index UUID.
     * @param shardId the shard's numeric id.
     * @param shardStateStore the write-capable (delete-denied) shard state store compaction rebases through.
     * @param compactionConfig the compaction scheduler's configuration, or {@code null} when the interval is unset.
     * @param gcConfig the GC sweep's configuration, or {@code null} when the interval is unset.
     */
    private void startShardMaintenanceTasks(
        String indexUuid,
        int shardId,
        ShardStateStore shardStateStore,
        CompactionSchedulerConfig compactionConfig,
        GcSchedulerConfig gcConfig
    ) {
        if (compactionConfig == null && gcConfig == null) {
            return;
        }
        if (threadPool == null) {
            // createComponents has not run, which only a partially-constructed plugin can manage --
            // a real node always supplies one. Returning is safe here in a way it is not for the WAL
            // service: not scheduling maintenance costs deferred reclamation, whereas not building a
            // WAL costs acknowledged writes, which is why createComponents refuses that case
            // outright rather than degrading like this one does.
            //
            // Reachable only with WAL mirroring disabled, since createComponents now throws before
            // this point otherwise. Kept rather than tightened to match: a scheduler that silently
            // does not start is the lesser evil of the two, and turning it into a throw would make
            // GC wiring a hard precondition of constructing any engine at all.
            return;
        }
        synchronized (shardMaintenanceTasksLock) {
            if (shardMaintenanceTasksClosed) {
                return;
            }
            shardMaintenanceTasks.computeIfAbsent(shardMaintenanceKey(indexUuid, shardId), key -> {
                CompactionSchedulerTask compactionTask = compactionConfig == null
                    ? null
                    : new CompactionSchedulerTask(
                        threadPool,
                        compactionConfig.interval(),
                        indexUuid,
                        shardId,
                        shardStateStore,
                        compactionConfig.manifestStore(),
                        compactionConfig.materializer(),
                        compactionConfig.commitPublisher(),
                        compactionConfig.policy(),
                        compactionConfig.rebaseExecutor(),
                        compactionConfig.admissionController(),
                        compactionConfig.mergeWorkRoot()
                    );
                GcSchedulerTask gcTask = gcConfig == null
                    ? null
                    : new GcSchedulerTask(threadPool, gcConfig.interval(), indexUuid, shardId, gcConfig);
                return new ShardMaintenanceTasks(compactionTask, gcTask);
            });
        }
    }

    /**
     * Cancels this shard's background compaction and GC schedulers on this node.
     *
     * <p>Called from {@code afterIndexShardClosed}, which fires on relocation as well as on real
     * closure -- and relocation is exactly the case that matters. The writer primary moving to
     * another node means that node starts its own pair; leaving this node's pair running would make
     * two nodes sweep and compact the same shard. Both operations are safe under that redundancy (a
     * sweep is idempotent, a publish is term-fenced), so this is a cost fix rather than a
     * correctness one -- but the cost is object-store requests, which is the thing &sect;6.5's whole
     * design exists to bound.
     *
     * <p>Idempotent: a shard that never started a pair here simply has no entry.
     *
     * @param indexUuid the shard's index UUID.
     * @param shardId the shard's numeric id.
     */
    private void stopShardMaintenanceTasks(String indexUuid, int shardId) {
        synchronized (shardMaintenanceTasksLock) {
            ShardMaintenanceTasks tasks = shardMaintenanceTasks.remove(shardMaintenanceKey(indexUuid, shardId));
            if (tasks != null) {
                tasks.close();
            }
        }
    }

    /**
     * Resolves this shard's own {@link BlobContainer} (index-UUID/shard-scoped, encryption-wrapped
     * if configured) -- shared by {@link #getEngineFactory} and {@link
     * #blobContainerForDirectoryFactory}, which both need exactly this same resolution.
     */
    private BlobContainer resolveBlobContainer(String indexUuid, int shardId) throws IOException {
        // Each shard gets its own child container (rfc-serverless-opensearch.md &sect;6.1's
        // indices/<index-uuid>/<shard>/ layout): CommitManifest#manifestName() is intentionally
        // just <term>-<generation> with no index/shard component, since it assumes the
        // container it lives in is already shard-scoped.
        BlobContainer blobContainer = blobContainerFor(indexUuid, shardId);
        if (encryptionKeyProvider != null) {
            // Wrapping here, at the one seam every downstream class already depends on
            // abstractly (BlobContainer), is the entire integration -- see
            // EncryptingBlobContainer's javadoc for the ranged-read tradeoff this implies.
            //
            // (indexUuid, shardId) are now passed in, where before this seam built an
            // index-agnostic wrapper. Two things follow, and only one of them is theoretical.
            // The theoretical one: encryption now calls EncryptionKeyProvider#currentKey(indexUuid)
            // rather than currentKey(), so a per-index-aware provider would finally be honoured --
            // no such provider can be configured today (see PerIndexEncryptionKeyProvider), so this
            // changes no behaviour yet, it just stops the call site from being the reason it
            // couldn't. The real one: every block written from here on is cryptographically bound
            // to this index and shard, so a block lifted out of another index's bundle no longer
            // authenticates inside this one. That holds under the single node-wide key we actually
            // ship, which is the point -- it is integrity, not key separation, and it does not need
            // key separation to work.
            blobContainer = new EncryptingBlobContainer(
                blobContainer,
                encryptionKeyProvider,
                indexUuid,
                shardId,
                requireAuthenticatedBlocks
            );
        }
        return blobContainer;
    }

    /**
     * Refuses to build an engine for a serverless index whose recovery strategy is not this plugin's.
     *
     * <p>Selection of {@link org.opensearch.index.shard.ShardRecoveryStrategy} is by index setting, injected by
     * {@code ServerlessStorageIndexSettingProvider} at creation. That makes an index created through any path the
     * provider does not cover fall back to core's {@code local-lucene} strategy -- which would then try to recover
     * a shard whose durable copy lives in the object store from local files that are not authoritative, or fail it
     * outright for having none. Both are silent: the engine below is still this plugin's, so the shard looks
     * correctly configured right up until it recovers.
     *
     * <p>This is the one place every serverless shard passes through with its settings in hand, so it is where the
     * mismatch can be caught. Failing here costs an unrecoverable shard with an explicit reason instead of a shard
     * that recovers wrongly, which is the direction this plugin fails in everywhere else.
     *
     * <p><b>This is the backstop, not the primary check.</b> A mismatch is refused at index creation by
     * {@code ServerlessStorageRecoveryStrategyValidator}, which is the only moment it can still be repaired --
     * both settings are final afterwards, so there is no fix short of deleting the index. This check remains
     * because creation is not the only way a shard arrives: an index restored, or created on a node whose
     * plugin set differs, reaches here without passing that validator. It should be unreachable, and if it
     * ever fires it is reporting a genuine invariant break rather than an operator mistake.
     *
     * <p><b>The one case it is expected to fire.</b> The setting is injected at creation, so an index created
     * before {@code index.recovery.strategy} existed carries no value and reads the {@code local-lucene}
     * default. Refusing it is correct -- core's strategy genuinely cannot recover such a shard, so opening it
     * would be the silent wrong answer -- but it means a cluster carrying pre-existing serverless indices
     * needs their settings back-filled first. No released build has such indices, which is why this throws
     * rather than carrying a shim for a case that cannot yet occur; if that changes, the fix is an
     * index-metadata upgrader that back-fills the setting, not a looser check here.
     */
    private static void requireObjectStoreRecoveryStrategy(IndexSettings indexSettings) {
        String strategy = org.opensearch.index.IndexModule.INDEX_RECOVERY_STRATEGY_SETTING.get(indexSettings.getSettings());
        if (org.opensearch.serverless.storage.writerengine.ObjectStoreShardRecoveryStrategy.NAME.equals(strategy) == false) {
            throw new IllegalStateException(
                "index ["
                    + indexSettings.getIndex().getName()
                    + "] has serverless storage enabled but its ["
                    + org.opensearch.index.IndexModule.INDEX_RECOVERY_STRATEGY_SETTING.getKey()
                    + "] is ["
                    + strategy
                    + "], not ["
                    + org.opensearch.serverless.storage.writerengine.ObjectStoreShardRecoveryStrategy.NAME
                    + "]; its durable copy lives in the object store and core's strategy cannot recover it"
            );
        }
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings, ShardRouting shardRouting) {
        if (SERVERLESS_STORAGE_ENABLED_SETTING.get(indexSettings.getSettings()) == false) {
            return Optional.empty();
        }
        requireObjectStoreRecoveryStrategy(indexSettings);

        try {
            String indexUuid = indexSettings.getIndex().getUUID();
            int shardIdValue = shardRouting != null ? shardRouting.shardId().getId() : 0;
            BlobContainer blobContainer = resolveBlobContainer(indexUuid, shardIdValue);
            // Credential scoping per tier (rfc-serverless-opensearch.md &sect;15): every consumer
            // built from this container below except GcSchedulerConfig's own store pair further
            // down only ever needs GET+PUT, never DELETE -- deletion is reserved to the
            // GC/reconciler role. Wrapping here, not at resolveBlobContainer itself, keeps
            // `blobContainer` available unrestricted for the two places that still need it
            // directly (the shard-partition descriptor read below and DedicatedWalGcConfig's own
            // WAL-container GC, unrelated to this credential-scoping change).
            BlobContainer scopedContainer = new org.opensearch.serverless.storage.security.RestrictingBlobContainer(blobContainer, false);
            ShardStateStore shardStateStore = new BlobContainerShardStateStore(scopedContainer);
            BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(scopedContainer);
            BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(scopedContainer);
            // Shared by both roles below: the reader branch's own background CompactionSchedulerTask
            // needs one just as much as the writer branch's ordinary commit-publish path does --
            // cheap and stateless to construct once here rather than duplicating it in each branch.
            ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
            // Same blob container every other per-shard store here is scoped to -- a durable pin
            // lives alongside the shard's manifests/registers, not in some separate namespace.
            // Always constructed, not gated on PITR being enabled: a snapshot can pin a manifest
            // independent of PITR, and the reader branch's own background GcSchedulerTask needs a
            // real registry to check regardless of whether PITR retention is configured on this node.
            // A pin is only ever added or overwritten by CAS, never deleted through this registry
            // itself, so the delete-denying scoped container is safe here too.
            DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(scopedContainer);
            // Resolved once, shared by every read path built below (reader query-serving, background
            // compaction, background partition-rewrite, and writer cross-node-failover recovery
            // alike): a shard freshly split off a parent (or several clone hops deep -- see
            // ShardCloner#resolveLineageChain's own javadoc) has a manifest referencing bundle files
            // that physically exist only in an ancestor's container until this shard republishes its
            // own commit. Without this, every one of those read paths throws NoSuchFileException the
            // moment anything tries to read such a manifest -- unlike TransportShardShrinkAction's
            // own resolveShrinkSource, which already does this, these engine-construction read paths
            // never did.
            List<BlobContainer> lineageChain = org.opensearch.serverless.storage.clone.ShardCloner.resolveLineageChain(
                blobContainer,
                indexUuid,
                shardIdValue,
                this::resolveBlobContainer
            );
            // Computed here, ahead of the reader/writer branch below, so both branches can schedule
            // their own PitrRetentionSchedulerTask -- previously this was only computed in the
            // writer branch, which meant PITR reconciliation (both adding new pins AND releasing
            // ones that have aged out of the window) silently stopped the moment a shard's writer
            // scaled to zero, leaving whatever was pinned at that moment retained forever (GC can
            // never delete a durably-pinned manifest). Mirrors gcConfig/pinRegistry's own
            // "always meaningful on either role" reasoning just above.
            PitrRetentionConfig pitrRetentionConfig = pitrWindowMillis > 0
                ? new PitrRetentionConfig(manifestStore, pinRegistry, pitrWindowMillis)
                : null;

            // Round 006 item 6. The PRIMARY of shard 0 only, and only for a real assignment --
            // shardRouting is null for administrative calls (mapping validation/update) that pass
            // through this same method with no real shard behind them, and shardIdValue's own
            // fallback-to-0 must not be read as "this node hosts shard 0" here the way it can be for
            // the read-path constructions below, which are already conditioned on
            // isReaderShard/a real routing entry.
            //
            // primary() matters as much as shardIdValue == 0: found by round 2 of the bug hunt that
            // landed this whole block. A serverless index is forced to zero ordinary replicas and
            // scales reads instead via search-only replicas of the SAME shard, so without this check
            // every reader-replica copy of shard 0 -- and there can be many, elastically -- would
            // independently start its own sweep task for the same index. The sweep's own correctness
            // does not need multiple sweepers (deleting an already-deleted ledger is the documented
            // no-op), so the redundancy bought nothing but contradicted this task's own "node-local,
            // bounded" cost model by scaling sweep traffic with read fan-out rather than staying at
            // roughly one sweeper per index.
            //
            // See PinLedgerSweepTask's own javadoc for why coverage is per-index, node-local rather
            // than fleet-wide, and why the task is left running (deduped by indexUuid) rather than
            // torn down when shard 0 later relocates off this node. Constructed here rather than
            // threaded through ObjectStoreWriterEngine/ReaderEngineFactory's own long constructor
            // chains: sweeping needs nothing from either engine, only the container resolver this
            // method already has in scope.
            //
            // Built from the unrestricted blobContainer, not scopedContainer: scopedContainer denies
            // delete (line above, "GET+PUT but no DELETE"), and a sweep's whole job is deleting a
            // cleared ledger -- the same reason GcSchedulerConfig a few lines below is also built
            // against blobContainer directly rather than the read/write-scoped wrapper.
            //
            // pinLedgerSweepTasksLock/pinLedgerSweepTasksClosed guard against the shutdown race round
            // 2 of the bug hunt found: close() drains this map in two non-atomic steps (forEach(close)
            // then clear()), and a shard-0 open racing shutdown could otherwise insert a
            // freshly-scheduled task after the drain, which clear() would then silently discard
            // without ever cancelling. See the field javadoc for why this needs a lock, not just a
            // flag.
            if (shardRouting != null && shardRouting.primary() && shardIdValue == 0 && pinLedgerSweepInterval != null) {
                synchronized (pinLedgerSweepTasksLock) {
                    if (pinLedgerSweepTasksClosed == false) {
                        pinLedgerSweepTasks.computeIfAbsent(
                            indexUuid,
                            uuid -> new PinLedgerSweepTask(
                                threadPool,
                                pinLedgerSweepInterval,
                                uuid,
                                new org.opensearch.serverless.storage.retention.BlobContainerPinLedgerStore(blobContainer),
                                this::resolveBlobContainer,
                                pinLedgerAbandonedAfterMillis
                            )
                        );
                    }
                }
            }

            // Compaction and GC configuration, built for EVERY serverless shard this node opens --
            // not, as before, only inside the reader-shard branch below.
            //
            // The old placement made both schedulers unreachable for the ordinary serverless index.
            // A reader shard only exists if the index has index.number_of_search_replicas > 0, which
            // in turn requires remote_store.enabled. The common shape -- one writer shard, no search
            // replicas -- therefore had no reader engine anywhere in the cluster, so no
            // CompactionSchedulerTask and no GcSchedulerTask were ever constructed, no matter what
            // serverless_storage.compaction.interval and serverless_storage.gc.interval were set to.
            // Storage grew without bound and quiescent shards never compacted, silently: the
            // settings were accepted, the intervals were parsed, and nothing ticked. This is the
            // failure mode where a feature is configured, believed to be running, and is not.
            //
            // The original reason for the reader-only placement no longer holds. It was that a
            // writer always holds its own lease, so a writer-hosted compaction scheduler would see
            // the lease held and never act -- but the lease gate was removed from maybeCompact, and
            // nothing has replaced it, so a writer-hosted scheduler is now simply a scheduler.
            CompactionSchedulerConfig compactionConfig = compactionInterval == null
                ? null
                : new CompactionSchedulerConfig(
                    compactionInterval,
                    manifestStore,
                    // Straight to the raw bundle store (via the lineage-fallback-aware read path,
                    // not the possibly-cache-wrapped reader read path) -- a merge reads every input
                    // segment file exactly once, so there's no hot-rereading benefit a cache would
                    // give, matching ObjectStoreShardRecoveryStrategy's own "no caching layer needed"
                    // choice for cross-node failover materialization.
                    new ObjectStoreCommitMaterializer(chainedBundleReadPath(scopedContainer, lineageChain)),
                    commitPublisher,
                    CompactionPolicy.withDefaults(),
                    new CompactionRebaseExecutor(shardStateStore, 5),
                    rewriteAdmissionController,
                    // Merge scratch on the node's own data path: a merge stages a whole shard's worth
                    // of segments, which the platform temp directory is routinely too small to hold.
                    compactionMergeWorkRoot()
                );
            // GC is the one tier &sect;15's credential-scoping model actually grants DELETE to
            // ("the GC/reconciler role is the only DELETE-capable principal") -- built against
            // its own store pair on the *unrestricted* blobContainer, deliberately not reusing
            // manifestStore/bundleStore above (which are wired through the delete-denying
            // scopedContainer and would throw the moment GcSchedulerTask's sweep tried to
            // delete anything through them).
            GcSchedulerConfig gcConfig = gcInterval == null
                ? null
                : new GcSchedulerConfig(
                    gcInterval,
                    gcRetentionWindowMillis,
                    new BlobContainerManifestStore(blobContainer),
                    // Raw bundle store, same "no hot-rereading benefit from a cache" reasoning as
                    // the compaction config just above -- a sweep lists/deletes bundle names, it
                    // never reads their contents at all.
                    new BlobContainerBundleStore(blobContainer),
                    pinRegistry,
                    // The head, on the same unrestricted container the rest of GC uses. A sweep that
                    // cannot read the head cannot tell a published manifest from one whose writer died
                    // between writing it and CAS-ing the head, and deleting on that mistake destroys the
                    // live head and its bundles -- see ManifestRetentionPolicy's own javadoc.
                    new BlobContainerShardStateStore(blobContainer),
                    // Cross-tick bookkeeping (the orphan clock, the last swept head) persisted per shard,
                    // so a restart or relocation no longer resets it and stops bundles ever being freed.
                    // That matters more now than it did when this config was built inside the reader
                    // branch: the sweep's host is the writer primary, which relocates on every failover,
                    // so an in-memory orphan clock would have been reset by exactly the events a
                    // long-running deployment sees most.
                    new org.opensearch.serverless.storage.gc.BlobContainerGcSweepStateStore(blobContainer, indexUuid, shardIdValue)
                );
            // One sweeper and one compactor per shard, on the node that holds its writer primary.
            //
            // Why the primary and not every copy: both tasks are node-local schedulers over
            // cluster-wide durable state, so N copies would do the same work N times. A GC sweep is
            // idempotent (deleting an already-deleted blob is the documented no-op) and a compaction
            // publish is CAS-fenced by primary term, so redundancy would be safe -- it just costs
            // object-store requests proportional to read fan-out, which is precisely the cost model
            // §6.5 says a sweep must not have. A writer primary exists for every assigned shard,
            // exactly once in the cluster, which is the property that makes it the right host.
            //
            // shardRouting is null for administrative calls (mapping validation and the like) that
            // pass through getEngineFactory with no real shard behind them; shardIdValue's own
            // fallback-to-0 must not be read as "this node hosts shard 0" in that case. Same guard
            // the pin-ledger sweep above already uses, for the same reason.
            if (shardRouting != null && shardRouting.primary() && shardRouting.isSearchOnly() == false) {
                startShardMaintenanceTasks(indexUuid, shardIdValue, shardStateStore, compactionConfig, gcConfig);
            }

            boolean isReaderShard = shardRouting != null && shardRouting.isSearchOnly();
            if (isReaderShard) {
                // Credential scoping per tier, bullet 1 (rfc-serverless-opensearch.md &sect;15):
                // "search-compute needs GET-only on data prefixes." A reader shard now genuinely
                // needs nothing beyond GET: the compaction and GC schedulers that used to hang off
                // its engine -- and were the only reason it ever needed write or delete -- moved to
                // the node-level tasks above, hosted on the writer primary. So the whole
                // query-serving trio (shardStateStore/manifestStore/readPath, what
                // ObjectStoreReaderEngine reads on every query and on its own background poll) gets
                // this strictly-narrower container, and nothing on the reader path is left outside
                // it.
                BlobContainer readOnlyContainer = new org.opensearch.serverless.storage.security.RestrictingBlobContainer(
                    scopedContainer,
                    false,
                    false
                );
                ShardStateStore readerShardStateStore = new BlobContainerShardStateStore(readOnlyContainer);
                BlobContainerManifestStore readerManifestStore = new BlobContainerManifestStore(readOnlyContainer);
                BundleFileReader readPath = chainedBundleReadPath(readOnlyContainer, lineageChain);
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
                    // The per-shard budget and the node-wide governor are independent bounds and
                    // either can be off; the governor is the one that actually protects the disk,
                    // since N shards times a per-shard budget is what lands on it.
                    LocalDiskCachingBundleStore diskCache = new LocalDiskCachingBundleStore(
                        readPath,
                        shardCacheDir,
                        encryptionKeyProvider,
                        localCacheMaxBytesPerShard,
                        localCacheSpaceGovernor
                    );
                    cacheStatsRegistry.register(indexUuid, shardIdValue, diskCache);
                    readPath = new CachingBundleFileReader(sharedBundleCache, diskCache);
                }
                org.opensearch.serverless.storage.resharding.BlobContainerShardPartitionStore rawPartitionStore =
                    new org.opensearch.serverless.storage.resharding.BlobContainerShardPartitionStore(blobContainer);
                org.opensearch.serverless.storage.resharding.ShardPartitionDescriptor partitionDescriptor;
                try {
                    partitionDescriptor = rawPartitionStore.readDescriptor().orElse(null);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                // §16 Phase 5's own background-scheduler gap: same shape as compactionConfig/gcConfig
                // above, but deliberately built against the *unrestricted* blobContainer, not
                // scopedContainer -- PartitionRewritePublisher#rewrite's own last step
                // (clearDescriptor) is a real delete (rfc-serverless-opensearch.md &sect;15's
                // credential-scoping model would deny it through the delete-denying container every
                // other reader-shard store here uses), the same reasoning
                // TransportShardPartitionRewriteAction's own on-demand trigger already established
                // for this exact action.
                org.opensearch.serverless.storage.resharding.PartitionRewriteSchedulerConfig partitionRewriteConfig =
                    partitionRewriteInterval == null
                        ? null
                        : new org.opensearch.serverless.storage.resharding.PartitionRewriteSchedulerConfig(
                            partitionRewriteInterval,
                            new BlobContainerShardStateStore(blobContainer),
                            new BlobContainerManifestStore(blobContainer),
                            new BlobContainerBundleStore(blobContainer),
                            new ObjectStoreCommitMaterializer(chainedBundleReadPath(blobContainer, lineageChain)),
                            new ObjectStoreCommitPublisher(
                                new BlobContainerBundleStore(blobContainer),
                                new BlobContainerManifestStore(blobContainer)
                            ),
                            rawPartitionStore,
                            rewriteAdmissionController
                        );
                return Optional.of(
                    new ReaderEngineFactory(
                        readerShardStateStore,
                        readerManifestStore,
                        new ObjectStoreCommitMaterializer(readPath),
                        shardDirectory,
                        localNodeId,
                        readerShardAdmissionController,
                        // Both null, deliberately: compaction and GC for this shard now run from
                        // the node-level tasks started above, on whichever node holds the writer
                        // primary. Leaving them here as well would double every sweep and every
                        // merge attempt for an index that happens to have search replicas -- and the
                        // whole point of the move is that an index without them is covered too.
                        null,
                        null,
                        readerShardActivityRegistry,
                        partitionDescriptor,
                        partitionRewriteConfig,
                        pitrRetentionConfig
                    )
                );
            }

            // §12's "dedicated WAL streams" bullet: an index opted into
            // SERVERLESS_STORAGE_WAL_DEDICATED_STREAM_SETTING gets its own WalChunkService pointed
            // at a container scoped to exactly this (indexUuid, shardId) -- never the node-shared
            // one -- so its WAL bytes can never land in the same object as any other index's,
            // independent of the per-record encryption every WAL record already gets regardless.
            // Only meaningful when WAL mirroring itself is on at all (resolveSharedWalChunkService()
            // returning null); an index requesting a dedicated stream on a node with WAL mirroring
            // off gets none, same as every other WAL-dependent feature already degrades in that
            // case. This is the "first real writer-shard use" resolveSharedWalChunkService()'s own
            // javadoc names -- the shared container and sharedWalChunkService itself are actually
            // built here, on this call, the first time any writer shard on this node needs one.
            org.opensearch.serverless.storage.wal.WalChunkService writerWalChunkService = resolveSharedWalChunkService();
            // The durability gap made observable. Without a WalChunkService this shard's writer
            // engine falls back to core's LocalTranslog, so everything not yet flushed lives on
            // local disk only -- which contradicts rfc-serverless-opensearch.md §2 goal 1 ("the
            // object store is the sole durable home of write-ahead data; local disk is strictly a
            // cache") for the default configuration. That was true before this line too; what was
            // missing was any way to find out. A warning per shard open is cheap and lands in the
            // log of the node that would lose the data.
            //
            // See SERVERLESS_STORAGE_WAL_MIRRORING_REQUIRED_SETTING for why refusing outright is
            // opt-in rather than the default, and for who owns flipping it.
            if (writerWalChunkService == null) {
                if (SERVERLESS_STORAGE_WAL_MIRRORING_REQUIRED_SETTING.get(settings)) {
                    throw new IllegalStateException(
                        "index ["
                            + indexSettings.getIndex().getName()
                            + "] has serverless storage enabled and ["
                            + SERVERLESS_STORAGE_WAL_MIRRORING_REQUIRED_SETTING.getKey()
                            + "] is set, but no write-ahead-log service is available on this node: set ["
                            + SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey()
                            + "] to true so unflushed operations are durable in the object store, or unset ["
                            + SERVERLESS_STORAGE_WAL_MIRRORING_REQUIRED_SETTING.getKey()
                            + "] to accept local-disk-only durability for them"
                    );
                }
                logger.warn(
                    "index [{}] shard [{}] is opening with serverless storage but no write-ahead-log service: "
                        + "operations not yet flushed to a published commit are durable only on this node's local disk "
                        + "and are lost if it is killed. Set [{}] to true for object-store durability, or [{}] to refuse "
                        + "to open instead of degrading silently.",
                    indexSettings.getIndex().getName(),
                    shardIdValue,
                    SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(),
                    SERVERLESS_STORAGE_WAL_MIRRORING_REQUIRED_SETTING.getKey()
                );
            }
            org.opensearch.serverless.storage.wal.DedicatedWalGcConfig dedicatedWalGcConfig = null;
            if (writerWalChunkService != null && SERVERLESS_STORAGE_WAL_DEDICATED_STREAM_SETTING.get(indexSettings.getSettings())) {
                BlobContainer dedicatedWalContainer = resolveDedicatedWalContainer(indexUuid, shardIdValue);
                // No per-shard budget needed here (0, disabled): that budget exists to siphon one
                // noisy shard's buffer out from under others sharing the SAME WalChunkService --
                // a dedicated container is already scoped to exactly this one shard, so there is no
                // "other shard" for it to protect against.
                writerWalChunkService = new org.opensearch.serverless.storage.wal.WalChunkService(
                    dedicatedWalContainer,
                    UUIDs.base64UUID(),
                    0L
                );
                if (walGcInterval != null && walGcInterval.millis() > 0) {
                    dedicatedWalGcConfig = new org.opensearch.serverless.storage.wal.DedicatedWalGcConfig(
                        dedicatedWalContainer,
                        blobContainer,
                        walGcInterval
                    );
                }
            }

            return Optional.of(
                new WriterEngineFactory(
                    new ObjectStoreCommitHeadPublisher(commitPublisher, shardStateStore, gcCandidateLogOrNull()),
                    shardDirectory,
                    localNodeId,
                    pitrRetentionConfig,
                    writerWalChunkService,
                    // WAL-mirrored records get the same at-rest protection bundles/manifests already
                    // have when this is configured (&sect;12 bullet 1) -- null (the default) leaves
                    // WAL mirroring's own on/off switch (sharedWalChunkService being non-null) as the
                    // only thing this depends on, unaffected by encryption being off.
                    encryptionKeyProvider,
                    shardActivityRegistry,
                    dedicatedWalGcConfig,
                    publicationRateLimitMillis,
                    writerPublicationNotifierForWriterEngine(),
                    // Same pinRegistry every other per-shard store above is scoped to -- engine-native
                    // snapshot creation pins alongside this shard's own manifests/registers, exactly
                    // like PITR's own pins do (see pinRegistry's own declaration above).
                    // Cross-node failover / in-place split / in-place merge store population and
                    // engine-native snapshot restore all used to be threaded in here too; they now live
                    // on ObjectStoreShardRecoveryStrategy (registered once, node-wide, in
                    // getShardRecoveryStrategies above) because none of them needs -- or can have -- an
                    // engine this factory built.
                    pinRegistry
                )
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Resolves this shard's own regular (non-WAL) {@link BlobContainer}: from a real registered
     * repository's {@code BlobStore} if {@link #SERVERLESS_STORAGE_REPOSITORY_SETTING} names one,
     * otherwise the local-filesystem container under {@link #basePath} exactly as before -- see
     * that setting's own javadoc for the full design and why this is the one seam swapping in
     * S3/GCS/Azure only ever needs to touch.
     */
    private BlobContainer blobContainerFor(String indexUuid, int shardId) throws IOException {
        BlobPath shardPath = BlobPath.cleanPath().add(indexUuid).add(String.valueOf(shardId));
        return resolveContainer(
            shardPath,
            "shard ["
                + indexUuid
                + "]["
                + shardId
                + "] has serverless storage enabled but neither ["
                + SERVERLESS_STORAGE_REPOSITORY_SETTING.getKey()
                + "] nor ["
                + SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey()
                + "] resolved to a usable container (the latter's node setting is either unset "
                + "or did not resolve to an allowed path)"
        );
    }

    /**
     * The one seam every container this plugin builds -- a shard's own regular container, the
     * shared WAL container, and a shard's own dedicated WAL container alike -- resolves through:
     * a real registered repository's {@code BlobStore} if {@link #SERVERLESS_STORAGE_REPOSITORY_SETTING}
     * names one (scoped under that repository's own {@code basePath()} plus a {@code
     * serverless_storage/} prefix, so this plugin's data can never collide with whatever snapshots
     * that same repository also stores, even though both share one underlying bucket), otherwise
     * the local-filesystem container under {@link #basePath} exactly as before. {@code
     * relativePath} is everything after that shared prefix -- e.g. {@code <indexUuid>/<shardId>}
     * for a shard's own container, {@code wal} for the shared WAL container, {@code
     * wal-dedicated/<indexUuid>/<shardId>} for a dedicated one -- so every container this plugin
     * builds lives under the exact same root regardless of which backend actually stores it.
     *
     * <p>Originally scoped to only the regular shard container, deliberately, not oversight --
     * "the shared/dedicated WAL containers remain local-filesystem-only, a natural follow-up once
     * this seam is proven in production rather than widening one change's blast radius" (this
     * class's own prior status note). Now that the seam has proven itself (real S3/GCS/Azure
     * verification via {@code ServerlessStorageRepositoryBackedContainerIT}), extending it to the
     * WAL containers below is exactly that follow-up: the same one seam, no new resolution logic.
     */
    /** {@link #resolveContainer} under a name that says why the descriptor plane needs its own. */
    private BlobContainer resolveContainerForDescriptors(BlobPath relativePath, String missingContainerErrorMessage) throws IOException {
        return resolveContainer(relativePath, missingContainerErrorMessage);
    }

    /** {@link #resolveContainer} under a name that says why {@link BlobGcCandidateLog} needs its own. */
    private BlobContainer resolveContainerForGcCandidates(BlobPath relativePath, String missingContainerErrorMessage) throws IOException {
        return resolveContainer(relativePath, missingContainerErrorMessage);
    }

    /**
     * Builds a {@link BlobGcCandidateLog} pointed at this node's shared {@code gc-candidates-root}, or
     * {@code null} if {@link #gcCandidateTailInterval} says the feature is off.
     *
     * <p>Cheap and stateless to construct (a function reference, a path, a clock) -- called fresh both here
     * and from {@link #createComponents} rather than threaded through as a single shared instance, the same
     * way {@code resolveContainerForDescriptors} itself is called fresh from more than one place rather than
     * cached. Every instance resolves to the same durable location, so there is nothing to keep in sync.
     */
    private BlobGcCandidateLog gcCandidateLogOrNull() {
        if (gcCandidateTailInterval == null) {
            return null;
        }
        return new BlobGcCandidateLog(path -> {
            try {
                return resolveContainerForGcCandidates(path, "no blob store for the GC candidate log");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, BlobPath.cleanPath().add("gc-candidates-root"));
    }

    private BlobContainer resolveContainer(BlobPath relativePath, String missingContainerErrorMessage) throws IOException {
        return new RequestCountingBlobContainer(resolveContainerUncounted(relativePath, missingContainerErrorMessage), requestCounter);
    }

    private BlobContainer resolveContainerUncounted(BlobPath relativePath, String missingContainerErrorMessage) throws IOException {
        if (repositoryName != null && repositoryName.isEmpty() == false) {
            return repositoryBackedBlobContainer(relativePath);
        }
        if (basePath == null) {
            throw new IllegalStateException(missingContainerErrorMessage);
        }
        FsBlobStore blobStore = new FsBlobStore(1024 * 1024, basePath, false);
        return blobStore.blobContainer(relativePath);
    }

    /**
     * Resolves {@code relativePath} against the {@link #repositoryName}-named repository's own
     * {@code BlobStore}, scoped under that repository's own {@code basePath()} plus a {@code
     * serverless_storage/} prefix -- see {@link #resolveContainer} for the full picture this is
     * one half of.
     */
    private BlobContainer repositoryBackedBlobContainer(BlobPath relativePath) throws IOException {
        if (repositoriesServiceSupplier == null) {
            throw new IllegalStateException(
                "container [" + relativePath + "] needs repository [" + repositoryName + "] before RepositoriesService is ready"
            );
        }
        org.opensearch.repositories.Repository repository;
        try {
            repository = repositoriesServiceSupplier.get().repository(repositoryName);
        } catch (RuntimeException e) {
            // RepositoriesService#repository throws RepositoryMissingException (a RuntimeException,
            // not IOException) for an unregistered name -- wrapped so every caller of
            // resolveBlobContainer only ever has to catch IOException, same as the local-filesystem
            // path already guarantees.
            throw new IOException(
                "container ["
                    + relativePath
                    + "] configured ["
                    + SERVERLESS_STORAGE_REPOSITORY_SETTING.getKey()
                    + "="
                    + repositoryName
                    + "] but that repository is not currently registered -- register it via the "
                    + "_snapshot API before serverless storage indices can use it",
                e
            );
        }
        if (repository instanceof org.opensearch.repositories.blobstore.BlobStoreRepository == false) {
            throw new IllegalStateException(
                "container ["
                    + relativePath
                    + "] configured ["
                    + SERVERLESS_STORAGE_REPOSITORY_SETTING.getKey()
                    + "="
                    + repositoryName
                    + "], but that repository is a ["
                    + repository.getClass().getName()
                    + "], not a BlobStoreRepository -- only blob-store-backed repositories (fs, s3, "
                    + "gcs, azure, ...) expose the BlobStore this plugin needs"
            );
        }
        org.opensearch.repositories.blobstore.BlobStoreRepository blobStoreRepository =
            (org.opensearch.repositories.blobstore.BlobStoreRepository) repository;
        BlobPath fullPath = blobStoreRepository.basePath().add("serverless_storage").add(relativePath);
        return blobStoreRepository.blobStore().blobContainer(fullPath);
    }

    /**
     * Resolves a shard's own dedicated WAL container (rfc-serverless-opensearch.md &sect;12's
     * "dedicated WAL streams" bullet, {@link #SERVERLESS_STORAGE_WAL_DEDICATED_STREAM_SETTING}) --
     * under a {@code wal-dedicated/} top-level prefix, distinct from both the shared {@code wal/}
     * container {@code createComponents} builds and this shard's own regular {@code
     * <indexUuid>/<shardId>/} container {@link #resolveBlobContainer} builds, so this shard's WAL
     * bytes can never land in the same object as either. Not wrapped in {@link
     * EncryptingBlobContainer} -- the shared {@code wal/} container isn't either; per-record
     * encryption is applied at the {@code WalChunkService} layer instead (via {@code
     * EncryptingWalChunkService}, wired in {@code ObjectStoreWriterEngine} whenever {@link
     * #encryptionKeyProvider} is configured, unaffected by whether the underlying container is
     * shared or dedicated) -- see rfc-serverless-opensearch.md &sect;12 bullet 1. Repository-backed
     * exactly like {@link #blobContainerFor} whenever {@link #SERVERLESS_STORAGE_REPOSITORY_SETTING}
     * names one -- see {@link #resolveContainer}'s own javadoc for why this is now unconditional
     * rather than local-filesystem-only.
     */
    private BlobContainer resolveDedicatedWalContainer(String indexUuid, int shardId) throws IOException {
        BlobPath dedicatedWalPath = BlobPath.cleanPath().add("wal-dedicated").add(indexUuid).add(String.valueOf(shardId));
        return resolveContainer(
            dedicatedWalPath,
            "shard ["
                + indexUuid
                + "]["
                + shardId
                + "] has "
                + SERVERLESS_STORAGE_WAL_DEDICATED_STREAM_SETTING.getKey()
                + " enabled but neither ["
                + SERVERLESS_STORAGE_REPOSITORY_SETTING.getKey()
                + "] nor ["
                + SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey()
                + "] resolved to a usable container"
        );
    }

    /**
     * The node-shared WAL container this node's writer shards mirror into
     * (rfc-serverless-opensearch.md &sect;6.4), and {@link #sharedWalChunkService} itself, resolved
     * lazily on first real writer-shard use rather than eagerly at {@code createComponents} time --
     * closing the gap that method's own status note previously left open. Eager resolution used to
     * always go straight to a local {@link FsBlobStore}, deliberately never through {@link
     * #resolveContainer} (unlike {@link #resolveDedicatedWalContainer}/{@link #blobContainerFor}):
     * {@code createComponents} runs at node startup, before an operator has necessarily registered
     * any repository via the {@code _snapshot} API, so routing this through the repository-backed
     * branch that early would fail loudly at startup instead of lazily like every other container
     * this plugin resolves -- a real internalClusterTest failure this design avoided by staying
     * local-filesystem-only, at the cost of never actually being repository-backed regardless of
     * {@link #SERVERLESS_STORAGE_REPOSITORY_SETTING}. Deferring to first writer-shard use (this
     * method's only caller) sidesteps that startup-ordering problem entirely: a writer shard is
     * only ever created well after node startup completes, by which point an operator wanting a
     * repository-backed WAL has had every opportunity to register it.
     *
     * <p>Double-checked-locking, matching the shape every other lazily-resolved container in this
     * class already uses ({@link #resolveContainer}'s own repository lookup, for instance) -- cheap
     * to re-check the already-populated field on every subsequent writer-shard creation, only ever
     * synchronizing on the one node incarnation's first call. {@code sharedWalChunkService()} (the
     * public getter {@code TransportNodeWalBacklogAction} and tests use) deliberately still just
     * reads the raw field rather than calling this method -- see that getter's own javadoc for why
     * it must stay a pure, no-I/O read.
     */
    private WalChunkService resolveSharedWalChunkService() throws IOException {
        if (walMirroringEnabled == false) {
            return null;
        }
        WalChunkService existing = sharedWalChunkService;
        if (existing != null) {
            return existing;
        }
        synchronized (walChunkServiceLock) {
            if (sharedWalChunkService != null) {
                return sharedWalChunkService;
            }
            BlobContainer walBlobContainer = resolveContainer(
                BlobPath.cleanPath().add("wal"),
                "the shared WAL container needs either ["
                    + SERVERLESS_STORAGE_REPOSITORY_SETTING.getKey()
                    + "] or ["
                    + SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey()
                    + "] configured"
            );
            // A fresh epoch per node incarnation (see WalChunkService's own javadoc for what this
            // identifies) -- fencing during replay is a per-record primaryTerm filter, not an
            // epoch-directory one, so nothing depends on this value being stable across restarts;
            // it only needs to be unique enough that this process's chunk sequence numbering never
            // collides with a prior incarnation's.
            WalChunkService built = new WalChunkService(walBlobContainer, UUIDs.base64UUID(), walPerShardBudgetBytes);
            if (walFlushBatchingEnabled) {
                // The node-shared group-commit processor, attached to the shared service so every
                // writer shard's WalMirroringTranslog reaches it via WalAppendTarget#batchingProcessor()
                // -- the presence of an attached processor is exactly what flips that shard's add()
                // from the synchronous legacy PUT-per-op path to real interval-batched group commit.
                // encryptionKeyProvider (node-level, may be null) is applied inside the processor's own
                // write(), standing in for the per-shard EncryptingWalChunkService the legacy path uses.
                org.opensearch.serverless.storage.wal.WalBatchingProcessor processor =
                    new org.opensearch.serverless.storage.wal.WalBatchingProcessor(
                        org.apache.logging.log4j.LogManager.getLogger(org.opensearch.serverless.storage.wal.WalBatchingProcessor.class),
                        walFlushQueueCapacity,
                        threadPool.getThreadContext(),
                        threadPool,
                        () -> walFlushInterval,
                        walFlushByteThreshold,
                        walFlushBacklogRejectThreshold,
                        built,
                        encryptionKeyProvider
                    );
                built.attachBatchingProcessor(processor);
                sharedWalBatchingProcessor = processor;
            }
            if (walGcInterval != null && walGcInterval.millis() > 0) {
                // clusterService: this container is shared cluster-wide (not a shard's own
                // dedicated WAL stream), and WalShardRegistry is a single durable register every
                // node reads identically -- running the sweep on every node would just multiply
                // the same object-store read traffic by the node count, see WalGcSchedulerTask's
                // own javadoc.
                walGcSchedulerTask = new org.opensearch.serverless.storage.wal.WalGcSchedulerTask(
                    threadPool,
                    walGcInterval,
                    walBlobContainer,
                    new org.opensearch.serverless.storage.wal.WalShardRegistry(walBlobContainer),
                    (indexUuid, shardId) -> {
                        try {
                            return resolveBlobContainer(indexUuid, shardId);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    },
                    clusterService
                );
            }
            sharedWalChunkService = built;
            return built;
        }
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return java.util.List.of(
            new ReaderShardPlacementAllocationDecider(),
            new SuspendedShardAllocationDecider(),
            new org.opensearch.serverless.storage.allocation.NodeWarmupAllocationDecider()
        );
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
        return Collections.singletonMap(ServerlessStorageExistingShardsAllocator.NAME, serverlessStorageExistingShardsAllocator);
    }

    /**
     * Phase C5 of {@code core-pluggability-refactor-plan.md}, since collapsed from two hooks into one.
     * Additive: {@link org.opensearch.serverless.storage.descriptor.DescriptorGate#install} still registers
     * with {@link org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers}, and {@link
     * org.opensearch.serverless.storage.placement.ComputedPlacementGate} still registers with {@code
     * AbsentIndexRoutingSuppliers}, exactly as before -- this only adds a second, generic path over both,
     * reachable exclusively through {@code Metadata#indexOrResolved(String)} (a caller has to opt in to
     * that method by name; the plain {@code Metadata#index(String)} every existing call site uses is
     * unaffected) and {@code ClusterState#getIndexRoutingTable(String)}, which {@link
     * SupplierBackedIndexCatalog} answers by delegating straight back to those two registries.
     *
     * <p>This replaces the two hooks it used to implement, {@code getIndexMetadataResolver()} and {@code
     * getIndexRoutingResolver()}, which returned two adapter objects over the same two registries for what
     * core asks as one question. See {@code IndexCatalog}'s own javadoc for the merge.
     *
     * <p>Safe to return unconditionally (including before {@code DescriptorGate.install} has run, and on a
     * node where descriptor gating or computed placement is disabled entirely): with nothing registered in
     * the static registries yet, the catalog resolves to {@code null} and reports {@link
     * org.opensearch.cluster.metadata.IndexCatalog#isActive()} as {@code false}, identical to today. That
     * last part is the whole reason {@code isActive()} exists rather than callers testing for a registered
     * catalog: this plugin always supplies one, and only the registries underneath it say whether the
     * feature is currently on.
     */
    @Override
    public Optional<IndexCatalog> getIndexCatalog() {
        return Optional.of(new SupplierBackedIndexCatalog());
    }

    /**
     * The {@link org.opensearch.serverless.storage.placement.ComputedPlacementMembership} cluster-state
     * custom, relocated here from core's {@code ClusterModule} registration when the class itself moved
     * into this plugin. The wire name ({@code computed_placement_membership}) is byte-identical to what
     * core registered, so a rolling restart across the relocation reads its own persisted membership
     * back.
     *
     * <p><b>This used to be NamedWriteable-only, on the reasoning that the custom "never had a
     * NamedXContent parser in core either, and registering one here would be a behavior change rather than
     * a move."</b> The move was faithful and the original was broken. {@link
     * org.opensearch.serverless.storage.placement.ComputedPlacementMembership#context()} declares {@code
     * API_AND_GATEWAY}, but gateway persistence round-trips through XContent, and {@code
     * Metadata.Builder.fromXContent} silently skips a custom with no registered parser. So the membership
     * was written to disk and never read back: every full-cluster restart began with an empty membership
     * and placement fell through to the live node list during the join window. {@link
     * #getNamedXContent()} below is the missing half.
     */
    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                Metadata.Custom.class,
                org.opensearch.serverless.storage.placement.ComputedPlacementMembership.TYPE,
                org.opensearch.serverless.storage.placement.ComputedPlacementMembership::new
            ),
            new NamedWriteableRegistry.Entry(
                NamedDiff.class,
                org.opensearch.serverless.storage.placement.ComputedPlacementMembership.TYPE,
                org.opensearch.serverless.storage.placement.ComputedPlacementMembership::readDiffFrom
            )
        );
    }

    /**
     * The gateway half of the {@link org.opensearch.serverless.storage.placement.ComputedPlacementMembership}
     * registration above.
     *
     * <p>Without this entry the custom is written to the gateway on every publication and dropped on every
     * read, because {@code Metadata.Builder.fromXContent} logs and skips a custom it has no parser for
     * rather than failing a node's startup over a plugin that might legitimately be gone. That silence is
     * why the gap survived: nothing errors, the cluster comes up, and placement simply computes against
     * whichever nodes happen to have joined so far.
     */
    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new org.opensearch.core.ParseField(org.opensearch.serverless.storage.placement.ComputedPlacementMembership.TYPE),
                org.opensearch.serverless.storage.placement.ComputedPlacementMembership::fromXContent
            )
        );
    }

    /**
     * Phase D2 of {@code core-pluggability-refactor-plan.md}. Additive, and behaviorally a no-op change:
     * {@link SupplierBackedIndexCreationStrategy} delegates straight back to {@code DescriptorOnlyCreation}'s
     * existing static registry, so this exposes the exact same gating decision every existing call site
     * already makes through a second, generic path -- not a new one. {@code DescriptorGate#install} still
     * registers with {@code DescriptorOnlyCreation} directly, exactly as before; this only adds a seam a
     * migrated core call site can reach the identical answer through (see that adapter's own javadoc for why
     * it is safe to return unconditionally, including before installation has run). Phase D3 later relocated
     * both classes from {@code server/} into this plugin (see {@code DescriptorOnlyCreation}'s own javadoc),
     * but the wiring here is unchanged.
     */
    @Override
    public Optional<IndexCreationStrategy> getIndexCreationStrategy() {
        return Optional.of(new SupplierBackedIndexCreationStrategy());
    }

    /**
     * The removal half of a deleted index's descriptor-plane record -- the durable tombstone and the
     * stored-mapping prune -- as one operation core performs rather than two seams core sequences. See
     * {@link org.opensearch.serverless.storage.descriptor.DescriptorBackedIndexLifecycle} for what moved
     * into it and from where.
     *
     * <p>Returned unconditionally, for the same reason {@link #getIndexCatalog()} is: the object is
     * node-lifetime and {@code DescriptorGate} is what fills and empties underneath it. With the gate
     * uninstalled -- an ordinary node, or one with descriptor gating switched off -- there is no store to
     * tombstone into and the operation completes at once, which is exactly what an unregistered {@code
     * DurableTombstones.Writer} used to answer.
     */
    @Override
    public Optional<ClaimedIndexLifecycle> getClaimedIndexLifecycle() {
        return Optional.of(new DescriptorBackedIndexLifecycle());
    }

    /**
     * Phase E2 of {@code core-pluggability-refactor-plan.md}. See {@link ServerlessGatedIndexResidencyPolicy}'s
     * own javadoc for what this hands core and why the settings it reads live on this plugin now rather
     * than on {@code IndicesClusterStateService}.
     */
    @Override
    public Optional<IndexResidencyPolicy> getIndexResidencyPolicy() {
        return Optional.of(new ServerlessGatedIndexResidencyPolicy(settings));
    }

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders() {
        return Collections.singletonList(serverlessStorageIndexSettingProvider);
    }

    /**
     * Registers {@link ServerlessStorageRemoteClusterStateValidator} -- see that class's own
     * javadoc for why this check belongs on {@link IndexCreationValidator}, not {@link
     * IndexSettingProvider} (which is where it originally lived).
     */
    @Override
    public Collection<IndexCreationValidator> getIndexCreationValidators() {
        return java.util.List.of(
            new ServerlessStorageRemoteClusterStateValidator(),
            new ServerlessStorageRecoveryStrategyValidator(),
            new UnbackedServerlessIndexValidator(settings)
        );
    }

    /**
     * Refuses to create a serverless-storage index on a cluster that has no object store configured
     * for it -- which, because of the {@code serverless_} name prefix, is a thing an operator can do
     * by accident with nothing but a {@code PUT}.
     *
     * <p><b>What is actually wrong today</b>
     *
     * <p>{@code ServerlessStorageIndexSettingProvider} sets {@code index.serverless_storage.enabled}
     * for <em>any</em> index whose name starts with {@code serverless_}, and then injects this
     * plugin's existing-shards allocator and {@code index.recovery.strategy=object-store} to match.
     * That derivation is gated by nothing: not by {@code serverless_storage.enabled}, not by whether
     * a blob store exists, not by anything. So on a cluster that installed this plugin and
     * configured none of it, {@code PUT /serverless_foo} is accepted, and its shards then fail to
     * open with an {@code IllegalStateException} out of {@code resolveContainer}, because there is
     * no {@code serverless_storage.base_path} and no {@code serverless_storage.repository} to
     * resolve a container from.
     *
     * <p>The failure is at least loud rather than silent, which is why this is a validator and not a
     * rewrite of the derivation. But it is loud in the wrong place and to the wrong person: it
     * surfaces as a red index on a cluster whose operator chose a name, not as an error on the
     * request that chose it. And "an index name in a namespace the operator did not know was
     * reserved silently selects a storage engine" is the shape of the problem, not the missing
     * setting.
     *
     * <p><b>Why this condition and not "is the node setting on"</b>
     *
     * <p>Refusing every {@code serverless_} name unless {@code serverless_storage.enabled} is set
     * would reserve the namespace more thoroughly, and it is what a from-scratch design would do.
     * It also refuses names on clusters where the derivation is currently harmless. The condition
     * here is the narrowest one that is unambiguously true: this index has asked for object-store
     * storage -- by name or by setting, it does not matter which -- and this node cannot provide
     * any. There is no configuration in which that combination is what someone wanted.
     *
     * <p>It deliberately catches the explicit opt-in too, not just the prefix. {@code PUT /idx}
     * with {@code index.serverless_storage.enabled: true} against an unconfigured cluster fails in
     * exactly the same way, and had exactly the same excuse.
     */
    static final class UnbackedServerlessIndexValidator implements IndexCreationValidator {

        private final Settings nodeSettings;

        /**
         * @param nodeSettings the node's settings, read for the two ways a blob store can be configured.
         */
        UnbackedServerlessIndexValidator(Settings nodeSettings) {
            this.nodeSettings = nodeSettings;
        }

        /** Decided entirely from settings; the mappings this would otherwise force core to build are not needed. */
        @Override
        public boolean requiresMappings() {
            return false;
        }

        @Override
        public void validate(org.opensearch.index.mapper.MapperService mapperService, org.opensearch.index.IndexSettings indexSettings) {
            String indexName = indexSettings.getIndex().getName();
            // Both, not just the setting. The setting alone would be enough in the normal flow --
            // ServerlessStorageIndexSettingProvider derives it from the name before any validator
            // runs -- but relying on that makes this check depend on the ordering of two things that
            // are ordered by core, not by this plugin. The name is the durable fact about what the
            // operator asked for, so it is checked directly.
            boolean optedInBySetting = SERVERLESS_STORAGE_ENABLED_SETTING.get(indexSettings.getSettings());
            boolean namedServerless = org.opensearch.serverless.storage.descriptor.DescriptorOnlyCreation.namesAServerlessIndex(indexName);
            if (optedInBySetting == false && namedServerless == false) {
                return;
            }
            // The raw settings, not the resolved basePath field: a cluster manager that never
            // reached createComponents (or reached it before this setting was added to its
            // keystore/config) would report a null basePath and refuse an index a data node could
            // have served perfectly well. What is being checked is "did the operator configure a
            // store at all", and that is a question about configuration.
            boolean hasRepository = SERVERLESS_STORAGE_REPOSITORY_SETTING.get(nodeSettings).isEmpty() == false;
            boolean hasBasePath = SERVERLESS_STORAGE_BASE_PATH_SETTING.get(nodeSettings).isEmpty() == false;
            if (hasRepository || hasBasePath) {
                return;
            }
            boolean byNameAlone = namedServerless && SERVERLESS_STORAGE_ENABLED_SETTING.exists(indexSettings.getSettings()) == false;
            throw new IllegalArgumentException(
                "index ["
                    + indexName
                    + "] would use serverless object-store storage"
                    + (byNameAlone
                        ? ", selected by its name alone: any index named ["
                            + org.opensearch.serverless.storage.descriptor.DescriptorOnlyCreation.SERVERLESS_NAME_PREFIX
                            + "...] is opted in automatically"
                        : "")
                    + ", but neither ["
                    + SERVERLESS_STORAGE_REPOSITORY_SETTING.getKey()
                    + "] nor ["
                    + SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey()
                    + "] is configured on this node, so its shards would have no object store to open from and "
                    + "would fail to allocate. Configure one of those settings, or"
                    + (byNameAlone ? " choose a name outside the reserved prefix." : " create the index without that setting.")
            );
        }
    }

    /**
     * Registers {@link org.opensearch.serverless.storage.scaletozero.ShardReactivationActionFilter}
     * -- the cold-start-reactivation-on-access half of scale-to-zero (rfc-serverless-opensearch.md
     * &sect;7.3), see that class's own javadoc -- and {@link
     * org.opensearch.serverless.storage.resharding.WritePartitionRoutingActionFilter}.
     *
     * <p>No longer registers a blanket legacy-snapshot veto: writer shards now support real,
     * engine-native {@code _snapshot}/{@code _restore} directly (see {@link
     * org.opensearch.serverless.storage.writerengine.ObjectStoreWriterEngine#attemptEngineNativeSnapshot}
     * and the design writeup at {@code docs-site/src/content/docs/design/snapshot-restore-proposal.md}),
     * so the previous blanket rejection would now incorrectly block a correctly-supported feature.
     */
    @Override
    public List<org.opensearch.action.support.ActionFilter> getActionFilters() {
        return List.of(shardReactivationActionFilter, writePartitionRoutingActionFilter, affinityForwardingActionFilter);
    }

    /**
     * Registers the listener that releases a clone's source-side GC pin when the clone's own index
     * is actually deleted (rfc-serverless-opensearch.md &sect;14) -- {@code ShardCloner.deleteClone}
     * itself has existed since the clone feature landed, but nothing called it automatically until
     * this. Uses {@code afterIndexRemoved} gated on {@code IndexRemovalReason.DELETED}, not the
     * shard-level {@code afterIndexShardDeleted}: that one fires whenever a shard's local copy is
     * physically wiped from a node's disk, including plain relocation or a node simply no longer
     * hosting a copy, not just real index deletion -- using it here would call {@code deleteClone}
     * spuriously on every relocation. {@code afterIndexRemoved} fires once per node that had the
     * index open, which for a multi-node cluster still means potentially several nodes independently
     * calling {@code deleteClone} for the same clone -- safe only because {@code deleteClone} is
     * already idempotent under concurrent/redundant calls (see its own javadoc): whichever call
     * loses the race just finds the lineage already gone and no-ops.
     *
     * @param indexModule the index module being set up, whose event listeners this registers against.
     */
    @Override
    public void onIndexModule(org.opensearch.index.IndexModule indexModule) {
        indexModule.setReaderWrapper(indexService -> directoryReader -> {
            org.opensearch.common.lucene.index.OpenSearchDirectoryReader openSearchDirectoryReader =
                org.opensearch.common.lucene.index.OpenSearchDirectoryReader.getOpenSearchDirectoryReader(directoryReader);
            if (openSearchDirectoryReader == null) {
                return directoryReader;
            }
            org.opensearch.cluster.metadata.SplitShardsMetadata splitShardsMetadata = indexService.getIndexSettings()
                .getIndexMetadata()
                .getSplitShardsMetadata();
            int shardNumber = openSearchDirectoryReader.shardId().id();
            org.opensearch.cluster.metadata.ShardRange range = splitShardsMetadata.getRangeOfShard(shardNumber);
            if (range == null) {
                // Fail closed. A shard that is known to be a split child but whose hash range this
                // node cannot resolve -- a lagging cluster-state apply, or a child whose routing entry
                // outlives its SplitShardsMetadata record through a merge commit -- used to fall
                // through here and serve its ENTIRE unfiltered document set, which is the whole
                // parent's. Two siblings in that state at once return every document twice, and the
                // caller has no way to tell: the response is a successful search with wrong results.
                //
                // A hard error on one shard is strictly better. It is loud, it is scoped to the shard
                // that is actually confused, and it is self-healing -- the next cluster-state apply
                // that carries the range resolves it. Silent duplicate results are none of those.
                //
                // Note the guard is deliberately "is this a split child at all", not "is a split in
                // progress": an ordinary, never-split shard has no range either, and that is the
                // common path this must not touch.
                if (splitShardsMetadata.getParentAndRangeOfChild(shardNumber) != null) {
                    throw new IllegalStateException(
                        "shard ["
                            + shardNumber
                            + "] of index ["
                            + indexService.index().getName()
                            + "] is an in-place split child but no hash range resolves for it on this node -- "
                            + "refusing to serve its unfiltered document set"
                    );
                }
                return directoryReader;
            }
            return new org.opensearch.serverless.storage.resharding.InPlaceSplitFilteringDirectoryReader(directoryReader, range);
        });
        indexModule.addIndexEventListener(new org.opensearch.index.shard.IndexEventListener() {
            @Override
            public void afterIndexRemoved(
                org.opensearch.core.index.Index index,
                IndexSettings indexSettings,
                org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason reason
            ) {
                if (reason != org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED) {
                    return;
                }
                releaseCloneLineageForDeletedIndex(index.getUUID(), indexSettings.getNumberOfShards());
                deregisterFromWalShardRegistryForDeletedIndex(index.getUUID(), indexSettings.getNumberOfShards());
                // The index's whole local disk cache subtree, in one go. afterIndexShardDeleted
                // below already removes each shard's own directory as its data is wiped, but that
                // leaves the now-empty <root>/<uuid> parent behind, and it does not fire at all for
                // a shard this node cached for but no longer hosted at deletion time.
                deleteLocalCacheDirectoryForDeletedIndex(index.getUUID());
                // Round 006 item 6, found by the bug hunt that followed it: without this, a
                // PinLedgerSweepTask started for this index (getEngineFactory, shard 0) was only ever
                // torn down at full node close, so a workload that creates and deletes many serverless
                // indices -- the ordinary shape of serverless usage -- accumulated one live,
                // GENERIC-scheduled background task per index for the rest of the node's process
                // lifetime. Idempotent: an index whose shard 0 never opened on this node simply has no
                // entry to remove.
                //
                // Synchronized on the same lock getEngineFactory's insert and close()'s drain use --
                // found missing by round 3 of the bug hunt: this is a third mutator of
                // pinLedgerSweepTasks, and without the lock here the field's own javadoc claim ("both
                // ... synchronize on this lock so the two sequences cannot interleave") was false. Not a
                // live leak today (ConcurrentHashMap#remove and Cancellable#cancel are both safe to call
                // unsynchronized), but closing the gap keeps the invariant the javadoc states actually
                // true for every mutator, not just two of the three.
                synchronized (pinLedgerSweepTasksLock) {
                    PinLedgerSweepTask sweepTask = pinLedgerSweepTasks.remove(index.getUUID());
                    if (sweepTask != null) {
                        sweepTask.close();
                    }
                }
            }

            /**
             * Deliberately the shard-level callback this class's own javadoc argues AGAINST for
             * {@code deleteClone} -- and the argument does not carry over, because the two are
             * cleaning up opposite kinds of thing. {@code deleteClone} touches state shared across
             * the cluster (a source index's GC pin), so firing it on a mere relocation would destroy
             * something another node still depends on. This shard cache is purely node-local and
             * purely derived: it holds a copy of bytes that still exist in the object store, for a
             * shard whose local data this callback fires precisely because it has just been wiped
             * from this node's disk. A cache for data that is gone is dead weight, and if the shard
             * relocates back here later, re-fetching is the correct behaviour rather than a loss --
             * which makes "fires on relocation too" a non-issue here instead of the bug it would be
             * there. Without this, the only thing that ever removed a shard cache directory was
             * deleting its whole index.
             */
            @Override
            public void afterIndexShardDeleted(org.opensearch.core.index.shard.ShardId shardId, Settings shardIndexSettings) {
                deleteLocalCacheDirectoryForDeletedShard(shardId.getIndex().getUUID(), shardId.id());
            }

            /**
             * Cancels the node-level compaction/GC pair {@code getEngineFactory} started for this
             * shard. Deliberately the <em>closed</em> callback, not the <em>deleted</em> one above:
             * a relocating shard is closed here but its data is never deleted, and a relocated
             * shard's schedulers must stop on the node it left, not only when the index is dropped.
             * See {@link #stopShardMaintenanceTasks}.
             */
            @Override
            public void afterIndexShardClosed(
                org.opensearch.core.index.shard.ShardId shardId,
                org.opensearch.index.shard.IndexShard indexShard,
                Settings shardIndexSettings
            ) {
                stopShardMaintenanceTasks(shardId.getIndex().getUUID(), shardId.id());
            }
        });
    }

    /**
     * The node-wide local disk cache budget this node will actually enforce: the explicitly
     * configured {@link #SERVERLESS_STORAGE_LOCAL_CACHE_MAX_BYTES_SETTING} if there is one (zero
     * included, which means unbounded), otherwise {@link #deriveLocalCacheBudgetBytes}.
     *
     * @param settings the node settings to read the budget from.
     * @param cacheRoot the cache root, used only to derive a default from its filesystem.
     * @return the budget in bytes; {@code 0} means unbounded.
     */
    public static long resolveNodeWideLocalCacheBudgetBytes(Settings settings, Path cacheRoot) {
        // Setting#exists, not "is the value zero": those mean opposite things here. Unset means
        // "derive something sane, because the alternative is an unbounded cache on every untuned
        // node"; an explicit zero is an operator deliberately asking for unbounded, and must survive
        // as one. Public only so this decision is unit-testable on its own, without a node.
        if (SERVERLESS_STORAGE_LOCAL_CACHE_MAX_BYTES_SETTING.exists(settings)) {
            return SERVERLESS_STORAGE_LOCAL_CACHE_MAX_BYTES_SETTING.get(settings).getBytes();
        }
        return deriveLocalCacheBudgetBytes(cacheRoot);
    }

    /**
     * 10% of the total space of the filesystem holding {@code cacheRoot}, floored at 64MB -- the
     * derived node-wide cache budget used when {@link #SERVERLESS_STORAGE_LOCAL_CACHE_MAX_BYTES_SETTING}
     * is unset. A guess, and said to be one: it is chosen to be obviously safer than the previous
     * behaviour (no bound at all on every untuned node), not because 10% is a number anyone measured.
     * An operator with real numbers should set the setting rather than treat this as authoritative.
     *
     * <p>Returns {@code 0} -- meaning unbounded, exactly today's behaviour -- if the file store
     * cannot be queried at all, rather than failing node startup over a cache sizing heuristic. The
     * path is named in the warning so an operator can tell which filesystem refused to answer.
     */
    static long deriveLocalCacheBudgetBytes(Path cacheRoot) {
        try {
            // The root itself usually does not exist yet at createComponents time -- shard
            // directories are created at reader-shard open -- so ask the nearest existing ancestor,
            // which is on the same file store by construction (it is a node data path).
            Path probe = cacheRoot;
            while (probe != null && java.nio.file.Files.exists(probe) == false) {
                probe = probe.getParent();
            }
            if (probe == null) {
                logger.warn("could not find an existing ancestor of local cache root [{}]; leaving the disk cache unbounded", cacheRoot);
                return 0L;
            }
            // Environment#getFileStore, not Files#getFileStore: core forbids the latter outright,
            // because it is impacted by JDK-8034057 on some filesystems.
            long totalSpace = org.opensearch.env.Environment.getFileStore(probe).getTotalSpace();
            if (totalSpace <= 0) {
                logger.warn("file store for local cache root [{}] reported no total space; leaving the disk cache unbounded", cacheRoot);
                return 0L;
            }
            return Math.max(totalSpace / 10, 64L * 1024L * 1024L);
        } catch (IOException | RuntimeException e) {
            logger.warn("could not query the file store for local cache root [" + cacheRoot + "]; leaving the disk cache unbounded", e);
            return 0L;
        }
    }

    /**
     * Registers the one-shot orphan sweep: a deletion that happened while this node was down leaves
     * a {@code <root>/<uuid>} directory that no lifecycle callback on this node will ever fire for,
     * so the first cluster state this node actually trusts is the only place the comparison can be
     * made. States still carrying {@code STATE_NOT_RECOVERED_BLOCK} are skipped precisely because
     * their metadata is not yet the real one -- acting on it would delete live indices' caches --
     * and the listener removes itself the moment it has run once, since every deletion after that
     * point is covered by the two lifecycle callbacks above.
     */
    private void registerOrphanedCacheSweep() {
        final Path sweepRoot = localCacheRoot;
        clusterService.addListener(new org.opensearch.cluster.ClusterStateListener() {
            @Override
            public void clusterChanged(org.opensearch.cluster.ClusterChangedEvent event) {
                if (event.state().blocks().hasGlobalBlock(org.opensearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
                    return;
                }
                clusterService.removeListener(this);
                try {
                    java.util.Set<String> liveIndexUuids = new java.util.HashSet<>();
                    for (org.opensearch.cluster.metadata.IndexMetadata indexMetadata : event.state().metadata().indices().values()) {
                        liveIndexUuids.add(indexMetadata.getIndexUUID());
                    }
                    int removed = org.opensearch.serverless.storage.format.DiskCacheSpaceGovernor.deleteOrphanedIndexCaches(
                        sweepRoot,
                        liveIndexUuids
                    );
                    if (removed > 0) {
                        logger.info("removed [{}] orphaned local disk cache directories under [{}]", removed, sweepRoot);
                    }
                } catch (Exception e) {
                    // A cluster-state listener that throws is a node-level problem; a cache
                    // directory that survives is wasted disk the next node start retries.
                    logger.warn("failed to sweep orphaned local disk cache directories under [" + sweepRoot + "]", e);
                }
            }
        });
    }

    /**
     * Removes {@code <root>/<uuid>/<shard>} and forgets its stats entry. Best-effort in the strict
     * sense: this runs inside an {@code IndexEventListener} callback, where throwing would break a
     * shard-removal sequence that has nothing to do with a cache, so a failure is logged and the
     * directory left for the next index deletion or the orphan sweep to catch.
     */
    private void deleteLocalCacheDirectoryForDeletedShard(String indexUuid, int shardId) {
        cacheStatsRegistry.deregister(indexUuid, shardId);
        if (localCacheRoot == null) {
            return;
        }
        org.opensearch.serverless.storage.format.DiskCacheSpaceGovernor.deleteShardCache(localCacheRoot, indexUuid, shardId);
    }

    /** The index-wide counterpart of {@link #deleteLocalCacheDirectoryForDeletedShard}, with the same never-throw contract. */
    private void deleteLocalCacheDirectoryForDeletedIndex(String indexUuid) {
        cacheStatsRegistry.deregisterIndex(indexUuid);
        if (localCacheRoot == null) {
            return;
        }
        org.opensearch.serverless.storage.format.DiskCacheSpaceGovernor.deleteIndexCache(localCacheRoot, indexUuid);
    }

    /**
     * The one case {@link org.opensearch.serverless.storage.wal.WalShardRegistry}'s own javadoc
     * names as safe to actually remove an entry for: a real index deletion is independent proof
     * this shard will never mirror into this container again, unlike mere staleness or inactivity.
     * Best-effort, same shape and same swallowed-failure tolerance as {@link
     * #releaseCloneLineageForDeletedIndex} right above -- a registry entry surviving a failed
     * deregistration attempt is not a correctness problem (see that registry's own grow-only-is-
     * safe argument), only a missed opportunity for a future sweep to reclaim more.
     */
    private void deregisterFromWalShardRegistryForDeletedIndex(String indexUuid, int numberOfShards) {
        if (sharedWalChunkService == null) {
            return;
        }
        org.opensearch.serverless.storage.wal.WalShardRegistry registry = new org.opensearch.serverless.storage.wal.WalShardRegistry(
            sharedWalChunkService.blobContainer()
        );
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            try {
                registry.deregister(indexUuid, shardId);
            } catch (Exception e) {
                // See this method's own javadoc: logged-and-swallowed, not fatal to index deletion.
                logger.warn("failed to deregister WAL shard registry entry for " + indexUuid + "/" + shardId, e);
            }
        }
    }

    /**
     * Best-effort, like every other background cleanup in this plugin ({@code
     * CompactionSchedulerTask}, {@code GcSchedulerTask}): a shard that was never a clone costs one
     * cheap single-blob miss per shard and is otherwise untouched; a genuine failure here (e.g. the
     * object store being briefly unreachable) is swallowed rather than blocking index deletion
     * itself, which must proceed regardless -- see {@code ShardCloner.deleteClone}'s own
     * idempotency for why simply not running to completion here is safe to leave for a later retry
     * rather than needing one.
     */
    private void releaseCloneLineageForDeletedIndex(String indexUuid, int numberOfShards) {
        if (basePath == null) {
            return;
        }
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            try {
                BlobContainer targetContainer = resolveBlobContainer(indexUuid, shardId);
                org.opensearch.serverless.storage.clone.BlobContainerCloneLineageStore lineageStore =
                    new org.opensearch.serverless.storage.clone.BlobContainerCloneLineageStore(targetContainer);
                org.opensearch.serverless.storage.clone.ShardCloner.deleteClone(
                    indexUuid,
                    shardId,
                    lineageStore,
                    (sourceIndexUuid, sourceShardId) -> {
                        try {
                            return new org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry(
                                resolveBlobContainer(sourceIndexUuid, sourceShardId)
                            );
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    },
                    // Releases this clone's pin at every hop of the lineage, not just the first -- the
                    // symmetric half of ShardCloner#clone's ancestor pinning. Without it, a chain pinned at
                    // three hops is released at one and the other two leak forever.
                    this::resolveBlobContainer
                );
                reclaimDeletedShardBytes(indexUuid, shardId, targetContainer);
            } catch (Exception e) {
                // See this method's own javadoc: logged-and-swallowed, not fatal to index deletion.
                logger.warn("failed to release clone lineage/pin for deleted index " + indexUuid + "/" + shardId, e);
            }
        }
    }

    /**
     * Actually reclaims a deleted shard's object-store bytes, gated behind
     * {@link #SERVERLESS_STORAGE_RECLAIM_ON_INDEX_DELETE_SETTING}.
     *
     * <p><b>Why this is needed at all</b>
     *
     * <p>Deleting a serverless index reclaimed <b>zero</b> object-store bytes -- ever, in any
     * configuration. GC is per shard and runs from a scheduler attached to a live shard, so the
     * moment the index is gone there is nothing left that could ever sweep its prefix. Every
     * manifest, every bundle and every register a deleted index ever wrote stayed in the bucket
     * permanently, and an index-per-tenant fleet -- the shape this whole design exists to make
     * possible -- is exactly the workload that creates and deletes indices constantly.
     *
     * <p>{@code DeletedShardReclaimer} refuses whenever a <em>live</em> pin names the shard. That is
     * the whole safety argument, and it is stronger than it looks: a clone pins every hop of its
     * lineage, a snapshot pins what it names, and PITR pins its window, so "no live pin" is exactly
     * "nothing outside this shard depends on these bytes" -- without scanning every other index's
     * lineage to find out.
     *
     * <p><b>Why it is off by default for now</b>
     *
     * <p>This is the first code path in the plugin that deletes a whole shard prefix in one call,
     * and it is reached from an index-deletion callback where the operator's intent is already
     * irreversible. Those two facts together are why it ships behind a flag for one release rather
     * than on: a bug here does not lose the bytes of the index being deleted (they were meant to
     * go), it loses the bytes of whatever the pin check got wrong about -- a clone source, a
     * snapshot -- and that is silent until someone tries to read it. A release of real-world
     * exercise with the flag on, in deployments that care more about the bill than the blast
     * radius, is cheap insurance for flipping the default afterwards.
     *
     * <p>Failures are logged and swallowed by the caller, which is the right tolerance: leaving
     * bytes behind costs money, and failing an index deletion costs availability.
     *
     * @param indexUuid the deleted index's UUID.
     * @param shardId the shard within it.
     * @param shardContainer that shard's own container, already resolved by the caller.
     */
    private void reclaimDeletedShardBytes(String indexUuid, int shardId, BlobContainer shardContainer) throws IOException {
        if (reclaimOnIndexDelete == false) {
            return;
        }
        org.opensearch.serverless.storage.gc.DeletedShardReclaimer.Outcome outcome =
            org.opensearch.serverless.storage.gc.DeletedShardReclaimer.reclaimShard(
                indexUuid,
                shardId,
                shardContainer,
                System.currentTimeMillis()
            );
        // Logged at info, not debug: "we did not reclaim this shard because something still pins it"
        // is the one outcome an operator chasing an unexpected storage bill needs to be able to find,
        // and it is rare enough that it will not be noise.
        logger.info("reclaim of deleted shard [{}][{}] finished with outcome [{}]", indexUuid, shardId, outcome);
    }

    /**
     * The user-facing REST/transport surface this plugin exposes: zero-copy clone
     * (rfc-serverless-opensearch.md &sect;14, {@code ShardCloner} itself existed since the clone
     * feature landed but was reachable only from Java code within the plugin until now) and an
     * on-demand compaction trigger (&sect;7.4, &sect;16 Phase 4.5's "narrower than originally
     * scoped" gap -- {@code CompactionSchedulerTask}'s own background schedule already existed;
     * this adds a way to trigger the same check immediately rather than waiting it out), and a
     * writer-shard idle-time query (&sect;16 Phase 4's "suspended writers, scale-to-zero/cold-start"
     * milestone -- the first real consumer of {@code ObjectStoreWriterEngine#millisSinceLastActivity()}
     * outside the engine itself), and a snapshot pin/release pair (&sect;14's "snapshot = pinned
     * manifest set" -- until now only the underlying {@code DurablePinRegistry} mechanism existed,
     * used by PITR retention and clone, with no way for an operator to actually pin a shard's
     * current manifest generation under a snapshot name).
     */
    @Override
    public
        java.util.List<ActionHandler<? extends org.opensearch.action.ActionRequest, ? extends org.opensearch.core.action.ActionResponse>>
        getActions() {
        return java.util.List.of(
            new ActionHandler<>(
                org.opensearch.serverless.storage.clone.action.ShardCloneAction.INSTANCE,
                org.opensearch.serverless.storage.clone.action.TransportShardCloneAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.nodecapacity.action.NodeCapacityAction.INSTANCE,
                org.opensearch.serverless.storage.nodecapacity.action.TransportNodeCapacityAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.nodecapacity.action.NodeDrainAction.INSTANCE,
                org.opensearch.serverless.storage.nodecapacity.action.TransportNodeDrainAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.nodecapacity.action.NodeWarmupAction.INSTANCE,
                org.opensearch.serverless.storage.nodecapacity.action.TransportNodeWarmupAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.compaction.action.CompactionTriggerAction.INSTANCE,
                org.opensearch.serverless.storage.compaction.action.TransportCompactionTriggerAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.writerengine.action.ShardIdleTimeAction.INSTANCE,
                org.opensearch.serverless.storage.writerengine.action.TransportShardIdleTimeAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.writerengine.action.NodeIdleShardsAction.INSTANCE,
                org.opensearch.serverless.storage.writerengine.action.TransportNodeIdleShardsAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.writerengine.action.RealtimeGetAction.INSTANCE,
                org.opensearch.serverless.storage.writerengine.action.TransportRealtimeGetAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.readerengine.action.NodeManifestLagAction.INSTANCE,
                org.opensearch.serverless.storage.readerengine.action.TransportNodeManifestLagAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.format.action.NodeCacheStatsAction.INSTANCE,
                org.opensearch.serverless.storage.format.action.TransportNodeCacheStatsAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.security.action.NodeObjectStoreRequestStatsAction.INSTANCE,
                org.opensearch.serverless.storage.security.action.TransportNodeObjectStoreRequestStatsAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.wal.action.NodeWalBacklogAction.INSTANCE,
                org.opensearch.serverless.storage.wal.action.TransportNodeWalBacklogAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.readerengine.action.WaitForGenerationAction.INSTANCE,
                org.opensearch.serverless.storage.readerengine.action.TransportWaitForGenerationAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.readerengine.action.PollNowAction.INSTANCE,
                org.opensearch.serverless.storage.readerengine.action.TransportPollNowAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidatesAction.INSTANCE,
                org.opensearch.serverless.storage.scaletozero.action.TransportScaleToZeroCandidatesAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.scaletozero.action.ReactivateShardsAction.INSTANCE,
                org.opensearch.serverless.storage.scaletozero.action.TransportReactivateShardsAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidatesAction.INSTANCE,
                org.opensearch.serverless.storage.scaleup.action.TransportScaleUpCandidatesAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.retention.action.SnapshotPinAction.INSTANCE,
                org.opensearch.serverless.storage.retention.action.TransportSnapshotPinAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.retention.action.SnapshotReleaseAction.INSTANCE,
                org.opensearch.serverless.storage.retention.action.TransportSnapshotReleaseAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.retention.action.SnapshotRestoreAction.INSTANCE,
                org.opensearch.serverless.storage.retention.action.TransportSnapshotRestoreAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.retention.action.IndexSnapshotPinAction.INSTANCE,
                org.opensearch.serverless.storage.retention.action.TransportIndexSnapshotPinAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.retention.action.IndexSnapshotReleaseAction.INSTANCE,
                org.opensearch.serverless.storage.retention.action.TransportIndexSnapshotReleaseAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.retention.action.IndexSnapshotRestoreAction.INSTANCE,
                org.opensearch.serverless.storage.retention.action.TransportIndexSnapshotRestoreAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.retention.action.ShardRetentionStatsAction.INSTANCE,
                org.opensearch.serverless.storage.retention.action.TransportShardRetentionStatsAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.resharding.action.ShardSplitAction.INSTANCE,
                org.opensearch.serverless.storage.resharding.action.TransportShardSplitAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.resharding.action.ShardPartitionRewriteAction.INSTANCE,
                org.opensearch.serverless.storage.resharding.action.TransportShardPartitionRewriteAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.resharding.action.ShardShrinkAction.INSTANCE,
                org.opensearch.serverless.storage.resharding.action.TransportShardShrinkAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesAction.INSTANCE,
                org.opensearch.serverless.storage.resharding.action.TransportShardSplitCandidatesAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.migration.action.MigrateShardAction.INSTANCE,
                org.opensearch.serverless.storage.migration.action.TransportMigrateShardAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.resharding.action.RetireShrinkSourceAction.INSTANCE,
                org.opensearch.serverless.storage.resharding.action.TransportRetireShrinkSourceAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.resharding.action.CutoverSplitRoutingAction.INSTANCE,
                org.opensearch.serverless.storage.resharding.action.TransportCutoverSplitRoutingAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.resharding.action.EnableWritePartitionRoutingAction.INSTANCE,
                org.opensearch.serverless.storage.resharding.action.TransportEnableWritePartitionRoutingAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.resharding.action.FenceSplitSourceAction.INSTANCE,
                org.opensearch.serverless.storage.resharding.action.TransportFenceSplitSourceAction.class
            ),
            // The escape hatch for the fence directly above, and the reason it is not optional: a
            // source fence is a cluster-state-durable write block on real data, and until this action
            // existed the only way out of one was deleting the index. Orchestrated split used to fence
            // by default, so a cluster can already be carrying an index that is unwritable and has no
            // recovery path -- this DELETE is that path, on the same route the POST fence uses.
            new ActionHandler<>(
                org.opensearch.serverless.storage.resharding.action.UnfenceSplitSourceAction.INSTANCE,
                org.opensearch.serverless.storage.resharding.action.TransportUnfenceSplitSourceAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.resharding.action.CancelInPlaceSplitAction.INSTANCE,
                org.opensearch.serverless.storage.resharding.action.TransportCancelInPlaceSplitAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.resharding.action.DisableWritePartitionRoutingAction.INSTANCE,
                org.opensearch.serverless.storage.resharding.action.TransportDisableWritePartitionRoutingAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.resharding.action.ProvisionSplitTargetsAction.INSTANCE,
                org.opensearch.serverless.storage.resharding.action.TransportProvisionSplitTargetsAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.resharding.action.OrchestrateShardSplitAction.INSTANCE,
                org.opensearch.serverless.storage.resharding.action.TransportOrchestrateShardSplitAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.deepsnapshot.action.ShardDeepSnapshotAction.INSTANCE,
                org.opensearch.serverless.storage.deepsnapshot.action.TransportShardDeepSnapshotAction.class
            ),
            new ActionHandler<>(
                org.opensearch.serverless.storage.deepsnapshot.action.IndexDeepSnapshotAction.INSTANCE,
                org.opensearch.serverless.storage.deepsnapshot.action.TransportIndexDeepSnapshotAction.class
            )
        );
    }

    /**
     * Supplies the serverless REST gate, or nothing at all when gating is off.
     *
     * <p>Returning {@code null} rather than an identity wrapper matters: {@code RestController} already
     * substitutes a passthrough when no plugin supplies one, and {@code ActionModule} rejects a second
     * plugin trying to install a wrapper. Handing back an identity function when this node is not gating
     * would consume that single slot and stop any other plugin from wrapping handlers, for no benefit.
     */
    @Override
    public java.util.function.UnaryOperator<org.opensearch.rest.RestHandler> getRestHandlerWrapper(
        org.opensearch.common.util.concurrent.ThreadContext threadContext,
        java.util.Set<org.opensearch.rest.RestHeaderDefinition> headersToCopy
    ) {
        if (SERVERLESS_STORAGE_REST_GATING_ENABLED_SETTING.get(settings) == false) {
            return null;
        }
        return new ServerlessRestGate();
    }

    @Override
    public java.util.List<org.opensearch.rest.RestHandler> getRestHandlers(
        Settings settings,
        org.opensearch.rest.RestController restController,
        ClusterSettings clusterSettings,
        org.opensearch.common.settings.IndexScopedSettings indexScopedSettings,
        org.opensearch.common.settings.SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        java.util.function.Supplier<org.opensearch.cluster.node.DiscoveryNodes> nodesInCluster
    ) {
        return java.util.List.of(
            new org.opensearch.serverless.storage.clone.action.RestShardCloneAction(),
            new org.opensearch.serverless.storage.compaction.action.RestCompactionTriggerAction(),
            new org.opensearch.serverless.storage.writerengine.action.RestShardIdleTimeAction(),
            new org.opensearch.serverless.storage.writerengine.action.RestNodeIdleShardsAction(),
            new org.opensearch.serverless.storage.writerengine.action.RestRealtimeGetAction(),
            new org.opensearch.serverless.storage.readerengine.action.RestNodeManifestLagAction(),
            new org.opensearch.serverless.storage.format.action.RestNodeCacheStatsAction(),
            new org.opensearch.serverless.storage.security.action.RestNodeObjectStoreRequestStatsAction(),
            new org.opensearch.serverless.storage.wal.action.RestNodeWalBacklogAction(),
            new org.opensearch.serverless.storage.readerengine.action.RestWaitForGenerationAction(),
            new org.opensearch.serverless.storage.readerengine.action.RestPollNowAction(),
            new org.opensearch.serverless.storage.nodecapacity.action.RestNodeCapacityAction(),
            new org.opensearch.serverless.storage.nodecapacity.action.RestNodeDrainAction(),
            new org.opensearch.serverless.storage.nodecapacity.action.RestNodeWarmupAction(),
            new org.opensearch.serverless.storage.scaletozero.action.RestScaleToZeroCandidatesAction(),
            new org.opensearch.serverless.storage.scaletozero.action.RestReactivateShardsAction(),
            new org.opensearch.serverless.storage.scaleup.action.RestScaleUpCandidatesAction(),
            new org.opensearch.serverless.storage.retention.action.RestShardRetentionStatsAction(),
            new org.opensearch.serverless.storage.resharding.action.RestShardSplitAction(),
            new org.opensearch.serverless.storage.resharding.action.RestShardPartitionRewriteAction(),
            new org.opensearch.serverless.storage.resharding.action.RestShardShrinkAction(),
            new org.opensearch.serverless.storage.resharding.action.RestShardSplitCandidatesAction(),
            new org.opensearch.serverless.storage.retention.action.RestSnapshotPinAction(),
            new org.opensearch.serverless.storage.retention.action.RestSnapshotReleaseAction(),
            new org.opensearch.serverless.storage.retention.action.RestSnapshotRestoreAction(),
            new org.opensearch.serverless.storage.retention.action.RestIndexSnapshotPinAction(),
            new org.opensearch.serverless.storage.retention.action.RestIndexSnapshotReleaseAction(),
            new org.opensearch.serverless.storage.retention.action.RestIndexSnapshotRestoreAction(),
            new org.opensearch.serverless.storage.migration.action.RestMigrateShardAction(),
            new org.opensearch.serverless.storage.resharding.action.RestRetireShrinkSourceAction(),
            new org.opensearch.serverless.storage.resharding.action.RestCutoverSplitRoutingAction(),
            new org.opensearch.serverless.storage.resharding.action.RestEnableWritePartitionRoutingAction(),
            new org.opensearch.serverless.storage.resharding.action.RestDisableWritePartitionRoutingAction(),
            new org.opensearch.serverless.storage.resharding.action.RestFenceSplitSourceAction(),
            new org.opensearch.serverless.storage.resharding.action.RestUnfenceSplitSourceAction(),
            new org.opensearch.serverless.storage.resharding.action.RestCancelInPlaceSplitAction(),
            new org.opensearch.serverless.storage.resharding.action.RestProvisionSplitTargetsAction(),
            new org.opensearch.serverless.storage.resharding.action.RestOrchestrateShardSplitAction(),
            new org.opensearch.serverless.storage.deepsnapshot.action.RestIndexDeepSnapshotAction(),
            new org.opensearch.serverless.storage.deepsnapshot.action.RestShardDeepSnapshotAction()
        );
    }

    /**
     * The node-shared in-memory bundle cache {@link #createComponents} built -- {@code null} until
     * that runs, and always {@code null} on a node with no {@code localCacheRoot} configured, since
     * the cache is only ever built for reader shards; public since {@code
     * TransportNodeCacheStatsAction}, not just tests, needs to reach it via {@code @Inject}.
     */
    public InMemoryPlaintextBundleCache sharedBundleCache() {
        return sharedBundleCache;
    }

    /**
     * The node-shared registry every reader shard's local disk cache on this node registers itself
     * into -- used by {@code TransportNodeCacheStatsAction} to answer cache hit-rate/cold-read-
     * latency queries; public for the same reason as {@link #shardActivityRegistry()}.
     */
    public org.opensearch.serverless.storage.format.CacheStatsRegistry cacheStatsRegistry() {
        return cacheStatsRegistry;
    }

    /**
     * The node-wide tally every real container this plugin resolves is wrapped against -- used by
     * {@code TransportNodeObjectStoreRequestStatsAction} to answer request-count queries; public
     * for the same reason as {@link #shardActivityRegistry()}.
     */
    public ObjectStoreRequestCounter objectStoreRequestCounter() {
        return requestCounter;
    }

    /**
     * The node-shared registry every writer shard's engine on this node registers itself into --
     * used by {@code TransportShardIdleTimeAction} to answer idle-time queries; public (unlike
     * this class's other {@code *ForTesting} accessors) since a real transport action, not just
     * tests, needs to reach it via {@code @Inject}.
     */
    public org.opensearch.serverless.storage.writerengine.ShardActivityRegistry shardActivityRegistry() {
        return shardActivityRegistry;
    }

    /**
     * The node-shared registry every reader shard's engine on this node registers itself into --
     * used by {@code TransportNodeManifestLagAction} to answer manifest-generation-lag queries;
     * public for the same reason as {@link #shardActivityRegistry()}.
     */
    public org.opensearch.serverless.storage.readerengine.ReaderShardActivityRegistry readerShardActivityRegistry() {
        return readerShardActivityRegistry;
    }

    /**
     * Captures this node's {@link TransportService} -- called exactly once, by {@link
     * org.opensearch.serverless.storage.readerengine.action.TransportPollNowAction}'s own
     * constructor, since {@link #createComponents} has no {@code TransportService} parameter to
     * capture it from directly. See that action's javadoc for why it's the seam that does this.
     *
     * @param transportService this node's transport service.
     */
    public void setTransportService(TransportService transportService) {
        this.transportService = transportService;
    }

    /**
     * Reads back what {@link #setTransportService} captured, or {@code null} before that has run --
     * for {@link org.opensearch.serverless.storage.resharding.AffinityForwardingActionFilter}, the
     * second consumer of this capture. Returns the live value on every call rather than snapshotting
     * once, since a caller wired before {@link #setTransportService} runs (every {@code ActionFilter}
     * is) needs to see it become non-null once node startup finishes populating it.
     */
    public TransportService getTransportService() {
        return transportService;
    }

    /**
     * Captures this node's {@link org.opensearch.indices.cluster.IndicesClusterStateService} -- same
     * seam as {@link #setTransportService}, called from the same constructor, for the same reason: no
     * parameter for it on {@link #createComponents}.
     *
     * @param indicesClusterStateService this node's indices cluster state service.
     */
    public void setIndicesClusterStateService(org.opensearch.indices.cluster.IndicesClusterStateService indicesClusterStateService) {
        this.indicesClusterStateService = indicesClusterStateService;
    }

    /**
     * Every gated index this node currently holds shards for, or empty before {@link
     * #setIndicesClusterStateService} has run (mirrors {@link #getTransportService}'s own "empty/null
     * until real node bootstrap reaches this seam" contract). {@link
     * org.opensearch.serverless.storage.placement.ReaderShardPreWarmCoordinator}'s only caller.
     */
    public List<org.opensearch.indices.cluster.IndicesClusterStateService.OnDemandOpenIndex> onDemandOpenGatedIndices() {
        org.opensearch.indices.cluster.IndicesClusterStateService service = indicesClusterStateService;
        return service == null ? List.of() : service.onDemandOpenIndices();
    }

    /**
     * Name-level companion to {@link #onDemandOpenGatedIndices()}, with the same "empty until
     * {@link #setIndicesClusterStateService} has run" contract. {@link
     * org.opensearch.serverless.storage.placement.GatedIndexPrewarmer}'s gated-index supplier.
     */
    public List<String> onDemandOpenGatedIndexNames() {
        org.opensearch.indices.cluster.IndicesClusterStateService service = indicesClusterStateService;
        return service == null ? List.of() : service.onDemandOpenIndexNames();
    }

    /**
     * Builds a fresh {@link WriterPublicationNotifier} for {@link #getEngineFactory} to hand to a
     * produced {@code WriterEngineFactory}, or {@code null} if {@link #setTransportService} hasn't
     * run yet -- in practice this never happens for a real writer shard (every registered transport
     * action, including {@code TransportPollNowAction}, is constructed as an eager Guice singleton
     * well before any shard exists), but a test building a {@code WriterEngineFactory} against a
     * bare plugin instance that never went through real node bootstrap gets a clean {@code null}
     * (disabling notification, same shape as every other optional feature in this plugin) instead
     * of a {@link NullPointerException}.
     */
    public WriterPublicationNotifier writerPublicationNotifierForWriterEngine() {
        TransportService currentTransportService = transportService;
        if (currentTransportService == null) {
            return null;
        }
        return new WriterPublicationNotifier(nodeSettings, currentTransportService, clusterService);
    }

    /**
     * This node's currently configured default idle-time threshold (millis) for {@code
     * ScaleToZeroCandidatesAction} -- see {@link #SERVERLESS_STORAGE_SCALE_TO_ZERO_IDLE_THRESHOLD_SETTING}.
     */
    public long scaleToZeroIdleThresholdMillis() {
        return scaleToZeroIdleThresholdMillis;
    }

    /**
     * This node's currently configured default manifest-generation-lag threshold for {@code
     * ScaleToZeroCandidatesAction} -- see {@link #SERVERLESS_STORAGE_SCALE_TO_ZERO_LAG_THRESHOLD_SETTING}.
     */
    public long scaleToZeroLagThreshold() {
        return scaleToZeroLagThreshold;
    }

    /**
     * This node's currently configured default queries-per-minute threshold for {@code
     * ScaleUpCandidatesAction} -- see {@link #SERVERLESS_STORAGE_SCALE_UP_QPM_THRESHOLD_SETTING}.
     */
    public long scaleUpQpmThreshold() {
        return scaleUpQpmThreshold;
    }

    /**
     * This node's currently configured default max search-replica cap for {@code
     * ScaleUpCandidatesAction} -- see {@link #SERVERLESS_STORAGE_SCALE_UP_MAX_SEARCH_REPLICAS_SETTING}.
     */
    public int scaleUpMaxSearchReplicas() {
        return scaleUpMaxSearchReplicas;
    }

    /**
     * This node's currently configured default writes-per-minute threshold for {@code
     * ShardSplitCandidatesAction} -- see {@link #SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_WPM_THRESHOLD_SETTING}.
     */
    public long reshardingSplitCandidateWritesPerMinuteThreshold() {
        return reshardingSplitCandidateWritesPerMinuteThreshold;
    }

    /**
     * This node's currently configured default size-in-bytes threshold for {@code
     * ShardSplitCandidatesAction} -- see {@link #SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_SIZE_THRESHOLD_BYTES_SETTING}.
     */
    public long reshardingSplitCandidateSizeThresholdBytes() {
        return reshardingSplitCandidateSizeThresholdBytes;
    }

    /**
     * This node's currently configured PITR window (millis), or a non-positive value if PITR
     * retention is disabled -- see {@link #SERVERLESS_STORAGE_PITR_WINDOW_SETTING}. Used by {@code
     * TransportShardRetentionStatsAction} to report the configured window alongside real pin/manifest counts.
     */
    public long pitrWindowMillis() {
        return pitrWindowMillis;
    }

    /**
     * This node's currently configured GC retention window (millis) -- see {@link
     * #SERVERLESS_STORAGE_GC_RETENTION_WINDOW_SETTING}. Used by {@code TransportShardRetentionStatsAction}
     * to report the configured window alongside real manifest/bundle deletability counts.
     */
    public long gcRetentionWindowMillis() {
        return gcRetentionWindowMillis;
    }

    /**
     * Releases this node's node-level (cluster-manager-only, one-instance-per-node -- not per-shard,
     * which is each engine's own responsibility) background schedulers on plugin shutdown: {@link
     * #walGcSchedulerTask}, {@link #scaleToZeroCandidatesSchedulerTask}, {@link
     * #scaleUpCandidatesSchedulerTask}, {@link #dataStreamShardCountAdvisorSchedulerTask}, {@link
     * #inPlaceSplitTriggerSchedulerTask}, and {@link #inPlaceMergeTriggerSchedulerTask} -- until this
     * override existed the plugin had no {@code close()} at all, so every one of these live scheduled
     * tasks was simply left running with nothing to cancel it. Harmless for correctness on a real node
     * shutdown (the thread pool they run on is torn down shortly after regardless), but a real
     * resource leak in any path that constructs and closes multiple plugin instances in one JVM (e.g.
     * integration tests, or a hot-reload of node modules) -- each leaked task keeps firing against a
     * stale {@code ClusterService}/coordinator, and two live instances of the same scheduler can
     * double-evaluate the same shards.
     *
     * <p>Deliberately does <em>not</em> force a final drain of the shared WAL group-commit processor:
     * under {@code index.translog.durability=REQUEST} a write's WAL upload has already completed
     * before that write was acknowledged (that is exactly what {@code WalMirroringTranslog#ensureSynced}
     * waits for), so no acknowledged operation is ever stranded in the queue at shutdown; under
     * {@code ASYNC}, an unsynced tail may be dropped on shutdown, which is precisely the
     * durability-for-latency trade {@code ASYNC} already makes for core's own translog and is not
     * weakened here. {@link org.opensearch.common.util.concurrent.BufferedAsyncIOProcessor} also
     * exposes no cross-package synchronous-drain seam (its queue-drain internals are package-private),
     * so a forced drain could not be implemented cleanly regardless, and correctness does not require
     * one -- see this method's design note in the WAL section of {@code dynamic-partitioning-progress.md}.
     */
    @Override
    public void close() throws IOException {
        // Static registries outlive the node that installed them, so a node closing without clearing them
        // leaves a dead Client answering resolution for whatever runs next. In a test JVM that is every
        // subsequent suite; in production it is a restart inheriting a closed node's seams. Computed
        // placement had the same leak and never cleared itself either, which is why both are here.
        //
        // One node's claim rather than everyone's. Clearing outright disarmed gating for every other node
        // still running in the same JVM, which InternalTestCluster always has several of. That is not only a
        // test problem: GatedIndexResidency.heldOnDemand reads whether a descriptor supplier is
        // registered to decide whether it is holding an index on demand, so an unbalanced uninstall can make
        // a live node stop recognising gated indices it is currently serving.
        org.opensearch.serverless.storage.descriptor.DescriptorGate.uninstallOneNode();
        org.opensearch.serverless.storage.placement.ComputedPlacementGate.uninstallOneNode();
        org.opensearch.serverless.storage.scaletozero.GatedShardSuspensionRegistry suspensions = gatedShardSuspensions;
        if (suspensions != null) {
            suspensions.uninstall();
        }

        org.opensearch.serverless.storage.wal.WalGcSchedulerTask walGcTask = walGcSchedulerTask;
        if (walGcTask != null) {
            walGcTask.close();
        }
        org.opensearch.serverless.storage.scaletozero.ScaleToZeroCandidatesSchedulerTask scaleToZeroTask =
            scaleToZeroCandidatesSchedulerTask;
        if (scaleToZeroTask != null) {
            scaleToZeroTask.close();
        }
        org.opensearch.serverless.storage.scaleup.ScaleUpCandidatesSchedulerTask scaleUpTask = scaleUpCandidatesSchedulerTask;
        if (scaleUpTask != null) {
            scaleUpTask.close();
        }
        org.opensearch.serverless.storage.resharding.DataStreamShardCountAdvisorSchedulerTask shardCountAdvisorTask =
            dataStreamShardCountAdvisorSchedulerTask;
        if (shardCountAdvisorTask != null) {
            shardCountAdvisorTask.close();
        }
        org.opensearch.serverless.storage.resharding.InPlaceSplitTriggerSchedulerTask splitTriggerTask = inPlaceSplitTriggerSchedulerTask;
        if (splitTriggerTask != null) {
            splitTriggerTask.close();
        }
        // Round 006 item 6. One entry per index whose shard 0 opened on this node; every entry
        // cancels its own scheduled task, the same as every other *SchedulerTask closed just above.
        // Setting the closed flag and draining happen under the same lock getEngineFactory's
        // check-then-insert uses, so a concurrent shard-0 open cannot slip a task in after the drain.
        synchronized (shardMaintenanceTasksLock) {
            shardMaintenanceTasksClosed = true;
            shardMaintenanceTasks.values().forEach(ShardMaintenanceTasks::close);
            shardMaintenanceTasks.clear();
        }
        synchronized (pinLedgerSweepTasksLock) {
            pinLedgerSweepTasksClosed = true;
            pinLedgerSweepTasks.values().forEach(PinLedgerSweepTask::close);
            pinLedgerSweepTasks.clear();
        }
        org.opensearch.serverless.storage.resharding.InPlaceMergeTriggerSchedulerTask mergeTriggerTask = inPlaceMergeTriggerSchedulerTask;
        if (mergeTriggerTask != null) {
            mergeTriggerTask.close();
        }
        org.opensearch.serverless.storage.nodecapacity.NodeCapacitySignalService nodeCapacityTask = nodeCapacitySignalService;
        if (nodeCapacityTask != null) {
            nodeCapacityTask.close();
        }
        org.opensearch.serverless.storage.nodecapacity.NodeSelfWarmupSchedulerTask selfWarmupTask = nodeSelfWarmupSchedulerTask;
        if (selfWarmupTask != null) {
            selfWarmupTask.close();
        }
    }

    /**
     * This node's {@code NodeCapacitySignalService}, or {@code null} if {@link
     * #SERVERLESS_STORAGE_NODE_CAPACITY_EVAL_INTERVAL_SETTING} is non-positive (the default) --
     * {@code TransportNodeCapacityAction} reads this and reports an empty signal when {@code null}.
     */
    public org.opensearch.serverless.storage.nodecapacity.NodeCapacitySignalService nodeCapacitySignalService() {
        return nodeCapacitySignalService;
    }

    /**
     * This node's {@code NodeSelfWarmupSchedulerTask}, or {@code null} if {@link
     * #SERVERLESS_STORAGE_NODE_SELF_WARMUP_EVAL_INTERVAL_SETTING} is non-positive (the default) --
     * test-only visibility.
     */
    public org.opensearch.serverless.storage.nodecapacity.NodeSelfWarmupSchedulerTask nodeSelfWarmupSchedulerTaskForTesting() {
        return nodeSelfWarmupSchedulerTask;
    }

    /**
     * The node-shared WAL chunk service, or {@code null} if WAL mirroring is off <em>or</em> no
     * writer shard on this node has triggered {@link #resolveSharedWalChunkService()} yet -- see
     * that method's own javadoc for why construction is now deferred there rather than happening
     * unconditionally in {@code createComponents}. Deliberately a raw, non-resolving field read,
     * not a call to {@link #resolveSharedWalChunkService()}: {@code TransportNodeWalBacklogAction}
     * documents itself as doing "no I/O, no dispatch needed," and this getter is its only dependency
     * -- forcing real container resolution (and its real I/O failure modes) from a stats-reporting
     * call would break that contract. {@code null} here honestly means "nothing has ever gone
     * through the shared WAL container on this node," which is exactly the state a genuine zero
     * backlog should report anyway. Public since {@code TransportNodeWalBacklogAction}, not just
     * tests, needs to reach it via {@code @Inject}.
     */
    public WalChunkService sharedWalChunkService() {
        return sharedWalChunkService;
    }

    /** The WAL GC scheduler task {@link #createComponents} built, or {@code null} if disabled -- test-only visibility. */
    org.opensearch.serverless.storage.wal.WalGcSchedulerTask walGcSchedulerTaskForTesting() {
        return walGcSchedulerTask;
    }

    /** The scale-to-zero candidate scheduler task {@link #createComponents} built, or {@code null} if disabled -- test-only visibility. */
    org.opensearch.serverless.storage.scaletozero.ScaleToZeroCandidatesSchedulerTask scaleToZeroCandidatesSchedulerTaskForTesting() {
        return scaleToZeroCandidatesSchedulerTask;
    }

    /** The scale-up candidate scheduler task {@link #createComponents} built, or {@code null} if disabled -- test-only visibility. */
    org.opensearch.serverless.storage.scaleup.ScaleUpCandidatesSchedulerTask scaleUpCandidatesSchedulerTaskForTesting() {
        return scaleUpCandidatesSchedulerTask;
    }

    /** The data-stream shard-count advisor scheduler task {@link #createComponents} built, or {@code null} if disabled -- test-only visibility. */
    org.opensearch.serverless.storage.resharding.DataStreamShardCountAdvisorSchedulerTask
        dataStreamShardCountAdvisorSchedulerTaskForTesting() {
        return dataStreamShardCountAdvisorSchedulerTask;
    }

    /**
     * The shared cache {@link ServerlessStorageIndexSettingProvider} reads from, always
     * non-{@code null} regardless of whether the scheduler task itself is enabled -- test-only
     * visibility, lets a test seed a recommendation directly without needing a real, sustained
     * write-load-driven evaluation to actually occur.
     */
    org.opensearch.serverless.storage.resharding.DataStreamShardCountAdvisorCache dataStreamShardCountAdvisorCacheForTesting() {
        return dataStreamShardCountAdvisorCache;
    }

    /** The reader-shard admission controller {@link #createComponents} built, or {@code null} if disabled -- test-only visibility. */
    ReaderShardAdmissionController readerShardAdmissionControllerForTesting() {
        return readerShardAdmissionController;
    }

    /**
     * The {@link PinLedgerSweepTask} {@link
     * #getEngineFactory} started for this index uuid, or {@code null} if none was -- test-only
     * visibility. Whether one exists at all is the assertion the bug hunt's regression tests need:
     * neither a null-{@code shardRouting} administrative call nor a delete-denied container is
     * something a test can otherwise observe from outside this class.
     */
    PinLedgerSweepTask pinLedgerSweepTaskForTesting(String indexUuid) {
        return pinLedgerSweepTasks.get(indexUuid);
    }
}
