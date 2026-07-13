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
import org.opensearch.index.IndexCreationValidator;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.shard.IndexSettingProvider;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
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

    /** Creates the plugin; all real wiring happens in {@link #createComponents} once node services are available. */
    public ServerlessStoragePlugin() {}

    /**
     * Constructed eagerly (not in {@link #createComponents}) because {@link #getActionFilters()} is
     * called before {@code createComponents} runs -- see the filter's own javadoc for why it takes
     * its {@code ClusterService} via a late setter instead of its constructor.
     */
    private final org.opensearch.serverless.storage.scaletozero.ShardReactivationActionFilter shardReactivationActionFilter =
        new org.opensearch.serverless.storage.scaletozero.ShardReactivationActionFilter();

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
     * How often the node-level {@code WalGcSchedulerTask} sweeps for WAL chunks safe to delete
     * (rfc-serverless-opensearch.md &sect;6.4's own status note on this gap). Unlike {@link
     * #SERVERLESS_STORAGE_GC_INTERVAL_SETTING}'s sweep, this one needs no separate time-based
     * retention window setting -- see that task's own javadoc for why the minimum covered {@code
     * WalPosition} across every {@code WalShardRegistry}-known shard is already an airtight bound
     * on its own. Non-positive (the default) disables it, and only takes effect when WAL mirroring
     * itself is also enabled -- there is nothing to sweep otherwise.
     */
    public static final Setting<TimeValue> SERVERLESS_STORAGE_WAL_GC_INTERVAL_SETTING = Setting.timeSetting(
        "serverless_storage.wal_gc.interval",
        TimeValue.MINUS_ONE,
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
    // Resolved once in createComponents, same "read the NodeScope setting where Environment is
    // actually available" reasoning as every other field in this group -- getEngineFactory reads
    // this to configure each reader shard's own LocalDiskCachingBundleStore eviction budget.
    private volatile long localCacheMaxBytesPerShard;
    private volatile ThreadPool threadPool;
    private volatile FileCache lazyDirectoryFileCache;
    private volatile EncryptionKeyProvider encryptionKeyProvider;
    private volatile String localNodeId = "unknown-node";
    private volatile InMemoryPlaintextBundleCache sharedBundleCache;
    private volatile long pitrWindowMillis = -1;
    private volatile ReaderShardAdmissionController readerShardAdmissionController;
    private volatile WalChunkService sharedWalChunkService;
    private volatile org.opensearch.serverless.storage.wal.WalGcSchedulerTask walGcSchedulerTask;
    private volatile TimeValue compactionInterval;
    private volatile TimeValue gcInterval;
    // Reused by getEngineFactory to schedule a dedicated-WAL-stream shard's own sweep at the same
    // configured cadence as the shared WAL container's node-level WalGcSchedulerTask -- non-positive
    // (the default) disables both the shared sweep (existing behavior) and any dedicated one.
    private volatile TimeValue walGcInterval;
    private volatile long gcRetentionWindowMillis;
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
    // Resolved once in createComponents, same "read the NodeScope setting where Environment is
    // actually available" reasoning as every other field in this group -- TransportScaleToZeroCandidatesAction
    // reads these as its per-request defaults, overridable per ScaleToZeroCandidatesRequest.
    private volatile long scaleToZeroIdleThresholdMillis = SERVERLESS_STORAGE_SCALE_TO_ZERO_IDLE_THRESHOLD_SETTING.getDefault(
        Settings.EMPTY
    ).millis();
    private volatile long scaleToZeroLagThreshold = SERVERLESS_STORAGE_SCALE_TO_ZERO_LAG_THRESHOLD_SETTING.getDefault(Settings.EMPTY);
    private volatile org.opensearch.serverless.storage.scaletozero.ScaleToZeroCandidatesSchedulerTask scaleToZeroCandidatesSchedulerTask;
    // Same "resolved once in createComponents" reasoning as the scale-to-zero thresholds above --
    // TransportScaleUpCandidatesAction reads these as its per-request defaults, overridable per
    // ScaleUpCandidatesRequest.
    private volatile long scaleUpQpmThreshold = SERVERLESS_STORAGE_SCALE_UP_QPM_THRESHOLD_SETTING.getDefault(Settings.EMPTY);
    private volatile int scaleUpMaxSearchReplicas = SERVERLESS_STORAGE_SCALE_UP_MAX_SEARCH_REPLICAS_SETTING.getDefault(Settings.EMPTY);
    private volatile long reshardingSplitCandidateWritesPerMinuteThreshold =
        SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_WPM_THRESHOLD_SETTING.getDefault(Settings.EMPTY);
    private volatile org.opensearch.serverless.storage.scaleup.ScaleUpCandidatesSchedulerTask scaleUpCandidatesSchedulerTask;
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
            SERVERLESS_STORAGE_WAL_GC_INTERVAL_SETTING,
            SERVERLESS_STORAGE_WAL_PER_SHARD_BUDGET_SETTING,
            SERVERLESS_STORAGE_PUBLICATION_RATE_LIMIT_SETTING,
            SERVERLESS_STORAGE_LAZY_DIRECTORY_CACHE_SIZE_SETTING,
            SERVERLESS_STORAGE_LOCAL_CACHE_MAX_BYTES_PER_SHARD_SETTING,
            SERVERLESS_STORAGE_LAZY_DIRECTORY_ENABLED_SETTING,
            SERVERLESS_STORAGE_WAL_DEDICATED_STREAM_SETTING,
            SERVERLESS_STORAGE_SCALE_TO_ZERO_IDLE_THRESHOLD_SETTING,
            SERVERLESS_STORAGE_SCALE_TO_ZERO_LAG_THRESHOLD_SETTING,
            SERVERLESS_STORAGE_SCALE_TO_ZERO_EVAL_INTERVAL_SETTING,
            SERVERLESS_STORAGE_SCALE_TO_ZERO_SUSPEND_ENABLED_SETTING,
            SERVERLESS_STORAGE_SCALE_TO_ZERO_SEARCH_REACTIVATION_WAIT_SETTING,
            SERVERLESS_STORAGE_SCALE_TO_ZERO_COOLDOWN_SETTING,
            SERVERLESS_STORAGE_READER_CACHE_AFFINITY_TTL_SETTING,
            SERVERLESS_STORAGE_SCALE_UP_QPM_THRESHOLD_SETTING,
            SERVERLESS_STORAGE_SCALE_UP_MAX_SEARCH_REPLICAS_SETTING,
            SERVERLESS_STORAGE_SCALE_UP_EVAL_INTERVAL_SETTING,
            SERVERLESS_STORAGE_SCALE_UP_ENABLED_SETTING,
            SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_WPM_THRESHOLD_SETTING,
            SERVERLESS_STORAGE_REPOSITORY_SETTING
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
        serverlessStorageExistingShardsAllocator.setDependencies(
            clusterService,
            SERVERLESS_STORAGE_READER_CACHE_AFFINITY_TTL_SETTING.get(environment.settings()).millis()
        );
        TimeValue scaleToZeroEvalInterval = SERVERLESS_STORAGE_SCALE_TO_ZERO_EVAL_INTERVAL_SETTING.get(environment.settings());
        if (scaleToZeroEvalInterval.millis() > 0) {
            boolean suspendEnabled = SERVERLESS_STORAGE_SCALE_TO_ZERO_SUSPEND_ENABLED_SETTING.get(environment.settings());
            long cooldownMillis = SERVERLESS_STORAGE_SCALE_TO_ZERO_COOLDOWN_SETTING.get(environment.settings()).millis();
            this.scaleToZeroCandidatesSchedulerTask = new org.opensearch.serverless.storage.scaletozero.ScaleToZeroCandidatesSchedulerTask(
                threadPool,
                scaleToZeroEvalInterval,
                client,
                clusterService,
                suspendEnabled
                    ? new org.opensearch.serverless.storage.scaletozero.ShardSuspensionCoordinator(clusterService, client, cooldownMillis)
                    : null
            );
        }
        this.scaleUpQpmThreshold = SERVERLESS_STORAGE_SCALE_UP_QPM_THRESHOLD_SETTING.get(environment.settings());
        this.scaleUpMaxSearchReplicas = SERVERLESS_STORAGE_SCALE_UP_MAX_SEARCH_REPLICAS_SETTING.get(environment.settings());
        this.reshardingSplitCandidateWritesPerMinuteThreshold = SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_WPM_THRESHOLD_SETTING.get(
            environment.settings()
        );
        TimeValue scaleUpEvalInterval = SERVERLESS_STORAGE_SCALE_UP_EVAL_INTERVAL_SETTING.get(environment.settings());
        if (scaleUpEvalInterval.millis() > 0) {
            boolean scaleUpEnabled = SERVERLESS_STORAGE_SCALE_UP_ENABLED_SETTING.get(environment.settings());
            this.scaleUpCandidatesSchedulerTask = new org.opensearch.serverless.storage.scaleup.ScaleUpCandidatesSchedulerTask(
                threadPool,
                scaleUpEvalInterval,
                client,
                clusterService,
                scaleUpEnabled
                    ? new org.opensearch.serverless.storage.scaleup.ReaderReplicaExpansionCoordinator(client, scaleUpMaxSearchReplicas)
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
        }
        localCacheMaxBytesPerShard = SERVERLESS_STORAGE_LOCAL_CACHE_MAX_BYTES_PER_SHARD_SETTING.get(environment.settings()).getBytes();
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
                //
                // Deliberately still local-filesystem-only, unlike resolveDedicatedWalContainer
                // and blobContainerFor below: this container (and sharedWalChunkService itself) is
                // resolved exactly once here, synchronously inside createComponents at node
                // startup -- before RepositoriesService necessarily has any repository registered
                // yet, since repository registration happens later via the _snapshot API, well
                // after this method returns. Routing this through resolveContainer would try the
                // repository-backed branch (SERVERLESS_STORAGE_REPOSITORY_SETTING configured) too
                // early and fail loudly at startup instead of lazily like every other container
                // this plugin resolves -- caught by a real internalClusterTest reproducing exactly
                // that failure, not reasoned out in advance. Making this genuinely lazy (matching
                // the per-shard containers' own on-demand resolution) needs sharedWalChunkService's
                // construction itself deferred to first real writer-shard use, a larger, separate
                // change from the WAL container repository-backing this pass otherwise closes.
                FsBlobStore walBlobStore = new FsBlobStore(1024 * 1024, basePath, false);
                BlobContainer walBlobContainer = walBlobStore.blobContainer(BlobPath.cleanPath().add("wal"));
                // A fresh epoch per node incarnation (see WalChunkService's own javadoc for what
                // this identifies) -- fencing during replay is a per-record primaryTerm filter, not
                // an epoch-directory one, so nothing depends on this value being stable across
                // restarts; it only needs to be unique enough that this process's chunk sequence
                // numbering never collides with a prior incarnation's.
                sharedWalChunkService = new WalChunkService(
                    walBlobContainer,
                    UUIDs.base64UUID(),
                    SERVERLESS_STORAGE_WAL_PER_SHARD_BUDGET_SETTING.get(environment.settings()).getBytes()
                );

                TimeValue configuredWalGcInterval = SERVERLESS_STORAGE_WAL_GC_INTERVAL_SETTING.get(environment.settings());
                this.walGcInterval = configuredWalGcInterval;
                if (configuredWalGcInterval.millis() > 0) {
                    walGcSchedulerTask = new org.opensearch.serverless.storage.wal.WalGcSchedulerTask(
                        threadPool,
                        configuredWalGcInterval,
                        walBlobContainer,
                        new org.opensearch.serverless.storage.wal.WalShardRegistry(walBlobContainer),
                        (indexUuid, shardId) -> {
                            try {
                                return resolveBlobContainer(indexUuid, shardId);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }
                    );
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        // This plugin instance itself, so TransportShardCloneAction (the only consumer) can be
        // constructor-injected with it and reach blobContainerForDirectoryFactory -- the same
        // resolution ServerlessStorageLazyDirectoryFactory already depends on, just handed to a
        // different consumer via a different injection path (Guice component vs. direct reference).
        return Collections.singletonList(this);
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

            boolean isReaderShard = shardRouting != null && shardRouting.isSearchOnly();
            if (isReaderShard) {
                // Credential scoping per tier, bullet 1 (rfc-serverless-opensearch.md &sect;15):
                // "search-compute needs GET-only on data prefixes." This reader shard's own
                // background CompactionSchedulerConfig/GcSchedulerConfig below still need write
                // (and, for GC, delete) access, so they keep using scopedContainer/blobContainer
                // directly -- only the query-serving trio (shardStateStore/manifestStore/readPath,
                // what ObjectStoreReaderEngine itself actually reads from on every query and on its
                // own background poll) gets this separate, strictly-narrower container.
                BlobContainer readOnlyContainer = new org.opensearch.serverless.storage.security.RestrictingBlobContainer(
                    scopedContainer,
                    false,
                    false
                );
                ShardStateStore readerShardStateStore = new BlobContainerShardStateStore(readOnlyContainer);
                BlobContainerManifestStore readerManifestStore = new BlobContainerManifestStore(readOnlyContainer);
                BundleFileReader readPath = new BlobContainerBundleStore(readOnlyContainer);
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
                    BundleFileReader diskCache = new LocalDiskCachingBundleStore(
                        readPath,
                        shardCacheDir,
                        encryptionKeyProvider,
                        localCacheMaxBytesPerShard
                    );
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
                        pinRegistry
                    );
                org.opensearch.serverless.storage.resharding.ShardPartitionDescriptor partitionDescriptor;
                try {
                    partitionDescriptor = new org.opensearch.serverless.storage.resharding.BlobContainerShardPartitionStore(blobContainer)
                        .readDescriptor()
                        .orElse(null);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return Optional.of(
                    new ReaderEngineFactory(
                        readerShardStateStore,
                        readerManifestStore,
                        new ObjectStoreCommitMaterializer(readPath),
                        shardDirectory,
                        localNodeId,
                        readerShardAdmissionController,
                        compactionConfig,
                        gcConfig,
                        readerShardActivityRegistry,
                        partitionDescriptor
                    )
                );
            }
            PitrRetentionConfig pitrRetentionConfig = pitrWindowMillis > 0
                ? new PitrRetentionConfig(manifestStore, pinRegistry, pitrWindowMillis)
                : null;

            // §12's "dedicated WAL streams" bullet: an index opted into
            // SERVERLESS_STORAGE_WAL_DEDICATED_STREAM_SETTING gets its own WalChunkService pointed
            // at a container scoped to exactly this (indexUuid, shardId) -- never the node-shared
            // one -- so its WAL bytes can never land in the same object as any other index's,
            // independent of the per-record encryption every WAL record already gets regardless.
            // Only meaningful when WAL mirroring itself is on at all (sharedWalChunkService != null);
            // an index requesting a dedicated stream on a node with WAL mirroring off gets none,
            // same as every other WAL-dependent feature already degrades in that case.
            org.opensearch.serverless.storage.wal.WalChunkService writerWalChunkService = sharedWalChunkService;
            org.opensearch.serverless.storage.wal.DedicatedWalGcConfig dedicatedWalGcConfig = null;
            if (sharedWalChunkService != null && SERVERLESS_STORAGE_WAL_DEDICATED_STREAM_SETTING.get(indexSettings.getSettings())) {
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
                    new ObjectStoreCommitHeadPublisher(commitPublisher, shardStateStore),
                    shardDirectory,
                    localNodeId,
                    pitrRetentionConfig,
                    writerWalChunkService,
                    // Cross-node failover materializes at most once per activation (rfc-serverless-opensearch.md
                    // &sect;7.1.2), not per-query like a reader shard -- no caching layer needed,
                    // straight to the bundle store, matching the "caching is wired in for reader
                    // shards only" note on the reader path just above.
                    new ObjectStoreCommitMaterializer(bundleStore),
                    // WAL-mirrored records get the same at-rest protection bundles/manifests already
                    // have when this is configured (&sect;12 bullet 1) -- null (the default) leaves
                    // WAL mirroring's own on/off switch (sharedWalChunkService being non-null) as the
                    // only thing this depends on, unaffected by encryption being off.
                    encryptionKeyProvider,
                    shardActivityRegistry,
                    dedicatedWalGcConfig,
                    publicationRateLimitMillis,
                    writerPublicationNotifierForWriterEngine()
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
    private BlobContainer resolveContainer(BlobPath relativePath, String missingContainerErrorMessage) throws IOException {
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

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return java.util.List.of(new ReaderShardPlacementAllocationDecider(), new SuspendedShardAllocationDecider());
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

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders() {
        return Collections.singletonList(new ServerlessStorageIndexSettingProvider());
    }

    /**
     * Registers {@link ServerlessStorageRemoteClusterStateValidator} -- see that class's own
     * javadoc for why this check belongs on {@link IndexCreationValidator}, not {@link
     * IndexSettingProvider} (which is where it originally lived).
     */
    @Override
    public Collection<IndexCreationValidator> getIndexCreationValidators() {
        return Collections.singletonList(new ServerlessStorageRemoteClusterStateValidator());
    }

    /**
     * Registers {@link org.opensearch.serverless.storage.scaletozero.ShardReactivationActionFilter}
     * -- the cold-start-reactivation-on-access half of scale-to-zero (rfc-serverless-opensearch.md
     * &sect;7.3), see that class's own javadoc.
     */
    @Override
    public List<org.opensearch.action.support.ActionFilter> getActionFilters() {
        return Collections.singletonList(shardReactivationActionFilter);
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
            }
        });
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
                    }
                );
            } catch (Exception e) {
                // See this method's own javadoc: logged-and-swallowed, not fatal to index deletion.
            }
        }
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
            )
        );
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
            new org.opensearch.serverless.storage.readerengine.action.RestWaitForGenerationAction(),
            new org.opensearch.serverless.storage.readerengine.action.RestPollNowAction(),
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
            new org.opensearch.serverless.storage.migration.action.RestMigrateShardAction()
        );
    }

    /** The node-shared bundle cache {@link #createComponents} built -- test-only visibility, not part of the plugin's contract. */
    InMemoryPlaintextBundleCache sharedBundleCacheForTesting() {
        return sharedBundleCache;
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

    /** The node-shared WAL chunk service {@link #createComponents} built, or {@code null} if WAL mirroring is off -- test-only visibility. */
    WalChunkService sharedWalChunkServiceForTesting() {
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

    /** The reader-shard admission controller {@link #createComponents} built, or {@code null} if disabled -- test-only visibility. */
    ReaderShardAdmissionController readerShardAdmissionControllerForTesting() {
        return readerShardAdmissionController;
    }
}
