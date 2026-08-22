/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.remote.RemoteWriteableEntityBlobStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest;
import org.opensearch.gateway.remote.model.RemoteClusterStateManifestInfo;
import org.opensearch.gateway.remote.model.RemoteManifestShard;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;

/**
 * A Manager which provides APIs to write and read {@link ClusterMetadataManifest} to remote store
 *
 * @opensearch.internal
 */
public class RemoteManifestManager {

    public static final TimeValue METADATA_MANIFEST_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.metadata_manifest.upload_timeout",
        METADATA_MANIFEST_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * How many shards the manifest's index list is partitioned into. {@code 0} (the default) means
     * unsharded -- every manifest continues to carry its full index list inline in {@code indices},
     * exactly as before this setting existed. A positive value turns sharding on for every manifest
     * written from that point forward; see {@link ClusterMetadataManifest#CODEC_V6}'s own doc for what
     * changes on the wire. This setting is read fresh from configuration only when *writing* a
     * manifest -- a reader always uses the
     * count found in the manifest it is reading, not this setting's current value.
     *
     * <p>Dynamic rather than fixed at startup: changing it mid-cluster-life is handled safely (see
     * {@link IndexMetadataManifestSharder#plan}'s {@code previousShardCount} parameter) by treating a
     * shard-count change as "nothing can be carried forward, rewrite every shard once" rather than
     * something that must be prevented -- correct, if not free, so there is no correctness reason to
     * forbid it.
     */
    public static final Setting<Integer> CLUSTER_REMOTE_STORE_STATE_MANIFEST_SHARD_COUNT_SETTING = Setting.intSetting(
        "cluster.remote_store.state.manifest.shard_count",
        0,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * {@link ClusterMetadataManifest#MANIFEST_CURRENT_CODEC_VERSION}
     * (currently {@link ClusterMetadataManifest#CODEC_V6}) is, by that class's own documented design,
     * written unconditionally by every node the moment it starts -- see {@code ClusterMetadataManifest}'s
     * static initializer comment for why that is deliberate for this fork rather than a bug. That is a
     * one-way door: once any node has written a {@code CODEC_V6} manifest, the repository is no longer
     * readable by a build that only understands up to {@link ClusterMetadataManifest#CODEC_V5} -- see
     * {@link ClusterMetadataManifest#FORK_CODEC_BASE}'s own javadoc for the sharper version of the same
     * property. This setting is the deliberately-provided escape hatch: an explicit, documented cluster
     * setting rather than only an unconditional bump.
     *
     * <p><b>Default {@code false} on purpose, not {@code true}</b> -- unlike most gates in this codebase,
     * this one does NOT flip to change default behavior. Every existing deployment of this fork already
     * writes {@code CODEC_V6} unconditionally; defaulting this setting to "pin back to V5" would silently
     * change that on the next upgrade, and defaulting the earlier draft of this setting to "must opt in
     * to V6" broke the migration tests that assert an incremental update lands on {@code
     * MANIFEST_CURRENT_CODEC_VERSION} -- exactly the kind of silent default-behavior change the ground
     * rule "no behavior change for a node without an explicit opt-in" exists to prevent. So: {@code
     * false} here means "today's behavior, unchanged" -- CODEC_V6 written unconditionally, same as
     * before this setting existed. An operator who wants the staged-rollout safety this setting exists to
     * provide sets it {@code true} to pin manifests at {@link ClusterMetadataManifest#CODEC_V5} (a fully
     * supported, actively-parsed format -- see {@code PARSER_V5}/{@code fromXContentV5}) until they are
     * ready to cross the one-way door deliberately.
     *
     * <p>Manifest sharding overrides this pin when enabled ({@link #manifestShardCount} {@code > 0}):
     * {@code CODEC_V6}'s shard-reference fields have no {@code CODEC_V5} representation, so an operator
     * who explicitly turns sharding on has already made the CODEC_V6 decision via that setting.
     */
    public static final Setting<Boolean> CLUSTER_REMOTE_STORE_STATE_PIN_CODEC_V5_SETTING = Setting.boolSetting(
        "cluster.remote_store.state.pin_manifest_codec_v5",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(RemoteManifestManager.class);

    /**
     * Floor for {@link #maxCachedShardIndexEntries}, so a small or not-yet-observed index population still
     * gets a cache worth having rather than one that evicts itself.
     */
    private static final int MIN_CACHED_SHARD_INDEX_ENTRIES = 1 << 16;

    private volatile TimeValue metadataManifestUploadTimeout;
    private volatile int manifestShardCount;
    private volatile boolean pinCodecV5;
    private final String nodeId;
    private final RemoteWriteableEntityBlobStore<ClusterMetadataManifest, RemoteClusterMetadataManifest> manifestBlobStore;
    private final RemoteWriteableEntityBlobStore<ManifestShardContent, RemoteManifestShard> manifestShardBlobStore;
    private final Compressor compressor;
    private final NamedXContentRegistry namedXContentRegistry;
    // todo remove blobStorerepo from here
    private final BlobStoreRepository blobStoreRepository;

    /**
     * Parsed shard-blob content, keyed by the shard blob's full name.
     *
     * <p><b>Sound because a shard blob is immutable.</b> {@link RemoteManifestShard#generateBlobFileName()}
     * mints a fresh name (shard id, term, version and a millisecond timestamp) for every write and nothing
     * ever overwrites an existing shard blob in place, so a given name's bytes never change. A carried-forward
     * shard reference (see {@link IndexMetadataManifestSharder#plan}) reuses the previous blob name verbatim,
     * which is what makes the hit rate near 100%: between two consecutive versions only the dirty shards get
     * new names.
     *
     * <p><b>What this fixes.</b> {@link #resolveIndices} used to be one sequential, uncached blob GET per
     * shard, on the cluster-manager's own publish path (twice per publish+commit cycle, since both
     * {@code writeIncrementalMetadata} and {@code markLastStateAsCommitted} resolve the previous manifest)
     * and on every follower's applied-diff path. At S=64..1024 shards and 20-40 ms per GET that is 1.3 s to
     * 30 s+ of serial latency each time, which defeated the entire point of sharding on the read side: the
     * write side is genuinely dirty-shards-only, and the read side re-fetched every shard anyway. With this
     * cache the cluster manager re-reads nothing at all (it populated the cache when it wrote those very
     * blobs) and a follower fetches only the shards that actually changed.
     *
     * <p>Access-ordered LRU, bounded by {@link #maxCachedShardIndexEntries} <em>index entries</em> rather
     * than by blob count, because a shard blob's size is the thing that varies (N/S entries each). Guarded
     * by its own monitor, not by {@code this}: {@link #uploadManifest} holds {@code this} for the whole
     * publish, and a follower's diff read must not queue behind that.
     */
    private final LinkedHashMap<String, List<UploadedIndexMetadata>> shardContentCache = new LinkedHashMap<>(64, 0.75f, true);

    /** Total {@link UploadedIndexMetadata} entries currently held across {@link #shardContentCache}. Guarded by that map. */
    private int cachedShardIndexEntries;

    /**
     * The entry bound for {@link #shardContentCache}, re-derived from the manifest being resolved (the sum
     * of its shard references' declared entry counts, which needs no I/O to read). Two generations' worth of
     * the current index list, floored at {@link #MIN_CACHED_SHARD_INDEX_ENTRIES} -- the same order of memory
     * an unsharded manifest holds inline anyway, and the same order {@code writeIncrementalMetadata} already
     * materialises per publish.
     */
    private volatile int maxCachedShardIndexEntries = MIN_CACHED_SHARD_INDEX_ENTRIES;

    RemoteManifestManager(
        ClusterSettings clusterSettings,
        String clusterName,
        String nodeId,
        BlobStoreRepository blobStoreRepository,
        BlobStoreTransferService blobStoreTransferService,
        ThreadPool threadpool
    ) {
        this.metadataManifestUploadTimeout = clusterSettings.get(METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING);
        this.manifestShardCount = clusterSettings.get(CLUSTER_REMOTE_STORE_STATE_MANIFEST_SHARD_COUNT_SETTING);
        this.pinCodecV5 = clusterSettings.get(CLUSTER_REMOTE_STORE_STATE_PIN_CODEC_V5_SETTING);
        this.nodeId = nodeId;
        this.manifestBlobStore = new RemoteWriteableEntityBlobStore<>(
            blobStoreTransferService,
            blobStoreRepository,
            clusterName,
            threadpool,
            ThreadPool.Names.REMOTE_STATE_READ,
            RemoteClusterStateUtils.CLUSTER_STATE_PATH_TOKEN
        );
        this.manifestShardBlobStore = new RemoteWriteableEntityBlobStore<>(
            blobStoreTransferService,
            blobStoreRepository,
            clusterName,
            threadpool,
            ThreadPool.Names.REMOTE_STATE_READ,
            RemoteClusterStateUtils.CLUSTER_STATE_PATH_TOKEN
        );
        clusterSettings.addSettingsUpdateConsumer(METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING, this::setMetadataManifestUploadTimeout);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_REMOTE_STORE_STATE_MANIFEST_SHARD_COUNT_SETTING, this::setManifestShardCount);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_REMOTE_STORE_STATE_PIN_CODEC_V5_SETTING, this::setPinCodecV5);
        this.compressor = blobStoreRepository.getCompressor();
        this.namedXContentRegistry = blobStoreRepository.getNamedXContentRegistry();
        this.blobStoreRepository = blobStoreRepository;
    }

    private void setManifestShardCount(int manifestShardCount) {
        this.manifestShardCount = manifestShardCount;
    }

    private void setPinCodecV5(boolean pinCodecV5) {
        this.pinCodecV5 = pinCodecV5;
    }

    /**
     * {@code MANIFEST_CURRENT_CODEC_VERSION}
     * (today's unconditional default) unless an operator has explicitly pinned to {@code CODEC_V5} --
     * and even then, sharding (an operator's own explicit opt-in) overrides the pin, since {@code
     * CODEC_V6}'s shard-reference fields have no {@code CODEC_V5} representation. See {@link
     * #CLUSTER_REMOTE_STORE_STATE_PIN_CODEC_V5_SETTING}'s own javadoc for why the default preserves
     * today's behavior rather than requiring opt-in to it.
     */
    // Package-private rather than private so RemoteManifestManagerTests can exercise it directly instead
    // of standing up a full uploadManifest() flow just to observe this one decision.
    int resolveCodecVersion(int shardCountForThisManifest) {
        if (shardCountForThisManifest > 0 || pinCodecV5 == false) {
            return ClusterMetadataManifest.MANIFEST_CURRENT_CODEC_VERSION;
        }
        return ClusterMetadataManifest.CODEC_V5;
    }

    RemoteClusterStateManifestInfo uploadManifest(
        ClusterState clusterState,
        RemoteClusterStateUtils.UploadedMetadataResults uploadedMetadataResult,
        String previousClusterUUID,
        ClusterStateDiffManifest clusterDiffManifest,
        ClusterStateChecksum clusterStateChecksum,
        boolean committed
    ) {
        return uploadManifest(
            clusterState,
            uploadedMetadataResult,
            previousClusterUUID,
            clusterDiffManifest,
            clusterStateChecksum,
            committed,
            null,
            Collections.emptySet()
        );
    }

    /**
     * @param previousManifestForSharding the previous version's manifest, used only to carry forward
     *                                     unchanged shard blob references (see {@link
     *                                     IndexMetadataManifestSharder#plan}) -- {@code null} when there
     *                                     is none (a fresh cluster, or a full/cold write). Ignored
     *                                     entirely when sharding is disabled ({@link
     *                                     #manifestShardCount} is {@code 0}).
     * @param changedOrDeletedIndexUUIDs indices newly uploaded, updated, or deleted this version -- see
     *                                   {@link IndexMetadataManifestSharder#plan}'s own doc on why a
     *                                   deleted index's UUID belongs here even though it is absent from
     *                                   {@code uploadedMetadataResult.uploadedIndexMetadata}.
     */
    RemoteClusterStateManifestInfo uploadManifest(
        ClusterState clusterState,
        RemoteClusterStateUtils.UploadedMetadataResults uploadedMetadataResult,
        String previousClusterUUID,
        ClusterStateDiffManifest clusterDiffManifest,
        ClusterStateChecksum clusterStateChecksum,
        boolean committed,
        ClusterMetadataManifest previousManifestForSharding,
        Set<String> changedOrDeletedIndexUUIDs
    ) {
        synchronized (this) {
            String clusterUUID = clusterState.metadata().clusterUUID();
            int shardCountForThisManifest = manifestShardCount;
            List<UploadedIndexMetadata> inlineIndices = uploadedMetadataResult.uploadedIndexMetadata;
            List<UploadedManifestShard> indexMetadataShards = Collections.emptyList();
            if (shardCountForThisManifest > 0) {
                List<UploadedManifestShard> previousShards = previousManifestForSharding != null
                    ? previousManifestForSharding.getIndexMetadataShards()
                    : Collections.emptyList();
                int previousShardCount = previousManifestForSharding != null ? previousManifestForSharding.getManifestShardCount() : 0;
                IndexMetadataManifestSharder.Plan plan = IndexMetadataManifestSharder.plan(
                    inlineIndices,
                    changedOrDeletedIndexUUIDs,
                    previousShards,
                    previousShardCount,
                    shardCountForThisManifest
                );
                List<UploadedManifestShard> written = new ArrayList<>(plan.getCarriedForward());
                written.addAll(writeManifestShards(clusterUUID, clusterState.term(), clusterState.getVersion(), plan.getShardsToWrite()));
                indexMetadataShards = written;
                // The index list now lives entirely behind the shard references above -- an inline copy
                // would defeat the entire point (reintroducing the per-version write amplification
                // sharding exists to remove) and give
                // every reader two disagreeing sources of truth for the same data.
                inlineIndices = Collections.emptyList();
            }

            ClusterMetadataManifest.Builder manifestBuilder = ClusterMetadataManifest.builder();
            manifestBuilder.clusterTerm(clusterState.term())
                .stateVersion(clusterState.getVersion())
                .clusterUUID(clusterUUID)
                .stateUUID(clusterState.stateUUID())
                .opensearchVersion(Version.CURRENT)
                .nodeId(nodeId)
                .committed(committed)
                .codecVersion(resolveCodecVersion(shardCountForThisManifest))
                .indices(inlineIndices)
                .previousClusterUUID(previousClusterUUID)
                .clusterUUIDCommitted(clusterState.metadata().clusterUUIDCommitted())
                .coordinationMetadata(uploadedMetadataResult.uploadedCoordinationMetadata)
                .settingMetadata(uploadedMetadataResult.uploadedSettingsMetadata)
                .templatesMetadata(uploadedMetadataResult.uploadedTemplatesMetadata)
                .customMetadataMap(uploadedMetadataResult.uploadedCustomMetadataMap)
                .routingTableVersion(clusterState.getRoutingTable().version())
                .indicesRouting(uploadedMetadataResult.uploadedIndicesRoutingMetadata)
                .discoveryNodesMetadata(uploadedMetadataResult.uploadedDiscoveryNodes)
                .clusterBlocksMetadata(uploadedMetadataResult.uploadedClusterBlocks)
                .diffManifest(clusterDiffManifest)
                .metadataVersion(clusterState.metadata().version())
                .transientSettingsMetadata(uploadedMetadataResult.uploadedTransientSettingsMetadata)
                .clusterStateCustomMetadataMap(uploadedMetadataResult.uploadedClusterStateCustomMetadataMap)
                .hashesOfConsistentSettings(uploadedMetadataResult.uploadedHashesOfConsistentSettings)
                .checksum(clusterStateChecksum)
                .manifestShardCount(shardCountForThisManifest)
                .indexMetadataShards(indexMetadataShards);
            final ClusterMetadataManifest manifest = manifestBuilder.build();
            logger.trace(() -> new ParameterizedMessage("[{}] uploading manifest", manifest));
            String manifestFileName = writeMetadataManifest(clusterUUID, manifest);
            return new RemoteClusterStateManifestInfo(manifest, manifestFileName);
        }
    }

    /**
     * Every current {@link UploadedIndexMetadata} a manifest's index list actually holds, transparently
     * resolving through {@link UploadedManifestShard} references when the manifest is sharded -- the
     * one place that decision should be made, so every caller that used to say {@code
     * manifest.getIndices()} can say {@code remoteManifestManager.resolveIndices(manifest)} instead and
     * not otherwise change.
     *
     * <p>Served from {@link #shardContentCache} wherever it can be, which in steady state is everywhere:
     * see that field's own javadoc for why a blob-name key is sound and why the hit rate is what it is.
     * A shard blob that is genuinely missing fails the whole resolution -- see
     * {@link #resolveIndicesToleratingMissingShards} for the one caller that must not.
     */
    public List<UploadedIndexMetadata> resolveIndices(ClusterMetadataManifest manifest) {
        return resolveIndices(manifest, false);
    }

    /**
     * The tolerant variant, for a caller that is resolving a manifest it has already decided is stale and
     * is about to delete. A shard blob that is simply not there any more is skipped rather than failing the
     * whole resolution.
     *
     * <p><b>Only safe in that direction, which is why it is a separate method rather than the default.</b>
     * For a stale manifest the resolved list produces deletion <em>candidates</em>, so under-resolving
     * under-deletes: some index blobs leak until a later sweep, which is a leak and not a loss. For an
     * <em>active</em> manifest the same list produces the keep-set, where under-resolving means deleting
     * blobs that are still referenced. {@link RemoteClusterStateCleanupManager} therefore keeps using the
     * strict {@link #resolveIndices(ClusterMetadataManifest)} for the manifests it is retaining.
     *
     * <p>Exists because the cleanup sweep could otherwise wedge permanently: if the shard blobs of a stale
     * manifest were deleted but the manifest delete itself then failed, every subsequent sweep threw on the
     * missing shard blob before it ever reached the manifest delete that would have ended the loop. The
     * delete order in that sweep now makes this shape unreachable, and this makes an already-wedged
     * repository recoverable.
     */
    public List<UploadedIndexMetadata> resolveIndicesToleratingMissingShards(ClusterMetadataManifest manifest) {
        return resolveIndices(manifest, true);
    }

    private List<UploadedIndexMetadata> resolveIndices(ClusterMetadataManifest manifest, boolean tolerateMissingShards) {
        if (manifest.getManifestShardCount() <= 0) {
            return manifest.getIndices();
        }
        List<UploadedManifestShard> shardRefs = manifest.getIndexMetadataShards();
        int declaredEntries = 0;
        for (UploadedManifestShard shardRef : shardRefs) {
            declaredEntries += shardRef.getEntryCount();
        }
        // Sized from the manifest itself rather than from a setting: the declared entry counts are already
        // in hand, so the bound tracks the actual index population without anybody having to configure it.
        maxCachedShardIndexEntries = Math.max(MIN_CACHED_SHARD_INDEX_ENTRIES, 2 * declaredEntries);

        List<UploadedIndexMetadata> resolved = new ArrayList<>(declaredEntries);
        for (UploadedManifestShard shardRef : shardRefs) {
            List<UploadedIndexMetadata> content = cachedShardContent(shardRef);
            if (content == null) {
                try {
                    content = readManifestShard(manifest.getClusterUUID(), shardRef);
                } catch (IllegalStateException e) {
                    if (tolerateMissingShards == false) {
                        throw e;
                    }
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "skipping unreadable manifest shard [{}] of a stale manifest; any index blobs it "
                                + "referenced are left in place rather than deleted, so this under-deletes "
                                + "instead of stalling cleanup",
                            shardRef.getBlobName()
                        ),
                        e
                    );
                    continue;
                }
                cacheShardContent(shardRef.getBlobName(), content);
            }
            resolved.addAll(content);
        }
        return resolved;
    }

    /**
     * The cached content for a shard reference, or null on a miss. The declared entry count is re-checked
     * on a hit for the same reason {@link #readManifestShard} checks it: it costs one {@code size()} call,
     * and a disagreement here means the reference and the blob are not describing the same partition, which
     * must never be resolved silently.
     */
    private List<UploadedIndexMetadata> cachedShardContent(UploadedManifestShard shardRef) {
        List<UploadedIndexMetadata> cached;
        synchronized (shardContentCache) {
            cached = shardContentCache.get(shardRef.getBlobName());
        }
        if (cached == null) {
            return null;
        }
        if (cached.size() != shardRef.getEntryCount()) {
            assert false : "cached manifest shard [" + shardRef.getBlobName() + "] disagrees with its reference's entry count";
            return null;
        }
        return cached;
    }

    private void cacheShardContent(String blobName, List<UploadedIndexMetadata> content) {
        if (blobName == null) {
            return;
        }
        List<UploadedIndexMetadata> immutable = Collections.unmodifiableList(content);
        synchronized (shardContentCache) {
            List<UploadedIndexMetadata> previous = shardContentCache.put(blobName, immutable);
            if (previous != null) {
                cachedShardIndexEntries -= previous.size();
            }
            cachedShardIndexEntries += immutable.size();
            int bound = maxCachedShardIndexEntries;
            var it = shardContentCache.entrySet().iterator();
            // Access-ordered, so the iterator yields least-recently-used first. Never evicts down to empty:
            // one shard blob larger than the whole bound would otherwise thrash itself out immediately.
            while (cachedShardIndexEntries > bound && shardContentCache.size() > 1 && it.hasNext()) {
                cachedShardIndexEntries -= it.next().getValue().size();
                it.remove();
            }
        }
    }

    /**
     * Writes every dirty shard for one manifest, with a single shared latch rather than one blocking wait
     * per shard.
     *
     * <p>The previous shape awaited each shard's own latch in turn, so the whole set cost the sum of the
     * per-shard round trips rather than the slowest of them -- and the cases that write <em>every</em> shard
     * (the first version after sharding is enabled, or any shard-count change, both of which
     * {@link IndexMetadataManifestSharder#plan} deliberately degrades to a full rewrite) stalled the
     * cluster-manager update thread for S serial uploads: roughly 30 s at S=1024. The uploads themselves
     * were already asynchronous; only the waiting was serial.
     *
     * <p>The timeout keeps its per-shard meaning of "no single shard may take longer than this", which is
     * now also the bound on the whole batch, since they overlap. Every failure is collected so the
     * exception names one real cause rather than whichever shard happened to be awaited first.
     */
    private List<UploadedManifestShard> writeManifestShards(
        String clusterUUID,
        long clusterTerm,
        long stateVersion,
        Map<Integer, List<UploadedIndexMetadata>> shardsToWrite
    ) {
        if (shardsToWrite.isEmpty()) {
            return Collections.emptyList();
        }
        ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
        CountDownLatch latch = new CountDownLatch(shardsToWrite.size());
        List<Integer> shardIds = new ArrayList<>(shardsToWrite.size());
        List<RemoteManifestShard> entities = new ArrayList<>(shardsToWrite.size());

        for (Map.Entry<Integer, List<UploadedIndexMetadata>> entry : shardsToWrite.entrySet()) {
            RemoteManifestShard remoteManifestShard = new RemoteManifestShard(
                new ManifestShardContent(entry.getValue()),
                entry.getKey(),
                clusterTerm,
                stateVersion,
                clusterUUID,
                compressor,
                namedXContentRegistry
            );
            shardIds.add(entry.getKey());
            entities.add(remoteManifestShard);
            LatchedActionListener<Void> completionListener = new LatchedActionListener<>(
                ActionListener.wrap(resp -> {}, exceptions::add),
                latch
            );
            manifestShardBlobStore.writeAsync(remoteManifestShard, completionListener);
        }

        try {
            if (latch.await(getMetadataManifestUploadTimeout().millis(), TimeUnit.MILLISECONDS) == false) {
                throw new RemoteStateTransferException(
                    String.format(Locale.ROOT, "Timed out waiting for transfer of [%d] manifest shards to complete", shardsToWrite.size())
                );
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RemoteStateTransferException(
                String.format(Locale.ROOT, "Timed out waiting for transfer of [%d] manifest shards to complete", shardsToWrite.size()),
                ex
            );
        }
        Exception failure = exceptions.poll();
        if (failure != null) {
            RemoteStateTransferException transferException = new RemoteStateTransferException(failure.getMessage(), failure);
            exceptions.forEach(transferException::addSuppressed);
            throw transferException;
        }

        List<UploadedManifestShard> written = new ArrayList<>(entities.size());
        for (int i = 0; i < entities.size(); i++) {
            List<UploadedIndexMetadata> entries = shardsToWrite.get(shardIds.get(i));
            String blobName = entities.get(i).getUploadedMetadata().getUploadedFilename();
            // Populated on the write path, which is what makes the cluster manager's own resolveIndices of
            // the manifest it just wrote a pure cache hit on the next publish: it already holds every
            // shard's content, and a carried-forward shard keeps the very blob name cached here.
            cacheShardContent(blobName, entries);
            written.add(new UploadedManifestShard(shardIds.get(i), blobName, entries.size()));
        }
        return written;
    }

    private List<UploadedIndexMetadata> readManifestShard(String clusterUUID, UploadedManifestShard shardRef) {
        try {
            RemoteManifestShard remoteManifestShard = new RemoteManifestShard(
                shardRef.getBlobName(),
                clusterUUID,
                compressor,
                namedXContentRegistry
            );
            ManifestShardContent content = manifestShardBlobStore.read(remoteManifestShard);
            // See UploadedManifestShard's own javadoc on why this exists: a shard read that returned
            // fewer entries than the reference declares is a truncated read, not an empty partition, and
            // must not be treated as if the missing entries simply do not exist -- that is exactly the
            // shape of bug that turns into deleting still-referenced index metadata (see
            // RemoteClusterStateCleanupManager, this method's most safety-sensitive caller).
            if (content.getIndices().size() != shardRef.getEntryCount()) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "manifest shard [%d] blob [%s] returned [%d] entries but the manifest reference declared [%d]",
                        shardRef.getShardId(),
                        shardRef.getBlobName(),
                        content.getIndices().size(),
                        shardRef.getEntryCount()
                    )
                );
            }
            return content.getIndices();
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading manifest shard - %s", shardRef.getBlobName()),
                e
            );
        }
    }

    /** Drops every cached shard blob. Package-private for tests, which need a cold cache to observe a read. */
    void clearShardContentCache() {
        synchronized (shardContentCache) {
            shardContentCache.clear();
            cachedShardIndexEntries = 0;
        }
    }

    private String writeMetadataManifest(String clusterUUID, ClusterMetadataManifest uploadManifest) {
        AtomicReference<String> result = new AtomicReference<String>();
        AtomicReference<Exception> exceptionReference = new AtomicReference<Exception>();

        // latch to wait until upload is not finished
        CountDownLatch latch = new CountDownLatch(1);

        LatchedActionListener completionListener = new LatchedActionListener<>(ActionListener.wrap(resp -> {
            logger.trace(String.format(Locale.ROOT, "Manifest file uploaded successfully."));
        }, ex -> { exceptionReference.set(ex); }), latch);

        RemoteClusterMetadataManifest remoteClusterMetadataManifest = new RemoteClusterMetadataManifest(
            uploadManifest,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        manifestBlobStore.writeAsync(remoteClusterMetadataManifest, completionListener);

        try {
            if (latch.await(getMetadataManifestUploadTimeout().millis(), TimeUnit.MILLISECONDS) == false) {
                RemoteStateTransferException ex = new RemoteStateTransferException(
                    String.format(Locale.ROOT, "Timed out waiting for transfer of manifest file to complete")
                );
                throw ex;
            }
        } catch (InterruptedException ex) {
            RemoteStateTransferException exception = new RemoteStateTransferException(
                String.format(Locale.ROOT, "Timed out waiting for transfer of manifest file to complete - %s"),
                ex
            );
            Thread.currentThread().interrupt();
            throw exception;
        }
        if (exceptionReference.get() != null) {
            throw new RemoteStateTransferException(exceptionReference.get().getMessage(), exceptionReference.get());
        }
        logger.debug(
            "Metadata manifest file [{}] written during [{}] phase. ",
            remoteClusterMetadataManifest.getBlobFileName(),
            uploadManifest.isCommitted() ? "commit" : "publish"
        );
        return remoteClusterMetadataManifest.getUploadedMetadata().getUploadedFilename();
    }

    /**
     * Fetch latest ClusterMetadataManifest from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    public Optional<ClusterMetadataManifest> getLatestClusterMetadataManifest(String clusterName, String clusterUUID) {
        Optional<String> latestManifestFileName = getLatestManifestFileName(clusterName, clusterUUID);
        return latestManifestFileName.map(s -> fetchRemoteClusterMetadataManifest(clusterName, clusterUUID, s));
    }

    public Optional<ClusterMetadataManifest> getClusterMetadataManifestByTermVersion(
        String clusterName,
        String clusterUUID,
        long term,
        long version
    ) {
        String prefix = RemoteManifestManager.getManifestFilePrefixForTermVersion(term, version);
        Optional<String> latestManifestFileName = getManifestFileNameByPrefix(clusterName, clusterUUID, prefix);
        return latestManifestFileName.map(s -> fetchRemoteClusterMetadataManifest(clusterName, clusterUUID, s));
    }

    public ClusterMetadataManifest getRemoteClusterMetadataManifestByFileName(String clusterUUID, String filename)
        throws IllegalStateException {
        try {
            RemoteClusterMetadataManifest remoteClusterMetadataManifest = new RemoteClusterMetadataManifest(
                filename,
                clusterUUID,
                compressor,
                namedXContentRegistry
            );
            return manifestBlobStore.read(remoteClusterMetadataManifest);
        } catch (IOException e) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Error while downloading cluster metadata - %s", filename), e);
        }
    }

    /**
     * Fetch ClusterMetadataManifest from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    ClusterMetadataManifest fetchRemoteClusterMetadataManifest(String clusterName, String clusterUUID, String filename)
        throws IllegalStateException {
        try {
            String fullBlobName = getManifestFolderPath(clusterName, clusterUUID).buildAsString() + filename;
            RemoteClusterMetadataManifest remoteClusterMetadataManifest = new RemoteClusterMetadataManifest(
                fullBlobName,
                clusterUUID,
                compressor,
                namedXContentRegistry
            );
            return manifestBlobStore.read(remoteClusterMetadataManifest);
        } catch (IOException e) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Error while downloading cluster metadata - %s", filename), e);
        }
    }

    Map<String, ClusterMetadataManifest> getLatestManifestForAllClusterUUIDs(String clusterName, Set<String> clusterUUIDs) {
        Map<String, ClusterMetadataManifest> manifestsByClusterUUID = new HashMap<>();
        for (String clusterUUID : clusterUUIDs) {
            try {
                Optional<ClusterMetadataManifest> manifest = getLatestClusterMetadataManifest(clusterName, clusterUUID);
                manifest.ifPresent(clusterMetadataManifest -> manifestsByClusterUUID.put(clusterUUID, clusterMetadataManifest));
            } catch (Exception e) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "Exception in fetching manifest for clusterUUID: %s", clusterUUID),
                    e
                );
            }
        }
        return manifestsByClusterUUID;
    }

    private BlobContainer manifestContainer(String clusterName, String clusterUUID) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/manifest
        return blobStoreRepository.blobStore().blobContainer(getManifestFolderPath(clusterName, clusterUUID));
    }

    BlobPath getManifestFolderPath(String clusterName, String clusterUUID) {
        return RemoteClusterStateUtils.getClusterMetadataBasePath(blobStoreRepository, clusterName, clusterUUID)
            .add(RemoteClusterMetadataManifest.MANIFEST);
    }

    public TimeValue getMetadataManifestUploadTimeout() {
        return this.metadataManifestUploadTimeout;
    }

    private void setMetadataManifestUploadTimeout(TimeValue newMetadataManifestUploadTimeout) {
        this.metadataManifestUploadTimeout = newMetadataManifestUploadTimeout;
    }

    /**
     * Fetch ClusterMetadataManifest files from remote state store in order
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @param limit max no of files to fetch
     * @return all manifest file names
     */
    private List<BlobMetadata> getManifestFileNames(String clusterName, String clusterUUID, String filePrefix, int limit)
        throws IllegalStateException {
        try {

            /*
              {@link BlobContainer#listBlobsByPrefixInSortedOrder} will list the latest manifest file first
              as the manifest file name generated via {@link RemoteClusterStateService#getManifestFileName} ensures
              when sorted in LEXICOGRAPHIC order the latest uploaded manifest file comes on top.
             */
            return manifestContainer(clusterName, clusterUUID).listBlobsByPrefixInSortedOrder(
                filePrefix,
                limit,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            );
        } catch (IOException e) {
            throw new IllegalStateException("Error while fetching latest manifest file for remote cluster state", e);
        }
    }

    public static String getManifestFilePrefixForTermVersion(long term, long version) {
        return String.join(
            DELIMITER,
            RemoteClusterMetadataManifest.MANIFEST,
            RemoteStoreUtils.invertLong(term),
            RemoteStoreUtils.invertLong(version)
        ) + DELIMITER;
    }

    /**
     * Fetch latest ClusterMetadataManifest file from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return latest ClusterMetadataManifest filename
     */
    private Optional<String> getLatestManifestFileName(String clusterName, String clusterUUID) throws IllegalStateException {
        List<BlobMetadata> manifestFilesMetadata = getManifestFileNames(
            clusterName,
            clusterUUID,
            RemoteClusterMetadataManifest.MANIFEST + DELIMITER,
            1
        );
        if (manifestFilesMetadata != null && !manifestFilesMetadata.isEmpty()) {
            return Optional.of(manifestFilesMetadata.get(0).name());
        }
        logger.info("No manifest file present in remote store for cluster name: {}, cluster UUID: {}", clusterName, clusterUUID);
        return Optional.empty();
    }

    private Optional<String> getManifestFileNameByPrefix(String clusterName, String clusterUUID, String filePrefix)
        throws IllegalStateException {
        List<BlobMetadata> manifestFilesMetadata = getManifestFileNames(clusterName, clusterUUID, filePrefix, 1);
        if (manifestFilesMetadata != null && !manifestFilesMetadata.isEmpty()) {
            return Optional.of(manifestFilesMetadata.get(0).name());
        }
        logger.info("No manifest file present in remote store for cluster name: {}, cluster UUID: {}", clusterName, clusterUUID);
        return Optional.empty();
    }
}
