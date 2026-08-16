/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.deepsnapshot.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.lucene.util.IOUtils;
import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.readerengine.lazydirectory.LazyBundleDirectory;
import org.opensearch.serverless.storage.retention.action.SnapshotPinAction;
import org.opensearch.serverless.storage.retention.action.SnapshotPinRequest;
import org.opensearch.serverless.storage.retention.action.SnapshotPinResponse;
import org.opensearch.serverless.storage.retention.action.SnapshotReleaseAction;
import org.opensearch.serverless.storage.retention.action.SnapshotReleaseRequest;
import org.opensearch.serverless.storage.security.RestrictingBlobContainer;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

/**
 * The actual work behind {@link ShardDeepSnapshotAction}, and the wiring
 * {@code DeepSnapshotOrchestrationIT} proved by hand before this class existed to do it as a real
 * transport action: pin the generation, build a {@link Store} over a {@link LazyBundleDirectory},
 * hand its {@link IndexCommit} to {@link Repository#snapshotShard}, and release the pin whether the
 * copy succeeded or not.
 *
 * <p>Requires no routing to a specific data node -- see {@link
 * org.opensearch.serverless.storage.retention.action.TransportSnapshotPinAction}'s own javadoc for
 * why: every input this reads comes from the object store or the target repository, never from
 * node-local shard state, so whichever node receives this request can execute it.
 *
 * <p>Dispatched onto {@link ThreadPool.Names#GENERIC}: the pin, the manifest read, and the copy
 * itself are all real I/O, and {@code repository.snapshotShard} blocks its calling thread on a
 * {@link PlainActionFuture} the same way {@code DeepSnapshotOrchestrationIT} does.
 */
public class TransportShardDeepSnapshotAction extends HandledTransportAction<ShardDeepSnapshotRequest, ShardDeepSnapshotResponse> {

    private static final Logger logger = LogManager.getLogger(TransportShardDeepSnapshotAction.class);

    /**
     * How long a scratch {@link TransferManager} block cache holds for the duration of one shard's copy.
     *
     * <p>Bounded and short-lived, unlike the shared reader-shard cache {@link
     * org.opensearch.serverless.storage.readerengine.lazydirectory.ServerlessStorageLazyDirectoryFactory}
     * uses: a deep snapshot reads every file in the commit exactly once (the opposite access pattern
     * from a query, which is what {@code BundleBackedCommitIsCopyableTests}' own javadoc notes), so
     * there is nothing here that benefits from surviving past this one request, and building a
     * dedicated cache keeps this action independent of whether the node's shared lazy-directory cache
     * is even configured -- {@code serverless_storage.lazy_directory.cache_size} defaults to zero.
     */
    private static final long SCRATCH_CACHE_BYTES = 64L * 1024 * 1024;

    /** How long one shard's copy may run before this action gives up on it. Copying is O(bytes), not O(shards). */
    private static final TimeValue COPY_TIMEOUT = TimeValue.timeValueMinutes(30);

    private final ServerlessStoragePlugin plugin;
    private final RepositoriesService repositoriesService;
    private final NodeClient client;
    private final ThreadPool threadPool;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves the request's shard {@link BlobContainer}.
     * @param repositoriesService resolves the request's target {@link Repository} by name.
     * @param client dispatches the pin and release calls this action wraps its copy in.
     * @param threadPool dispatches the pin/copy/release sequence off the transport thread.
     */
    @Inject
    public TransportShardDeepSnapshotAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        RepositoriesService repositoriesService,
        NodeClient client,
        ThreadPool threadPool
    ) {
        super(ShardDeepSnapshotAction.NAME, transportService, actionFilters, ShardDeepSnapshotRequest::new);
        this.plugin = plugin;
        this.repositoriesService = repositoriesService;
        this.client = client;
        this.threadPool = threadPool;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the shard, the target repository, and the snapshot identity to copy under.
     * @param listener notified with the shard generation the repository reports, or the failure.
     */
    @Override
    protected void doExecute(Task task, ShardDeepSnapshotRequest request, ActionListener<ShardDeepSnapshotResponse> listener) {
        // Pinned under a name derived from the snapshot uuid rather than the caller's own
        // snapshotName, so a deep copy can never collide with a shallow pin the caller (or PITR)
        // took under the plain snapshot name on the same shard.
        String pinId = "deep-" + request.snapshotUuid();
        // Not dispatched onto GENERIC here: doExecute already runs off the transport thread the way
        // every HandledTransportAction's does, and firing an async client.execute call from it is
        // the ordinary, non-blocking pattern TransportIndexSnapshotPinAction itself uses directly.
        // The bug this replaces did dispatch onto GENERIC and then blocked with actionGet() waiting
        // on this exact call -- which TransportSnapshotPinAction ALSO dispatches onto GENERIC
        // internally, so under load every GENERIC thread this action's own callers occupied while
        // blocked was a thread the pin call needed to ever run at all. Found by a real hang: the
        // gated arm of IndexDeepSnapshotActionIT tripped the internalClusterTest suite's 20-minute
        // timeout rather than failing, with every thread in the resulting dump parked waiting on a
        // queue -- a live deadlock, not a slow test.
        client.execute(
            SnapshotPinAction.INSTANCE,
            new SnapshotPinRequest(request.indexUuid(), request.shardId(), pinId),
            ActionListener.wrap(pin -> threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                // The heavy, genuinely blocking work -- Lucene reads, checksums, and a wait on
                // the repository's own async snapshotShard -- runs here instead, on a thread this
                // action dispatched for itself rather than one a listener callback merely
                // happened to complete on. copyPinnedGeneration's own blocking waits are on the
                // repository's SNAPSHOT-pool work, a different pool from GENERIC, so there is no
                // repeat of the same self-wait this replaces.
                try {
                    listener.onResponse(copyPinnedGeneration(request, pin));
                } catch (Exception e) {
                    listener.onFailure(e);
                } finally {
                    // Released on both paths, per the plan's own instruction. SnapshotReleaseAction's
                    // removePin is idempotent, so a release racing a retry of this same request costs
                    // nothing beyond the round trip. Non-blocking, so it adds no further nesting risk.
                    releasePin(request.indexUuid(), request.shardId(), pinId);
                }
            }), listener::onFailure)
        );
    }

    private ShardDeepSnapshotResponse copyPinnedGeneration(ShardDeepSnapshotRequest request, SnapshotPinResponse pin) throws Exception {
        // Least-privilege scoping (rfc-serverless-opensearch.md &sect;15): this action only ever
        // reads bundle bytes out of the source container, so it is wrapped read-only the same way
        // TransportSnapshotPinAction wraps its own container delete-denied for a narrower reason.
        BlobContainer shardContainer = new RestrictingBlobContainer(
            plugin.blobContainerForDirectoryFactory(request.indexUuid(), request.shardId()),
            false,
            false
        );
        CommitManifest manifest = new BlobContainerManifestStore(shardContainer).readManifest(pin.primaryTerm(), pin.generation());

        Repository repository = repositoriesService.repository(request.repositoryName());
        ShardId shardId = new ShardId(request.indexName(), request.indexUuid(), request.shardId());
        IndexId indexId = new IndexId(request.indexName(), request.indexUuid());
        SnapshotId snapshotId = new SnapshotId(request.snapshotName(), request.snapshotUuid());
        // A fabricated IndexMetadata/IndexSettings: this node holds no shard of the index and no
        // IndexService to ask for one, and Store needs only the pieces every index carries (version,
        // uuid, shard count) rather than anything this specific index declared -- the same minimal
        // shape DeepSnapshotOrchestrationIT's own test-only IndexSettingsModule builds, reproduced
        // here with the production two-argument IndexSettings constructor instead.
        IndexMetadata fabricatedMetadata = IndexMetadata.builder(request.indexName())
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, request.indexUuid())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .build()
            )
            .build();
        IndexSettings indexSettings = new IndexSettings(fabricatedMetadata, Settings.EMPTY);

        FileCache scratchCache = FileCacheFactory.createConcurrentLRUFileCache(SCRATCH_CACHE_BYTES, 1);
        Path scratchDir = Files.createTempDirectory("serverless-deep-snapshot-" + request.shardId() + "-");
        try {
            // No live shard, no engine: the copy is driven from the manifest and the object store
            // alone, the same LazyBundleDirectory construction BundleBackedCommitIsCopyableTests
            // proved reads end to end. The lock this Store takes is a no-op the same way
            // IndexService's own remoteStoreLock is for a Store built off no NodeEnvironment shard
            // path -- there is no local directory here for a real ShardLock to protect.
            ShardLock shardLock = new ShardLock(shardId) {
                @Override
                protected void closeInternal() {}
            };
            // Not part of the try-with-resources below: TransferManager holds no closeable resource
            // of its own (it reads through fileCache and shardContainer, both owned elsewhere), only
            // the directory and store built around it do.
            TransferManager transferManager = new TransferManager(
                new BlobContainerBundleStore(shardContainer)::openRange,
                scratchCache,
                threadPool
            );
            try (
                MMapDirectory cacheDirectory = new MMapDirectory(scratchDir, SimpleFSLockFactory.INSTANCE);
                LazyBundleDirectory lazyDirectory = new LazyBundleDirectory(manifest, cacheDirectory, transferManager);
                Store store = new Store(shardId, indexSettings, lazyDirectory, shardLock)
            ) {
                // Listed from the store's own directory, not the raw lazy one -- Store#getMetadata
                // asserts identity equality between the two, which is what stops a snapshot copying
                // files from one directory while describing another. See
                // DeepSnapshotOrchestrationIT's own comment at the equivalent line for why this
                // assertion exists and is worth keeping rather than working around.
                IndexCommit commit = DirectoryReader.listCommits(store.directory()).get(0);
                IndexShardSnapshotStatus status = IndexShardSnapshotStatus.newInitializing(null);
                PlainActionFuture<String> copied = PlainActionFuture.newFuture();
                store.incRef();
                try {
                    repository.snapshotShard(
                        store,
                        null,
                        snapshotId,
                        indexId,
                        commit,
                        null,
                        status,
                        Version.CURRENT,
                        Collections.emptyMap(),
                        copied
                    );
                    String shardGeneration = copied.actionGet(COPY_TIMEOUT);
                    return new ShardDeepSnapshotResponse(shardGeneration);
                } finally {
                    store.decRef();
                }
            }
        } finally {
            scratchCache.clear();
            try {
                IOUtils.rm(scratchDir);
            } catch (Exception e) {
                // Best effort: a leftover scratch directory under the JVM temp root costs disk, not
                // correctness, and must not be allowed to turn a successful copy into a failure.
                logger.warn("could not remove deep-snapshot scratch directory [{}]", scratchDir, e);
            }
        }
    }

    private void releasePin(String indexUuid, int shardId, String pinId) {
        client.execute(
            SnapshotReleaseAction.INSTANCE,
            new SnapshotReleaseRequest(indexUuid, shardId, pinId),
            ActionListener.wrap(
                response -> {},
                e -> logger.warn("could not release deep-snapshot pin [{}] on shard [{}/{}]", pinId, indexUuid, shardId, e)
            )
        );
    }
}
