/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.compaction.CompactionPolicy;
import org.opensearch.serverless.storage.compaction.CompactionRebaseExecutor;
import org.opensearch.serverless.storage.compaction.CompactionSchedulerTask;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.util.IndexMetadataUuidIndex;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * The actual work behind {@link CompactionTriggerAction}: builds the same shape of stores {@link
 * CompactionSchedulerTask}'s own background schedule already needs, from {@link
 * ServerlessStoragePlugin#blobContainerForDirectoryFactory} -- exactly as {@link
 * org.opensearch.serverless.storage.clone.action.TransportShardCloneAction} already does for
 * clone -- then delegates to the same {@code maybeCompact} logic the scheduled task uses, so a
 * one-off trigger and the recurring background tick can never drift apart.
 *
 * <p>Requires no routing to a specific data node: the compaction attempt operates purely against
 * the shared object store via {@link BlobContainer}s and the shard's CAS-guarded head, never
 * against any node-local shard state (not even a live writer's lease, which
 * {@code CompactionSchedulerTask}'s own javadoc explains this deliberately does not gate on), so
 * whichever node receives this request can execute it directly.
 *
 * <p>Dispatched onto {@link ThreadPool.Names#GENERIC}, not run on the transport thread directly:
 * a compaction attempt does real blob-store I/O (reads the manifest, potentially opens and merges
 * segments, and CASes a new head), which must never block a transport/network thread.
 *
 * <p><b>Resolves the request's {@code indexUuid} against cluster metadata before touching the
 * object store</b>, exactly as {@code TransportShardSplitAction} and {@code
 * TransportMigrateShardAction} already do. {@link
 * ServerlessStoragePlugin#blobContainerForDirectoryFactory} builds a blob path out of whatever
 * string it is handed ({@code BlobPath.cleanPath().add(indexUuid)}), and the underlying store
 * resolves path segments without normalising them -- so an unvalidated uuid is a path the caller
 * chooses, not a shard the caller owns. Requiring the uuid to name a real, currently-existing index
 * makes the check "this names a shard that exists" rather than the far weaker "this string looks
 * harmless".
 */
public class TransportCompactionTriggerAction extends HandledTransportAction<CompactionTriggerRequest, CompactionTriggerResponse> {

    /** Bounded rebase-and-retry budget for a one-off trigger, matching this plugin's other CAS-retry defaults. */
    private static final int MAX_REBASE_ATTEMPTS = 5;

    private final ServerlessStoragePlugin plugin;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    // A real Guice singleton (see this constructor's own @Inject), so doExecute can use this
    // concurrently across different in-flight requests -- IndexMetadataUuidIndex's own volatile-field
    // cache is documented safe under exactly that access pattern. Same field, same reasoning, as
    // TransportShardSplitAction's own copy.
    private final IndexMetadataUuidIndex uuidIndex = new IndexMetadataUuidIndex();

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves each request's shard {@link BlobContainer}.
     * @param threadPool dispatches the actual compaction attempt off the transport thread.
     * @param clusterService resolves whether the requested {@code (indexUuid, shardId)} is a real shard.
     */
    @Inject
    public TransportCompactionTriggerAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        super(CompactionTriggerAction.NAME, transportService, actionFilters, CompactionTriggerRequest::new);
        this.plugin = plugin;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    /**
     * Resolves {@code indexUuid} to a real index in {@code metadata}, and {@code shardId} to a real
     * shard of it, or throws.
     *
     * <p>The same resolution {@code TransportShardSplitAction#requireRealShard} performs, and the
     * same error-message shape, deliberately: a uuid that names nothing is reported as "does not
     * exist" rather than being passed through to build a blob path out of.
     *
     * <p>The shard bound accepts an id at or beyond {@code getNumberOfShards()} when the index's
     * split metadata knows it: an in-place split reserves child shard ids past the base range
     * <em>without</em> growing {@code number_of_shards} (see {@code
     * IndexMetadata#inSyncAllocationIds}), and a split child is a perfectly ordinary compaction
     * target. Rejecting on the base count alone would have made this refuse real shards.
     *
     * @throws IllegalArgumentException if {@code indexUuid} doesn't resolve to a real,
     *                                  currently-existing index, or {@code shardId} isn't one of its
     *                                  shards.
     */
    // Visible for testing: pure metadata resolution, exercisable without a plugin or a blob store.
    static void requireRealShard(IndexMetadataUuidIndex uuidIndex, Metadata metadata, String indexUuid, int shardId) {
        IndexMetadata indexMetadata = uuidIndex.findByUuid(metadata, indexUuid);
        if (indexMetadata == null) {
            throw new IllegalArgumentException("index [" + indexUuid + "] does not exist");
        }
        if (shardId >= indexMetadata.getNumberOfShards() && indexMetadata.getSplitShardsMetadata().getRangeOfShard(shardId) == null) {
            throw new IllegalArgumentException(
                "shard ["
                    + shardId
                    + "] is out of bounds for index ["
                    + indexMetadata.getIndex().getName()
                    + "]["
                    + indexUuid
                    + "], which has "
                    + indexMetadata.getNumberOfShards()
                    + " shard(s)"
            );
        }
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the shard to attempt compaction on.
     * @param listener notified with the result once the attempt (dispatched off-thread) completes.
     */
    @Override
    protected void doExecute(Task task, CompactionTriggerRequest request, ActionListener<CompactionTriggerResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                requireRealShard(uuidIndex, clusterService.state().metadata(), request.indexUuid(), request.shardId());

                // Credential scoping per tier (rfc-serverless-opensearch.md &sect;15): the
                // compaction service needs GET+PUT but no DELETE (deletion stays with GC) --
                // CompactionSchedulerTask#maybeCompact never calls delete through any of the
                // stores built here, so wrapping in RestrictingBlobContainer is a safe, real
                // application of the same defense-in-depth ServerlessStoragePlugin#getEngineFactory
                // already applies to the scheduled compaction path.
                BlobContainer container = new org.opensearch.serverless.storage.security.RestrictingBlobContainer(
                    plugin.blobContainerForDirectoryFactory(request.indexUuid(), request.shardId()),
                    false
                );
                BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(container);
                ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);
                ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(container));
                ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
                    new BlobContainerBundleStore(container),
                    manifestStore
                );
                CompactionPolicy policy = CompactionPolicy.withDefaults();
                CompactionRebaseExecutor rebaseExecutor = new CompactionRebaseExecutor(shardStateStore, MAX_REBASE_ATTEMPTS);

                boolean attempted = CompactionSchedulerTask.maybeCompact(
                    request.indexUuid(),
                    request.shardId(),
                    shardStateStore,
                    manifestStore,
                    materializer,
                    commitPublisher,
                    policy,
                    rebaseExecutor,
                    // Operator-triggered compaction stages its merge in the same place the scheduled
                    // one does; both are bounded by the node's data path, not the platform temp dir.
                    plugin.compactionMergeWorkRootForTrigger()
                );
                listener.onResponse(new CompactionTriggerResponse(attempted));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
