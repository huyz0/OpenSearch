/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.migration.action;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexService;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.indices.IndicesService;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.migration.ClassicIndexMigrator;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * The actual work behind {@link MigrateShardAction}: resolves the real, currently-open {@code
 * IndexShard} named by the request via {@link IndicesService} -- the exact seam {@link
 * org.opensearch.serverless.storage.migration.ClassicIndexMigrator}'s own javadoc named as
 * deliberately not built yet -- reads its current committed {@link Directory}/{@link
 * SegmentInfos} and sequencing metadata straight off {@link Store} (the same {@code
 * segmentInfos.userData} fields {@code ObjectStoreWriterEngine#commitIndexWriter} reads, not the
 * shard's live in-memory state, which could have advanced past what this specific commit actually
 * covers), and delegates to {@link ClassicIndexMigrator#migrate} unchanged.
 *
 * <p>Requires routing to the specific node actually hosting this shard's live, locally-recovered
 * copy, same "the caller already knows which node to ask" contract as {@code PollNowAction}/{@code
 * WaitForGenerationAction} -- {@link IndicesService#indexServiceSafe} throws a clear {@code
 * IndexNotFoundException} if this node doesn't have it.
 *
 * <p>Dispatched onto {@link ThreadPool.Names#GENERIC}, not run on the transport thread directly:
 * real blob-store I/O (a bundle upload, a manifest write, and the head CAS), same reasoning as
 * {@code TransportShardSplitAction}'s own dispatch.
 *
 * <p><b>Requires the index already be write-blocked</b> ({@code index.blocks.write=true}) --
 * unlike {@link org.opensearch.serverless.storage.migration.ClassicIndexMigrator}, this action
 * reads a real, currently-open shard's <em>current</em> committed state, with no fencing of
 * concurrent indexing on that shard. A document indexed/committed after this action's snapshot
 * read would otherwise be silently absent from the adopted manifest while the response still
 * reports success. Refusing outright when the index isn't already quiesced makes that a loud,
 * caller-visible precondition failure instead of a silent data-loss gap.
 *
 * <p><b>Deliberately out of scope</b> (rfc-serverless-opensearch.md &sect;16 Phase 6's own status
 * note): reading a classic repository's or remote-store's own on-disk metadata format directly, as
 * an alternative that could avoid requiring the shard be locally recovered first. That is real,
 * separate future work depending on those formats' internals -- this action only ever adopts a
 * shard that is already open and recoverable on this node, exactly as {@link
 * ClassicIndexMigrator}'s own javadoc scopes it.
 */
public class TransportMigrateShardAction extends HandledTransportAction<MigrateShardRequest, MigrateShardResponse> {

    private final ServerlessStoragePlugin plugin;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final ThreadPool threadPool;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves the target shard's own {@link BlobContainer}.
     * @param clusterService resolves the request's {@code indexUuid} against a real {@link IndexMetadata}.
     * @param indicesService resolves the real, currently-open {@link IndexShard} to migrate.
     * @param threadPool dispatches the actual migration off the transport thread.
     */
    @Inject
    public TransportMigrateShardAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool
    ) {
        super(MigrateShardAction.NAME, transportService, actionFilters, MigrateShardRequest::new);
        this.plugin = plugin;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.threadPool = threadPool;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the shard to migrate, by its own real {@code (indexUuid, shardId)}.
     * @param listener notified with the result once the migration (dispatched off-thread) completes.
     */
    @Override
    protected void doExecute(Task task, MigrateShardRequest request, ActionListener<MigrateShardResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                IndexMetadata indexMetadata = findByUuid(clusterService.state().metadata(), request.indexUuid());
                if (indexMetadata == null) {
                    throw new IllegalArgumentException("index [" + request.indexUuid() + "] does not exist");
                }
                // indexService(Index) (nullable), not indexServiceSafe (throws a raw core
                // IndexNotFoundException): the index existing in cluster state doesn't mean THIS
                // node hosts it -- this is the same "caller already knows which node to ask"
                // single-node-routing contract PollNowAction/WaitForGenerationAction already use,
                // so a request that lands on the wrong node needs a clear, actionable error, not an
                // opaque exception a caller could easily mistake for the index truly not existing.
                IndexService indexService = indicesService.indexService(indexMetadata.getIndex());
                if (indexService == null) {
                    throw new IllegalArgumentException(
                        "index [" + request.indexUuid() + "] is not hosted on this node; route this request to a node that hosts it"
                    );
                }
                IndexShard shard = indexService.getShardOrNull(request.shardId());
                if (shard == null) {
                    throw new IllegalArgumentException(
                        "shard ["
                            + request.shardId()
                            + "] of index ["
                            + request.indexUuid()
                            + "] is not hosted on this node; route this request to a node that hosts it"
                    );
                }
                // This reads a snapshot of the shard's CURRENT committed state (below) with no
                // fencing of concurrent indexing -- a document indexed/committed after that read
                // is silently never captured in the adopted manifest. Requiring the index already
                // be write-blocked (the operator's own explicit "quiesce first" step, the same
                // "index.blocks.write" any other admin action would set before a similar
                // point-in-time operation) closes that silent-data-loss window: refuse outright
                // rather than let a caller who forgot to quiesce first get a false "success".
                if (IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(indexMetadata.getSettings()) == false) {
                    throw new IllegalStateException(
                        "index ["
                            + request.indexUuid()
                            + "] is not write-blocked -- migration snapshots the shard's current commit with no "
                            + "fencing of concurrent indexing, so writes after the snapshot would be silently lost; "
                            + "set index.blocks.write=true first to quiesce the index, then retry"
                    );
                }

                Store store = shard.store();
                store.incRef();
                try {
                    SegmentInfos segmentInfos = store.readLastCommittedSegmentsInfo();
                    Directory directory = store.directory();
                    long primaryTerm = shard.getOperationPrimaryTerm();
                    long maxSeqNo = Long.parseLong(segmentInfos.userData.get(SequenceNumbers.MAX_SEQ_NO));
                    long localCheckpoint = Long.parseLong(segmentInfos.userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));

                    BlobContainer container = plugin.blobContainerForDirectoryFactory(request.indexUuid(), request.shardId());
                    ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
                        new BlobContainerBundleStore(container),
                        new BlobContainerManifestStore(container)
                    );
                    ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);

                    ClassicIndexMigrator.migrate(
                        directory,
                        segmentInfos,
                        request.indexUuid(),
                        request.shardId(),
                        primaryTerm,
                        maxSeqNo,
                        localCheckpoint,
                        commitPublisher,
                        shardStateStore
                    );
                    listener.onResponse(new MigrateShardResponse(true));
                } finally {
                    store.decRef();
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private static IndexMetadata findByUuid(Metadata metadata, String indexUuid) {
        for (IndexMetadata indexMetadata : metadata.indices().values()) {
            if (indexUuid.equals(indexMetadata.getIndexUUID())) {
                return indexMetadata;
            }
        }
        return null;
    }
}
