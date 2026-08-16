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
import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The actual work behind {@link IndexDeepSnapshotAction}: resolve the index (gated or ordinary),
 * copy each shard in turn via {@link ShardDeepSnapshotAction}, then finalize once this call is
 * itself already running on the cluster manager -- {@link TransportClusterManagerNodeAction}'s own
 * base machinery is what guarantees that, forwarding the request here if the node that first
 * received it was not the cluster manager.
 *
 * <p>Sequential across shards, the same choice {@link
 * org.opensearch.serverless.storage.retention.action.TransportIndexSnapshotPinAction} makes and for
 * the same reason: it keeps "which shards actually finished" trivial to reason about, and this is an
 * infrequent, already-slow-relative-to-a-single-blob-write operation where the extra round trips are
 * not the cost that matters. True node-parallel dispatch -- {@link ShardDeepSnapshotAction} needs no
 * specific node, so nothing here stops it -- is a follow-on, not a correctness requirement.
 */
public class TransportIndexDeepSnapshotAction extends TransportClusterManagerNodeAction<
    IndexDeepSnapshotRequest,
    IndexDeepSnapshotResponse> {

    private static final Logger logger = LogManager.getLogger(TransportIndexDeepSnapshotAction.class);

    private final RepositoriesService repositoriesService;
    private final NodeClient client;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link TransportClusterManagerNodeAction} to register this action.
     * @param clusterService resolves the request's index name and reads cluster metadata for {@code finalizeSnapshot}.
     * @param threadPool used by {@link TransportClusterManagerNodeAction}'s own base machinery.
     * @param actionFilters applied by {@link TransportClusterManagerNodeAction} around every request.
     * @param indexNameExpressionResolver required by {@link TransportClusterManagerNodeAction}'s constructor, unused here.
     * @param repositoriesService resolves the request's target {@link Repository} by name.
     * @param client dispatches each shard's {@link ShardDeepSnapshotAction} call.
     */
    @Inject
    public TransportIndexDeepSnapshotAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        RepositoriesService repositoriesService,
        NodeClient client
    ) {
        super(
            IndexDeepSnapshotAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            IndexDeepSnapshotRequest::new,
            indexNameExpressionResolver
        );
        this.repositoriesService = repositoriesService;
        this.client = client;
    }

    @Override
    protected String executor() {
        // The real work is dispatched by TransportShardDeepSnapshotAction (GENERIC) and by the
        // repository's own async finalizeSnapshot/getRepositoryData -- this method only chains
        // listeners between them, the same SAME-executor choice
        // TransportEnableWritePartitionRoutingAction makes for the equivalent reason.
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(IndexDeepSnapshotRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected IndexDeepSnapshotResponse read(StreamInput in) throws IOException {
        return new IndexDeepSnapshotResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        IndexDeepSnapshotRequest request,
        ClusterState state,
        ActionListener<IndexDeepSnapshotResponse> listener
    ) {
        // Resolved through the descriptor supplier as well as cluster state, the same gated-index
        // resolution item 1 of this round built for the shallow pin/restore/release trio --
        // metadata.index(name) alone answers null for a gated index, which reported
        // IndexNotFoundException for an index that exists, is serving traffic, and has manifests to
        // copy. Safe here: clusterManagerOperation runs on this action's own executor() (SAME,
        // dispatched off the transport thread by the outer transport layer already), not on the
        // cluster state applier thread AbsentIndexDescriptorSuppliers forbids a blocking read from.
        IndexMetadata indexMetadata = AbsentIndexDescriptorSuppliers.metadataOrDescriptor(state.metadata(), request.indexName());
        if (indexMetadata == null) {
            listener.onFailure(new IndexNotFoundException(request.indexName()));
            return;
        }

        Repository repository;
        try {
            repository = repositoriesService.repository(request.repositoryName());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        IndexId indexId = new IndexId(request.indexName(), indexMetadata.getIndexUUID());
        int numberOfShards = indexMetadata.getNumberOfShards();
        // Captured synchronously, before the getRepositoryData round trip below -- found by round 5 of
        // the bug hunt: round 4 moved this action's first async hop earlier (was previously only inside
        // finalizeSnapshot), and capturing startTime after that hop instead of before it would silently
        // exclude the round trip's own latency from SnapshotInfo's reported start time, understating how
        // long the deep snapshot actually took end to end.
        long startTime = System.currentTimeMillis();
        // Checked up front, before a single byte is copied, rather than left to surface however
        // Repository#finalizeSnapshot's own bookkeeping reacts to it -- found by round 4 of the bug
        // hunt. indexId above is built from the index's own real UUID, not resolved through {@link
        // RepositoryData#resolveNewIndices} the way a real _snapshot request resolves its own IndexId
        // (SnapshotsService does exactly that): ShardDeepSnapshotRequest's indexUuid field is
        // deliberately the real object-store UUID, doing double duty as this repository-facing
        // identity too (see that request's own javadoc), so this repository-facing IndexId cannot
        // simply be re-resolved independently without also re-plumbing how each shard locates its data
        // in the object store -- a larger change than this finding's severity currently justifies. If
        // this same target repository already holds a snapshot of an index with this same name under a
        // *different* repository-internal id (any snapshot -- ordinary or an earlier deep snapshot --
        // this index, or a since-deleted index of the same name, was ever written into this repository
        // under), RepositoryData's own indices-by-name map has no merge function for two different
        // IndexIds sharing one name -- finalizeSnapshot's downstream addSnapshot call would otherwise
        // throw IllegalStateException: Duplicate key partway through finalizing, after every shard's
        // bytes were already copied. Failing loudly here instead costs nothing (no shard has been
        // touched yet) and names the actual conflict rather than surfacing it as an opaque exception
        // deep in core's own bookkeeping. Checked again in finalizeSnapshot, against the RepositoryData
        // that check reads fresh for its own reasons -- see this action's own round-5 note there for
        // why checking only here leaves a real, if narrow, race open.
        //
        // Known, accepted limitation, not fixed here: an index deleted and recreated under the same
        // name permanently blocks every future deep snapshot of the new incarnation into any repository
        // that ever held a snapshot of the old one, since nothing here (or in finalizeSnapshot) ever
        // prunes the stale entry -- only deleting the old snapshot(s) from that repository through core's
        // own means clears it. The same structural cause as the collision this check exists to catch,
        // and the same "re-plumbing the wire protocol is disproportionate to this finding" reasoning
        // above applies to actually resolving it, so it is named here rather than silently hit later.
        repository.getRepositoryData(ActionListener.wrap(repositoryData -> {
            IndexId existing = repositoryData.getIndices().get(request.indexName());
            if (identityConflictsWithExistingSnapshot(existing, indexId)) {
                listener.onFailure(conflictException(request.repositoryName(), request.indexName(), existing, indexId));
                return;
            }
            CopyContext context = new CopyContext(
                indexId,
                new SnapshotId(request.snapshotName(), UUIDs.randomBase64UUID()),
                repository,
                numberOfShards,
                startTime,
                listener
            );
            copyShard(0, context, ShardGenerations.builder());
        }, listener::onFailure));
    }

    /**
     * True if {@code existing} -- whatever {@link org.opensearch.repositories.RepositoryData#getIndices()}
     * already has on file for this index name, or {@code null} if nothing does -- names a different
     * repository-internal identity than {@code candidate}, the one this deep snapshot is about to use.
     * Extracted as a small, pure predicate (rather than left inline) so both of its call sites' checks
     * (see {@link #clusterManagerOperation} and {@link #finalizeSnapshot}) share one definition, and so
     * a plain unit test can exercise it without a live repository -- see {@code
     * TransportIndexDeepSnapshotActionTests}.
     */
    static boolean identityConflictsWithExistingSnapshot(IndexId existing, IndexId candidate) {
        return existing != null && existing.getId().equals(candidate.getId()) == false;
    }

    /**
     * The exception both {@link #clusterManagerOperation} and {@link #finalizeSnapshot} raise when
     * {@link #identityConflictsWithExistingSnapshot} finds a real conflict -- one message, shared, so
     * the two call sites (a cheap up-front check and its narrower-window finalize-time repeat) cannot
     * drift into describing the same failure two different ways.
     */
    private static IllegalStateException conflictException(String repositoryName, String indexName, IndexId existing, IndexId candidate) {
        return new IllegalStateException(
            "repository ["
                + repositoryName
                + "] already holds a snapshot of index ["
                + indexName
                + "] under a different identity ("
                + existing
                + " vs "
                + candidate
                + ") -- deep snapshots of this index must target a repository that has never held a snapshot"
                + " of an index with this name under a different identity"
        );
    }

    /**
     * Everything one {@link #clusterManagerOperation} call carries through the whole recursive
     * shard-copy fan-out and into {@link #finalizeSnapshot} unchanged -- bundled after the bug hunt
     * that followed round 006 found the previous shape (8-11 positional parameters, most invariant
     * across every recursive call) made adding any new piece of per-request state touch two method
     * signatures and every call site, and made the {@code copyShard}-to-{@code finalizeSnapshot} call
     * a long positional-argument list a reviewer had to diff by eye. {@code shardGenerations} is
     * deliberately not a field here: it is the one thing that is genuinely per-call state (a mutable
     * builder accumulated across the recursion), not per-request context, and keeping it a separate
     * parameter says so.
     *
     * <p>{@code numberOfShards} lives here rather than being re-derived from an {@code IndexMetadata}
     * field, because this context deliberately carries no {@code IndexMetadata} at all -- {@link
     * #finalizeSnapshot} re-resolves it fresh (see that method's own javadoc for why) rather than
     * reusing one captured here, so the shard count this loop bounds itself by is the only piece of
     * that original resolution this record still needs.
     *
     * <p>No separate {@code repositoryName} field either -- found duplicated by round 3 of the bug
     * hunt: {@code repository} already carries its own name via {@code getMetadata().name()}, so the
     * one call site that needs it as a string ({@link #copyShard}, building each shard's request) reads
     * it from there instead of a second field that could drift from the {@code Repository} object next
     * to it.
     */
    private record CopyContext(IndexId indexId, SnapshotId snapshotId, Repository repository, int numberOfShards, long startTime,
        ActionListener<IndexDeepSnapshotResponse> listener) {
    }

    private void copyShard(int shardId, CopyContext context, ShardGenerations.Builder shardGenerations) {
        if (shardId == context.numberOfShards()) {
            finalizeSnapshot(context, shardGenerations.build());
            return;
        }
        client.execute(
            ShardDeepSnapshotAction.INSTANCE,
            new ShardDeepSnapshotRequest(
                context.indexId().getName(),
                context.indexId().getId(),
                shardId,
                context.repository().getMetadata().name(),
                context.snapshotId().getName(),
                context.snapshotId().getUUID()
            ),
            ActionListener.wrap(response -> {
                shardGenerations.put(context.indexId(), shardId, response.shardGeneration());
                copyShard(shardId + 1, context, shardGenerations);
            }, context.listener()::onFailure)
        );
    }

    /**
     * The bookkeeping that turns copied files into a snapshot, mirroring {@code
     * DeepSnapshotOrchestrationIT}'s own manual call exactly, with one difference: this reads {@link
     * RepositoryData#getGenId} fresh right before calling rather than caching it from an earlier
     * step, since any real gap between resolving the repository and finishing every shard's copy is
     * exactly the window a concurrent repository write could move the generation in.
     *
     * <p><b>What this deliberately does not do</b>, stated rather than discovered later: it does not
     * replicate {@code SnapshotsService}'s in-memory tracking of concurrent in-progress snapshots
     * against the same repository, so two deep snapshots into the same repository at the same moment
     * race on {@code repositoryStateId} the way any two direct {@code Repository#finalizeSnapshot}
     * callers outside that service would -- one loses with a version-conflict-shaped failure and must
     * be retried. Every action under {@code retention.action} already scopes itself out of core's
     * real {@code _snapshot} machinery for the same reason (see {@link
     * org.opensearch.serverless.storage.retention.action.SnapshotPinAction}'s own javadoc); a single
     * shipped deep snapshot at a time per repository is round 006's own scope, not a limitation
     * introduced here.
     *
     * <p><b>The gated-index NPE this found.</b> {@code BlobStoreRepository#finalizeSnapshot} writes
     * each named index's own metadata into the repository, looking it up as {@code
     * clusterMetadata.index(name)} -- which answers null for a gated index, the same absence every
     * other consumer in this plugin has had to learn to route around. Resolving {@code indexMetadata}
     * through {@code AbsentIndexDescriptorSuppliers} folded into a copy of cluster metadata is what
     * makes that lookup answer instead of NPEing -- found by {@code IndexDeepSnapshotActionIT}'s gated
     * case, not reasoned in advance, the same "probe, do not reason" pattern this round's own
     * predecessors document repeatedly.
     *
     * <p><b>Why {@code indexMetadata} is resolved here rather than reused from {@link
     * #clusterManagerOperation}.</b> Found by round 2 of the bug hunt that landed this whole action: a
     * sequential multi-shard copy can run long enough for a mapping update or index deletion to land
     * mid-flight, and reusing the metadata object {@code clusterManagerOperation} resolved before that
     * loop started would silently snapshot stale mappings/settings -- or, for a deleted index, publish
     * metadata for something that no longer exists. Re-resolving fresh here, right before it is folded
     * into {@code clusterMetadata}, narrows that window to as small as this method can make it, the
     * same reasoning this method already applies to {@code repositoryData.getGenId()} one line below.
     *
     * <p><b>The identity check round 3 of the bug hunt added.</b> A fresh resolution answers "does an
     * index named this still exist," not "is it the same index" -- an index deleted and a same-named
     * index created in its place mid-copy would resolve to a real, non-null {@code IndexMetadata} with
     * a different {@link IndexMetadata#getIndexUUID()}, and combining that new identity's metadata with
     * {@code shardGenerations} built against the original {@link CopyContext#indexId()} would finalize
     * a snapshot whose own metadata and shard-generation data describe two different indices. Checked
     * explicitly below rather than left to whatever inconsistency that would eventually surface as
     * downstream.
     *
     * <p><b>The shard-count check round 4 of the bug hunt added.</b> An in-place shard split (this
     * plugin's own {@code resharding} package) changes an index's shard count without changing its
     * UUID, so a split completing mid-copy would pass the identity check above -- three independent
     * finder angles converged on this exact gap across rounds 3 and 4, and while a full fix needs
     * either blocking resharding for the duration of a deep snapshot or discarding and restarting the
     * copy on a detected split (out of this round's scope, the same as the concurrent-repository-write
     * race already accepted above), turning the silent corruption into a loud, named failure is cheap
     * enough that leaving it merely documented was not: {@code shardGenerations} was built against
     * {@code context.numberOfShards()}, fixed at {@link #clusterManagerOperation} time, so it is
     * compared against the freshly-resolved {@code indexMetadata}'s current count below.
     *
     * <p><b>The repeated identity-conflict check round 5 of the bug hunt added.</b> {@link
     * #clusterManagerOperation}'s own check against {@link #identityConflictsWithExistingSnapshot} runs
     * once, before any shard is copied -- but the copy loop it guards can run long, and a concurrent
     * writer (an ordinary {@code _snapshot} call, or another deep snapshot) can write a conflicting
     * {@code IndexId} for this same index name into this repository during that window, which the
     * up-front check has no way to see. Re-running the identical check here, against the {@code
     * repositoryData} this method already re-fetches fresh for {@code getGenId()}'s sake, closes that
     * window down to the same size every other freshness re-check in this method already accepts, at no
     * extra repository-read cost.
     */
    private void finalizeSnapshot(CopyContext context, ShardGenerations shardGenerations) {
        Repository repository = context.repository();
        SnapshotId snapshotId = context.snapshotId();
        int numberOfShards = context.numberOfShards();
        ActionListener<IndexDeepSnapshotResponse> listener = context.listener();
        ClusterState clusterState = clusterService.state();
        IndexMetadata indexMetadata = AbsentIndexDescriptorSuppliers.metadataOrDescriptor(
            clusterState.metadata(),
            context.indexId().getName()
        );
        if (indexMetadata == null || indexMetadata.getIndexUUID().equals(context.indexId().getId()) == false) {
            // Either the index was deleted while this copy was in flight, or it was deleted and a
            // same-named index created in its place (a different UUID) -- either way, every shard's
            // bytes were successfully copied under the original identity, but there is nothing left (or
            // nothing matching) to describe in the finalized snapshot's own metadata, so this is
            // reported as a failure rather than finalizing with stale or mismatched data.
            listener.onFailure(new IndexNotFoundException(context.indexId().getName()));
            return;
        }
        if (indexMetadata.getNumberOfShards() != numberOfShards) {
            // An in-place shard split completed mid-copy -- shardGenerations only has entries for the
            // shard count this copy started with, so finalizing against the index's now-different
            // shard count would publish a snapshot whose own metadata and shard-generation data
            // disagree, discoverable only later, on restore. Failing now names the cause instead.
            listener.onFailure(
                new IllegalStateException(
                    "index ["
                        + context.indexId().getName()
                        + "] shard count changed from "
                        + numberOfShards
                        + " to "
                        + indexMetadata.getNumberOfShards()
                        + " while this deep snapshot was in flight -- retry after the split completes"
                )
            );
            return;
        }
        repository.getRepositoryData(ActionListener.wrap(repositoryData -> {
            IndexId existing = repositoryData.getIndices().get(context.indexId().getName());
            if (identityConflictsWithExistingSnapshot(existing, context.indexId())) {
                listener.onFailure(
                    conflictException(repository.getMetadata().name(), context.indexId().getName(), existing, context.indexId())
                );
                return;
            }
            // Metadata.builder(state) starts from the real cluster metadata rather than an empty one,
            // so a plain (non-gated) serverless index -- already present there -- is untouched, and
            // only the gated case gains the entry it was missing. indices(Map.of(...)) rather than
            // put(indexMetadata, false): put's identity short-circuit (indices.get(name) ==
            // indexMetadata) protects the ordinary-index case, since indexMetadata came straight out
            // of state.metadata() there, but for a gated index the resolved indexMetadata is a fresh
            // object every call, so the short-circuit never fires and put falls through to
            // publishDescriptorIfIncremental -- which unconditionally republishes the descriptor (and
            // appends a change-log entry) from metadata captured before the shard-copy loop ran,
            // silently clobbering a descriptor an operator may have legitimately changed since.
            // indices(Map) is a plain putAll with no publish hook either way, and does not bump the
            // metadata version: this Metadata is genuinely never published, only handed to one
            // repository call that reads it and discards it. Seeded from the same `clusterState`
            // already resolved above (not a second clusterService.state() read here) so the identity
            // check just performed and this Metadata are guaranteed built from one consistent snapshot.
            Metadata clusterMetadata = Metadata.builder(clusterState.metadata())
                .indices(Map.of(indexMetadata.getIndex().getName(), indexMetadata))
                .build();
            long endTime = System.currentTimeMillis();
            SnapshotInfo snapshotInfo = new SnapshotInfo(
                snapshotId,
                List.of(context.indexId().getName()),
                Collections.emptyList(),
                context.startTime(),
                null,
                endTime,
                numberOfShards,
                Collections.emptyList(),
                false,
                Collections.emptyMap(),
                // Deliberately false: this is the deep, byte-copying tier, the opposite of what this
                // flag names for core's own remote-store shallow snapshot feature.
                false
            );
            repository.finalizeSnapshot(
                shardGenerations,
                repositoryData.getGenId(),
                clusterMetadata,
                snapshotInfo,
                Version.CURRENT,
                state -> state,
                ActionListener.wrap(
                    finalizedData -> listener.onResponse(
                        new IndexDeepSnapshotResponse(snapshotId.getName(), snapshotId.getUUID(), numberOfShards)
                    ),
                    listener::onFailure
                )
            );
        }, e -> {
            logger.warn("could not read repository data to finalize deep snapshot [{}]", snapshotId, e);
            listener.onFailure(e);
        }));
    }
}
