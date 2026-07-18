/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.util.List;

/**
 * The actual work behind {@link OrchestrateShardSplitAction} -- see that class's own javadoc for
 * the full resumability/idempotency design rationale. Every stage is dispatched through {@link
 * Client#execute}, the same "compose existing actions, don't reimplement their logic" shape this
 * avoids duplicating any of {@link ProvisionSplitTargetsAction}'s, {@link ShardSplitAction}'s,
 * {@link CutoverSplitRoutingAction}'s, or {@link EnableWritePartitionRoutingAction}'s own
 * validation and cluster-state-mutation logic.
 *
 * <p><b>WARNING -- the source index is fenced only from cutover onward, not for the whole
 * operation.</b> {@link ShardSplitAction} clones the source's object-store state as of a single
 * point-in-time generation; nothing in this class (or in {@link CutoverSplitRoutingAction} /
 * {@link EnableWritePartitionRoutingAction} upstream of it) stops the source index from continuing
 * to accept writes under its own original name before or during that clone point and the stages
 * that follow it. A document written to the source in that window is captured only by the source,
 * never by any split target -- it will not appear through the new cutover alias. <b>Once cutover
 * succeeds, this class now fences the source automatically</b> (see {@link FenceSplitSourceAction},
 * called immediately after {@link CutoverSplitRoutingAction} below): a further direct write against
 * the source's own name is rejected by {@link
 * org.opensearch.serverless.storage.resharding.WritePartitionRoutingActionFilter}, so the
 * previously permanently-open "keep writing to the source forever, silently diverging, until {@link
 * RetireShrinkSourceAction} eventually deletes it" gap is now closed from cutover onward. <b>What
 * remains open, and must still be a caller/operator precondition</b>: the earlier window, between
 * the split's clone point and cutover actually completing. Closing that fully needs either true
 * write-blocking synchronized with the clone itself or a dual-write bridge mirroring writes to both
 * source and targets until cutover -- both remain a genuinely new, separate mechanism, out of scope
 * here. See rfc-serverless-opensearch.md &sect;16 Phase 4 for the full design-level discussion.
 */
public class TransportOrchestrateShardSplitAction extends HandledTransportAction<
    OrchestrateShardSplitRequest,
    OrchestrateShardSplitResponse> {

    private final ClusterService clusterService;
    private final Client client;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param clusterService reads cluster state to validate the source and resolve target index UUIDs.
     * @param client dispatches every chained stage as an ordinary {@link Client#execute} call.
     */
    @Inject
    public TransportOrchestrateShardSplitAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client
    ) {
        super(OrchestrateShardSplitAction.NAME, transportService, actionFilters, OrchestrateShardSplitRequest::new);
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, OrchestrateShardSplitRequest request, ActionListener<OrchestrateShardSplitResponse> listener) {
        String sourceIndexName = request.sourceIndexName();
        IndexMetadata sourceMetadata = clusterService.state().metadata().index(sourceIndexName);
        if (sourceMetadata == null) {
            listener.onFailure(new IllegalArgumentException("source index [" + sourceIndexName + "] does not exist"));
            return;
        }
        // ProvisionSplitTargetsAction/ShardSplitAction both already commit to a fixed
        // number_of_shards=1 per target -- a multi-shard source has no defined mapping onto that
        // target shape, so this is refused explicitly rather than guessed at.
        if (sourceMetadata.getNumberOfShards() != 1) {
            listener.onFailure(
                new IllegalArgumentException(
                    "source index ["
                        + sourceIndexName
                        + "] has "
                        + sourceMetadata.getNumberOfShards()
                        + " shard(s); orchestrated split only supports a single-shard source"
                )
            );
            return;
        }

        provisionStage(request, sourceMetadata.getIndex().getUUID(), listener);
    }

    private void provisionStage(
        OrchestrateShardSplitRequest request,
        String sourceIndexUuid,
        ActionListener<OrchestrateShardSplitResponse> listener
    ) {
        List<String> targetIndexNames = request.targetIndexNames();
        int existingTargetCount = 0;
        for (String targetIndexName : targetIndexNames) {
            if (clusterService.state().metadata().index(targetIndexName) != null) {
                existingTargetCount++;
            }
        }
        if (existingTargetCount == targetIndexNames.size()) {
            // Every target already exists -- resumed retry after a prior provision succeeded.
            splitStage(request, sourceIndexUuid, 0, listener);
            return;
        }
        if (existingTargetCount > 0) {
            listener.onFailure(
                new IllegalStateException(
                    "only "
                        + existingTargetCount
                        + " of "
                        + targetIndexNames.size()
                        + " target index(es) for source ["
                        + request.sourceIndexName()
                        + "] exist -- refusing to guess whether this is a partial prior "
                        + "provisioning attempt or a real naming conflict; resolve manually before retrying"
                )
            );
            return;
        }
        client.execute(
            ProvisionSplitTargetsAction.INSTANCE,
            new ProvisionSplitTargetsRequest(request.sourceIndexName(), targetIndexNames),
            ActionListener.wrap(response -> splitStage(request, sourceIndexUuid, 0, listener), listener::onFailure)
        );
    }

    private void splitStage(
        OrchestrateShardSplitRequest request,
        String sourceIndexUuid,
        int partitionIndex,
        ActionListener<OrchestrateShardSplitResponse> listener
    ) {
        List<String> targetIndexNames = request.targetIndexNames();
        if (partitionIndex == targetIndexNames.size()) {
            cutoverStage(request, listener);
            return;
        }
        String targetIndexName = targetIndexNames.get(partitionIndex);
        IndexMetadata targetMetadata = clusterService.state().metadata().index(targetIndexName);
        if (targetMetadata == null) {
            listener.onFailure(
                new IllegalStateException("target index [" + targetIndexName + "] does not exist after provisioning -- cannot split")
            );
            return;
        }
        client.execute(
            ShardSplitAction.INSTANCE,
            new ShardSplitRequest(sourceIndexUuid, 0, targetMetadata.getIndex().getUUID(), 0, partitionIndex, targetIndexNames.size()),
            ActionListener.wrap(response -> splitStage(request, sourceIndexUuid, partitionIndex + 1, listener), e -> {
                // A resumed retry re-splitting an already-split target hits one of two "already
                // done" conditions, depending on exactly how far the prior attempt got before
                // failing: ShardCloner#clone's own explicit refusal to CAS onto an already-
                // published head ("already has a published head"), or -- since clone lineage is
                // written *before* that CAS (see ShardCloner#clone's own javadoc for why: "so
                // whenever a clone is visible, deleteClone can already find its way back to the
                // pin it must remove") -- the underlying blob store's own atomic-write refusal to
                // overwrite an already-written lineage blob ("already exists"). Both are treated
                // as "already done," not a failure, the same resumability every other stage here
                // has.
                if (isAlreadyDoneSplit(e)) {
                    splitStage(request, sourceIndexUuid, partitionIndex + 1, listener);
                } else {
                    listener.onFailure(e);
                }
            })
        );
    }

    private void cutoverStage(OrchestrateShardSplitRequest request, ActionListener<OrchestrateShardSplitResponse> listener) {
        client.execute(
            CutoverSplitRoutingAction.INSTANCE,
            new CutoverSplitRoutingRequest(request.routingAliasName(), request.targetIndexNames()),
            ActionListener.wrap(response -> fenceSourceStage(request, listener), listener::onFailure)
        );
    }

    /**
     * Fences the source immediately once cutover has actually landed -- narrowing (not eliminating,
     * see {@link org.opensearch.serverless.storage.resharding.SourceSplitFenceMetadata}'s own
     * javadoc for the honest limitation) the "source keeps silently accepting writes forever" gap
     * this class's own javadoc used to only be able to warn operators about, not close.
     */
    private void fenceSourceStage(OrchestrateShardSplitRequest request, ActionListener<OrchestrateShardSplitResponse> listener) {
        client.execute(
            FenceSplitSourceAction.INSTANCE,
            new FenceSplitSourceRequest(request.sourceIndexName(), request.routingAliasName()),
            ActionListener.wrap(response -> writeRoutingStage(request, listener), listener::onFailure)
        );
    }

    private void writeRoutingStage(OrchestrateShardSplitRequest request, ActionListener<OrchestrateShardSplitResponse> listener) {
        if (request.enableWriteRouting() == false) {
            listener.onResponse(new OrchestrateShardSplitResponse(true, true, true, true, false));
            return;
        }
        client.execute(
            EnableWritePartitionRoutingAction.INSTANCE,
            new EnableWritePartitionRoutingRequest(request.routingAliasName(), request.targetIndexNames()),
            ActionListener.wrap(
                response -> listener.onResponse(new OrchestrateShardSplitResponse(true, true, true, true, true)),
                listener::onFailure
            )
        );
    }

    /**
     * Whether a failed {@link ShardSplitAction} call represents a target that some prior attempt
     * already split, not a genuine new failure.
     *
     * <p><b>Known limitation, not a hidden one</b>: {@code ShardCloner#clone} writes a target's
     * clone lineage blob *before* CAS-ing its head (see that method's own javadoc: "so whenever a
     * clone is visible, deleteClone can already find its way back to the pin it must remove"). If
     * a prior attempt crashed in the narrow window between those two writes, this treats the
     * target as already-done even though its head was never actually published -- the same
     * "logical-first, not a fully transactional multi-step primitive" shape {@code
     * ShardSplitter}/{@code ShardCloner} already have everywhere else in this plugin, not a new gap
     * this orchestration action introduces. A future strict-verification pass could check the
     * target's head really is published (not just that lineage exists) before treating this as
     * resumable; not attempted here.
     */
    private static boolean isAlreadyDoneSplit(Exception e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            String message = cause.getMessage();
            if (message != null && (message.contains("already has a published head") || message.contains("already exists"))) {
                return true;
            }
        }
        return false;
    }
}
