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
 * never by any split target -- it will not appear through the new cutover alias. <b>When (and only
 * when) the caller asks for write routing, this class fences the source once that routing is
 * live</b> (see {@link FenceSplitSourceAction}, called from {@code fenceSourceStage} below, after
 * {@link EnableWritePartitionRoutingAction} rather than before it -- see {@code writeRoutingStage}'s
 * javadoc, finding R-7, for why fencing must never run on its own): a further direct write against
 * the source's own name is rejected by {@link
 * org.opensearch.serverless.storage.resharding.WritePartitionRoutingActionFilter}, so the
 * previously permanently-open "keep writing to the source forever, silently diverging, until {@link
 * RetireShrinkSourceAction} eventually deletes it" gap is now closed from cutover onward. <b>What
 * remains open, and must still be a caller/operator precondition</b>: the earlier window, between
 * the split's clone point and cutover actually completing. Closing that fully needs either true
 * write-blocking synchronized with the clone itself or a dual-write bridge mirroring writes to both
 * source and targets until cutover -- both remain a genuinely new, separate mechanism, out of scope
 * here. See rfc-serverless-opensearch.md &sect;16 Phase 4 for the full design-level discussion.
 *
 * <p><b>A failure reported after cutover does not mean cutover didn't happen.</b> {@link
 * CutoverSplitRoutingAction} durably repoints the routing alias before this class ever calls {@link
 * FenceSplitSourceAction}; a subsequent failure fencing the source (for example a transient {@code
 * METADATA_WRITE} cluster block) is surfaced to the caller as an overall failure of this action, but
 * traffic has already moved to the new targets by that point -- this is <i>not</i> rolled back.
 * Callers must not treat a failure response as proof nothing durable happened. Retrying the whole
 * call is always safe: every stage (including {@link CutoverSplitRoutingAction} and {@link
 * FenceSplitSourceAction} itself) is idempotent against its own already-applied state, so a retry
 * simply re-confirms or completes whatever stage previously failed rather than erroring out or
 * double-applying anything.
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
                // A resumed retry re-splitting an already-split target hits ShardCloner#clone's
                // own explicit refusal to CAS onto an already-published head, which is treated as
                // "already done" rather than a failure -- the same resumability every other stage
                // here has. Nothing else is: see isAlreadyDoneSplit's javadoc (finding R-10) for
                // why the broader "already exists" match this used to also accept was unsafe.
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
            ActionListener.wrap(response -> {
                if (response.isAcknowledged() == false) {
                    // The alias update was committed but not acknowledged by every node within the
                    // request's ack timeout, so some nodes may still resolve the old alias. Fencing
                    // the source on top of that would take away the only entry point those nodes can
                    // still see. Report the partial cutover instead of proceeding and hard-coding
                    // `cutover=true` over it (finding R-15); a retry of the whole call is safe and
                    // re-confirms every stage.
                    listener.onFailure(
                        new IllegalStateException(
                            "cutover of alias ["
                                + request.routingAliasName()
                                + "] committed but was not acknowledged by all nodes within the timeout -- "
                                + "refusing to fence the source or enable write routing on top of a partially-visible "
                                + "cutover; retry this call once the cluster has caught up"
                        )
                    );
                    return;
                }
                writeRoutingStage(request, listener);
            }, listener::onFailure)
        );
    }

    /**
     * <b>Finding R-7.</b> Write routing and source fencing are now one decision, in that order, and
     * neither happens when {@code enable_write_routing} is false.
     *
     * <p>This used to be two independent stages, with fencing running <em>unconditionally</em>
     * immediately after cutover and the write-routing stage returning early right afterwards when
     * the (default, {@code false}) flag said not to enable routing. The result was that the plainly
     * documented default invocation -- {@code POST .../_orchestrate_split/src?target_indices=a,b} --
     * returned a success response and left the dataset with <b>no writable entry point at all</b>:
     * direct writes to {@code src} rejected by the fence, and writes to the alias rejected by core
     * because a multi-index alias with no {@code is_write_index} and no partition assignment has no
     * write index. With no unfence primitive in existence at the time, the only escape was deleting
     * the index.
     *
     * <p>Two changes close it, and both are needed. Here: a fence is only ever applied once write
     * routing has actually been enabled, so the alias is a working write entry point before the
     * source stops being one -- and a caller who does not ask for write routing keeps the source
     * writable, which is the only coherent meaning "cutover reads, leave writes alone" can have.
     * Separately, {@code UnfenceSplitSourceAction} now exists, so even a fence applied in error is
     * recoverable without deleting data.
     */
    private void writeRoutingStage(OrchestrateShardSplitRequest request, ActionListener<OrchestrateShardSplitResponse> listener) {
        if (request.enableWriteRouting() == false) {
            listener.onResponse(new OrchestrateShardSplitResponse(true, true, true, false, false));
            return;
        }
        client.execute(
            EnableWritePartitionRoutingAction.INSTANCE,
            new EnableWritePartitionRoutingRequest(request.routingAliasName(), request.targetIndexNames()),
            ActionListener.wrap(response -> fenceSourceStage(request, listener), listener::onFailure)
        );
    }

    /**
     * Fences the source once write routing is live, so a direct write to the source's own name is
     * rejected rather than silently diverging from the targets the alias now serves -- narrowing
     * (not eliminating, see {@link
     * org.opensearch.serverless.storage.resharding.SourceSplitFenceMetadata}'s own javadoc for the
     * honest limitation) the "source keeps silently accepting writes forever" gap.
     */
    private void fenceSourceStage(OrchestrateShardSplitRequest request, ActionListener<OrchestrateShardSplitResponse> listener) {
        client.execute(
            FenceSplitSourceAction.INSTANCE,
            new FenceSplitSourceRequest(request.sourceIndexName(), request.routingAliasName()),
            ActionListener.wrap(
                response -> listener.onResponse(new OrchestrateShardSplitResponse(true, true, true, true, true)),
                listener::onFailure
            )
        );
    }

    /**
     * The one sentinel that genuinely proves a target was already split: {@code ShardCloner#clone}
     * refuses to CAS onto a head that is already published, and says so in exactly these words.
     * Matching on a message is still a weak contract, but this one is authored by this plugin, is
     * only ever produced by that single refusal, and positively establishes the thing the resume
     * path needs to know -- that the target has a published head.
     */
    private static final String ALREADY_PUBLISHED_HEAD_SENTINEL = "already has a published head";

    /**
     * Whether a failed {@link ShardSplitAction} call represents a target that some prior attempt
     * already split, not a genuine new failure.
     *
     * <p><b>Finding R-10: this used to also match {@code "already exists"}, and that was unsafe.</b>
     * Blob stores, alias requests, {@code ResourceAlreadyExistsException} and a long tail of {@code
     * IOException}s all produce messages containing that phrase for entirely unrelated reasons. A
     * target whose split failed on, say, a blob-store error whose message happened to contain it was
     * treated as done: cutover then put the alias in front of a target with <em>no published
     * head</em>, and (before R-7 was fixed) fenced the source behind it, so every document hashing
     * to that partition became unreachable and every write to it failed. Silently proceeding past a
     * failure we did not understand is exactly how an empty index ends up serving live traffic.
     *
     * <p>The original reason for the broader match was real but much narrower: {@code
     * ShardCloner#clone} writes a target's clone-lineage blob <em>before</em> CAS-ing its head, so a
     * prior attempt that crashed between those two writes leaves a lineage blob whose re-write the
     * blob store refuses with "already exists". That case now surfaces as an ordinary failure, which
     * is the correct outcome: the target's head was never published, so it is <em>not</em> already
     * split, and an operator being told so is strictly better than the alias silently pointing at
     * it. The clean long-term fix -- a positive check that reads the target's head and its {@code
     * shard-partition} descriptor and resumes only when the head is published with the expected
     * {@code (partitionIndex, numPartitions)} -- needs a node-level action against the target's blob
     * container, which this cluster-manager-side coordinator has no route to today; it is written up
     * as deferred work rather than approximated here.
     */
    private static boolean isAlreadyDoneSplit(Exception e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            String message = cause.getMessage();
            if (message != null && message.contains(ALREADY_PUBLISHED_HEAD_SENTINEL)) {
                return true;
            }
        }
        return false;
    }
}
