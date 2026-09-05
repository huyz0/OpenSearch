/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;

/**
 * Abandons an in-progress in-place split, releasing the parent shard's write block.
 *
 * <p><b>Finding R-11: why an operator needs this.</b> {@code IndexShard#ensureNotInProgressSplitParent}
 * rejects <em>every primary write</em> to a shard for the whole in-progress split window -- which is
 * the correct design, since it is what closes the lost-write hole an in-place split would otherwise
 * have. The problem is what ends that window. Core's {@code MetadataInPlaceSplitShardCommitService}
 * commits when every child primary reaches {@code STARTED}, and cancels on exactly one condition:
 * a child whose {@code UnassignedInfo.getNumFailedAllocations()} has reached {@code
 * index.allocation.max_retries}. That counter counts failed <em>attempts</em>. A child that is never
 * attempted at all -- because no node passes a disk watermark, allocation filtering, awareness,
 * throttling, or this plugin's own {@code NodeWarmupAllocationDecider}/{@code
 * SuspendedShardAllocationDecider} -- sits {@code UNASSIGNED} with the counter at zero, forever.
 * Neither {@code READY_TO_COMMIT} nor {@code SHOULD_CANCEL} ever fires; there is no deadline and,
 * before this action, there was no way for an operator to intervene. The observable symptom is a
 * shard rejecting 100% of its writes with a retriable exception and no explanation anywhere.
 *
 * <p>This is the operator half of the fix and is deliberately manual: it never fires on its own, so
 * it cannot race a split that was about to succeed. The automatic half -- a split-start deadline
 * after which core's own completion evaluation returns {@code SHOULD_CANCEL} -- belongs in that
 * commit service, which this plugin does not own; it is written up as a precise, paste-ready change
 * rather than approximated here.
 *
 * <p>Cancelling is safe by construction: an in-progress split has published nothing. The children
 * are not search-visible as committed shards and hold no acknowledged writes of their own, so
 * dropping their reserved ranges and routing entries returns the index to exactly the state it was
 * in before the split was requested. It is the same transition core's own cancel path performs.
 */
public class CancelInPlaceSplitAction extends ActionType<AcknowledgedResponse> {

    /** Singleton instance, matching the shape every other {@link ActionType} in this plugin uses. */
    public static final CancelInPlaceSplitAction INSTANCE = new CancelInPlaceSplitAction();

    /** This action's registered transport name. */
    public static final String NAME = "cluster:admin/serverless/storage/resharding/cancel_in_place_split";

    private CancelInPlaceSplitAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
