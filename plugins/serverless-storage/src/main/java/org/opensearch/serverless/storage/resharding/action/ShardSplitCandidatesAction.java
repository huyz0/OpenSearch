/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.ActionType;

/**
 * Cluster-wide split-candidate policy evaluator: fans out a per-node writes-per-minute snapshot
 * across every data node, merges each shard's write-rate signal by {@code (indexUuid, shardId)},
 * and applies this plugin's write-rate threshold to flag which shards a sustained-high write rate
 * makes worth an operator's consideration as a manual {@code ShardSplitAction} target.
 *
 * <p>Deliberately read-only and unpaired with any mechanism half, unlike {@code
 * org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidatesAction} (paired with
 * {@code ShardSuspensionCoordinator}) or {@code
 * org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidatesAction} (paired with {@code
 * ReaderReplicaExpansionCoordinator}). {@code ShardSplitter#split} only re-points an
 * already-provisioned target shard identity -- it does not create new indices/shards, allocate
 * them, or cut over routing from the source shard to the split targets, and none of that
 * orchestration exists in this plugin yet. Auto-triggering a split from this signal alone would be
 * premature; this action exists purely to surface the signal, same "policy without a mechanism
 * yet" posture {@code ObjectStoreWriterEngine#writesPerMinute()} itself was deliberately left in.
 */
public class ShardSplitCandidatesAction extends ActionType<ShardSplitCandidatesResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final ShardSplitCandidatesAction INSTANCE = new ShardSplitCandidatesAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:monitor/serverless/storage/resharding/split_candidates";

    private ShardSplitCandidatesAction() {
        super(NAME, ShardSplitCandidatesResponse::new);
    }
}
