/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaleup.action;

import org.opensearch.action.ActionType;

/**
 * Cluster-wide scale-up candidate policy evaluator (see the RFC's scale-up autoscaling
 * subsection): fans out a per-node queries-per-minute snapshot across every data node, merges
 * each shard's query-rate signal by {@code (indexUuid, shardId)}, joins it with that index's
 * current {@code index.number_of_search_replicas} read from cluster metadata, and applies this
 * plugin's query-rate/replica-cap thresholds to flag which shards are worth a real expansion
 * controller's attention.
 *
 * <p>Mirrors {@code org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidatesAction}'s
 * "policy, not mechanism" split -- this action never itself adds a search-only replica, it is
 * read-only. See {@link org.opensearch.serverless.storage.scaleup.ReaderReplicaExpansionCoordinator}
 * for the mechanism half.
 */
public class ScaleUpCandidatesAction extends ActionType<ScaleUpCandidatesResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final ScaleUpCandidatesAction INSTANCE = new ScaleUpCandidatesAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:monitor/serverless/storage/scale_up/candidates";

    private ScaleUpCandidatesAction() {
        super(NAME, ScaleUpCandidatesResponse::new);
    }
}
