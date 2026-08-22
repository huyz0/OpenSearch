/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity.action;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;

/**
 * Marks or unmarks a node as warming (node autoscaling design doc part 2, "pre-warm before
 * rotation" -- Phase 3). Whoever boots a node (an external control plane, or a future in-repo
 * warmup hook) calls this before the node should receive reader shard copies, then again to clear
 * it once warm. See {@link org.opensearch.serverless.storage.nodecapacity.NodeWarmupCoordinator}
 * for the mechanism and {@link org.opensearch.serverless.storage.allocation.NodeWarmupAllocationDecider}
 * for what actually withholds allocation while a node is marked.
 */
public class NodeWarmupAction extends ActionType<AcknowledgedResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final NodeWarmupAction INSTANCE = new NodeWarmupAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/node_capacity/warmup";

    private NodeWarmupAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
