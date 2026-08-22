/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity.action;

import org.opensearch.action.ActionType;

/**
 * Reads the cluster-manager's cached {@code NodeCapacitySignalService} output (node autoscaling
 * design doc part 1) -- the read-only contract an external control plane polls to decide node-count
 * changes. This plugin never provisions or terminates compute itself; see the design doc's "The
 * boundary rule" for why.
 */
public class NodeCapacityAction extends ActionType<NodeCapacityResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final NodeCapacityAction INSTANCE = new NodeCapacityAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:monitor/serverless/storage/node_capacity";

    private NodeCapacityAction() {
        super(NAME, NodeCapacityResponse::new);
    }
}
