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
 * Marks or unmarks a node as draining (node autoscaling design doc part 1, "Drain mode") -- the one
 * place an external control plane reaches back into the cluster; everything else about node
 * autoscaling is read-only from this plugin's side. See {@link
 * org.opensearch.serverless.storage.nodecapacity.DrainCoordinator} for the actual mechanism.
 */
public class NodeDrainAction extends ActionType<AcknowledgedResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final NodeDrainAction INSTANCE = new NodeDrainAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/node_capacity/drain";

    private NodeDrainAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
