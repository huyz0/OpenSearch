/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.action;

import org.opensearch.action.ActionType;

/**
 * Read-after-write for a specific reader shard copy (rfc-serverless-opensearch.md &sect;8): "a
 * search request may carry a minimum visible generation... the reader waits for it (with
 * timeout)." A caller who already knows (from the routing table) which node holds the reader
 * shard copy it is about to query sends this request directly to that node -- the same
 * deliberately single-node, no-fan-out shape as {@link NodeManifestLagAction} -- and blocks until
 * that specific engine instance has materialized at least the requested generation, or the
 * timeout elapses. {@link WaitForGenerationRequest} names the shard and the generation to wait
 * for; {@link TransportWaitForGenerationAction} does the actual waiting.
 */
public class WaitForGenerationAction extends ActionType<WaitForGenerationResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final WaitForGenerationAction INSTANCE = new WaitForGenerationAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:monitor/serverless/storage/node/wait_for_generation";

    private WaitForGenerationAction() {
        super(NAME, WaitForGenerationResponse::new);
    }
}
