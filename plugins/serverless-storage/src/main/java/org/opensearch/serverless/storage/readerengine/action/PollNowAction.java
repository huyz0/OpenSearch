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
 * The receiving side of rfc-serverless-opensearch.md &sect;8's publication notification mechanism
 * ("a small publication notification... to the shard's current readers... generalize the existing
 * segment-replication checkpoint publisher"): forces the reader engine for a specific (index,
 * shard) on whichever node receives this request to check for a newer manifest generation right
 * now, rather than waiting out its own background poll schedule. Same deliberately single-node,
 * no-fan-out shape as {@link WaitForGenerationAction} -- a caller who already knows which node
 * holds a reader copy sends this request directly to that node. Purely an optimization: {@link
 * org.opensearch.serverless.storage.readerengine.ObjectStoreReaderEngine#waitForGeneration} and
 * the background poll schedule both already converge correctly with nobody ever calling this, per
 * this section's own "notifications are an optimization... the object store is the truth" framing.
 * {@link PollNowRequest} names the shard; {@link TransportPollNowAction} does the actual work.
 */
public class PollNowAction extends ActionType<PollNowResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final PollNowAction INSTANCE = new PollNowAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/node/poll_now";

    private PollNowAction() {
        super(NAME, PollNowResponse::new);
    }
}
