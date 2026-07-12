/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine.action;

import org.opensearch.action.ActionType;

/**
 * Real-time {@code _get}, explicitly routed to a shard's writer engine (rfc-serverless-opensearch.md
 * &sect;8: "{@code _get} by document id can optionally route to the writer shard for true realtime
 * gets (the writer has the live version map), controlled per request"). A caller who already knows
 * (from the routing table) which node holds the writer for the target shard sends this request
 * directly to that node -- the same deliberately single-node, no-fan-out shape as {@link
 * org.opensearch.serverless.storage.readerengine.action.WaitForGenerationAction}. {@link
 * RealtimeGetRequest} names the shard and document id; {@link TransportRealtimeGetAction} does the
 * actual lookup.
 */
public class RealtimeGetAction extends ActionType<RealtimeGetResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final RealtimeGetAction INSTANCE = new RealtimeGetAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:monitor/serverless/storage/node/realtime_get";

    private RealtimeGetAction() {
        super(NAME, RealtimeGetResponse::new);
    }
}
