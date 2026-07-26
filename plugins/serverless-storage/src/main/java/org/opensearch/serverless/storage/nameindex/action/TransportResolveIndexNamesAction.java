/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.nameindex.NameIndexService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Answers a resolve request from the local name index.
 *
 * <p>Deliberately local rather than routed to a leader. Every node's index holds the whole name space,
 * which is what S13's 4.2 GiB figure buys, so resolution needs no hop. That is the point of replicating
 * the tier for availability rather than sharding it for capacity.
 */
public class TransportResolveIndexNamesAction extends HandledTransportAction<ResolveIndexNamesRequest, ResolveIndexNamesResponse> {

    private final NameIndexService nameIndexService;

    @Inject
    public TransportResolveIndexNamesAction(
        TransportService transportService,
        ActionFilters actionFilters,
        NameIndexService nameIndexService
    ) {
        super(ResolveIndexNamesAction.NAME, transportService, actionFilters, ResolveIndexNamesRequest::new);
        this.nameIndexService = nameIndexService;
    }

    @Override
    protected void doExecute(Task task, ResolveIndexNamesRequest request, ActionListener<ResolveIndexNamesResponse> listener) {
        if (nameIndexService.isEnabled() == false) {
            listener.onFailure(
                new IllegalStateException("the name index is disabled; set " + NameIndexService.ENABLED_SETTING_KEY + " to true to use it")
            );
            return;
        }
        try {
            listener.onResponse(new ResolveIndexNamesResponse(nameIndexService.resolve(request.indicesOptions(), request.expressions())));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
