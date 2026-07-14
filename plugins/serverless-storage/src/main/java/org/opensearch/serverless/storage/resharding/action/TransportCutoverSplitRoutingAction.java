/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * The actual work behind {@link CutoverSplitRoutingAction} -- see that class's own javadoc for the
 * full "search-only, additive-not-destructive" design rationale.
 */
public class TransportCutoverSplitRoutingAction extends HandledTransportAction<CutoverSplitRoutingRequest, AcknowledgedResponse> {

    private final ClusterService clusterService;
    private final Client client;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param clusterService verifies every named target index genuinely exists.
     * @param client dispatches the actual alias update, once verified safe.
     */
    @Inject
    public TransportCutoverSplitRoutingAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client
    ) {
        super(CutoverSplitRoutingAction.NAME, transportService, actionFilters, CutoverSplitRoutingRequest::new);
        this.clusterService = clusterService;
        this.client = client;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the alias and the real split-target indices it should point at.
     * @param listener notified with the alias-update result.
     */
    @Override
    protected void doExecute(Task task, CutoverSplitRoutingRequest request, ActionListener<AcknowledgedResponse> listener) {
        for (String targetIndexName : request.targetIndexNames()) {
            IndexMetadata targetMetadata = clusterService.state().metadata().index(targetIndexName);
            if (targetMetadata == null) {
                listener.onFailure(
                    new IllegalArgumentException(
                        "split target index [" + targetIndexName + "] does not exist -- refusing to route traffic to it"
                    )
                );
                return;
            }
        }

        IndicesAliasesRequest aliasRequest = new IndicesAliasesRequest();
        for (String targetIndexName : request.targetIndexNames()) {
            aliasRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(targetIndexName).alias(request.aliasName()));
        }
        client.admin()
            .indices()
            .aliases(
                aliasRequest,
                ActionListener.wrap(
                    aliasResponse -> listener.onResponse(new AcknowledgedResponse(aliasResponse.isAcknowledged())),
                    listener::onFailure
                )
            );
    }
}
