/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.resharding.WritePartitionRoutingMetadata;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The actual work behind {@link ProvisionSplitTargetsAction} -- see that class's own javadoc for
 * the full design rationale.
 */
public class TransportProvisionSplitTargetsAction extends HandledTransportAction<ProvisionSplitTargetsRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportProvisionSplitTargetsAction.class);

    private final ClusterService clusterService;
    private final Client client;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param clusterService reads the source index's settings/mapping to validate and inherit from.
     * @param client dispatches the real target-index creations, once verified safe.
     */
    @Inject
    public TransportProvisionSplitTargetsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client
    ) {
        super(ProvisionSplitTargetsAction.NAME, transportService, actionFilters, ProvisionSplitTargetsRequest::new);
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, ProvisionSplitTargetsRequest request, ActionListener<AcknowledgedResponse> listener) {
        String sourceIndexName = request.sourceIndexName();
        IndexMetadata sourceMetadata = clusterService.state().metadata().index(sourceIndexName);
        if (sourceMetadata == null) {
            listener.onFailure(new IllegalArgumentException("source index [" + sourceIndexName + "] does not exist"));
            return;
        }
        // A split target is never itself re-split in place -- same "not reachable, don't build
        // against a mechanism that doesn't exist" reasoning this plugin already applies elsewhere.
        if (WritePartitionRoutingMetadata.isWriteRoutingTarget(sourceMetadata)) {
            listener.onFailure(
                new IllegalArgumentException(
                    "source index [" + sourceIndexName + "] is itself already a split target -- refusing to split it again"
                )
            );
            return;
        }

        List<String> targetIndexNames = request.targetIndexNames();
        for (String targetIndexName : targetIndexNames) {
            if (clusterService.state().metadata().index(targetIndexName) != null) {
                listener.onFailure(
                    new IllegalArgumentException("target index [" + targetIndexName + "] already exists -- refusing to overwrite it")
                );
                return;
            }
        }

        // number_of_shards is fixed at 1 per target, matching ShardSplitter's own per-shard model
        // -- each named target is its own single-shard index identity, not a multi-shard resize the
        // way core's native _split produces. number_of_replicas and (if set) serverless-storage
        // opt-in are inherited from the source, the same "target settings default to the source's"
        // behavior core's own TransportResizeAction already has for every setting the caller
        // doesn't explicitly override.
        Settings.Builder targetSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, sourceMetadata.getNumberOfReplicas());
        if (ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(sourceMetadata.getSettings())) {
            targetSettings.put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true);
        }
        MappingMetadata sourceMapping = sourceMetadata.mapping();

        // Track exactly which targets THIS call's own CreateIndexRequest actually succeeded on --
        // not just every name in the request -- so a partial-failure rollback below only deletes
        // indices this call created. Rolling back the full requested name list would be wrong
        // whenever a name in it was concurrently, legitimately created by a *different* racing
        // provision call (the pre-existence check above is TOCTOU, not atomic with creation): this
        // call's own failure must never delete another call's already-successful target.
        Set<String> createdByThisCall = ConcurrentHashMap.newKeySet();
        AtomicInteger remaining = new AtomicInteger(targetIndexNames.size());
        AtomicReference<Exception> firstFailure = new AtomicReference<>();
        for (String targetIndexName : targetIndexNames) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(targetIndexName).settings(targetSettings);
            if (sourceMapping != null) {
                createIndexRequest.mapping(sourceMapping.sourceAsMap());
            }
            client.admin().indices().create(createIndexRequest, ActionListener.wrap(response -> {
                createdByThisCall.add(targetIndexName);
                completeOne(remaining, firstFailure, createdByThisCall, listener);
            }, failure -> {
                firstFailure.compareAndSet(null, failure);
                completeOne(remaining, firstFailure, createdByThisCall, listener);
            }));
        }
    }

    private void completeOne(
        AtomicInteger remaining,
        AtomicReference<Exception> firstFailure,
        Set<String> createdByThisCall,
        ActionListener<AcknowledgedResponse> listener
    ) {
        if (remaining.decrementAndGet() != 0) {
            return;
        }
        Exception failure = firstFailure.get();
        if (failure == null) {
            listener.onResponse(new AcknowledgedResponse(true));
            return;
        }
        if (createdByThisCall.isEmpty()) {
            listener.onFailure(failure);
            return;
        }
        // Roll back only the targets this call itself created (LENIENT_EXPAND_OPEN so a target
        // that's already gone by the time this runs is a harmless no-op) before surfacing the
        // original failure, so a retry of the same request always starts from a clean slate.
        client.admin()
            .indices()
            .delete(
                new DeleteIndexRequest(createdByThisCall.toArray(new String[0])).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN),
                ActionListener.wrap(deleteResponse -> listener.onFailure(failure), rollbackFailure -> {
                    logger.warn(
                        "failed to roll back partially-provisioned split targets " + createdByThisCall + " after a "
                            + "provisioning failure -- manual cleanup may be required",
                        rollbackFailure
                    );
                    listener.onFailure(failure);
                })
            );
    }
}
