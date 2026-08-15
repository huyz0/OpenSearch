/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.indices.create;

import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.alias.IndicesAliasesAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportIndicesResolvingAction;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.mapper.MappingTransformerRegistry;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.stream.Collectors;

/**
 * Create index action.
 *
 * @opensearch.internal
 */
public class TransportCreateIndexAction extends TransportClusterManagerNodeAction<CreateIndexRequest, CreateIndexResponse>
    implements
        TransportIndicesResolvingAction<CreateIndexRequest> {

    private static final org.apache.logging.log4j.Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(
        TransportCreateIndexAction.class
    );

    private final MetadataCreateIndexService createIndexService;
    private final MappingTransformerRegistry mappingTransformerRegistry;

    @Inject
    public TransportCreateIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataCreateIndexService createIndexService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        MappingTransformerRegistry mappingTransformerRegistry
    ) {
        super(
            CreateIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CreateIndexRequest::new,
            indexNameExpressionResolver
        );
        this.createIndexService = createIndexService;
        this.mappingTransformerRegistry = mappingTransformerRegistry;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CreateIndexResponse read(StreamInput in) throws IOException {
        return new CreateIndexResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(CreateIndexRequest request, ClusterState state) {
        ClusterBlockException clusterBlockException = state.blocks()
            .indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());

        if (clusterBlockException == null) {
            return state.blocks().createIndexBlockedException(ClusterBlockLevel.CREATE_INDEX);
        }
        return clusterBlockException;
    }

    /**
     * A gated creation runs on the node that received it, rather than being sent to the cluster manager.
     *
     * <h4>Why the cluster manager is not needed</h4>
     *
     * It was never needed for this. Uniqueness comes from the descriptor store's register
     * compare-and-swap, which is what makes two nodes admitting the same name at once resolvable rather
     * than forbidden: one write wins and the loser is told the index already exists, exactly as a duplicate
     * has always been told. What creation needs from cluster state is a *read* -- templates, scoped
     * settings, the collision check against ordinary indices -- and every node has a snapshot of it.
     *
     * <p>T49 took gated creation off the cluster manager's state update thread, and measuring it a round
     * later showed the node was still a funnel: every creation in the cluster executed on one machine's
     * GENERIC pool, at a rate one machine can sustain. This is the other half of that, and it is what turns
     * a per-cluster creation rate into a per-node one.
     *
     * <h4>Why only the certain case</h4>
     *
     * {@code MetadataCreateIndexService#certainlyGated} carries the argument, and the short version is that
     * an admitted creation may still turn out to need cluster state, and the fallback that handles it can
     * only run on the cluster manager. Anything short of certain keeps today's behaviour: sent to the
     * cluster manager, decided there. An ordinary index is never certain, so an ordinary cluster is
     * untouched by this -- {@code hasAdmissionCheck} answers false when nothing installed the gate, and the
     * first condition inside is the gated setting.
     */
    @Override
    protected boolean localExecute(CreateIndexRequest request) {
        try {
            final CreateIndexClusterStateUpdateRequest updateRequest = new CreateIndexClusterStateUpdateRequest(
                request.cause().length() == 0 ? "api" : request.cause(),
                resolveIndexName(request),
                request.index()
            ).settings(request.settings()).aliases(request.aliases()).context(request.context());
            return createIndexService.certainlyGated(updateRequest, clusterService.state());
        } catch (Exception e) {
            // Deciding where to run must not be able to fail the request. The cluster manager is always a
            // correct answer, so anything unexpected here becomes one.
            LOGGER.debug("could not decide locally where to create [{}]: {}", request.index(), e);
            return false;
        }
    }

    @Override
    protected void clusterManagerOperation(
        final CreateIndexRequest request,
        final ClusterState state,
        final ActionListener<CreateIndexResponse> listener
    ) {
        String cause = request.cause();
        if (cause.length() == 0) {
            cause = "api";
        }

        final String indexName = resolveIndexName(request);

        final String finalCause = cause;
        final ActionListener<String> mappingTransformListener = ActionListener.wrap(transformedMappings -> {
            final CreateIndexClusterStateUpdateRequest updateRequest = new CreateIndexClusterStateUpdateRequest(
                finalCause,
                indexName,
                request.index()
            ).ackTimeout(request.timeout())
                .clusterManagerNodeTimeout(request.clusterManagerNodeTimeout())
                .settings(request.settings())
                .mappings(transformedMappings)
                .aliases(request.aliases())
                .context(request.context())
                .waitForActiveShards(request.waitForActiveShards());

            createIndexService.createIndex(
                updateRequest,
                ActionListener.map(
                    listener,
                    response -> new CreateIndexResponse(response.isAcknowledged(), response.isShardsAcknowledged(), indexName)
                )
            );
        }, listener::onFailure);

        mappingTransformerRegistry.applyTransformers(request.mappings(), null, mappingTransformListener);
    }

    @Override
    public ResolvedIndices resolveIndices(CreateIndexRequest request) {
        ResolvedIndices result = ResolvedIndices.of(resolveIndexName(request));

        if (request.aliases().isEmpty()) {
            return result;
        } else {
            return result.withLocalSubActions(
                IndicesAliasesAction.INSTANCE,
                ResolvedIndices.Local.of(request.aliases().stream().map(Alias::name).collect(Collectors.toSet()))
            );
        }
    }

    private String resolveIndexName(CreateIndexRequest request) {
        return indexNameExpressionResolver.resolveDateMathExpression(request.index());
    }
}
