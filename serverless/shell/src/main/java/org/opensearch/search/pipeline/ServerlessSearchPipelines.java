/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.metrics.OperationMetrics;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.script.ScriptService;
import org.opensearch.search.pipeline.common.SearchPipelineCommonModulePlugin;
import org.opensearch.threadpool.ThreadPool;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Search pipelines on a node with no cluster state, over core's own pipeline and the common module's
 * processors.
 *
 * <p>In this package because the pieces it composes are core's package-private ones: the pipeline factory
 * {@code PipelineWithMetrics.create}, the {@code PipelinedRequest} that carries a request through its
 * processors, and the holder of the system pipelines that wrap a user's. Nothing here interprets a
 * processor; the module's factories build them and the pipeline runs them, exactly as
 * {@code SearchPipelineService} would, minus the cluster state it reads named pipelines from -- those come
 * from a register instead.
 */
public final class ServerlessSearchPipelines {

    private final Map<String, Processor.Factory<SearchRequestProcessor>> requestFactories;
    private final Map<String, Processor.Factory<SearchResponseProcessor>> responseFactories;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final SystemGeneratedPipelineHolder noSystemPipelines;

    /**
     * Builds the processor factories from the common module, with what this node has.
     *
     * @param environment the node environment
     * @param scriptService the script service, for the script processor
     * @param analysisRegistry the analysis registry
     * @param threadPool the thread pool
     * @param xContentRegistry the registry that parses a query, for the filter_query processor
     * @param namedWriteableRegistry the writeable registry
     */
    public ServerlessSearchPipelines(
        Environment environment,
        ScriptService scriptService,
        AnalysisRegistry analysisRegistry,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        // No SearchPipelineService and no Client: nothing in the common module needs either, and a
        // processor from elsewhere that did would fail to build, at PUT, naming itself.
        final SearchPipelinePlugin.Parameters parameters = new SearchPipelinePlugin.Parameters(
            environment,
            scriptService,
            analysisRegistry,
            threadPool.getThreadContext(),
            threadPool::relativeTimeInMillis,
            (delay, command) -> threadPool.schedule(command, TimeValue.timeValueMillis(delay), ThreadPool.Names.GENERIC),
            null,
            null,
            threadPool.generic()::execute,
            xContentRegistry
        );
        final SearchPipelineCommonModulePlugin common = new SearchPipelineCommonModulePlugin();
        this.requestFactories = Map.copyOf(common.getRequestProcessors(parameters));
        this.responseFactories = Map.copyOf(common.getResponseProcessors(parameters));
        this.namedWriteableRegistry = namedWriteableRegistry;
        final SystemGeneratedProcessorMetrics metrics = new SystemGeneratedProcessorMetrics();
        this.noSystemPipelines = new SystemGeneratedPipelineHolder(
            new SystemGeneratedPipelineWithMetrics(
                "_none",
                null,
                null,
                List.of(),
                List.of(),
                List.of(),
                namedWriteableRegistry,
                System::nanoTime,
                metrics
            ),
            new SystemGeneratedPipelineWithMetrics(
                "_none",
                null,
                null,
                List.of(),
                List.of(),
                List.of(),
                namedWriteableRegistry,
                System::nanoTime,
                metrics
            )
        );
    }

    /**
     * Returns the processor names this node can build, for a refusal that names them.
     *
     * @return request processor names, then response processor names
     */
    public List<String> available() {
        final List<String> names = new java.util.ArrayList<>(requestFactories.keySet());
        names.addAll(responseFactories.keySet());
        return names;
    }

    /**
     * Compiles a pipeline from its stored JSON, to refuse one that cannot be built where it is stored.
     *
     * @param id the pipeline id
     * @param json the pipeline, as core's put-search-pipeline API takes it
     * @throws Exception if a processor is unknown or misconfigured
     */
    public void validate(String id, String json) throws Exception {
        compile(id, parse(json));
    }

    private static Map<String, Object> parse(String json) {
        return XContentHelper.convertToMap(new BytesArray(json.getBytes(StandardCharsets.UTF_8)), false, XContentType.JSON).v2();
    }

    /**
     * Compiles a stored pipeline and runs its request processors over a request.
     *
     * @param id the pipeline id
     * @param json the pipeline, as stored
     * @param request the request as the caller sent it
     * @return the request carrying its transformed self, to hand back to {@link #transformResponse}
     * @throws Exception if a processor is unknown, misconfigured, or fails
     */
    public PipelinedRequest transformRequest(String id, String json, SearchRequest request) throws Exception {
        return transformRequest(compile(id, parse(json)), request);
    }

    /**
     * Compiles an inline pipeline and runs its request processors over a request.
     *
     * @param id the pipeline id
     * @param config the pipeline, as the body carried it
     * @param request the request as the caller sent it
     * @return the request carrying its transformed self, to hand back to {@link #transformResponse}
     * @throws Exception if a processor is unknown, misconfigured, or fails
     */
    public PipelinedRequest transformRequest(String id, Map<String, Object> config, SearchRequest request) throws Exception {
        return transformRequest(compile(id, config), request);
    }

    private Pipeline compile(String id, Map<String, Object> config) throws Exception {
        if (config.get(Pipeline.PHASE_PROCESSORS_KEY) instanceof List<?> phase && phase.isEmpty() == false) {
            throw new IllegalArgumentException(
                "phase_results_processors are not supported: they run between a coordinator's query and fetch "
                    + "phases, and this fan-out has no seam there"
            );
        }
        return PipelineWithMetrics.create(
            id,
            new java.util.HashMap<>(config),
            requestFactories,
            responseFactories,
            Map.of(),
            namedWriteableRegistry,
            new OperationMetrics(),
            new OperationMetrics(),
            new Processor.PipelineContext(Processor.PipelineSource.SEARCH_REQUEST)
        );
    }

    private PipelinedRequest transformRequest(Pipeline pipeline, SearchRequest request) throws Exception {
        final PipelinedRequest pipelined = new PipelinedRequest(pipeline, request, new PipelineProcessingContext(), noSystemPipelines);
        final Answer<SearchRequest> answer = new Answer<>();
        pipelined.transformRequest(answer);
        // Answered before this returns, or refused: every processor in the common module answers its
        // listener synchronously, and this runs on the thread that received the request, which must not
        // block. A processor that answered later would be answering a search already refused.
        answer.get("request");
        return pipelined;
    }

    /**
     * A listener that expects to be answered before the call that took it returns.
     *
     * <p>Not a future to wait on: the request side runs on a transport thread, where waiting resets the
     * caller's connection rather than blocking. Every processor in the common module is synchronous.
     */
    private static final class Answer<T> implements org.opensearch.core.action.ActionListener<T> {
        private volatile T value;
        private volatile Exception failure;
        private volatile boolean answered;

        @Override
        public void onResponse(T response) {
            value = response;
            answered = true;
        }

        @Override
        public void onFailure(Exception e) {
            failure = e;
            answered = true;
        }

        T get(String what) throws Exception {
            if (answered == false) {
                throw new IllegalStateException(
                    "a search processor answered the " + what + " asynchronously, which this fan-out does not wait for"
                );
            }
            if (failure != null) {
                throw failure;
            }
            return value;
        }
    }

    /**
     * Runs the response processors.
     *
     * @param pipelined what {@link #transformRequest} returned
     * @param response the response as the fan-out built it
     * @return the response as the processors left it
     */
    public SearchResponse transformResponse(PipelinedRequest pipelined, SearchResponse response) {
        final Answer<SearchResponse> answer = new Answer<>();
        pipelined.transformResponseListener(answer).onResponse(response);
        try {
            return answer.get("response");
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new org.opensearch.OpenSearchException("a search response processor failed", e);
        }
    }
}
