/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.pipeline;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SearchPipelineCompilationCostTests extends OpenSearchTestCase {
    private static String definition(int factor) {
        return "{\"request_processors\":[{\"oversample\":{\"sample_factor\":" + factor + "}}]}";
    }

    private static SearchRequest request(int size) {
        return new SearchRequest("docs").source(new SearchSourceBuilder().size(size));
    }

    private void withCompiler(org.opensearch.common.CheckedConsumer<ServerlessSearchPipelines, Exception> work) throws Exception {
        final var threadPool = new TestThreadPool("search-pipeline-compilation");
        try {
            final var environment = new Environment(Settings.builder().put("path.home", createTempDir()).build(), null);
            work.accept(
                new ServerlessSearchPipelines(
                    environment,
                    null,
                    null,
                    threadPool,
                    NamedXContentRegistry.EMPTY,
                    new NamedWriteableRegistry(List.of())
                )
            );
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        }
    }

    public void testStoredDefinitionReusesCompilationButKeepsRequestStateSeparate() throws Exception {
        withCompiler(compiler -> {
            final var first = compiler.transformRequest("stored", definition(2), request(2));
            first.getPipelineProcessingContext().setAttribute("private", "first request");
            final var second = compiler.transformRequest("stored", definition(2), request(3));
            assertSame(first.getPipeline(), second.getPipeline());
            assertNotSame(first.getPipelineProcessingContext(), second.getPipelineProcessingContext());
            assertNull(second.getPipelineProcessingContext().getAttribute("private"));
            assertEquals(4, first.source().size());
            assertEquals(6, second.source().size());
        });
    }

    public void testConcurrentStoredRequestsShareTheirCompiledPipeline() throws Exception {
        withCompiler(compiler -> {
            final var start = new CyclicBarrier(8);
            try (var workers = Executors.newFixedThreadPool(8)) {
                final var requests = new java.util.ArrayList<java.util.concurrent.Future<PipelinedRequest>>();
                for (int i = 0; i < 8; i++) {
                    requests.add(workers.submit(() -> {
                        start.await(10, TimeUnit.SECONDS);
                        return compiler.transformRequest("stored", definition(2), request(2));
                    }));
                }
                final var first = requests.getFirst().get(10, TimeUnit.SECONDS);
                for (var result : requests) {
                    final var transformed = result.get(10, TimeUnit.SECONDS);
                    assertSame(first.getPipeline(), transformed.getPipeline());
                    assertEquals(4, transformed.source().size());
                }
            }
        });
    }

    public void testUpdatedStoredDefinitionAppliesImmediately() throws Exception {
        withCompiler(compiler -> {
            final var first = compiler.transformRequest("stored", definition(2), request(2));
            final var second = compiler.transformRequest("stored", definition(3), request(2));
            assertNotSame(first.getPipeline(), second.getPipeline());
            assertEquals(4, first.source().size());
            assertEquals(6, second.source().size());
        });
    }

    public void testDifferentIdsKeepDistinctNativePipelines() throws Exception {
        withCompiler(compiler -> {
            final var first = compiler.transformRequest("first", definition(2), request(2));
            final var second = compiler.transformRequest("second", definition(2), request(2));
            assertNotSame(first.getPipeline(), second.getPipeline());
            assertEquals("first", first.getPipeline().getId());
            assertEquals("second", second.getPipeline().getId());
        });
    }

    public void testInlineDefinitionsKeepTheirOwnCompilationAndRemainUnchanged() throws Exception {
        withCompiler(compiler -> {
            final Map<String, Object> definition = Map.of("request_processors", List.of(Map.of("oversample", Map.of("sample_factor", 2))));
            final var first = compiler.transformRequest("inline", definition, request(2));
            final var second = compiler.transformRequest("inline", definition, request(3));
            assertNotSame(first.getPipeline(), second.getPipeline());
            assertEquals(4, first.source().size());
            assertEquals(6, second.source().size());
            assertTrue(definition.containsKey("request_processors"));
        });
    }

    public void testOutOfOrderResponsesUseTheirOwnRequestContext() throws Exception {
        withCompiler(compiler -> {
            final String definition = "{\"request_processors\":[{\"oversample\":{\"sample_factor\":2}}],"
                + "\"response_processors\":[{\"truncate_hits\":{}}]}";
            final var first = compiler.transformRequest("stored", definition, request(1));
            final var second = compiler.transformRequest("stored", definition, request(3));
            assertSame(first.getPipeline(), second.getPipeline());
            assertEquals(3, compiler.transformResponse(second, response(6)).getHits().getHits().length);
            assertEquals(1, compiler.transformResponse(first, response(2)).getHits().getHits().length);
        });
    }

    private static org.opensearch.action.search.SearchResponse response(int count) {
        final var hits = new org.opensearch.search.SearchHit[count];
        for (int i = 0; i < count; i++) {
            hits[i] = new org.opensearch.search.SearchHit(i);
        }
        final var internal = new org.opensearch.search.internal.InternalSearchResponse(
            new org.opensearch.search.SearchHits(
                hits,
                new org.apache.lucene.search.TotalHits(count, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO),
                Float.NaN
            ),
            null,
            null,
            null,
            false,
            null,
            1
        );
        return new org.opensearch.action.search.SearchResponse(
            internal,
            null,
            1,
            1,
            0,
            1L,
            org.opensearch.action.search.ShardSearchFailure.EMPTY_ARRAY,
            org.opensearch.action.search.SearchResponse.Clusters.EMPTY,
            null
        );
    }

}
