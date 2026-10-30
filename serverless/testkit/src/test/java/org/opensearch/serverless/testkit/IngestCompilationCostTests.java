/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.serverless.ingest.IngestPipelines;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class IngestCompilationCostTests extends OpenSearchTestCase {
    private static String definition(String value) {
        return "{\"processors\":[{\"counted\":{\"value\":\"" + value + "\"}}]}";
    }

    private static IngestPipelines compiler(AtomicInteger compilations, int firstFailure) {
        final Processor.Factory factory = (registry, tag, description, config) -> {
            final int attempt = compilations.incrementAndGet();
            if (attempt == 1 && firstFailure == 1) {
                throw new IllegalArgumentException("compile failed");
            }
            if (attempt == 1 && firstFailure == 2) {
                throw new AssertionError("factory stopped");
            }
            final String value = (String) config.remove("value");
            return new AbstractProcessor(tag, description) {
                @Override
                public String getType() {
                    return "counted";
                }

                @Override
                public IngestDocument execute(IngestDocument document) {
                    document.setFieldValue("tag", value);
                    return document;
                }
            };
        };
        return new IngestPipelines(Map.of("counted", factory), null);
    }

    public void testRepeatedDocumentsDoNotRecompileAnUnchangedPipeline() throws Exception {
        final var compilations = new AtomicInteger();
        final var compiler = compiler(compilations, 0);
        for (int i = 0; i < 10; i++) {
            final var pipeline = compiler.compile("stored", definition("value"));
            assertTrue(compiler.run(pipeline, "docs", "id-" + i, "{}").source().contains("value"));
        }
        assertEquals(1, compilations.get());
    }

    public void testConcurrentCompilesOfOneRevisionAreShared() throws Exception {
        final var compilations = new AtomicInteger();
        final var compiler = compiler(compilations, 0);
        final var start = new CyclicBarrier(8);
        try (var workers = Executors.newFixedThreadPool(8)) {
            final var results = new java.util.ArrayList<java.util.concurrent.Future<String>>();
            for (int i = 0; i < 8; i++) {
                results.add(workers.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    return compiler.compile("stored", definition("value")).getId();
                }));
            }
            for (var result : results) {
                assertEquals("stored", result.get(10, TimeUnit.SECONDS));
            }
        }
        assertEquals(1, compilations.get());
    }

    public void testChangedDefinitionsApplyImmediately() throws Exception {
        final var compilations = new AtomicInteger();
        final var compiler = compiler(compilations, 0);
        assertTrue(compiler.run(compiler.compile("stored", definition("before")), "docs", "id", "{}").source().contains("before"));
        assertTrue(compiler.run(compiler.compile("stored", definition("after")), "docs", "id", "{}").source().contains("after"));
        assertEquals(2, compilations.get());
    }

    public void testIdenticalDefinitionsKeepTheirOwnPipelineIds() throws Exception {
        final var compilations = new AtomicInteger();
        final var compiler = compiler(compilations, 0);
        assertEquals("first", compiler.compile("first", definition("value")).getId());
        assertEquals("second", compiler.compile("second", definition("value")).getId());
        assertEquals(2, compilations.get());
    }

    public void testACompilationFailureCanRetryAndCacheItsSuccess() throws Exception {
        final var compilations = new AtomicInteger();
        final var compiler = compiler(compilations, 1);
        expectThrows(Exception.class, () -> compiler.compile("stored", definition("value")));
        compiler.compile("stored", definition("value"));
        compiler.compile("stored", definition("value"));
        assertEquals(2, compilations.get());
    }

    public void testFatalFactoryFailureDoesNotLeaveAWaitingCompilation() throws Exception {
        final var compilations = new AtomicInteger();
        final var compiler = compiler(compilations, 2);
        expectThrows(AssertionError.class, () -> compiler.compile("stored", definition("value")));
        try (var worker = Executors.newSingleThreadExecutor()) {
            assertEquals("stored", worker.submit(() -> compiler.compile("stored", definition("value")).getId()).get(10, TimeUnit.SECONDS));
        }
        assertEquals(2, compilations.get());
    }

    public void testOversizedDefinitionsAreNotRetained() throws Exception {
        final var compilations = new AtomicInteger();
        final var compiler = compiler(compilations, 0);
        final String source = "{\"description\":\""
            + "x".repeat(3 * 1024 * 1024)
            + "\",\"processors\":[{\"counted\":{\"value\":\"value\"}}]}";
        compiler.compile("large", source);
        compiler.compile("large", source);
        assertEquals(2, compilations.get());
    }

    public void testOldDefinitionsAreEvictedUnderChurn() throws Exception {
        final var compilations = new AtomicInteger();
        final var compiler = compiler(compilations, 0);
        for (int i = 0; i < 300; i++) {
            compiler.compile("pipeline-" + i, definition("value"));
        }
        compiler.compile("pipeline-0", definition("value"));
        assertEquals(301, compilations.get());
    }
}
