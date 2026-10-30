/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.CompoundProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Pipeline;
import org.opensearch.serverless.ingest.IngestPipelines;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class IngestCancellationTests extends OpenSearchTestCase {
    public void testWaitingForAnAsyncProcessorPreservesInterruption() throws Exception {
        final AtomicReference<Runnable> complete = new AtomicReference<>();
        final var processor = new AbstractProcessor("pending", null) {
            @Override
            public String getType() {
                return "pending";
            }

            @Override
            public IngestDocument execute(IngestDocument document) {
                throw new UnsupportedOperationException("this processor completes asynchronously");
            }

            @Override
            public void execute(IngestDocument document, BiConsumer<IngestDocument, Exception> handler) {
                complete.set(() -> handler.accept(document, null));
            }
        };
        final var pipeline = new Pipeline("pending", null, null, new CompoundProcessor(processor));
        final var runner = new IngestPipelines(Map.of(), null);
        Thread.currentThread().interrupt();
        try {
            expectThrows(InterruptedException.class, () -> runner.run(pipeline, "docs", "1", "{}"));
            assertTrue("the worker's cancellation must survive the interrupted wait", Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            if (complete.get() != null) {
                complete.get().run();
            }
        }
    }
}
