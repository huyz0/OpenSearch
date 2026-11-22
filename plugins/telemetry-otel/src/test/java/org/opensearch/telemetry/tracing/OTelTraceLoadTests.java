/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.OTelTelemetrySettings;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.tracing.sampler.OTelSamplerFactory;
import org.opensearch.telemetry.tracing.sampler.RequestSampler;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;

public class OTelTraceLoadTests extends OpenSearchTestCase {
    public void testSamplingAndExporterBackpressure() throws Exception {
        final Settings settings = Settings.builder()
            .put(TelemetrySettings.TRACER_SAMPLER_PROBABILITY.getKey(), 0)
            .put(TelemetrySettings.METRICS_FEATURE_ENABLED_SETTING.getKey(), true)
            .put(OTelTelemetrySettings.TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING.getKey(), 8)
            .put(OTelTelemetrySettings.TRACER_EXPORTER_BATCH_SIZE_SETTING.getKey(), 1)
            .build();
        final TelemetrySettings telemetry = new TelemetrySettings(
            settings,
            new ClusterSettings(settings, Set.of(TelemetrySettings.TRACER_ENABLED_SETTING, TelemetrySettings.TRACER_SAMPLER_PROBABILITY))
        );
        final Sampler sampler = Sampler.parentBased(new RequestSampler(OTelSamplerFactory.create(telemetry, settings)));
        final var exporter = new PausedExporter();
        final var metrics = InMemoryMetricReader.create();
        GlobalOpenTelemetry.resetForTest();
        try (var sdk = OTelResourceProvider.get(settings, exporter, ContextPropagators.noop(), sampler, metrics)) {
            try {
                final var tracer = sdk.getTracer("trace-load-test");
                final Span root = tracer.spanBuilder("root").setNoParent().startSpan();
                assertFalse(root.getSpanContext().isSampled());
                root.end();
                final Span child = tracer.spanBuilder("first").setParent(sampledParent()).startSpan();
                assertTrue(child.getSpanContext().isSampled());
                child.end();
                assertTrue("the exporter did not start", exporter.started.await(10, TimeUnit.SECONDS));

                final Thread producer = new Thread(() -> {
                    for (int i = 0; i < 1_000; i++) {
                        tracer.spanBuilder("load").setParent(sampledParent()).startSpan().end();
                    }
                });
                producer.start();
                producer.join(TimeUnit.SECONDS.toMillis(10));
                final boolean drainedWithoutExporter = producer.isAlive() == false;
                exporter.resume.succeed();
                producer.join(TimeUnit.SECONDS.toMillis(10));
                assertTrue("span producers waited for a stalled exporter", drainedWithoutExporter);
                assertTrue(sdk.getSdkTracerProvider().forceFlush().join(10, TimeUnit.SECONDS).isSuccess());
                assertEquals("one in-flight export plus the bounded queue", 9, exporter.exported.get());
                final var processedSpans = metrics.collectAllMetrics()
                    .stream()
                    .filter(metric -> metric.getName().equals("processedSpans"))
                    .findFirst()
                    .orElseThrow();
                assertEquals("io.opentelemetry.sdk.trace", processedSpans.getInstrumentationScopeInfo().getName());
                final var dropped = processedSpans.getLongSumData()
                    .getPoints()
                    .stream()
                    .filter(point -> Boolean.TRUE.equals(point.getAttributes().get(AttributeKey.booleanKey("dropped"))))
                    .toList();
                assertEquals(1, dropped.size());
                assertEquals("BatchSpanProcessor", dropped.getFirst().getAttributes().get(AttributeKey.stringKey("processorType")));
                assertEquals(2, dropped.getFirst().getAttributes().size());
                assertEquals(992, dropped.getFirst().getValue());
            } finally {
                exporter.resume.succeed();
            }
        } finally {
            GlobalOpenTelemetry.resetForTest();
        }
    }

    private static Context sampledParent() {
        return Context.root()
            .with(
                Span.wrap(
                    SpanContext.createFromRemoteParent(
                        "4bf92f3577b34da6a3ce929d0e0e4736",
                        "00f067aa0ba902b7",
                        TraceFlags.getSampled(),
                        TraceState.getDefault()
                    )
                )
            );
    }

    private static final class PausedExporter implements SpanExporter {
        private final CountDownLatch started = new CountDownLatch(1);
        private final AtomicInteger exported = new AtomicInteger();
        private final CompletableResultCode resume = new CompletableResultCode();

        @Override
        public CompletableResultCode export(Collection<SpanData> spans) {
            exported.addAndGet(spans.size());
            started.countDown();
            return resume;
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }
    }
}
