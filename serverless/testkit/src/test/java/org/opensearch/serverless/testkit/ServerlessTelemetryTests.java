/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.serverless.forwarding.ForwardedStatsRequest;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.telemetry.Telemetry;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.metrics.MetricsTelemetry;
import org.opensearch.telemetry.tracing.AttributeNames;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.telemetry.MockTelemetry;
import org.opensearch.test.telemetry.MockTelemetryPlugin;
import org.opensearch.test.telemetry.TestInMemoryCounter;
import org.opensearch.test.telemetry.TestInMemoryHistogram;
import org.opensearch.test.telemetry.tracing.MockSpanData;
import org.opensearch.test.telemetry.tracing.StrictCheckSpanProcessor;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServerlessTelemetryTests extends OpenSearchTestCase {

    private static final String RESPONSES = "opensearch.serverless.rest.responses";
    private static final String RESPONSE_TIME = "opensearch.serverless.rest.response_time";

    public void testHttpTraceFollowsFleetStatsAcrossNodes() throws Exception {
        FeatureFlags.TestUtils.with(FeatureFlags.TELEMETRY, () -> {
            final var metadata = new MetadataPlane(
                new FsBlobStore(1024, createTempDir(), false),
                BlobPath.cleanPath(),
                () -> 1_000L,
                30_000L
            );
            final var senderTelemetry = new RecordingPlugin();
            try (
                var sender = new ServerlessNode(settings("telemetry-sender"), List.of(senderTelemetry));
                var receiver = new ServerlessNode(settings("telemetry-receiver"), List.of(new RecordingPlugin()));
                HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()
            ) {
                sender.startup().start();
                receiver.startup().start();
                sender.setMetadataPlane(metadata);
                receiver.setMetadataPlane(metadata);
                new BackgroundReconciler(sender, metadata).tick(1_000L);
                new BackgroundReconciler(receiver, metadata).tick(1_000L);

                final String trace = randomAlphaOfLength(20);
                final var response = request(client, sender, "/_serverless/stats?nodes=_all", trace);
                assertEquals(response.body(), 200, response.statusCode());
                assertTrue(response.body(), response.body().contains("\"successful\":2"));
                assertTrue(response.body(), response.body().contains(receiver.localNode().getId()));

                assertBusy(() -> {
                    final List<MockSpanData> spans = spans(trace);
                    final var forwarded = spans.stream()
                        .filter(span -> ForwardedStatsRequest.ACTION.equals(span.getAttributes().get(AttributeNames.TRANSPORT_ACTION)))
                        .toList();
                    assertEquals(spans.toString(), 2, forwarded.size());
                    assertTrue(
                        forwarded.toString(),
                        forwarded.stream()
                            .anyMatch(child -> forwarded.stream().anyMatch(parent -> child.getParentSpanID().equals(parent.getSpanID())))
                    );
                    assertTrue(spans.toString(), spans.stream().allMatch(MockSpanData::isHasEnded));
                    assertTrue(spans.toString(), spans.stream().anyMatch(span -> span.getParentSpanID().equals("external-parent")));
                });

                final String privateTrace = randomAlphaOfLength(20);
                final String privateSelector = "private-customer-identifier";
                final var failure = request(client, sender, "/_serverless/stats?nodes=" + privateSelector, privateTrace);
                assertEquals(failure.body(), 404, failure.statusCode());
                assertBusy(() -> {
                    final List<MockSpanData> spans = spans(privateTrace);
                    assertFalse(spans.isEmpty());
                    assertTrue(spans.toString(), spans.stream().noneMatch(span -> span.toString().contains(privateSelector)));
                    assertTrue(spans.toString(), spans.stream().allMatch(MockSpanData::isHasEnded));
                });

                final Map<String, Object> ok = Map.of("method", "GET", "status", 200L);
                final Map<String, Object> missing = Map.of("method", "GET", "status", 404L);
                assertEquals(Map.of(ok, 1.0, missing, 1.0), senderTelemetry.responses.getCounterValueForTags());
                assertEquals(
                    senderTelemetry.responses.getCounterValueForTags().keySet(),
                    senderTelemetry.latency.getHistogramValueForTags().keySet()
                );
            }
        });
    }

    public void testStreamingBulkContinuesTheIncomingTrace() throws Exception {
        FeatureFlags.TestUtils.with(FeatureFlags.TELEMETRY, () -> {
            final var telemetry = new RecordingPlugin();
            final var metadata = new MetadataPlane(
                new FsBlobStore(1024, createTempDir(), false),
                BlobPath.cleanPath(),
                () -> 1_000L,
                30_000L
            );
            try (
                var node = new ServerlessNode(settings("telemetry-streaming"), List.of(telemetry));
                HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()
            ) {
                node.startup().start();
                node.setMetadataPlane(metadata);
                final var address = node.boundHttpAddress().publishAddress();
                final String trace = randomAlphaOfLength(20);
                final var response = client.send(
                    HttpRequest.newBuilder(
                        URI.create("http://" + address.getAddress() + ":" + address.getPort() + "/private-index/_bulk?require_alias=true")
                    )
                        .header("traceparent", trace + "~external-parent")
                        .header("Content-Type", "application/x-ndjson")
                        .timeout(Duration.ofSeconds(30))
                        .POST(HttpRequest.BodyPublishers.ofString("{\"index\":{\"_index\":\"private-index\"}}\n{}\n"))
                        .build(),
                    HttpResponse.BodyHandlers.ofString()
                );
                assertEquals(response.body(), 501, response.statusCode());
                assertBusy(() -> {
                    final var spans = spans(trace);
                    assertEquals(spans.toString(), 1, spans.size());
                    final var span = spans.getFirst();
                    assertEquals("external-parent", span.getParentSpanID());
                    assertTrue(span.toString(), span.isHasEnded());
                    assertFalse(span.toString(), span.toString().contains("private-index"));
                });
                assertEquals(Map.of(Map.of("method", "POST", "status", 501L), 1.0), telemetry.responses.getCounterValueForTags());
            }
        });
    }

    private Settings settings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "telemetry-test")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", 0)
            .put("transport.port", 0)
            .put("serverless.roles", "ingest")
            .put(TelemetrySettings.TRACER_FEATURE_ENABLED_SETTING.getKey(), true)
            .put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), true)
            .put(TelemetrySettings.METRICS_FEATURE_ENABLED_SETTING.getKey(), true)
            .build();
    }

    private static List<MockSpanData> spans(String traceId) {
        return new StrictCheckSpanProcessor().getFinishedSpanItems().stream().filter(span -> traceId.equals(span.getTraceID())).toList();
    }

    private static HttpResponse<String> request(HttpClient client, ServerlessNode node, String path, String traceId) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        return client.send(
            HttpRequest.newBuilder(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .header("Traceparent", traceId + "~external-parent")
                .header("X-Opaque-Id", "private-caller-identifier")
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );
    }

    private static final class RecordingPlugin extends MockTelemetryPlugin {
        private final TestInMemoryCounter responses = new TestInMemoryCounter();
        private final TestInMemoryHistogram latency = new TestInMemoryHistogram();

        @Override
        public Optional<Telemetry> getTelemetry(TelemetrySettings settings) {
            final MetricsTelemetry metrics = mock(MetricsTelemetry.class);
            when(metrics.createCounter(eq(RESPONSES), anyString(), anyString())).thenReturn(responses);
            when(metrics.createHistogram(eq(RESPONSE_TIME), anyString(), anyString())).thenReturn(latency);
            return Optional.of(new MockTelemetry(settings) {
                @Override
                public MetricsTelemetry getMetricsTelemetry() {
                    return metrics;
                }
            });
        }
    }
}
