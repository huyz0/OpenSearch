/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.plugins.TelemetryAwarePlugin;
import org.opensearch.plugins.TelemetryPlugin;
import org.opensearch.telemetry.TelemetryModule;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.MetricsRegistryFactory;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.TracerFactory;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.List;

/** Owns the OpenSearch telemetry services shared by the shell and its installed plugins. */
record NodeTelemetry(Tracer tracer, MetricsRegistry metrics) implements Closeable {

    static NodeTelemetry create(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool, List<TelemetryPlugin> plugins) {
        if (FeatureFlags.isEnabled(FeatureFlags.TELEMETRY) == false
            || (TelemetrySettings.TRACER_FEATURE_ENABLED_SETTING.get(settings) == false
                && TelemetrySettings.METRICS_FEATURE_ENABLED_SETTING.get(settings) == false)) {
            return new NodeTelemetry(NoopTracer.INSTANCE, NoopMetricsRegistry.INSTANCE);
        }
        if (plugins.stream().anyMatch(TelemetryAwarePlugin.class::isInstance)) {
            throw new IllegalStateException("Telemetry providers must not implement TelemetryAwarePlugin");
        }
        final TelemetrySettings telemetrySettings = new TelemetrySettings(settings, clusterSettings);
        final var telemetry = new TelemetryModule(plugins, telemetrySettings).getTelemetry();
        final Tracer tracer = telemetrySettings.isTracingFeatureEnabled()
            ? new TracerFactory(telemetrySettings, telemetry, threadPool.getThreadContext()).getTracer()
            : NoopTracer.INSTANCE;
        try {
            final MetricsRegistry metrics = telemetrySettings.isMetricsFeatureEnabled()
                ? new MetricsRegistryFactory(telemetrySettings, telemetry).getMetricsRegistry()
                : NoopMetricsRegistry.INSTANCE;
            return new NodeTelemetry(tracer, metrics);
        } catch (RuntimeException | Error e) {
            IOUtils.closeWhileHandlingException(tracer);
            throw e;
        }
    }

    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(metrics, tracer);
    }
}
