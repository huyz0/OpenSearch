/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.rest.RestRequest;
import org.opensearch.telemetry.tracing.AttributeNames;
import org.opensearch.telemetry.tracing.ScopedSpan;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanContext;
import org.opensearch.telemetry.tracing.SpanCreationContext;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.attributes.Attributes;
import org.opensearch.telemetry.tracing.noop.NoopSpan;
import org.opensearch.telemetry.tracing.noop.NoopTracer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

/** Restricts the data exported by the core HTTP and transport instrumentation. */
record ProtocolTracer(Tracer delegate, Predicate<String> registeredAction) implements Tracer {

    @Override
    public Span startSpan(SpanCreationContext context) {
        return isRecording() ? new ProtocolSpan(delegate.startSpan(safeContext(context))) : NoopSpan.INSTANCE;
    }

    @Override
    public Span startSpan(SpanCreationContext context, Map<String, Collection<String>> headers) {
        if (isRecording() == false) {
            return NoopSpan.INSTANCE;
        }
        Map<String, Collection<String>> propagation = new HashMap<>();
        headers.forEach((name, values) -> {
            String key = name.toLowerCase(Locale.ROOT);
            if (key.equals("traceparent") || key.equals("tracestate")) {
                propagation.put(key, values);
            }
        });
        return new ProtocolSpan(delegate.startSpan(safeContext(context), propagation));
    }

    private SpanCreationContext safeContext(SpanCreationContext context) {
        Map<String, ?> attributes = context.getAttributes() == null ? Map.of() : context.getAttributes().getAttributesMap();
        if (attributes.get(AttributeNames.TRANSPORT_ACTION) instanceof String action) {
            if (registeredAction.test(action)) {
                return context.name(action).attributes(Attributes.create().addAttribute(AttributeNames.TRANSPORT_ACTION, action));
            }
            return context.name("Serverless transport").attributes(Attributes.EMPTY);
        }
        if (attributes.get(AttributeNames.HTTP_METHOD) instanceof String method) {
            for (RestRequest.Method known : RestRequest.Method.values()) {
                if (known.name().equals(method)) {
                    return context.name("Serverless HTTP").attributes(Attributes.create().addAttribute(AttributeNames.HTTP_METHOD, method));
                }
            }
        }
        return context.name("Serverless REST").attributes(Attributes.EMPTY);
    }

    @Override
    public SpanContext getCurrentSpan() {
        return delegate.getCurrentSpan();
    }

    @Override
    public ScopedSpan startScopedSpan(SpanCreationContext context) {
        return isRecording() ? delegate.startScopedSpan(safeContext(context)) : NoopTracer.INSTANCE.startScopedSpan(context);
    }

    @Override
    public SpanScope withSpanInScope(Span span) {
        if (span == NoopSpan.INSTANCE) {
            return NoopTracer.INSTANCE.withSpanInScope(span);
        }
        return delegate.withSpanInScope(span instanceof ProtocolSpan protocolSpan ? protocolSpan.delegate() : span);
    }

    @Override
    public boolean isRecording() {
        return delegate.isRecording();
    }

    @Override
    public void close() {
        // NodeTelemetry owns the provider and closes it after the network services stop.
    }

    private record ProtocolSpan(Span delegate) implements Span {
        @Override
        public void endSpan() {
            delegate.endSpan();
        }

        @Override
        public Span getParentSpan() {
            return delegate.getParentSpan();
        }

        @Override
        public String getSpanName() {
            return delegate.getSpanName();
        }

        @Override
        public void addAttribute(String key, String value) {
            delegate.addAttribute(key, value);
        }

        @Override
        public void addAttribute(String key, Long value) {
            delegate.addAttribute(key, value);
        }

        @Override
        public void addAttribute(String key, Double value) {
            delegate.addAttribute(key, value);
        }

        @Override
        public void addAttribute(String key, Boolean value) {
            delegate.addAttribute(key, value);
        }

        @Override
        public void setError(Exception exception) {
            delegate.setError(new ProtocolFailure(exception.getClass().getSimpleName()));
        }

        @Override
        public void addEvent(String event) {
            delegate.addEvent(event);
        }

        @Override
        public String getTraceId() {
            return delegate.getTraceId();
        }

        @Override
        public String getSpanId() {
            return delegate.getSpanId();
        }
    }

    private static final class ProtocolFailure extends Exception {
        private ProtocolFailure(String type) {
            super(type, null, false, false);
        }
    }
}
