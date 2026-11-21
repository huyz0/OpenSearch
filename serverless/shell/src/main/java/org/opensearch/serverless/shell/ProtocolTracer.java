/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.common.concurrent.CompletableContext;
import org.opensearch.rest.RestRequest;
import org.opensearch.telemetry.tracing.AttributeNames;
import org.opensearch.telemetry.tracing.CompletionAwareSpan;
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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** Restricts the data exported by the core HTTP and transport instrumentation. */
record ProtocolTracer(Tracer delegate, Predicate<String> registeredAction, TraceAdmission admission) implements Tracer {

    @Override
    public Span startSpan(SpanCreationContext context) {
        return admit(() -> delegate.startSpan(safeContext(context)));
    }

    @Override
    public Span startSpan(SpanCreationContext context, Map<String, Collection<String>> headers) {
        return admit(() -> {
            Map<String, Collection<String>> propagation = new HashMap<>();
            headers.forEach((name, values) -> {
                String key = name.toLowerCase(Locale.ROOT);
                if (key.equals("traceparent") || key.equals("tracestate")) {
                    propagation.put(key, values);
                }
            });
            return delegate.startSpan(safeContext(context), propagation);
        });
    }

    private Span admit(Supplier<Span> create) {
        if (isRecording() == false || admission.tryAcquire() == false) {
            return NoopSpan.INSTANCE;
        }
        final Span span;
        try {
            span = create.get();
        } catch (RuntimeException | Error exception) {
            admission.release();
            throw exception;
        }
        if (span == NoopSpan.INSTANCE) {
            admission.release();
            return span;
        }
        final ProtocolSpan tracked = new ProtocolSpan(span);
        try {
            tracked.expiry = admission.expire(tracked::endSpan);
            return tracked;
        } catch (RejectedExecutionException shutdown) {
            tracked.endSpan();
            return NoopSpan.INSTANCE;
        }
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
        if (startSpan(context) instanceof ProtocolSpan span) {
            span.scope = withSpanInScope(span);
            return span;
        }
        return ScopedSpan.NO_OP;
    }

    @Override
    public SpanScope withSpanInScope(Span span) {
        if (span == NoopSpan.INSTANCE) {
            return NoopTracer.INSTANCE.withSpanInScope(span);
        }
        return delegate.withSpanInScope(span instanceof ProtocolSpan protocolSpan ? protocolSpan.delegate : span);
    }

    @Override
    public boolean isRecording() {
        return delegate.isRecording();
    }

    @Override
    public void close() {
        // NodeTelemetry owns the provider and closes it after the network services stop.
    }

    private final class ProtocolSpan implements CompletionAwareSpan, ScopedSpan {
        private final Span delegate;
        private final CompletableContext<Void> completion = new CompletableContext<>();
        private volatile ScheduledFuture<?> expiry;
        private SpanScope scope;

        private ProtocolSpan(Span delegate) {
            this.delegate = delegate;
        }

        @Override
        public void close() {
            try {
                scope.close();
            } finally {
                endSpan();
            }
        }

        @Override
        public void endSpan() {
            if (completion.complete(null)) {
                try {
                    if (expiry != null) {
                        expiry.cancel(false);
                    }
                    delegate.endSpan();
                } finally {
                    admission.release();
                }
            }
        }

        @Override
        public void addCompletionListener(Runnable listener) {
            completion.addListener((unused, failure) -> listener.run());
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
        public void addAttribute(String key, long value) {
            delegate.addAttribute(key, value);
        }

        @Override
        public void addAttribute(String key, double value) {
            delegate.addAttribute(key, value);
        }

        @Override
        public void addAttribute(String key, boolean value) {
            delegate.addAttribute(key, value);
        }

        @Override
        public void setError(Exception exception) {
            if (exception != null) {
                delegate.setError(new ProtocolFailure(exception.getClass().getSimpleName()));
            }
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
