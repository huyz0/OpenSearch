/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.common.util.TokenBucket;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanCreationContext;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.channels.TraceableTcpTransportChannel;
import org.opensearch.telemetry.tracing.noop.NoopSpan;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TcpTransportChannel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServerlessTraceBoundsTests extends OpenSearchTestCase {
    private static final Map<String, java.util.Collection<String>> SAMPLED = Map.of(
        "traceparent",
        List.of("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
    );

    public void testSampledParentsCannotBypassTheRateLimit() {
        final var clock = new AtomicLong();
        final Tracer provider = provider();
        final var tracer = new ProtocolTracer(
            provider,
            action -> true,
            new TraceAdmission(new TokenBucket(clock::get, 1, 2), new Semaphore(5), mock(ScheduledExecutorService.class), 1)
        );
        for (int i = 0; i < 2; i++) {
            final Span admitted = tracer.startSpan(SpanCreationContext.server(), SAMPLED);
            assertNotSame(NoopSpan.INSTANCE, admitted);
            admitted.endSpan();
        }
        for (int i = 0; i < 1_000; i++) {
            assertSame(NoopSpan.INSTANCE, tracer.startSpan(SpanCreationContext.server(), SAMPLED));
        }
        clock.incrementAndGet();
        final Span admitted = tracer.startSpan(SpanCreationContext.server(), SAMPLED);
        assertNotSame(NoopSpan.INSTANCE, admitted);
        admitted.endSpan();
        assertSame(NoopSpan.INSTANCE, tracer.startSpan(SpanCreationContext.server(), SAMPLED));
        verify(provider, times(3)).startSpan(any(SpanCreationContext.class), eq(SAMPLED));
    }

    public void testActiveSpanLimitRecoversOnExpiryAndCompletion() {
        final Tracer provider = provider();
        final Span first = mock(Span.class);
        final Span second = mock(Span.class);
        when(provider.startSpan(any(SpanCreationContext.class), anyMap())).thenReturn(first, second);
        final var scheduler = mock(ScheduledExecutorService.class);
        final var expirations = new ArrayList<Runnable>();
        when(scheduler.schedule(any(Runnable.class), eq(100L), eq(TimeUnit.NANOSECONDS))).thenAnswer(invocation -> {
            expirations.add(invocation.getArgument(0));
            return null;
        });
        final var tracer = new ProtocolTracer(
            provider,
            action -> true,
            new TraceAdmission(new TokenBucket(() -> 0L, 1, 10), new Semaphore(1), scheduler, 100)
        );
        final Span active = tracer.startSpan(SpanCreationContext.server(), SAMPLED);
        assertNotSame(NoopSpan.INSTANCE, active);
        assertSame(NoopSpan.INSTANCE, tracer.startSpan(SpanCreationContext.server(), SAMPLED));
        final var tcp = mock(TcpChannel.class);
        final var exchange = mock(TcpTransportChannel.class);
        when(exchange.getChannel()).thenReturn(tcp);
        final var onClose = new AtomicReference<ActionListener<Void>>();
        doAnswer(invocation -> {
            onClose.set(invocation.getArgument(0));
            return null;
        }).when(tcp).addCloseListener(any());
        TraceableTcpTransportChannel.create(exchange, active, tracer);
        expirations.getFirst().run();
        onClose.get().onResponse(null);
        verify(first, never()).addEvent(any());
        active.endSpan();
        verify(first, times(1)).endSpan();
        final Span next = tracer.startSpan(SpanCreationContext.server(), SAMPLED);
        assertNotSame(NoopSpan.INSTANCE, next);
        assertSame(NoopSpan.INSTANCE, tracer.startSpan(SpanCreationContext.server(), SAMPLED));
        next.endSpan();
        expirations.getLast().run();
        verify(second, times(1)).endSpan();
        assertNotSame(NoopSpan.INSTANCE, tracer.startSpan(SpanCreationContext.server(), SAMPLED));
    }

    public void testTcpExchangesShareOneCloseListener() throws Exception {
        final var tcp = mock(TcpChannel.class);
        final var exchange = mock(TcpTransportChannel.class);
        when(exchange.getChannel()).thenReturn(tcp);
        final var closeListener = new AtomicReference<ActionListener<Void>>();
        doAnswer(invocation -> {
            assertTrue(closeListener.compareAndSet(null, invocation.getArgument(0)));
            return null;
        }).when(tcp).addCloseListener(any());
        final Tracer provider = provider();
        when(provider.withSpanInScope(any(Span.class))).thenAnswer(invocation -> NoopTracer.INSTANCE.withSpanInScope(null));

        assertSame(exchange, TraceableTcpTransportChannel.create(exchange, NoopSpan.INSTANCE, provider));
        verify(tcp, never()).addCloseListener(any());
        final List<Span> completed = new ArrayList<>();
        for (int i = 0; i < 1_024; i++) {
            final Span span = mock(Span.class);
            completed.add(span);
            TraceableTcpTransportChannel.create(exchange, span, provider).sendResponse(TransportResponse.Empty.INSTANCE);
        }
        final Span unfinished = mock(Span.class);
        TraceableTcpTransportChannel.create(exchange, unfinished, provider);
        closeListener.get().onResponse(null);
        verify(tcp, times(1)).addCloseListener(any());
        verify(unfinished, times(1)).endSpan();
        for (Span span : completed) {
            verify(span, times(1)).endSpan();
            verify(span, never()).addEvent(any());
        }
    }

    private static Tracer provider() {
        final Tracer provider = mock(Tracer.class);
        when(provider.isRecording()).thenReturn(true);
        when(provider.startSpan(any(SpanCreationContext.class), anyMap())).thenAnswer(invocation -> mock(Span.class));
        return provider;
    }
}
