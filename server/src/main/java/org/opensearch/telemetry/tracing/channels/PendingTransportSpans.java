/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.channels;

import org.opensearch.core.action.ActionListener;
import org.opensearch.telemetry.tracing.CompletionAwareSpan;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.transport.TcpChannel;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Shares a connection close listener across only the spans whose exchanges are still pending. */
final class PendingTransportSpans {
    private static final int MAX_PENDING = 1024;
    private static final Map<TcpChannel, PendingTransportSpans> CONNECTIONS = new ConcurrentHashMap<>();

    private final Set<Span> pending = new HashSet<>();
    private boolean listening;
    private boolean closed;

    static Runnable track(TcpChannel channel, Span span) {
        final PendingTransportSpans connection = CONNECTIONS.computeIfAbsent(channel, ignored -> new PendingTransportSpans());
        return connection.add(channel, span);
    }

    private Runnable add(TcpChannel channel, Span span) {
        final boolean register;
        synchronized (this) {
            if (closed || pending.size() >= MAX_PENDING) {
                return span::endSpan;
            }
            pending.add(span);
            register = listening == false;
            listening = true;
        }
        if (span instanceof CompletionAwareSpan timed) {
            timed.addCompletionListener(() -> forget(span));
        }
        if (register) {
            channel.addCloseListener(ActionListener.wrap(unused -> close(channel, null), error -> close(channel, error)));
        }
        return () -> complete(span);
    }

    private void complete(Span span) {
        if (forget(span)) {
            span.endSpan();
        }
    }

    private synchronized boolean forget(Span span) {
        return pending.remove(span);
    }

    private void close(TcpChannel channel, Exception failure) {
        final List<Span> unfinished;
        synchronized (this) {
            closed = true;
            unfinished = new ArrayList<>(pending);
            pending.clear();
        }
        CONNECTIONS.remove(channel, this);
        for (Span span : unfinished) {
            try {
                span.addEvent("The TransportChannel was closed without sending the response");
                if (failure != null) {
                    span.setError(failure);
                }
            } finally {
                span.endSpan();
            }
        }
    }
}
