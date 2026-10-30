/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.serverless.shell.ServerlessNode;

/** One batch's retained responses, including work completing after its caller gives up. */
final class ResponseBudget implements AutoCloseable {
    private final CircuitBreaker breaker;
    private long reserved;
    private volatile boolean closed;

    ResponseBudget(ServerlessNode serving) {
        breaker = serving.circuitBreakerService().getBreaker(CircuitBreaker.REQUEST);
    }

    void keep(Writeable value, Runnable retain) {
        if (closed) {
            throw new IllegalStateException("the batch no longer accepts responses");
        }
        keep(ResponseMemory.of(value), retain);
    }

    synchronized void keep(long bytes, Runnable retain) {
        if (closed) {
            throw new IllegalStateException("the batch no longer accepts responses");
        }
        breaker.addEstimateBytesAndMaybeBreak(bytes, "serverless_batch_response");
        reserved += bytes;
        try {
            retain.run();
        } catch (RuntimeException | Error e) {
            reserved -= bytes;
            breaker.addWithoutBreaking(-bytes);
            throw e;
        }
    }

    synchronized void ensureAtLeast(long bytes) {
        if (bytes > reserved) {
            keep(bytes - reserved, () -> {});
        }
    }

    synchronized void abandon(Runnable discard) {
        try {
            discard.run();
        } finally {
            close();
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (reserved != 0L) {
            breaker.addWithoutBreaking(-reserved);
            reserved = 0L;
        }
    }
}
