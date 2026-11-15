/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.restcore;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.serverless.node.NodeContext;
import org.opensearch.serverless.ops.ResponseMemory;

/** One batch's retained responses, including work completing after its caller gives up. */
public final class ResponseBudget implements AutoCloseable {
    private final CircuitBreaker breaker;
    private long reserved;
    private volatile boolean closed;

    public ResponseBudget(NodeContext serving) {
        breaker = serving.services().circuitBreakerService().getBreaker(CircuitBreaker.REQUEST);
    }

    public void keep(Writeable value, Runnable retain) {
        if (closed) {
            throw new IllegalStateException("the batch no longer accepts responses");
        }
        keep(ResponseMemory.of(value), retain);
    }

    public synchronized void keep(long bytes, Runnable retain) {
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

    public synchronized void ensureAtLeast(long bytes) {
        if (bytes > reserved) {
            keep(bytes - reserved, () -> {});
        }
    }

    public synchronized void abandon(Runnable discard) {
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
