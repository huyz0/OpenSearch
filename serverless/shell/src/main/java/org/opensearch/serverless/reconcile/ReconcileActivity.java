/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.reconcile;

import org.opensearch.common.lease.Releasable;

/** Stops admission and waits for existing work without holding a lock across its I/O or child tasks. */
final class ReconcileActivity implements java.io.Closeable {
    private volatile boolean closed;
    private long active;

    synchronized Releasable enter() {
        if (closed) {
            return null;
        }
        active++;
        return this::leave;
    }

    boolean isClosed() {
        return closed;
    }

    synchronized void stopAccepting() {
        closed = true;
    }

    private synchronized void leave() {
        assert active > 0;
        if (--active == 0) {
            notifyAll();
        }
    }

    @Override
    public void close() {
        boolean interrupted = false;
        synchronized (this) {
            closed = true;
            while (active != 0) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
