/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Node-wide proposals remain charged until the underlying persistence future completes. */
final class JournalAdmission {
    private final int maximumRequests;
    private final long maximumBytes;
    private int requests;
    private long bytes;
    private boolean closed;

    JournalAdmission(JournalConfiguration.Limits limits) {
        maximumRequests = limits.maximumPendingRequests();
        maximumBytes = limits.maximumPendingBytes();
    }

    synchronized Permit acquire(int payloadBytes) throws IOException {
        final long charge = 6L * (payloadBytes + 512L);
        if (closed || requests >= maximumRequests || charge > maximumBytes - bytes) {
            throw new IOException("journal admission unavailable: closed or pending proposal limit reached");
        }
        requests++;
        bytes += charge;
        return new Permit(charge);
    }

    synchronized void close() {
        closed = true;
    }

    synchronized int pendingRequests() {
        return requests;
    }

    synchronized long pendingBytes() {
        return bytes;
    }

    final class Permit implements AutoCloseable {
        private final long charge;
        private final AtomicBoolean released = new AtomicBoolean();

        private Permit(long charge) {
            this.charge = charge;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                synchronized (JournalAdmission.this) {
                    requests--;
                    bytes -= charge;
                }
            }
        }
    }
}
