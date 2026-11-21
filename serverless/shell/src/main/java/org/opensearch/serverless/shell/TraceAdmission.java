/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.common.util.TokenBucket;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** Caps the work offered to the provider, including spans with a sampled parent. */
record TraceAdmission(TokenBucket rate, Semaphore active, ScheduledExecutorService scheduler, long maxAgeNanos) {
    TraceAdmission(ScheduledExecutorService scheduler) {
        this(
            new TokenBucket(System::nanoTime, 512.0 / TimeUnit.SECONDS.toNanos(1), 512),
            new Semaphore(1024),
            scheduler,
            TimeUnit.MINUTES.toNanos(5)
        );
    }

    boolean tryAcquire() {
        if (active.tryAcquire() == false) {
            return false;
        }
        if (rate.request()) {
            return true;
        }
        release();
        return false;
    }

    void release() {
        active.release();
    }

    ScheduledFuture<?> expire(Runnable finish) {
        return scheduler.schedule(finish, maxAgeNanos, TimeUnit.NANOSECONDS);
    }
}
