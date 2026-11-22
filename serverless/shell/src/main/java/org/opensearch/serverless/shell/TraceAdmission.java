/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.common.util.TokenBucket;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** Caps the work offered to the provider, including spans with a sampled parent. */
record TraceAdmission(TokenBucket rate, Semaphore active, ScheduledExecutorService scheduler, long maxAgeNanos, Counter dropped) {
    static final String DROPPED_METRIC = "opensearch.serverless.trace.dropped_spans";
    private static final Tags RATE_LIMIT = Tags.of("reason", "rate_limit");
    private static final Tags IN_FLIGHT_LIMIT = Tags.of("reason", "in_flight_limit");

    TraceAdmission(ScheduledExecutorService scheduler, MetricsRegistry metrics) {
        this(
            new TokenBucket(System::nanoTime, 512.0 / TimeUnit.SECONDS.toNanos(1), 512),
            new Semaphore(1024),
            scheduler,
            TimeUnit.MINUTES.toNanos(5),
            metrics.createCounter(DROPPED_METRIC, "Protocol span starts refused by the shell tracing limits", "1")
        );
    }

    boolean tryAcquire() {
        if (active.tryAcquire() == false) {
            dropped.add(1, IN_FLIGHT_LIMIT);
            return false;
        }
        if (rate.request()) {
            return true;
        }
        release();
        dropped.add(1, RATE_LIMIT);
        return false;
    }

    void release() {
        active.release();
    }

    ScheduledFuture<?> expire(Runnable finish) {
        return scheduler.schedule(finish, maxAgeNanos, TimeUnit.NANOSECONDS);
    }
}
