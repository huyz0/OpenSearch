/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.apache.ratis.util.Daemon;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Fixed worker and queue limits, with host-owned thread groups and explicit lifecycle ownership. */
final class JournalExecutors {
    private JournalExecutors() {}

    static ThreadPoolExecutor bounded(String name, int threads, int queue) {
        final var sequence = new AtomicLong();
        final var group = Thread.currentThread().getThreadGroup();
        final var executor = new ThreadPoolExecutor(
            threads,
            threads,
            30,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(queue),
            runnable -> Daemon.newBuilder()
                .setName(name + "-" + sequence.getAndIncrement())
                .setThreadGroup(group)
                .setRunnable(runnable)
                .build(),
            new ThreadPoolExecutor.AbortPolicy()
        );
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }
}
