/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.node;

import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.serverless.usage.Guardrails;

import java.util.concurrent.Semaphore;

/**
 * Where document writes run, and how many apply to the engine at once.
 *
 * <p><b>Why writes do not run on core's {@code write} pool.</b> A write is acknowledged only once its record has landed in
 * the node log, and the log lands one window at a time (250 ms by default), so a write's thread spends most of its time
 * waiting, not working. Core's {@code write} pool has as many threads as cores, plus one at most: on four cores, four
 * threads each holding a write for about a quarter of a second took about sixteen REST writes a second, whatever the
 * document size, and the rest waited in its queue for seconds. The pool a forwarded write is applied on at its owner
 * was sized the same way. Both have {@link org.opensearch.serverless.node.WriteSettings#WAIT_POOL_THREADS_PER_CORE} threads a core, enough for the
 * waiting a node's CPU can feed.
 *
 * <p><b>What stays bounded by cores.</b> Applying an operation to the engine is the CPU-bound part, and the number of
 * threads indexing into a shard at once is the number of in-memory segments Lucene keeps for it, hence the segments each
 * refresh publishes. So the engine apply goes through a gate of as many permits as core's {@code write} pool had threads:
 * as many concurrent applies as before, only the waiting for the log is spread over more threads. A write waiting for the
 * gate has been admitted by the write limiter, which counts it as waiting, so a node short of CPU still sheds load with a
 * 429 as before.
 *
 * <p><b>The in-flight cap.</b> With the waiting no longer bounded by the thread count, the limiter's cap on writes in
 * flight is the bound, and a node needs about 0.35 s times its write rate of it; with this on, its default grows with the
 * cores, {@link #IN_FLIGHT_PER_CORE} a core and never below the old 1,024.
 */
public final class WritePools {

    /** The default for {@link org.opensearch.serverless.node.WriteSettings#WAIT_POOL_THREADS_PER_CORE}. */
    public static final int DEFAULT_THREADS_PER_CORE = 64;

    /** Writes in flight per core the limiter allows by default. */
    public static final int IN_FLIGHT_PER_CORE = 384;

    /** The limiter's in-flight cap before it grew with cores, and its floor since. */
    public static final int MIN_IN_FLIGHT = 1_024;

    /** The pool document writes run on. */
    public static final String POOL = "serverless_write";

    private final Semaphore gate;
    private final Guardrails guardrails;

    private WritePools(Semaphore gate, Guardrails guardrails) {
        this.gate = gate;
        this.guardrails = guardrails;
    }

    /**
     * Builds the gate for a node.
     *
     * @param settings the node settings
     * @return the gate
     */
    public static WritePools of(Settings settings) {
        return of(settings, null);
    }

    /**
     * Builds the gate for a node, counting each write's documents for its index.
     *
     * @param settings the node settings
     * @param guardrails the node's cost guardrails, or null for none
     * @return the gate
     */
    public static WritePools of(Settings settings, Guardrails guardrails) {
        // Not fair: a fair gate hands each released permit to the longest waiter, a thread switch per apply, and with
        // thousands of writers queued that handing-over, not CPU, was what bounded a node's bulk rate.
        return new WritePools(new Semaphore(applyPermits(settings), false), guardrails);
    }

    /**
     * How many threads a pool that waits for writes has.
     *
     * @param settings the node settings
     * @return the thread count
     */
    public static int threads(Settings settings) {
        final int processors = OpenSearchExecutors.allocatedProcessors(settings);
        return Math.max(processors, processors * Math.max(1, WriteSettings.WAIT_POOL_THREADS_PER_CORE.get(settings)));
    }

    /**
     * As many concurrent engine applies as core's {@code write} pool had threads.
     *
     * @param settings the node settings
     * @return the permits
     */
    public static int applyPermits(Settings settings) {
        return OpenSearchExecutors.allocatedProcessors(settings) + 1;
    }

    /**
     * The limiter's default cap on writes in flight.
     *
     * @param settings the node settings
     * @return {@link #IN_FLIGHT_PER_CORE} a core, and at least 1,024
     */
    public static int defaultMaxInFlight(Settings settings) {
        return Math.max(MIN_IN_FLIGHT, IN_FLIGHT_PER_CORE * OpenSearchExecutors.allocatedProcessors(settings));
    }

    /**
     * Takes a permit to apply documents to a shard, counting them for its index.
     *
     * @param shard the shard written to
     * @param docs how many documents the apply writes
     * @return gives the permit back
     * @throws java.io.IOException if interrupted while waiting, before anything was applied
     */
    public Releasable enterApply(IndexShard shard, int docs) throws java.io.IOException {
        wrote(shard, docs);
        return enterApply();
    }

    /**
     * Counts documents written to a shard for its index.
     *
     * @param shard the shard
     * @param docs how many
     */
    public void wrote(IndexShard shard, int docs) {
        if (guardrails != null) {
            final String index = shard.shardId().getIndexName();
            guardrails.note(index, shard.indexSettings().getSettings());
            guardrails.wrote(index, docs);
        }
    }

    /**
     * Takes a permit to apply to the engine, waiting for one if all are taken.
     *
     * @return gives the permit back
     * @throws java.io.IOException if interrupted while waiting, before anything was applied
     */
    public Releasable enterApply() throws java.io.IOException {
        try {
            gate.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.io.IOException("interrupted waiting to apply a write; nothing was applied", e);
        }
        return gate::release;
    }

    /**
     * Reports how many applies could start now, for tests.
     *
     * @return free permits
     */
    public int freePermits() {
        return gate.availablePermits();
    }
}
