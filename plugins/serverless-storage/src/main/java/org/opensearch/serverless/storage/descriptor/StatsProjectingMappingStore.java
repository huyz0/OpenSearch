/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executor;

/**
 * The authoritative mapping store, plus a write-behind projection kept only so the gated population's
 * field type counts stay answerable in one query.
 *
 * <p><b>The gap this closes.</b> Mappings moved into {@link org.opensearch.cluster.metadata.IndexDescriptor}
 * blobs, and {@link DescriptorBackedMappingStore} became the registered store. Nothing moved the aggregate
 * with them: {@link IndexBackedMappingStatsAggregator} still reads {@code .opensearch-index-mappings}, whose
 * only writer is {@link IndexBackedMappingStore}, which stopped being registered at the same moment. So the
 * aggregation searched an index nothing wrote, failed, was swallowed at debug, and {@code _cluster/stats}
 * reported the ordinary population's field types as though they were the whole cluster's. That is exactly
 * the failure H19 pinned and H20 built the seam to close, arrived at a second time from the other side --
 * and it is the least visible of this area's recurring shape, because a statistic that omits a population
 * returns a plausible number rather than an obvious gap.
 *
 * <p><b>Why a projection rather than an aggregate over the descriptors.</b> Summing field types across the
 * gated population means reading every descriptor, which is the population-sized cost this whole area
 * exists to remove; H20 shaped {@link org.opensearch.action.admin.cluster.stats.GatedMappingStatsAggregator}
 * so per-index iteration could not even be expressed through it. The mapping index is the materialised form
 * of that aggregate: one nested terms aggregation, one search, cost set by the number of distinct field
 * types rather than by the number of indices.
 *
 * <p><b>Why the projection is asynchronous, and what that costs.</b> T40 measured the index-backed store at
 * at least about 80% of what a declared mapping costs a creation, and T41 removed one of its two round
 * trips to get that back. Putting it in front of the mapping write again, synchronously, would pay the
 * whole of that for a statistic. So the descriptor write is what the caller waits on, and the projection
 * follows on the supplied executor. Two consequences, both deliberate:
 *
 * <ul>
 *   <li>the counts are eventually consistent, on top of the refresh bound H18 already established for any
 *       aggregation over an index; and
 *   <li>a projection write that fails is logged and dropped, so the statistic can under-report while the
 *       mapping itself is intact. A statistic must not fail a write, and the alternative -- failing the
 *       mapping write because its bookkeeping copy did not land -- is plainly worse.
 * </ul>
 *
 * <p><b>Reordering is safe without ordering the executor.</b> {@link IndexBackedMappingStore#compareAndSwap}
 * writes the generation as an external version, so a projection write that arrives after a newer one is
 * refused by the store rather than overwriting it. Two concurrent swaps therefore leave the projection at
 * the higher generation whichever order they land in, which is what makes "fire it at an executor" correct
 * here rather than merely convenient.
 *
 * <p><b>Reads never touch the projection.</b> It is a derived copy, and a read that could be served from
 * either would make it a second source of truth for mappings -- the dual-write the descriptor plane
 * deliberately removed when the descriptor system index went away.
 */
public final class StatsProjectingMappingStore implements MappingGenerationStore.Store {

    private static final Logger logger = LogManager.getLogger(StatsProjectingMappingStore.class);

    private final MappingGenerationStore.Store authoritative;
    private final MappingGenerationStore.Store projection;
    private final Executor executor;

    /** Projection writes handed to the executor and not yet finished. Doubles as the wait monitor. */
    private final java.util.concurrent.atomic.AtomicInteger inFlight = new java.util.concurrent.atomic.AtomicInteger();

    /**
     * How many projection writes may be in flight at once, and why there is a limit at all.
     *
     * <p>The executor this is given is {@code GENERIC}, which is also where a gated creation runs and where
     * the descriptor write that acknowledges it runs. A projection is a blocking indexing round trip, so
     * without a bound, N concurrent mapped creations put N blocked threads into that pool -- and measuring
     * creation throughput found exactly that: 127 of 132 {@code GENERIC} threads parked inside this class,
     * creations unable to get a thread at all, and an arm of 300 mapped creations that never finished. A
     * statistic had stopped the thing it is a statistic about.
     *
     * <p>Small on purpose. The projection exists so cluster stats can answer in one search; it is not on
     * any request's critical path, and the work it does is one document per mapping change. Four threads
     * absorb an ordinary rate of mapping changes and cannot crowd out anything, which is the trade this
     * class already made in the other direction when it chose to drop failed projections rather than fail
     * the mapping write.
     */
    private static final int MAX_IN_FLIGHT = 4;

    /**
     * @param authoritative the store that owns the mapping. Its answer is the caller's answer.
     * @param projection    the store whose only purpose is to make the aggregate cheap. Never read from.
     * @param executor      where projection writes run, off the caller's thread.
     */
    public StatsProjectingMappingStore(
        MappingGenerationStore.Store authoritative,
        MappingGenerationStore.Store projection,
        Executor executor
    ) {
        this.authoritative = java.util.Objects.requireNonNull(authoritative, "authoritative store is required");
        this.projection = java.util.Objects.requireNonNull(projection, "projection store is required");
        this.executor = java.util.Objects.requireNonNull(executor, "executor is required");
    }

    @Override
    public MappingGenerationStore.MappingGeneration read(String indexUuid) {
        return authoritative.read(indexUuid);
    }

    @Override
    public boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGenerationStore.MappingGeneration updated) {
        // The authoritative swap decides. A refused swap must project nothing: the caller is about to
        // re-read and merge, and recording a mapping that was never accepted would put a generation in the
        // projection that no descriptor ever held.
        if (authoritative.compareAndSwap(indexUuid, expectedGeneration, updated) == false) {
            return false;
        }
        project(() -> projection.compareAndSwap(indexUuid, expectedGeneration, updated), "record", indexUuid);
        return true;
    }

    @Override
    public void delete(String indexUuid) {
        // Authoritative first, and its failure propagates: the deletion path decides what a mapping it
        // could not remove means, and that decision is not this class's to pre-empt. The projection is
        // cleaned up only once the real one is gone, so a failed delete cannot leave the projection empty
        // while the mapping is still there.
        authoritative.delete(indexUuid);
        project(() -> {
            projection.delete(indexUuid);
            return true;
        }, "remove", indexUuid);
    }

    /**
     * Projects a mapping this store never saw, because the descriptor write carried it.
     *
     * <p><b>Why this exists, and why the projection would be half-blind without it.</b> A gated creation does
     * not route its declared mapping through {@link MappingGenerationStore} at all. T58 put the mapping
     * inside {@code IndexDescriptor} and {@code MetadataCreateIndexService} writes it with the descriptor in
     * one atomic operation, which is better than the round trip it replaced: the creation is acknowledged
     * exactly when the record of it lands, and there is no window where a name resolves to an index whose
     * declared fields are missing.
     *
     * <p>The consequence for the aggregate is that {@link #compareAndSwap} only ever sees *dynamic* field
     * updates. An index that declared its mapping at creation and never changed it -- which is most of them
     * -- would contribute nothing to cluster stats, which is the same silent omission this class was built to
     * fix, reached by a different road. So the descriptor write path calls this directly.
     *
     * <p>Idempotent by the same external-version rule as everything else here: called twice for one
     * generation, the second is refused by the store rather than duplicated.
     */
    public void projectDescriptorMapping(String indexUuid, long generation, java.util.Map<String, Object> fields) {
        if (indexUuid == null || fields == null || fields.isEmpty()) {
            // Nothing declared means nothing to count. Writing an empty projection document would put a row
            // in the aggregate for an index with no fields, which is not wrong so much as pure noise at a
            // hundred million of them.
            return;
        }
        MappingGenerationStore.MappingGeneration mapping = new MappingGenerationStore.MappingGeneration(Math.max(generation, 1L), fields);
        project(() -> projection.compareAndSwap(indexUuid, 0L, mapping), "record", indexUuid);
    }

    /**
     * Runs a projection write off-thread, swallowing everything.
     *
     * <p>Both failure modes are logged at warn rather than debug. The reason this class exists is a
     * projection failure that was invisible, and a silent aggregate is the exact symptom to make loud.
     */
    private void project(java.util.function.BooleanSupplier write, String what, String indexUuid) {
        if (reserve() == false) {
            // Dropped rather than queued, which is the same answer this class gives a projection that
            // fails: the counts under-report for this index until its next mapping change, and nothing the
            // caller is doing is affected. Queueing instead would only move the starvation into the queue,
            // since what is scarce is threads to block in and every queued projection eventually wants one.
            logger.debug(
                "not projecting the mapping stats for [{}]: {} projections already in flight; gated field "
                    + "type counts for it will under-report until its next mapping change",
                indexUuid,
                MAX_IN_FLIGHT
            );
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    if (write.getAsBoolean() == false) {
                        // A refusal here is ordinary: an external-version conflict means a newer generation
                        // is already projected, which is the reordering case working as intended.
                        logger.debug("mapping stats projection for [{}] was superseded by a newer generation", indexUuid);
                    }
                } catch (Exception e) {
                    logger.warn(
                        "could not {} the mapping stats projection for [{}]; gated field type counts in cluster "
                            + "stats will under-report until the next mapping change for it",
                        what,
                        indexUuid,
                        e
                    );
                } finally {
                    settle();
                }
            });
        } catch (Exception e) {
            // Everything the submission itself can throw, not only a full queue. A rejection is the expected
            // one and a pool shutting down is the other, and both have to land here for two reasons: so the
            // failure does not propagate into the mapping write this class exists to keep clean, and so the
            // in-flight count is settled. Missing the second would leave awaitQuiescence waiting out its whole
            // timeout on a task that will never run, which is a hang dressed as a slow shutdown.
            settle();
            logger.warn("mapping stats projection for [{}] could not be submitted; counts will under-report", indexUuid, e);
        }
    }

    /**
     * Claims one of the in-flight slots, or refuses.
     *
     * <p>A compare-and-set loop rather than {@code incrementAndGet} followed by a check, because the latter
     * has a window where the count reads above the bound: two callers can both increment past it and both
     * see a number they then have to undo, and a concurrent {@link #awaitQuiescence} would meanwhile be
     * waiting on a count that describes work nobody is doing.
     */
    private boolean reserve() {
        while (true) {
            int current = inFlight.get();
            if (current >= MAX_IN_FLIGHT) {
                return false;
            }
            if (inFlight.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    private void settle() {
        synchronized (inFlight) {
            if (inFlight.decrementAndGet() == 0) {
                inFlight.notifyAll();
            }
        }
    }

    /**
     * Waits for every projection write handed to the executor to finish.
     *
     * <p>An asynchronous write with no way to ask whether it has landed is a loose end, and this one has two
     * callers that need it. A node closing wants its last projections flushed rather than abandoned
     * mid-request, since an abandoned one leaves the counts under-reporting until that index's next mapping
     * change. And a test that ends while a projection is still running tears the cluster down underneath it,
     * which surfaces as a shard that is "still locked" rather than as anything pointing at the cause.
     *
     * @return true if everything settled within the timeout
     */
    public boolean awaitQuiescence(long timeoutMillis) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        synchronized (inFlight) {
            while (inFlight.get() > 0) {
                long remaining = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (remaining <= 0) {
                    return false;
                }
                try {
                    inFlight.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return true;
    }
}
