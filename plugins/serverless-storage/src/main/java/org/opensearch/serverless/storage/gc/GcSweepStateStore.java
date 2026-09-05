/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import java.io.IOException;

/**
 * Where one shard's {@link GcSweepState} is kept between sweeps.
 *
 * <p>An interface rather than the blob implementation directly for the same reason {@code ShardStateStore}
 * is one: the sweep does not care what is underneath, and a test that wants to drive the orphan clock by
 * hand should not have to stand up a blob store to do it.
 *
 * <p>Deliberately last-writer-wins rather than compare-and-swap. This is bookkeeping, not truth: two nodes
 * sweeping the same shard concurrently (which the current wiring allows -- one sweep per reader replica)
 * each hold a correct-but-partial view, and the loser of a write race simply re-observes the bundles it was
 * tracking on its next tick, costing a delayed deletion rather than an early one. Paying for CAS retries to
 * protect a hint would be the wrong trade; the values that must be linearizable (the head, the pins) live in
 * registers and are read fresh on every sweep.
 */
public interface GcSweepStateStore {

    /**
     * The state left by the previous sweep, or {@link GcSweepState#empty()} if there has never been one.
     *
     * @return the persisted state; never {@code null}.
     * @throws IOException if the underlying store could not be read at all -- absence is an answer and is
     *                     reported as {@link GcSweepState#empty()}, so an exception here means a real
     *                     store problem and the sweep must not proceed on a guess.
     */
    GcSweepState read() throws IOException;

    /**
     * Records the state for the next sweep to pick up.
     *
     * @param state what this sweep observed.
     * @throws IOException if the write failed.
     */
    void write(GcSweepState state) throws IOException;
}
