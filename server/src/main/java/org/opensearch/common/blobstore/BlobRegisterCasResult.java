/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * The outcome of a {@link BlobContainer#compareAndSwapRegister} attempt: either the write applied
 * ({@link #applied()} is {@code true} and {@link #currentGeneration()} is the new generation), or
 * it lost the race and {@link #currentGeneration()} reports an observed conflicting generation or
 * {@link #UNKNOWN_GENERATION} when the backend cannot report that generation atomically. A later read
 * may observe a different state, so callers must read the current value before making a new decision.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class BlobRegisterCasResult {

    /** A refused write whose conflicting generation was not returned by the backend; never a valid stored generation. */
    public static final long UNKNOWN_GENERATION = -1L;

    private final boolean applied;
    private final long currentGeneration;

    private BlobRegisterCasResult(boolean applied, long currentGeneration) {
        this.applied = applied;
        this.currentGeneration = currentGeneration;
    }

    public static BlobRegisterCasResult applied(long newGeneration) {
        return new BlobRegisterCasResult(true, newGeneration);
    }

    public static BlobRegisterCasResult conflict(long actualCurrentGeneration) {
        return new BlobRegisterCasResult(false, actualCurrentGeneration);
    }

    public boolean applied() {
        return applied;
    }

    /**
     * The new generation if applied; otherwise an observed conflicting generation or {@link #UNKNOWN_GENERATION}.
     * A conflict does not require a subsequent read, whose result could describe a different state.
     */
    public long currentGeneration() {
        return currentGeneration;
    }

    @Override
    public String toString() {
        return "BlobRegisterCasResult{applied=" + applied + ", currentGeneration=" + currentGeneration + '}';
    }
}
