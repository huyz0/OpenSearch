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
 * it lost the race and {@link #currentGeneration()} reports what the register is actually at now
 * so the caller can re-read and retry rather than guessing.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class BlobRegisterCasResult {

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

    /** The generation now stored in the register: the new one if {@link #applied()}, otherwise the one that caused the conflict. */
    public long currentGeneration() {
        return currentGeneration;
    }

    @Override
    public String toString() {
        return "BlobRegisterCasResult{applied=" + applied + ", currentGeneration=" + currentGeneration + '}';
    }
}
