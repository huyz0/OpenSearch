/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.bytes.BytesReference;

import java.util.Objects;

/**
 * The current value of a "register" blob together with its generation, as read via
 * {@link BlobContainer#readRegister}. A register is a small blob whose writes are arbitrated by
 * {@link BlobContainer#compareAndSwapRegister}, giving callers a generic compare-and-swap
 * primitive over whatever atomicity the underlying blob store natively provides (e.g. S3
 * If-Match, GCS generation preconditions, or local file locking on a filesystem repository).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class BlobRegister {

    /** The generation reported for a register that has never been written. */
    public static final long ABSENT_GENERATION = 0L;

    private final long generation;
    private final BytesReference value;

    public BlobRegister(long generation, BytesReference value) {
        this.generation = generation;
        this.value = Objects.requireNonNull(value, "value");
    }

    /** Monotonically increasing with each successful {@link BlobContainer#compareAndSwapRegister} write. */
    public long generation() {
        return generation;
    }

    public BytesReference value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BlobRegister)) return false;
        BlobRegister that = (BlobRegister) o;
        return generation == that.generation && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(generation, value);
    }

    @Override
    public String toString() {
        return "BlobRegister{generation=" + generation + ", valueLength=" + value.length() + '}';
    }
}
