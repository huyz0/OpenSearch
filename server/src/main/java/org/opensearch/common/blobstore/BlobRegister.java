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

/**
 * The current value of a "register" blob together with its generation, as read via
 * {@link BlobContainer#readRegister}. A register is a small blob whose writes are arbitrated by
 * {@link BlobContainer#compareAndSwapRegister}, giving callers a generic compare-and-swap
 * primitive over whatever atomicity the underlying blob store natively provides (e.g. S3
 * If-Match, GCS generation preconditions, or local file locking on a filesystem repository).
 *
 * @param generation the generation the value was read at, for a later compare-and-swap
 * @param value the bytes the register held
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record BlobRegister(long generation, BytesReference value) {

    /** The generation reported for a register that has never been written. */
    public static final long ABSENT_GENERATION = 0L;

    /**
     * Describes the register without its contents.
     *
     * <p>The length rather than the bytes, deliberately: a register's value is arbitrary and can be
     * large, and a {@code toString} that rendered it would put it into every log line that mentions one.
     */
    @Override
    public String toString() {
        return "BlobRegister{generation=" + generation + ", valueLength=" + value.length() + '}';
    }
}
