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
     * Where starting generations come from: one secure source for the whole process, never a seeded or
     * per-thread one. Uniqueness is the property, and a test framework hands threads generators derived from one
     * seed, which drew the same "random" generation for two registers created at once.
     */
    private static final java.security.SecureRandom GENERATIONS = org.opensearch.common.Randomness.createSecure();

    /**
     * The generation a register starts at when it is created from absent.
     *
     * <p>Random, not 1, so that generations never repeat across a register's incarnations: a register
     * deleted and created again starts somewhere its predecessor's generations almost surely never were.
     * Starting at 1 made a compare-and-swap carrying a generation read before a delete succeed against the
     * register created after it -- the same number, a different incarnation -- which is why callers had to
     * keep a tombstone in place of every deleted register rather than delete it. Generations still only
     * increase within an incarnation, and callers only ever compare them for equality.
     *
     * <p>Drawn from {@code [2^32, 2^62)}: clear of the small numbers a caller might mistake for a count, and
     * with room for any number of increments before overflow.
     *
     * @return a fresh starting generation
     */
    public static long initialGeneration() {
        return 1L << 32 | (GENERATIONS.nextLong() & ((1L << 62) - 1));
    }

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
