/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An object store that is wrong in a specific, chosen way.
 *
 * <p><b>What this is for, and what it is not.</b> It cannot tell anybody whether S3 is linearizable — no
 * fake can, and pretending otherwise would be worse than not having one. What it answers is the question
 * underneath R11 that has never been asked at all: <em>if the store did deviate, what would this system
 * do about it?</em> "Everything is downstream of that assumption" is a sentence; these turn it into a
 * measurement of which things, how, and whether anybody would notice.
 *
 * <p>It is also how the linearizability checker earns its keep. A checker that has never rejected anything
 * is a checker nobody should believe, and the honest way to make it reject something is to run it against
 * a store that really is broken rather than against a history typed out by hand.
 *
 * <p><b>Each deviation is one a real provider could plausibly have.</b> None of them is arbitrary
 * corruption:
 *
 * <ul>
 *   <li><b>Stale reads</b> — a replica answering with a value that has already been overwritten. Atomic
 *       per operation, not ordered in real time. The classic eventually-consistent read.</li>
 *   <li><b>Two winners</b> — a conditional write whose precondition was evaluated somewhere that had not
 *       seen the previous write. The failure that puts two nodes on one shard.</li>
 *   <li><b>Ambiguous outcomes</b> — the write applied and the response was lost. Produced by the client's
 *       own retries as much as by the provider, which is why a perfectly linearizable provider can still
 *       be composed into something that is not.</li>
 *   <li><b>Lying about the current generation on a refusal</b> — a failed compare-and-swap reporting a
 *       generation that was never stored. The caller re-reads on that number and swaps against something
 *       that never existed.</li>
 * </ul>
 *
 * <p>Deviations are off unless switched on, so a container built from this and left alone behaves exactly
 * like the one it wraps.
 */
public final class MisbehavingBlobContainer extends DelegatingBlobContainer {

    /** Thrown when a call's outcome is deliberately made unknowable. */
    public static final class AmbiguousOutcomeException extends IOException {
        AmbiguousOutcomeException(String message) {
            super(message);
        }
    }

    private final BlobContainer honest;
    private final Map<String, BlobRegister> previous = new ConcurrentHashMap<>();

    private volatile int staleReadEveryNth;
    private volatile int twoWinnersEveryNth;
    private volatile int ambiguousCasEveryNth;
    private volatile long refusalReportsGeneration = Long.MIN_VALUE;

    private final AtomicInteger reads = new AtomicInteger();
    private final AtomicInteger swaps = new AtomicInteger();

    /**
     * Wraps a container.
     *
     * @param delegate the honest one
     */
    public MisbehavingBlobContainer(BlobContainer delegate) {
        super(delegate);
        this.honest = delegate;
    }

    /**
     * Serves an already-overwritten value on every nth read.
     *
     * @param everyNth how often; zero or less to stop
     * @return this
     */
    public MisbehavingBlobContainer staleReadEvery(int everyNth) {
        this.staleReadEveryNth = everyNth;
        return this;
    }

    /**
     * Lets every nth compare-and-swap win regardless of the generation it swapped against.
     *
     * @param everyNth how often; zero or less to stop
     * @return this
     */
    public MisbehavingBlobContainer twoWinnersEvery(int everyNth) {
        this.twoWinnersEveryNth = everyNth;
        return this;
    }

    /**
     * Applies every nth compare-and-swap and then loses the answer.
     *
     * @param everyNth how often; zero or less to stop
     * @return this
     */
    public MisbehavingBlobContainer ambiguousCasEvery(int everyNth) {
        this.ambiguousCasEveryNth = everyNth;
        return this;
    }

    /**
     * Makes a refused compare-and-swap report a generation that was never stored.
     *
     * @param generation the fiction to report; {@link Long#MIN_VALUE} to stop
     * @return this
     */
    public MisbehavingBlobContainer refusalsReportGeneration(long generation) {
        this.refusalReportsGeneration = generation;
        return this;
    }

    @Override
    public Optional<BlobRegister> readRegister(String blobName) throws IOException {
        final Optional<BlobRegister> current = honest.readRegister(blobName);
        if (staleReadEveryNth > 0 && reads.incrementAndGet() % staleReadEveryNth == 0) {
            final BlobRegister stale = previous.get(blobName);
            if (stale != null) {
                return Optional.of(stale);
            }
        }
        current.ifPresent(register -> previous.put(blobName, register));
        return current;
    }

    @Override
    public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
        throws IOException {
        final int attempt = swaps.incrementAndGet();

        if (twoWinnersEveryNth > 0 && attempt % twoWinnersEveryNth == 0) {
            // The precondition is evaluated against a replica that has not caught up, so it passes on a
            // generation that is no longer current. Forced by swapping against whatever is actually there.
            final long actual = honest.readRegister(blobName).map(BlobRegister::generation).orElse(BlobRegister.ABSENT_GENERATION);
            final BlobRegisterCasResult forced = honest.compareAndSwapRegister(blobName, actual, newValue);
            if (forced.applied()) {
                // Reported as if the caller's own expected generation had been the current one.
                return forced;
            }
            return forced;
        }

        final BlobRegisterCasResult result = honest.compareAndSwapRegister(blobName, expectedGeneration, newValue);

        if (result.applied() && ambiguousCasEveryNth > 0 && attempt % ambiguousCasEveryNth == 0) {
            throw new AmbiguousOutcomeException(
                "the write to [" + blobName + "] applied and the response was lost; the caller cannot know which"
            );
        }
        if (result.applied() == false && refusalReportsGeneration != Long.MIN_VALUE) {
            return BlobRegisterCasResult.conflict(refusalReportsGeneration);
        }
        return result;
    }
}
