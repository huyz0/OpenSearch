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

    private volatile String doubleWinBlob;
    private volatile long doubleWinGeneration;
    private final Map<String, BlobRegisterCasResult> firstWin = new ConcurrentHashMap<>();

    private volatile String readsAsAbsentBlob;

    private volatile String partitionedBlob;
    private volatile BlobRegister shadow;

    /**
     * Cuts one register off from the store, so this caller sees a private copy of it.
     *
     * <p><b>This is what two winners actually look like.</b> Neither node's store is misbehaving in
     * isolation — each is atomic, each honours its own preconditions — and the register has simply
     * diverged, so a claim that is impossible against the true state is perfectly legal against this one.
     * Modelling it any other way needs two callers to be told different things by one object, which is not
     * a thing an object can do.
     *
     * <p>Reads see the private copy, which starts absent; creates and swaps apply to it and to nothing
     * else. The rest of the store is untouched, which is the point: the partition is one key wide, exactly
     * as a replica that has missed one write is.
     *
     * @param blobName the register to cut off, or null to reconnect
     * @return this
     */
    public MisbehavingBlobContainer partitionRegister(String blobName) {
        this.partitionedBlob = blobName;
        return this;
    }

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

    private volatile String doubleCreateBlob;
    private final Map<String, BlobRegisterCasResult> firstCreate = new ConcurrentHashMap<>();
    private volatile int ambiguousCreateEveryNth;
    private final AtomicInteger creates = new AtomicInteger();

    /**
     * Lets two put-if-absent calls on one name both be told they created it.
     *
     * <p>The same failure as two winners on a swap, at the moment it is most likely to happen for real: a
     * shard nobody has ever owned, claimed by two nodes at once at cold start, with the precondition
     * "this must not exist" evaluated on two replicas neither of which has seen the other.
     *
     * <p>Index name uniqueness rests on this too, and on the same call.
     *
     * @param blobName the register
     * @return this
     */
    public MisbehavingBlobContainer admitASecondCreatorOf(String blobName) {
        this.doubleCreateBlob = blobName;
        return this;
    }

    /**
     * Applies every nth put-if-absent and then loses the answer.
     *
     * @param everyNth how often; zero or less to stop
     * @return this
     */
    public MisbehavingBlobContainer ambiguousCreateEvery(int everyNth) {
        this.ambiguousCreateEveryNth = everyNth;
        return this;
    }

    @Override
    public BlobRegisterCasResult createRegisterIfAbsent(String blobName, BytesReference value) throws IOException {
        final int attempt = creates.incrementAndGet();
        if (blobName.equals(partitionedBlob)) {
            if (shadow != null) {
                return BlobRegisterCasResult.conflict(shadow.generation());
            }
            shadow = new BlobRegister(1L, value);
            return BlobRegisterCasResult.applied(1L);
        }
        if (blobName.equals(doubleCreateBlob)) {
            final BlobRegisterCasResult already = firstCreate.get(blobName);
            if (already != null) {
                // The second caller is told exactly what the first was told, and has no way to know.
                return already;
            }
            final BlobRegisterCasResult first = honest.createRegisterIfAbsent(blobName, value);
            if (first.applied()) {
                firstCreate.put(blobName, first);
            }
            return first;
        }
        final BlobRegisterCasResult result = honest.createRegisterIfAbsent(blobName, value);
        if (result.applied() && ambiguousCreateEveryNth > 0 && attempt % ambiguousCreateEveryNth == 0) {
            throw new AmbiguousOutcomeException(
                "the create of [" + blobName + "] applied and the response was lost; the caller cannot know which"
            );
        }
        return result;
    }

    /**
     * Lets two swaps against one generation both be told they won, with the same answer.
     *
     * <p><b>The failure the whole design rests on not happening</b>, modelled as it would actually occur:
     * the precondition passes on two replicas that have not seen each other's write, the writes collapse to
     * one under last-write-wins, and <em>both</em> callers are told they hold the register at the same
     * generation. Note the difference from a swap that merely wins against a newer generation — that is an
     * ordinary takeover, which this design fences correctly. Two callers holding the same generation is the
     * one it cannot.
     *
     * @param blobName the register
     * @param expectedGeneration the generation both swaps will claim
     * @return this
     */
    public MisbehavingBlobContainer admitASecondWinnerOn(String blobName, long expectedGeneration) {
        this.doubleWinBlob = blobName;
        this.doubleWinGeneration = expectedGeneration;
        return this;
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

    /**
     * Makes one register read as though it had never been written.
     *
     * <p>A replica that has not seen the write at all, rather than one showing an older value. It is the
     * state a second node has to be in for two of them to claim a shard nobody has ever owned: each looks,
     * sees nothing, and creates.
     *
     * @param blobName the register, or null to stop
     * @return this
     */
    public MisbehavingBlobContainer readsAsAbsent(String blobName) {
        this.readsAsAbsentBlob = blobName;
        return this;
    }

    @Override
    public Optional<BlobRegister> readRegister(String blobName) throws IOException {
        if (blobName.equals(partitionedBlob)) {
            return Optional.ofNullable(shadow);
        }
        if (blobName.equals(readsAsAbsentBlob)) {
            return Optional.empty();
        }
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

        if (blobName.equals(partitionedBlob)) {
            final long current = shadow == null ? BlobRegister.ABSENT_GENERATION : shadow.generation();
            if (current != expectedGeneration) {
                return BlobRegisterCasResult.conflict(current);
            }
            shadow = new BlobRegister(current + 1, newValue);
            return BlobRegisterCasResult.applied(current + 1);
        }

        if (blobName.equals(doubleWinBlob) && expectedGeneration == doubleWinGeneration) {
            final BlobRegisterCasResult already = firstWin.get(blobName);
            if (already != null) {
                // The second caller is told exactly what the first was told. Neither has any way to know.
                return already;
            }
            final BlobRegisterCasResult first = honest.compareAndSwapRegister(blobName, expectedGeneration, newValue);
            if (first.applied()) {
                firstWin.put(blobName, first);
            }
            return first;
        }

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
