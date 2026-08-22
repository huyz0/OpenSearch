/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.e2e;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.storage.security.RegisterDelegatingBlobContainer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Random;

/**
 * rfc-serverless-opensearch.md &sect;17's "broader probabilistic multi-operation throttling/5xx-storm
 * chaos injection" gap: extends the existing one-shot fixtures (e.g. {@code
 * ObjectStoreCommitHeadPublisherTests.OneShotFailingBlobContainer}, {@code
 * WalReplayRecoveryTests.OneShotFailingOnWriteBlobContainer} -- each fails exactly one call, once,
 * then stops) into a <em>sustained, randomized, multi-operation-shape</em> fault injector: every
 * write-shaped call this container serves (not just the first) independently rolls the dice against
 * {@code failureProbability} and may throw, across {@code writeBlob}/{@code writeBlobAtomic}/{@code
 * deleteBlobsIgnoringIfNotExists}/{@code compareAndSwapRegister} alike -- a real caller retrying a
 * genuinely flaky object store sees exactly this shape: any call might fail, repeatedly, not just
 * the very first one.
 *
 * <p><b>Silent write corruption is also modeled</b>, independently of {@code failureProbability}:
 * with probability {@code corruptionProbability}, a {@code writeBlob}/{@code writeBlobAtomic} call
 * that survives the fail roll still <em>succeeds</em> from the caller's point of view, but the bytes
 * actually persisted have one byte flipped first -- simulating an object store that silently
 * persists bit-rotted/truncated-then-padded data while reporting success, the "the store lied about
 * durability" failure mode a clean thrown exception can never model. This is deliberately layered on
 * top of this plugin's own per-file manifest checksums (see {@code
 * BlobContainerBundleStore#readFile}), not duplicated logic from {@code BundleWriterReaderTests}'
 * own lower-level randomized single-byte-flip unit test: that test proves {@code BundleWriter}/{@code
 * BundleReader} detect a flipped byte in isolation; this class proves the same detection actually
 * fires end-to-end, through a real chaos ingest workload, when the underlying object store -- not
 * the bundle format code -- is the one that corrupted the bytes.
 *
 * <p><b>Deliberately scoped down, not attempting every possible chaos mode at once</b> (see this
 * class's own call sites for what actually exercises it): only write/delete/register-write-shaped
 * calls are ever faulted or corrupted -- reads and lists always succeed, so a caller can always at
 * least observe current state to decide how to retry. Throttling (a slow-but-eventually-successful
 * response) is deliberately NOT modeled here to avoid duplicating {@code LatencyInjectingBlobContainer}'s
 * own sleep-before-delegating logic; a caller wanting both sustained failures/corruption and added
 * latency at once composes the two decorators instead (see {@code
 * ChaosMultiOperationRegressionTests#testSustainedIngestConvergesDespiteSustainedRandomizedFaultsAndSimulatedLatency}).
 */
public final class ProbabilisticFailingBlobContainer extends RegisterDelegatingBlobContainer {

    private final Random random;
    private final double failureProbability;
    private final double corruptionProbability;

    /**
     * Wraps {@code delegate}, independently failing each write/delete/register-write call with
     * probability {@code failureProbability}. Equivalent to {@code
     * ProbabilisticFailingBlobContainer(delegate, random, failureProbability, 0.0)} -- never
     * corrupts, only ever cleanly fails.
     *
     * @param delegate the real container to inject faults in front of.
     * @param random the source of randomness deciding each call's fate -- pass a seeded {@code
     *               random()} from the calling test for reproducibility.
     * @param failureProbability the probability, in {@code [0, 1]}, that any single write-shaped
     *                           call fails; {@code 0} never fails, {@code 1} always fails.
     */
    public ProbabilisticFailingBlobContainer(BlobContainer delegate, Random random, double failureProbability) {
        this(delegate, random, failureProbability, 0.0);
    }

    /**
     * Wraps {@code delegate}, independently failing each write/delete/register-write call with
     * probability {@code failureProbability}, and -- for {@code writeBlob}/{@code writeBlobAtomic}
     * calls that survive that roll -- independently corrupting the persisted bytes with probability
     * {@code corruptionProbability}.
     *
     * @param delegate the real container to inject faults in front of.
     * @param random the source of randomness deciding each call's fate -- pass a seeded {@code
     *               random()} from the calling test for reproducibility.
     * @param failureProbability the probability, in {@code [0, 1]}, that any single write-shaped
     *                           call fails; {@code 0} never fails, {@code 1} always fails.
     * @param corruptionProbability the probability, in {@code [0, 1]}, that a write call which did
     *                              not fail instead silently persists one flipped byte;
     *                              {@code failureProbability + corruptionProbability} must not
     *                              exceed {@code 1.0}.
     */
    public ProbabilisticFailingBlobContainer(
        BlobContainer delegate,
        Random random,
        double failureProbability,
        double corruptionProbability
    ) {
        super(delegate);
        if (failureProbability < 0.0 || failureProbability > 1.0) {
            throw new IllegalArgumentException("failureProbability must be in [0, 1], got " + failureProbability);
        }
        if (corruptionProbability < 0.0 || corruptionProbability > 1.0) {
            throw new IllegalArgumentException("corruptionProbability must be in [0, 1], got " + corruptionProbability);
        }
        if (failureProbability + corruptionProbability > 1.0) {
            throw new IllegalArgumentException(
                "failureProbability + corruptionProbability must not exceed 1.0, got " + failureProbability + " + " + corruptionProbability
            );
        }
        this.random = random;
        this.failureProbability = failureProbability;
        this.corruptionProbability = corruptionProbability;
    }

    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        return new ProbabilisticFailingBlobContainer(child, random, failureProbability, corruptionProbability);
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        maybeFail("writeBlob", blobName);
        InputStream toWrite = maybeCorrupt(inputStream);
        super.writeBlob(blobName, toWrite, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        maybeFail("writeBlobAtomic", blobName);
        InputStream toWrite = maybeCorrupt(inputStream);
        super.writeBlobAtomic(blobName, toWrite, blobSize, failIfAlreadyExists);
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        maybeFail("deleteBlobsIgnoringIfNotExists", String.join(",", blobNames));
        super.deleteBlobsIgnoringIfNotExists(blobNames);
    }

    @Override
    public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
        throws IOException {
        maybeFail("compareAndSwapRegister", blobName);
        return super.compareAndSwapRegister(blobName, expectedGeneration, newValue);
    }

    private void maybeFail(String operation, String target) throws IOException {
        if (random.nextDouble() < failureProbability) {
            throw new IOException("injected chaos fault: " + operation + " [" + target + "]");
        }
    }

    /**
     * With probability {@link #corruptionProbability}, buffers {@code inputStream} fully and flips
     * one random byte before returning it -- the call that goes on to write these bytes will still
     * report success, exactly the "storage silently lied" shape {@link #maybeFail} can't produce.
     * Empty payloads have nothing to flip and pass through unmodified regardless. Buffers into
     * memory rather than corrupting in place on the wire since these test fixtures only ever see
     * small illustrative payloads, never real production-sized segment files.
     */
    private InputStream maybeCorrupt(InputStream inputStream) throws IOException {
        if (corruptionProbability <= 0.0 || random.nextDouble() >= corruptionProbability) {
            return inputStream;
        }
        byte[] bytes = inputStream.readAllBytes();
        if (bytes.length > 0) {
            int index = random.nextInt(bytes.length);
            bytes[index] ^= 0xFF;
        }
        return new ByteArrayInputStream(bytes);
    }
}
