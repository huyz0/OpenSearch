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
 * <p><b>Deliberately scoped down, not attempting every possible chaos mode at once</b> (see this
 * class's own call sites for what actually exercises it): only write/delete/register-write-shaped
 * calls are ever faulted -- reads and lists always succeed, so a caller can always at least observe
 * current state to decide how to retry. Throttling (a slow/rejected-then-retry-succeeds response) and
 * partial-write corruption (as opposed to a clean call failure) are both out of scope for this slice;
 * only "the whole call throws before doing anything" is simulated, matching every fault this plugin's
 * existing chaos tests already inject elsewhere.
 */
public final class ProbabilisticFailingBlobContainer extends RegisterDelegatingBlobContainer {

    private final Random random;
    private final double failureProbability;

    /**
     * Wraps {@code delegate}, independently failing each write/delete/register-write call with
     * probability {@code failureProbability}.
     *
     * @param delegate the real container to inject faults in front of.
     * @param random the source of randomness deciding each call's fate -- pass a seeded {@code
     *               random()} from the calling test for reproducibility.
     * @param failureProbability the probability, in {@code [0, 1]}, that any single write-shaped
     *                           call fails; {@code 0} never fails, {@code 1} always fails.
     */
    public ProbabilisticFailingBlobContainer(BlobContainer delegate, Random random, double failureProbability) {
        super(delegate);
        if (failureProbability < 0.0 || failureProbability > 1.0) {
            throw new IllegalArgumentException("failureProbability must be in [0, 1], got " + failureProbability);
        }
        this.random = random;
        this.failureProbability = failureProbability;
    }

    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        return new ProbabilisticFailingBlobContainer(child, random, failureProbability);
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        maybeFail("writeBlob", blobName);
        super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        maybeFail("writeBlobAtomic", blobName);
        super.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
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
}
