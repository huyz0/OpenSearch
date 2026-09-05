/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Does the conformance suite actually catch a backend that does not conform?
 *
 * <p>It will be pointed at implementations nobody here controls, so the one thing that must not be
 * true of it is that it passes regardless. These plant the two failures that matter — a
 * compare-and-swap that always claims success, and a ranged read that ignores the range — and confirm
 * the properties the suite asserts on would come out differently.
 */
public class ConformanceSuiteSelfTests extends OpenSearchTestCase {

    private BlobContainer real() throws Exception {
        return new FsBlobStore(1024, createTempDir(), false).blobContainer(BlobPath.cleanPath());
    }

    /** A backend whose compare-and-swap always claims to have applied. */
    private static final class AlwaysWinsContainer extends DelegatingBlobContainer {
        AlwaysWinsContainer(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue) {
            return BlobRegisterCasResult.applied(expectedGeneration + 1);
        }
    }

    /** A backend that ignores the requested range and returns the whole blob. */
    private static final class IgnoresRangeContainer extends DelegatingBlobContainer {
        IgnoresRangeContainer(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            return super.readBlob(blobName);
        }
    }

    public void testTheSuiteWouldCatchACompareAndSwapThatAlwaysWins() throws Exception {
        final BlobContainer container = new AlwaysWinsContainer(real());
        container.createRegisterIfAbsent("head", new BytesArray("initial".getBytes(StandardCharsets.UTF_8)));
        final long generation = container.readRegister("head").orElseThrow().generation();

        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger winners = new AtomicInteger();
        final Thread[] threads = new Thread[8];
        for (int i = 0; i < threads.length; i++) {
            final int id = i;
            threads[i] = new Thread(() -> {
                try {
                    start.await();
                    if (container.compareAndSwapRegister(
                        "head",
                        generation,
                        new BytesArray(("owner-" + id).getBytes(StandardCharsets.UTF_8))
                    ).applied()) {
                        winners.incrementAndGet();
                    }
                } catch (Exception e) {
                    // counted as a non-win
                }
            });
            threads[i].start();
        }
        start.countDown();
        for (Thread t : threads) {
            t.join(60_000);
        }

        // The suite asserts exactly one. A backend like this would give eight, and two nodes would
        // both believe they owned the same shard.
        assertTrue("the suite's winner count must differ on a broken backend, but was " + winners.get(), winners.get() > 1);
    }

    public void testTheSuiteWouldCatchARangedReadThatIgnoresItsRange() throws Exception {
        final BlobContainer container = new IgnoresRangeContainer(real());
        final byte[] content = new byte[4096];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) (i % 251);
        }
        container.writeBlob("data", new java.io.ByteArrayInputStream(content), content.length, false);

        boolean detected = false;
        final int offset = 100;
        final int length = 16;
        final byte[] actual = new byte[length];
        try (InputStream in = container.readBlob("data", offset, length)) {
            int read = 0;
            while (read < length) {
                final int n = in.read(actual, read, length - read);
                if (n <= 0) {
                    break;
                }
                read += n;
            }
            // The suite checks both that the bytes are right and that the stream ends where it should.
            if (in.read() != -1) {
                detected = true;
            }
        }
        for (int i = 0; i < length && detected == false; i++) {
            if (actual[i] != content[offset + i]) {
                detected = true;
            }
        }
        assertTrue("a backend ignoring the range must not pass the suite's range checks", detected);
    }

    // ---- the linearizability checker, against stores that really do misbehave --------------------

    /**
     * Records a scripted workload through whatever container it is given.
     *
     * <p>Scripted rather than concurrent, deliberately. A concurrent workload against a deliberately broken
     * store <em>usually</em> produces a violation, and a test that usually fails when it should is a flaky
     * test rather than a strict one — a stale read that happens to overlap the write it is stale about is
     * perfectly legal. A script makes each deviation land where it cannot be excused.
     */
    private RegisterHistory record(BlobContainer container, String name) throws Exception {
        final RegisterHistory history = new RegisterHistory();
        container.createRegisterIfAbsent(name, new BytesArray("v0"));
        long known = container.readRegister(name).orElseThrow().generation();

        for (int i = 1; i <= 3; i++) {
            final String value = "v" + i;
            long invoked = history.invoke();
            final var result = container.compareAndSwapRegister(name, known, new BytesArray(value));
            history.completed(new RegisterHistory.Cas(known, value, result.applied(), result.currentGeneration()), invoked);
            known = result.currentGeneration();

            invoked = history.invoke();
            final var seen = container.readRegister(name);
            history.completed(
                new RegisterHistory.Read(
                    seen.map(org.opensearch.common.blobstore.BlobRegister::generation).orElse(RegisterHistory.ABSENT),
                    seen.map(r -> r.value().utf8ToString()).orElse(null)
                ),
                invoked
            );
        }
        return history;
    }

    private long initialGenerationOf(BlobContainer container, String name) throws Exception {
        return container.readRegister(name).orElseThrow().generation();
    }

    /** The same script over an honest store must be accepted, or the rejections below prove nothing. */
    public void testTheCheckerAcceptsAnHonestStore() throws Exception {
        final BlobContainer container = real();
        container.createRegisterIfAbsent("honest", new BytesArray("v0"));
        final long initial = initialGenerationOf(container, "honest");
        final RegisterHistory history = record(container, "honest");
        LinearizabilityChecker.check(history.entries(), initial, "v0").assertLinearizable("an honest filesystem store");
    }

    /**
     * A store serving a value that has already been overwritten is caught.
     *
     * <p>The deviation a provider is most likely to actually have, and the one no property in the suite
     * above asks about: every individual call is atomic and well-formed, and only the order they imply is
     * impossible.
     */
    public void testTheCheckerCatchesAStaleRead() throws Exception {
        final BlobContainer honest = real();
        final var broken = new MisbehavingBlobContainer(honest);
        broken.createRegisterIfAbsent("stale", new BytesArray("v0"));
        final long initial = initialGenerationOf(broken, "stale");

        // Scripted rather than left to a counter: the point is a read that is provably stale, and
        // "every second read" leaves which read that was to arithmetic somebody would have to redo after
        // any change to the script.
        final RegisterHistory history = new RegisterHistory();
        long invoked = history.invoke();
        final var applied = broken.compareAndSwapRegister("stale", initial, new BytesArray("v1"));
        history.completed(new RegisterHistory.Cas(initial, "v1", applied.applied(), applied.currentGeneration()), invoked);
        assertTrue("the write must really have happened", applied.applied());

        broken.staleReadEvery(1);
        invoked = history.invoke();
        final var seen = broken.readRegister("stale").orElseThrow();
        history.completed(new RegisterHistory.Read(seen.generation(), seen.value().utf8ToString()), invoked);

        assertEquals("this test is meaningless unless the read really was stale", "v0", seen.value().utf8ToString());
        assertFalse(
            "a store serving overwritten values must be caught, or R11 would pass against one",
            LinearizabilityChecker.check(history.entries(), initial, "v0").linearizable()
        );
    }

    /**
     * A store admitting two winners on one generation is caught.
     *
     * <p>The failure the entire design rests on not happening: two nodes owning one shard, both publishing
     * at the same term, with the fence never firing because each believes it holds it.
     */
    public void testTheCheckerCatchesTwoWinners() throws Exception {
        final BlobContainer honest = real();
        final var broken = new MisbehavingBlobContainer(honest);
        broken.createRegisterIfAbsent("winners", new BytesArray("v0"));
        final long initial = initialGenerationOf(broken, "winners");

        final RegisterHistory history = new RegisterHistory();
        long invoked = history.invoke();
        var result = broken.compareAndSwapRegister("winners", initial, new BytesArray("a"));
        history.completed(new RegisterHistory.Cas(initial, "a", result.applied(), result.currentGeneration()), invoked);

        // The second swap uses the generation the first one replaced -- a stale precondition -- and the
        // store lets it through anyway, as a replica that had not seen the first write would.
        broken.twoWinnersEvery(1);
        invoked = history.invoke();
        result = broken.compareAndSwapRegister("winners", initial, new BytesArray("b"));
        history.completed(new RegisterHistory.Cas(initial, "b", result.applied(), result.currentGeneration()), invoked);

        assertTrue("this test is meaningless unless the store really did admit the second swap", result.applied());
        assertFalse(
            "two winners on one generation must be caught",
            LinearizabilityChecker.check(history.entries(), initial, "v0").linearizable()
        );
    }

    /**
     * A store whose refusal reports a generation nobody stored is caught.
     *
     * <p>The contract is that a failed swap reports the generation now actually stored, and a caller
     * re-reads on that number. A fictitious one sends every retry against something that never existed, so
     * a shard that could be taken never is — a liveness failure that looks like contention.
     */
    public void testTheCheckerCatchesARefusalThatLiesAboutTheGeneration() throws Exception {
        final BlobContainer honest = real();
        final var broken = new MisbehavingBlobContainer(honest).refusalsReportGeneration(9_999L);
        broken.createRegisterIfAbsent("lying", new BytesArray("v0"));
        final long initial = initialGenerationOf(broken, "lying");

        final RegisterHistory history = new RegisterHistory();
        long invoked = history.invoke();
        var result = broken.compareAndSwapRegister("lying", initial, new BytesArray("a"));
        history.completed(new RegisterHistory.Cas(initial, "a", result.applied(), result.currentGeneration()), invoked);

        invoked = history.invoke();
        result = broken.compareAndSwapRegister("lying", initial, new BytesArray("b"));
        history.completed(new RegisterHistory.Cas(initial, "b", result.applied(), result.currentGeneration()), invoked);

        assertFalse("the second swap must have been refused", result.applied());
        assertEquals("and must have lied about where the register is", 9_999L, result.currentGeneration());
        assertFalse(
            "a refusal reporting a generation nobody stored must be caught",
            LinearizabilityChecker.check(history.entries(), initial, "v0").linearizable()
        );
    }
}
