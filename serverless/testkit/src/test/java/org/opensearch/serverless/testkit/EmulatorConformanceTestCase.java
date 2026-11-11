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
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.ConditionalBlobContainer;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.metadata.ConditionalDeleteProbe;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The conformance suite against a provider emulator reached over HTTP, plus checks that the suite can still
 * fail on that transport.
 *
 * <p>A green run against an emulator only means something if a broken store would have turned it red through
 * the same client, retries and error mapping. Each target therefore also runs the suite's key assertions
 * against a deliberately broken wrapper of its own container and expects them to catch it.
 *
 * <p>Tests are skipped, not failed, when the emulator is not running; {@link #howToStart()} is quoted in the
 * skip message.
 */
public abstract class EmulatorConformanceTestCase extends BlobContainerConformanceTestCase {

    private BlobStore lastStore;

    /** @return the emulator's base URL */
    protected abstract String endpoint();

    /** @return a URL under {@link #endpoint()} that answers any HTTP status when the emulator is up */
    protected abstract String probeUrl();

    /** @return the command that starts the emulator, quoted when it is not reachable */
    protected abstract String howToStart();

    /**
     * Makes a fresh, empty store on the emulator. Called once per container.
     *
     * @return the store
     * @throws Exception if the emulator cannot be reached
     */
    protected abstract BlobStore newStore() throws Exception;

    /** @return a short provider name for log lines */
    protected abstract String provider();

    @Override
    protected final BlobContainer newContainer() throws Exception {
        assumeTrue("no " + provider() + " emulator at " + endpoint() + "; start one with: " + howToStart(), reachable(probeUrl()));
        lastStore = newStore();
        return lastStore.blobContainer(BlobPath.cleanPath().add("conformance"));
    }

    @Override
    protected BlobStore store() {
        return lastStore;
    }

    private Boolean honoursConditionalDelete;

    /**
     * Asks the store, with the same probe a node runs at startup. A store that ignores the condition on a delete
     * (fake-gcs-server does) is one the shell keeps tombstones for, so its deletes are not held to the stricter
     * history.
     */
    @Override
    protected boolean honoursConditionalDelete() throws Exception {
        if (honoursConditionalDelete == null) {
            assumeTrue("no " + provider() + " emulator at " + endpoint(), reachable(probeUrl()));
            honoursConditionalDelete = ConditionalDeleteProbe.honoured(newStore(), BlobPath.cleanPath().add("probe"));
            logger.info("{} {} a conditional delete", provider(), honoursConditionalDelete ? "honours" : "does NOT honour");
        }
        return honoursConditionalDelete;
    }

    private static BytesArray bytes(String value) {
        return new BytesArray(value.getBytes(StandardCharsets.UTF_8));
    }

    public void testTheSuiteWouldCatchACompareAndSwapThatAlwaysWinsOnThisTransport() throws Exception {
        final BlobContainer honest = newContainer();
        final BlobContainer liar = new DelegatingBlobContainer(honest) {
            @Override
            public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue) {
                return BlobRegisterCasResult.applied(expectedGeneration + 1);
            }
        };
        ConditionalBlobContainer.of(liar).createRegisterIfAbsent("head", bytes("initial"));
        final long generation = ConditionalBlobContainer.of(liar).readRegister("head").orElseThrow().generation();

        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger winners = new AtomicInteger();
        final Thread[] threads = new Thread[contenders()];
        for (int i = 0; i < threads.length; i++) {
            final int id = i;
            threads[i] = new Thread(() -> {
                try {
                    start.await();
                    if (ConditionalBlobContainer.of(liar).compareAndSwapRegister("head", generation, bytes("w" + id)).applied()) {
                        winners.incrementAndGet();
                    }
                } catch (Exception e) {
                    // An exception is not a win; counting rather than asserting is the point.
                }
            });
            threads[i].start();
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join(60_000);
        }
        logger.info("{} self-check: a lying compare-and-swap produced {} winners", provider(), winners.get());
        assertTrue("the one-winner assertion must be able to fail on " + provider() + "; saw " + winners.get(), winners.get() > 1);
    }

    public void testTheLinearizabilityCheckWouldCatchAStaleReadOnThisTransport() throws Exception {
        final BlobContainer honest = newContainer();
        final MisbehavingBlobContainer broken = new MisbehavingBlobContainer(honest);
        broken.createRegisterIfAbsent("stale", bytes("v0"));
        final long initial = broken.readRegister("stale").orElseThrow().generation();

        final RegisterHistory history = new RegisterHistory();
        long invoked = history.invoke();
        final BlobRegisterCasResult applied = broken.compareAndSwapRegister("stale", initial, bytes("v1"));
        history.completed(new RegisterHistory.Cas(initial, "v1", applied.applied(), applied.currentGeneration()), invoked);
        assertTrue("the write must really have happened", applied.applied());

        broken.staleReadEvery(1);
        invoked = history.invoke();
        final var seen = broken.readRegister("stale").orElseThrow();
        history.completed(new RegisterHistory.Read(seen.generation(), seen.value().utf8ToString()), invoked);
        assertEquals("this check is meaningless unless the read really was stale", "v0", seen.value().utf8ToString());

        assertFalse(
            "a stale read on " + provider() + " must be caught",
            LinearizabilityChecker.check(history.entries(), initial, "v0").linearizable()
        );
    }

    /**
     * Whether anything answers at {@code url}. Any HTTP status counts: an unauthenticated probe is often refused,
     * and a refusal still proves the emulator is listening.
     *
     * @param url the URL to probe
     * @return true if an HTTP response came back
     */
    static boolean reachable(String url) {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()) {
            final HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(3)).GET().build();
            client.send(request, HttpResponse.BodyHandlers.discarding());
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
