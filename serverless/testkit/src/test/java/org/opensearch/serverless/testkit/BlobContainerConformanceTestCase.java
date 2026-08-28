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
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The properties the serverless design assumes of its object store, written down as tests.
 *
 * <p>This is R11. Every safety argument in {@code rfc-serverless-shell.md} rests on behaviour nobody has
 * verified outside a local filesystem: that a compare-and-swap admits exactly one winner, that a range
 * read returns exactly the bytes asked for, that a listing is complete. `FsBlobContainer` provides all
 * of that with real filesystem atomicity, which is why every phase so far has passed and why none of
 * those passes is evidence about S3.
 *
 * <p><b>Subclass this and bind a container to check a backend.</b> Running it against a filesystem
 * proves the suite works, and nothing else. The value is that R11's remaining cost is now "point it at
 * an endpoint" rather than "write the tests".
 *
 * <p>What each property is load-bearing for:
 *
 * <table>
 *   <caption>Assumptions and what depends on them</caption>
 *   <tr><th>Property</th><th>Depended on by</th></tr>
 *   <tr><td>put-if-absent admits one creator</td><td>index name uniqueness, with no cluster-manager</td></tr>
 *   <tr><td>CAS admits one winner</td><td>shard ownership, fencing, every failover</td></tr>
 *   <tr><td>a stale generation loses</td><td>zombie fencing, ordered index deletes</td></tr>
 *   <tr><td>generations increase</td><td>the term fed to {@code updateShardState}</td></tr>
 *   <tr><td>read-after-write</td><td>a node believing its own successful write</td></tr>
 *   <tr><td>ranged reads are exact</td><td>lazy block reads — the newest and least tested</td></tr>
 *   <tr><td>listings are complete and sized</td><td>paged enumeration, GC, block-cache file lengths</td></tr>
 * </table>
 */
public abstract class BlobContainerConformanceTestCase extends OpenSearchTestCase {

    /**
     * Binds the backend under test. Called once per test with a container that starts empty.
     *
     * @return a container to exercise
     * @throws Exception if the backend cannot be reached
     */
    protected abstract BlobContainer newContainer() throws Exception;

    /** How many threads contend in the concurrency tests. */
    protected int contenders() {
        return 8;
    }

    private static BytesArray bytes(String value) {
        return new BytesArray(value.getBytes(StandardCharsets.UTF_8));
    }

    public void testPutIfAbsentAdmitsExactlyOneCreator() throws Exception {
        final BlobContainer container = newContainer();
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger created = new AtomicInteger();
        final AtomicInteger errors = new AtomicInteger();

        final Thread[] threads = new Thread[contenders()];
        for (int i = 0; i < threads.length; i++) {
            final int id = i;
            threads[i] = new Thread(() -> {
                try {
                    start.await();
                    if (container.createRegisterIfAbsent("unique", bytes("creator-" + id)).applied()) {
                        created.incrementAndGet();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
            });
            threads[i].start();
        }
        start.countDown();
        for (Thread t : threads) {
            t.join(60_000);
        }

        assertEquals("no contender should have errored", 0, errors.get());
        assertEquals("index name uniqueness depends on exactly one creator", 1, created.get());
        assertTrue(container.readRegister("unique").isPresent());
    }

    public void testCompareAndSwapAdmitsExactlyOneWinner() throws Exception {
        final BlobContainer container = newContainer();
        container.createRegisterIfAbsent("head", bytes("initial"));
        final long generation = container.readRegister("head").orElseThrow().generation();

        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger winners = new AtomicInteger();
        final AtomicInteger errors = new AtomicInteger();
        final Thread[] threads = new Thread[contenders()];
        for (int i = 0; i < threads.length; i++) {
            final int id = i;
            threads[i] = new Thread(() -> {
                try {
                    start.await();
                    // Every contender swaps against the same generation, as two nodes racing to take a
                    // shard do.
                    if (container.compareAndSwapRegister("head", generation, bytes("owner-" + id)).applied()) {
                        winners.incrementAndGet();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
            });
            threads[i].start();
        }
        start.countDown();
        for (Thread t : threads) {
            t.join(60_000);
        }

        assertEquals("no contender should have errored", 0, errors.get());
        assertEquals("shard ownership depends on exactly one winner", 1, winners.get());
    }

    public void testAStaleGenerationLoses() throws Exception {
        final BlobContainer container = newContainer();
        container.createRegisterIfAbsent("head", bytes("v1"));
        final long stale = container.readRegister("head").orElseThrow().generation();

        assertTrue(container.compareAndSwapRegister("head", stale, bytes("v2")).applied());
        assertFalse(
            "a writer holding a generation that has moved on must lose; this is what fences a zombie",
            container.compareAndSwapRegister("head", stale, bytes("v3")).applied()
        );
        assertEquals("v2", container.readRegister("head").orElseThrow().value().utf8ToString());
    }

    public void testGenerationsIncreaseAndAreVisibleImmediately() throws Exception {
        final BlobContainer container = newContainer();
        container.createRegisterIfAbsent("head", bytes("v0"));

        long previous = container.readRegister("head").orElseThrow().generation();
        for (int i = 1; i <= 5; i++) {
            final var result = container.compareAndSwapRegister("head", previous, bytes("v" + i));
            assertTrue("swap " + i + " should have applied", result.applied());
            final BlobRegister read = container.readRegister("head").orElseThrow();
            // Read-after-write: a node has to be able to believe its own successful write, and the term
            // it feeds to updateShardState is this generation.
            assertEquals("a register must read back what was just written", "v" + i, read.value().utf8ToString());
            assertTrue("generations must increase: " + read.generation() + " after " + previous, read.generation() > previous);
            previous = read.generation();
        }
    }

    public void testRangedReadsReturnExactlyWhatWasAskedFor() throws Exception {
        final BlobContainer container = newContainer();
        final int size = 8192;
        final byte[] content = new byte[size];
        for (int i = 0; i < size; i++) {
            content[i] = (byte) (i % 251);
        }
        container.writeBlob("data", new ByteArrayInputStream(content), size, false);

        // The property lazy block reads depend on, and the one nothing has checked off a filesystem.
        final int[][] ranges = { { 0, 16 }, { 1, 1 }, { 4095, 2 }, { 4096, 1024 }, { size - 1, 1 }, { size - 100, 100 } };
        for (int[] range : ranges) {
            final int offset = range[0];
            final int length = range[1];
            final byte[] actual = new byte[length];
            try (InputStream in = container.readBlob("data", offset, length)) {
                int read = 0;
                while (read < length) {
                    final int n = in.read(actual, read, length - read);
                    assertTrue("range [" + offset + "," + length + ") ended early after " + read + " bytes", n > 0);
                    read += n;
                }
                assertEquals("range [" + offset + "," + length + ") returned more than asked for", -1, in.read());
            }
            for (int i = 0; i < length; i++) {
                assertEquals("range [" + offset + "," + length + ") differed at byte " + i, content[offset + i], actual[i]);
            }
        }
    }

    public void testListingIsCompleteAndReportsLengths() throws Exception {
        final BlobContainer container = newContainer();
        final Set<String> written = new HashSet<>();
        for (int i = 0; i < 25; i++) {
            final byte[] body = new byte[10 + i];
            container.writeBlob("blob-" + i, new ByteArrayInputStream(body), body.length, false);
            written.add("blob-" + i);
        }
        container.writeBlob("other", new ByteArrayInputStream(new byte[3]), 3, false);

        final var listed = container.listBlobsByPrefix("blob-");
        assertEquals("a prefix listing must return exactly the matching blobs", written, listed.keySet());
        for (int i = 0; i < 25; i++) {
            // The block cache asks a listing for file lengths before it will read a byte.
            assertEquals("blob-" + i + " reported the wrong length", 10 + i, listed.get("blob-" + i).length());
        }
        assertTrue("an unrelated blob must not appear under the prefix", listed.containsKey("other") == false);
    }

    public void testADeletedRegisterReadsAsAbsent() throws Exception {
        final BlobContainer container = newContainer();
        container.createRegisterIfAbsent("head", bytes("v1"));
        assertTrue(container.readRegister("head").isPresent());

        container.deleteBlobsIgnoringIfNotExists(java.util.List.of("head"));
        assertTrue("a deleted register must read as absent, not as stale", container.readRegister("head").isEmpty());

        // And the name is free again, which index deletion depends on.
        assertTrue(container.createRegisterIfAbsent("head", bytes("v2")).applied());
    }
}
