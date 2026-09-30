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

    /**
     * Whether this store refuses a conditional delete at a stale generation, as the node's probe would find. A store
     * that does not is kept on tombstones, and its deletes are not held to linearizability.
     *
     * @return true unless a target says otherwise
     * @throws Exception if the store cannot be reached
     */
    protected boolean honoursConditionalDelete() throws Exception {
        return true;
    }

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

    public void testAPagedListingWalksEveryBlobOnceInOrder() throws Exception {
        final BlobContainer container = newContainer();
        final java.util.TreeSet<String> written = new java.util.TreeSet<>();
        for (int i = 0; i < 37; i++) {
            final String name = "page-" + String.format(java.util.Locale.ROOT, "%03d", (i * 17) % 37);
            final byte[] body = new byte[1 + i % 5];
            container.writeBlob(name, new ByteArrayInputStream(body), body.length, false);
            written.add(name);
        }
        container.writeBlob("pagf-after", new ByteArrayInputStream(new byte[1]), 1, false);
        container.writeBlob("pagd-before", new ByteArrayInputStream(new byte[1]), 1, false);

        final int pageSize = randomIntBetween(1, 10);
        final java.util.List<String> walked = new java.util.ArrayList<>();
        String cursor = null;
        int pages = 0;
        while (true) {
            final var page = container.listBlobsByPrefix("page-", cursor, pageSize);
            assertTrue("a page must not exceed its limit", page.size() <= pageSize);
            for (var blob : page) {
                assertEquals("the length of " + blob.name(), 1 + indexOf(blob.name()) % 5, blob.length());
                walked.add(blob.name());
            }
            if (page.size() < pageSize) {
                break;
            }
            cursor = page.get(page.size() - 1).name();
            assertTrue("a walk this size cannot take this many pages", ++pages < 100);
        }
        assertEquals("a paged walk returns every blob once, in lexicographic order", new java.util.ArrayList<>(written), walked);
        assertEquals("a cursor at the last name is an empty page", 0, container.listBlobsByPrefix("page-", written.last(), 5).size());
        assertEquals(
            "the cursor itself is never returned, and a cursor need not name a blob",
            java.util.List.of("page-001", "page-002"),
            container.listBlobsByPrefix("page-", "page-000", 2).stream().map(b -> b.name()).toList()
        );
        assertEquals(
            java.util.List.of("page-011"),
            container.listBlobsByPrefix("page-", "page-0105", 1).stream().map(b -> b.name()).toList()
        );
        assertEquals("a zero limit is an empty page", 0, container.listBlobsByPrefix("page-", null, 0).size());
    }

    private int indexOf(String name) {
        final int n = Integer.parseInt(name.substring("page-".length()));
        // name = (i * 17) % 37; 17 * 24 = 408 = 11 * 37 + 1, so i = 24 * n mod 37
        return (24 * n) % 37;
    }

    /**
     * A create or delete between two pages: ahead of the cursor it is seen, behind it it is not, and every blob
     * that existed for the whole walk is returned exactly once. The pages are not a snapshot.
     */
    public void testAPagedWalkSeesChangesAheadOfTheCursorOnly() throws Exception {
        final BlobContainer container = newContainer();
        for (String name : java.util.List.of("walk-b", "walk-d", "walk-f", "walk-h")) {
            container.writeBlob(name, new ByteArrayInputStream(new byte[1]), 1, false);
        }
        final var first = container.listBlobsByPrefix("walk-", null, 2);
        assertEquals(java.util.List.of("walk-b", "walk-d"), first.stream().map(b -> b.name()).toList());

        container.writeBlob("walk-a", new ByteArrayInputStream(new byte[1]), 1, false); // behind the cursor
        container.writeBlob("walk-e", new ByteArrayInputStream(new byte[1]), 1, false); // ahead of it
        container.deleteBlobsIgnoringIfNotExists(java.util.List.of("walk-b", "walk-h")); // one behind, one ahead

        final var rest = container.listBlobsByPrefix("walk-", "walk-d", 10);
        assertEquals(java.util.List.of("walk-e", "walk-f"), rest.stream().map(b -> b.name()).toList());
    }

    /**
     * Child containers by page, in key order -- the name followed by {@code /}, which is how every store lists
     * a prefix, so {@code c-1} and {@code c.x} come before {@code c}. The cursor's own subtree must be skipped
     * whole -- a store resuming at the cursor's name would hand the same child back, since {@code c/} sorts
     * after {@code c} -- and the children after it in key order must all come back. RustFS showed the first
     * version of this, which ordered by name, losing {@code c-1} and {@code c.x} after a cursor at {@code c}.
     */
    public void testChildrenPageInOrderAndSkipTheCursorsSubtree() throws Exception {
        final BlobContainer root = newContainer();
        final java.util.List<String> names = java.util.List.of("c", "c-1", "c.x", "c0", "d", "e#uuid#0", "e#uuid#1", "f");
        for (String name : names) {
            final BlobContainer child = childOf(root, name);
            child.writeBlob("x", new ByteArrayInputStream(new byte[1]), 1, false);
            child.writeBlob("y", new ByteArrayInputStream(new byte[1]), 1, false);
        }
        root.writeBlob("c-blob", new ByteArrayInputStream(new byte[1]), 1, false); // a blob, never a child

        final int pageSize = randomIntBetween(1, 4);
        final java.util.List<String> walked = new java.util.ArrayList<>();
        String cursor = null;
        while (true) {
            final java.util.List<String> page = new java.util.ArrayList<>(root.children(cursor, pageSize).keySet());
            page.removeIf(n -> n.startsWith("extra"));
            walked.addAll(page);
            if (page.size() < pageSize) {
                break;
            }
            cursor = page.get(page.size() - 1);
        }
        final java.util.List<String> keyOrder = new java.util.ArrayList<>(names);
        keyOrder.sort(BlobContainer.CHILD_KEY_ORDER);
        assertEquals(java.util.List.of("c-1", "c.x", "c", "c0", "d", "e#uuid#0", "e#uuid#1", "f"), keyOrder);
        assertEquals("children walked by page, each once, in key order", keyOrder, walked);
        final var afterC = root.children("c", 2);
        assertEquals(java.util.List.of("c0", "d"), new java.util.ArrayList<>(afterC.keySet()));
        final var afterDash = root.children("c-1", 2);
        assertEquals(java.util.List.of("c.x", "c"), new java.util.ArrayList<>(afterDash.keySet()));
        assertTrue("a paged child is a usable container", afterDash.get("c.x").blobExists("x"));
    }

    /** A child container of {@code root}; a subclass whose containers do not know their store overrides it. */
    protected BlobContainer childOf(BlobContainer root, String name) throws Exception {
        return store().blobContainer(root.path().add(name));
    }

    /** The store {@link #newContainer()} last bound. */
    protected org.opensearch.common.blobstore.BlobStore store() {
        assumeTrue("this target does not expose its store", false);
        return null;
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

    /**
     * Concurrent put-if-absent writes of one blob admit exactly one, and the others are refused rather than
     * overwriting it. The write-ahead log's fence is exactly this: a successor and a stale writer racing for
     * one slot, of which the store must let only one land.
     *
     * @throws Exception if the backend cannot be reached
     */
    public void testPutIfAbsentBlobWritesAdmitExactlyOne() throws Exception {
        final BlobContainer container = newContainer();
        for (int round = 0; round < 5; round++) {
            final String name = "slot-" + round;
            final int contenders = Math.min(8, contenders());
            final CountDownLatch start = new CountDownLatch(1);
            final java.util.concurrent.atomic.AtomicInteger landed = new java.util.concurrent.atomic.AtomicInteger();
            final java.util.concurrent.atomic.AtomicInteger refused = new java.util.concurrent.atomic.AtomicInteger();
            final java.util.List<Exception> errors = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
            final Thread[] threads = new Thread[contenders];
            for (int c = 0; c < contenders; c++) {
                final byte[] body = ("contender-" + c).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                threads[c] = new Thread(() -> {
                    try {
                        start.await();
                        container.writeBlob(name, new ByteArrayInputStream(body), body.length, true);
                        landed.incrementAndGet();
                    } catch (java.nio.file.FileAlreadyExistsException e) {
                        refused.incrementAndGet();
                    } catch (Exception e) {
                        errors.add(e);
                    }
                });
                threads[c].start();
            }
            start.countDown();
            for (Thread t : threads) {
                t.join(120_000);
            }
            assertEquals("no contender should have errored: " + errors, java.util.List.of(), errors);
            assertEquals("exactly one put-if-absent write of a blob may land", 1, landed.get());
            assertEquals("and every other must be refused", contenders - 1, refused.get());
        }
    }

    /**
     * A register created again does not reuse its predecessor's generation, so a swap carrying a generation
     * read before the delete is refused by what was created after it. This is what lets a deleted register
     * be removed outright rather than tombstoned.
     */
    public void testARecreatedRegisterDoesNotReuseAGeneration() throws Exception {
        final BlobContainer container = newContainer();
        container.createRegisterIfAbsent("head", bytes("first"));
        final long before = container.readRegister("head").orElseThrow().generation();
        container.deleteBlobsIgnoringIfNotExists(java.util.List.of("head"));
        // The same bytes again: a store keying its token on content must still not repeat one.
        assertTrue(container.createRegisterIfAbsent("head", bytes("first")).applied());
        final long after = container.readRegister("head").orElseThrow().generation();
        assertNotEquals("a recreated register must not reuse a generation", before, after);
        assertFalse(
            "a swap carrying the old incarnation's generation must lose",
            container.compareAndSwapRegister("head", before, bytes("stale")).applied()
        );
        assertEquals("first", container.readRegister("head").orElseThrow().value().utf8ToString());
    }

    /** A conditional delete removes a register only at the generation it holds. */
    public void testAConditionalDeleteRefusesAStaleGeneration() throws Exception {
        final BlobContainer container = newContainer();
        container.createRegisterIfAbsent("head", bytes("v1"));
        final long stale = container.readRegister("head").orElseThrow().generation();
        final long current = container.compareAndSwapRegister("head", stale, bytes("v2")).currentGeneration();

        assertFalse(
            "a delete at a generation the register no longer holds must be refused",
            container.deleteRegisterIfUnchanged("head", stale)
        );
        assertEquals("v2", container.readRegister("head").orElseThrow().value().utf8ToString());
        assertTrue(container.deleteRegisterIfUnchanged("head", current));
        assertTrue("and one at the generation it holds removes it", container.readRegister("head").isEmpty());
        assertFalse("an absent register is not deleted twice", container.deleteRegisterIfUnchanged("head", current));
    }

    /**
     * The same question with deletes in it: concurrent reads, swaps, conditional deletes and creates on one
     * register, which is what deleting an index without a tombstone now depends on. A delete conditioned on a
     * generation the register no longer holds must be refused, and a register created again must not reuse a
     * generation, or some order of these calls will fail to explain what they returned.
     *
     * @throws Exception if the backend cannot be reached
     */
    public void testAConcurrentHistoryWithDeletesIsLinearizable() throws Exception {
        final BlobContainer container = newContainer();
        assumeTrue(
            "this store does not honour a conditional delete, so nothing relies on its deletes being linearizable: a node on it keeps tombstones",
            honoursConditionalDelete()
        );
        final int workers = Math.min(4, contenders());
        final int callsEach = 12;

        for (int round = 0; round < 4; round++) {
            final String name = "deleting-" + round;
            container.createRegisterIfAbsent(name, bytes("v0"));
            final long initial = container.readRegister(name).orElseThrow().generation();

            final RegisterHistory history = new RegisterHistory();
            final CountDownLatch start = new CountDownLatch(1);
            final java.util.List<Exception> failures = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
            final Thread[] threads = new Thread[workers];
            for (int w = 0; w < workers; w++) {
                final int worker = w;
                threads[w] = new Thread(() -> {
                    long known = initial;
                    try {
                        start.await();
                        for (int call = 0; call < callsEach; call++) {
                            final String value = "w" + worker + "-" + call;
                            final long invoked = history.invoke();
                            switch ((call + worker) % 4) {
                                case 0 -> {
                                    final var seen = container.readRegister(name);
                                    history.completed(
                                        new RegisterHistory.Read(
                                            seen.map(BlobRegister::generation).orElse(RegisterHistory.ABSENT),
                                            seen.map(r -> r.value().utf8ToString()).orElse(null)
                                        ),
                                        invoked
                                    );
                                    known = seen.map(BlobRegister::generation).orElse(BlobRegister.ABSENT_GENERATION);
                                }
                                case 1 -> {
                                    final var result = container.compareAndSwapRegister(name, known, bytes(value));
                                    history.completed(
                                        new RegisterHistory.Cas(known, value, result.applied(), result.currentGeneration()),
                                        invoked
                                    );
                                    known = result.currentGeneration();
                                }
                                case 2 -> {
                                    final boolean deleted = container.deleteRegisterIfUnchanged(name, known);
                                    history.completed(new RegisterHistory.Delete(known, deleted), invoked);
                                    if (deleted) {
                                        known = BlobRegister.ABSENT_GENERATION;
                                    }
                                }
                                default -> {
                                    final var result = container.createRegisterIfAbsent(name, bytes(value));
                                    history.completed(
                                        new RegisterHistory.Create(value, result.applied(), result.currentGeneration()),
                                        invoked
                                    );
                                    if (result.applied()) {
                                        known = result.currentGeneration();
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        failures.add(e);
                    }
                });
                threads[w].start();
            }
            start.countDown();
            for (Thread t : threads) {
                t.join(120_000);
            }

            assertEquals("no call should have errored: " + failures, java.util.List.of(), failures);
            LinearizabilityChecker.check(history.entries(), initial, "v0")
                .assertLinearizable("round " + round + " of the workload with conditional deletes");
        }
    }

    /**
     * The check above must be able to fail: a store that deletes despite a stale generation -- accepting
     * {@code If-Match} and ignoring it, or checking it on arrival and deleting whatever is current -- is
     * caught by it on this transport.
     *
     * @throws Exception if the backend cannot be reached
     */
    public void testTheCheckerWouldCatchADeleteThatIgnoresItsGeneration() throws Exception {
        final BlobContainer honest = newContainer();
        final BlobContainer careless = new DelegatingBlobContainer(honest) {
            @Override
            public boolean deleteRegisterIfUnchanged(String blobName, long expectedGeneration) throws java.io.IOException {
                final boolean existed = readRegister(blobName).isPresent();
                deleteBlobsIgnoringIfNotExists(java.util.List.of(blobName));
                return existed;
            }
        };
        careless.createRegisterIfAbsent("careless", bytes("v0"));
        final long initial = careless.readRegister("careless").orElseThrow().generation();
        final RegisterHistory history = new RegisterHistory();

        long invoked = history.invoke();
        final var moved = careless.compareAndSwapRegister("careless", initial, bytes("v1"));
        history.completed(new RegisterHistory.Cas(initial, "v1", moved.applied(), moved.currentGeneration()), invoked);
        invoked = history.invoke();
        // Conditioned on the generation before the swap: a correct store refuses this.
        history.completed(new RegisterHistory.Delete(initial, careless.deleteRegisterIfUnchanged("careless", initial)), invoked);

        assertFalse(
            "the checker must reject a delete that removed a register at a generation it no longer held",
            LinearizabilityChecker.check(history.entries(), initial, "v0").linearizable()
        );
    }

    /**
     * A concurrent workload must have a sequential order that explains it.
     *
     * <p><b>Why this is not one more property.</b> The tests above ask questions somebody thought of: two
     * contenders, one winner; a stale generation loses. Each is real and each looks at one scenario.
     * Linearizability is not a scenario — it is a claim about every interleaving — so this runs a workload
     * shaped like the real one (each worker holds the generation it last saw and swaps against it, exactly
     * as a node taking a shard does), writes down what every call returned and when, and asks whether any
     * single sequential order could have produced all of it.
     *
     * <p>That is what catches the deviations nobody wrote a scenario for: a read served from a replica that
     * had not caught up, a refusal reporting a generation nobody stored, an interleaving that is fine
     * pairwise and impossible together.
     *
     * <p><b>Short rounds rather than one long history.</b> The search is exponential in the worst case, and
     * a violation shows up in a small window far more often than it needs a large one. Several independent
     * rounds are more chances to catch something and each is cheap to judge.
     *
     * @throws Exception if the backend cannot be reached
     */
    public void testAConcurrentHistoryIsLinearizable() throws Exception {
        final BlobContainer container = newContainer();
        final int workers = Math.min(4, contenders());
        final int callsEach = 12;

        for (int round = 0; round < 4; round++) {
            final String name = "history-" + round;
            container.createRegisterIfAbsent(name, bytes("v0"));
            final long initial = container.readRegister(name).orElseThrow().generation();

            final RegisterHistory history = new RegisterHistory();
            final CountDownLatch start = new CountDownLatch(1);
            final java.util.List<Exception> failures = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
            final Thread[] threads = new Thread[workers];
            for (int w = 0; w < workers; w++) {
                final int worker = w;
                threads[w] = new Thread(() -> {
                    long known = initial;
                    try {
                        start.await();
                        for (int call = 0; call < callsEach; call++) {
                            if (call % 3 == 0) {
                                final long invoked = history.invoke();
                                final var seen = container.readRegister(name);
                                history.completed(
                                    new RegisterHistory.Read(
                                        seen.map(BlobRegister::generation).orElse(RegisterHistory.ABSENT),
                                        seen.map(r -> r.value().utf8ToString()).orElse(null)
                                    ),
                                    invoked
                                );
                                known = seen.map(BlobRegister::generation).orElse(RegisterHistory.ABSENT);
                            } else {
                                final String value = "w" + worker + "-" + call;
                                final long invoked = history.invoke();
                                final var result = container.compareAndSwapRegister(name, known, bytes(value));
                                history.completed(
                                    new RegisterHistory.Cas(known, value, result.applied(), result.currentGeneration()),
                                    invoked
                                );
                                // Whether it won or lost, the register tells the caller where it now is.
                                // Believing that is exactly what the design does.
                                known = result.currentGeneration();
                            }
                        }
                    } catch (Exception e) {
                        failures.add(e);
                    }
                });
                threads[w].start();
            }
            start.countDown();
            for (Thread t : threads) {
                t.join(120_000);
            }

            assertEquals("no call should have errored: " + failures, java.util.List.of(), failures);
            LinearizabilityChecker.check(history.entries(), initial, "v0")
                .assertLinearizable("round " + round + " of the concurrent register workload");
        }
    }
}
