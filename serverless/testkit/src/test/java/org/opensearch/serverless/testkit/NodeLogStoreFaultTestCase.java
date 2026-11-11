/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.serverless.store.NodeLogBlob;
import org.opensearch.serverless.store.NodeLogBlob.Range;
import org.opensearch.serverless.store.NodeLogBlob.ShardBatch;
import org.opensearch.serverless.store.NodeLogBlob.ShardKey;
import org.opensearch.serverless.store.NodeLogStore;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.serverless.testkit.CrashPointBlobStore.Kind;
import org.opensearch.serverless.testkit.CrashPointExplorer.Ledger;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The per-node log store under faults, races and concurrent successors.
 *
 * <p>The writer here follows the store's contract by hand, as the committer will: one blob per window, ordinals
 * in order with create-if-absent, and an epoch abandoned after any failed or ambiguous append. Recovery is a
 * successor's: fence the epoch, read the last header below the fence, and fetch the shard's slices. What is checked
 * is the store's half of the fencing argument: every blob the writer landed is below the fence, so every
 * acknowledged record is recovered, and nothing the writer never sent appears.
 *
 * <p>Subclasses bind a store: the filesystem in the default test run, the GCS and Azure emulators in
 * {@code emulatorTest}.
 */
public abstract class NodeLogStoreFaultTestCase extends OpenSearchTestCase {

    private static final String NODE = "node-a";
    private static final long EPOCH = 1L;
    private static final ShardKey ALPHA = new ShardKey("alpha-uuid", 0);
    private static final ShardKey BETA = new ShardKey("beta-uuid", 3);

    /**
     * Returns the store every run shares. Each run writes under its own path.
     *
     * @return the store
     * @throws Exception if it cannot be made
     */
    protected abstract BlobStore newStore() throws Exception;

    private static WalRecord record(String id, long seqNo, long term) {
        return new WalRecord(id, "{\"msg\":\"" + id + "\"}", seqNo, term, 1L);
    }

    /** A writer over one epoch, keeping the running index as the committer does. */
    private static final class Writer {
        private final NodeLogStore log;
        private final long epoch;
        private Map<ShardKey, List<Range>> index = new TreeMap<>();
        private volatile long next;
        private boolean abandoned;
        private volatile IOException refusal;

        Writer(NodeLogStore log, long epoch) {
            this.log = log;
            this.epoch = epoch;
        }

        /**
         * Appends one window holding a record for each of alpha and beta, and acknowledges both only if the append
         * returned. Any failure abandons the epoch, so nothing is ever appended to it again.
         *
         * @return whether the window landed
         */
        boolean window(String id, Ledger ledger) throws IOException {
            if (abandoned) {
                return false;
            }
            final String a = "alpha-" + id;
            final String b = "beta-" + id;
            ledger.attempted(a);
            ledger.attempted(b);
            final NodeLogBlob.Encoded encoded = NodeLogBlob.encode(
                NODE,
                epoch,
                next,
                0L,
                List.of(new ShardBatch(ALPHA, 1L, List.of(record(a, next, 1L))), new ShardBatch(BETA, 1L, List.of(record(b, next, 1L)))),
                index
            );
            try {
                log.append(epoch, next, encoded.bytes());
            } catch (IOException e) {
                abandoned = true;
                refusal = e;
                if (e instanceof CrashPointBlobStore.CrashedException crashed) {
                    throw crashed;
                }
                return false;
            }
            index = encoded.header().index();
            next++;
            ledger.acknowledged(a);
            ledger.acknowledged(b);
            return true;
        }
    }

    /** A successor's recovery of one shard: fence, last header below the fence, then just that shard's slices. */
    private static Set<String> recover(NodeLogStore log, ShardKey shard, String successor) throws IOException {
        final long fence = log.fence(EPOCH, 0L, successor, 2L);
        final Optional<NodeLogBlob.Header> last = log.lastHeaderBelow(EPOCH, fence, 0L);
        final Set<String> ids = new HashSet<>();
        if (last.isEmpty()) {
            return ids;
        }
        for (Range range : last.get().ordinalsOf(shard)) {
            assertTrue("the index lists only slots below the fence: " + range + " vs " + fence, range.to() < fence);
            for (long ordinal = range.from(); ordinal <= range.to(); ordinal++) {
                final NodeLogBlob.Header header = (NodeLogBlob.Header) log.readEntry(EPOCH, ordinal).orElseThrow();
                for (NodeLogBlob.Slice slice : header.slicesOf(shard)) {
                    for (WalRecord record : log.readSlice(EPOCH, header, slice)) {
                        ids.add(record.id());
                    }
                }
            }
        }
        return ids;
    }

    private static Set<String> recoverBoth(BlobStore store, BlobPath root) throws IOException {
        final NodeLogStore log = new NodeLogStore(store, root, NODE);
        final Set<String> ids = recover(log, ALPHA, "node-b");
        ids.addAll(recover(log, BETA, "node-c"));
        return ids;
    }

    private static byte[] window(long ordinal) throws IOException {
        return NodeLogBlob.encode(
            NODE,
            EPOCH,
            ordinal,
            0L,
            List.of(new ShardBatch(ALPHA, 1L, List.of(record("x" + ordinal, ordinal, 1L)))),
            Map.of()
        ).bytes();
    }

    public void testEveryAcknowledgedWindowIsRecoveredAfterAFaultAtAnyCallOfTheWriter() throws Exception {
        final BlobStore store = newStore();
        final var result = CrashPointExplorer.exploreAll(store, BlobPath.cleanPath().add("writer"), (faulty, root, ledger) -> {
            final Writer writer = new Writer(new NodeLogStore(faulty, root, NODE), EPOCH);
            for (int i = 0; i < 4; i++) {
                writer.window(Integer.toString(i), ledger);
            }
        }, (honest, root, ledger, where) -> ledger.assertRecovered(recoverBoth(honest, root), where));
        logger.info("node-log writer: {} calls, {} faulted runs", result.recorded().size(), result.explored().size());
        assertEquals("one PUT per window, nothing else", 4, result.recorded().size());
    }

    /**
     * A successor whose fence is interrupted at any call. A second successor must still close the epoch, the writer's
     * next append must be refused, and both shards must recover everything acknowledged.
     */
    public void testAFenceInterruptedAtAnyCallStillLetsTheNextSuccessorCloseTheEpoch() throws Exception {
        final BlobStore store = newStore();
        final var result = CrashPointExplorer.exploreAll(store, BlobPath.cleanPath().add("fence"), (faulty, root, ledger) -> {
            final Writer writer = new Writer(new NodeLogStore(store, root, NODE), EPOCH);
            for (int i = 0; i < 3; i++) {
                writer.window(Integer.toString(i), ledger);
            }
            new NodeLogStore(faulty, root, NODE).fence(EPOCH, 0L, "node-b", 2L);
        }, (honest, root, ledger, where) -> {
            final Set<String> recovered = recoverBoth(honest, root);
            expectThrows(FileAlreadyExistsException.class, () -> new NodeLogStore(honest, root, NODE).append(EPOCH, 3L, window(3L)));
            ledger.assertRecovered(recovered, where);
        });
        logger.info("node-log fence: {} calls, {} faulted runs", result.recorded().size(), result.explored().size());
    }

    /**
     * The writer, pausing briefly between windows as a lightly loaded node does, and a successor race for slots, many
     * times. Exactly one of them gets each slot: the writer's append to the fence's slot is refused, every slot it
     * landed is below the fence, and nothing acknowledged is lost.
     */
    public void testAnAppendAndAFenceRacingForASlotHaveOneWinner() throws Exception {
        race(25, 2_000_000L);
    }

    /**
     * The same race against a writer that flushes back to back, as a node under load does when the next window is
     * ready by the time the previous append lands. A fence aiming only at the first free slot never wins here, since
     * the writer's next append is already headed for it; the fence has to get ahead of the writer.
     */
    public void testAFenceOvertakesAWriterAppendingBackToBack() throws Exception {
        race(10, 0L);
    }

    private void race(int rounds, long maxPauseNanos) throws Exception {
        final BlobStore store = newStore();
        for (int round = 0; round < rounds; round++) {
            final BlobPath root = BlobPath.cleanPath().add("race-" + round);
            final Ledger ledger = new Ledger();
            final NodeLogStore log = new NodeLogStore(store, root, NODE);
            final Writer writer = new Writer(log, EPOCH);
            final AtomicReference<Long> refusedAt = new AtomicReference<>();
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
            final CountDownLatch started = new CountDownLatch(1);
            final long seed = randomLong();
            final Thread writing = new Thread(() -> {
                final java.util.Random pauses = new java.util.Random(seed);
                try {
                    for (int i = 0; stop.get() == false; i++) {
                        final long slot = writer.next;
                        if (i == 2) {
                            started.countDown();
                        }
                        if (writer.window(Integer.toString(i), ledger) == false) {
                            refusedAt.set(slot);
                            return;
                        }
                        if (maxPauseNanos > 0) {
                            java.util.concurrent.locks.LockSupport.parkNanos((long) (pauses.nextDouble() * maxPauseNanos));
                        }
                    }
                } catch (Throwable t) {
                    failure.set(t);
                } finally {
                    started.countDown();
                }
            });
            writing.start();
            final long fence;
            try {
                started.await();
                fence = log.fence(EPOCH, 0L, "node-b", 2L);
            } catch (Exception e) {
                // Only a failed fence stops the writer early; otherwise it runs until the fence refuses it.
                stop.set(true);
                throw e;
            } finally {
                writing.join(60_000);
            }
            assertFalse("round " + round + ": the writer must stop at the fence", writing.isAlive());
            assertNull("round " + round + ": the writer failed: " + failure.get(), failure.get());
            assertNotNull("round " + round + ": the writer must have been refused", refusedAt.get());
            assertTrue(
                "round " + round + ": refused by the fence, not by a store error: " + writer.refusal,
                writer.refusal instanceof FileAlreadyExistsException
            );
            assertEquals("round " + round + ": the writer is refused exactly at the fence", fence, (long) refusedAt.get());
            assertEquals("round " + round + ": every landed slot is below the fence", fence, writer.next);
            ledger.assertRecovered(recoverBoth(store, root), "round " + round);
        }
    }

    /** Five successors fencing an idle epoch at once share one fence: one fence blob, the same ordinal for each. */
    public void testConcurrentSuccessorsShareOneFence() throws Exception {
        fenceConcurrently(false);
    }

    /**
     * Five successors fencing one epoch at once while its writer keeps appending. Racing a live writer, successors
     * may land more than one fence; what must hold is that the writer is refused at the lowest of them, every fence is
     * at or above it, and every successor recovers everything acknowledged below its own fence.
     */
    public void testConcurrentSuccessorsRacingALiveWriterAllCloseTheEpoch() throws Exception {
        fenceConcurrently(true);
    }

    private void fenceConcurrently(boolean liveWriter) throws Exception {
        final BlobStore store = newStore();
        final int successors = 5;
        for (int round = 0; round < 10; round++) {
            final String at = "round " + round + ": ";
            final BlobPath root = BlobPath.cleanPath().add((liveWriter ? "live-" : "idle-") + round);
            final NodeLogStore log = new NodeLogStore(store, root, NODE);
            final Ledger ledger = new Ledger();
            final Writer writer = new Writer(log, EPOCH);
            for (int i = 0; i < 3; i++) {
                writer.window(Integer.toString(i), ledger);
            }
            final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
            final Thread writing = new Thread(() -> {
                try {
                    for (int i = 3; writer.window(Integer.toString(i), ledger); i++) {
                        java.util.concurrent.locks.LockSupport.parkNanos(200_000L);
                    }
                } catch (Throwable t) {
                    writerFailure.set(t);
                }
            });
            if (liveWriter) {
                writing.start();
            }
            final CyclicBarrier barrier = new CyclicBarrier(successors);
            final List<Long> fences = java.util.Collections.synchronizedList(new ArrayList<>());
            final List<Throwable> errors = java.util.Collections.synchronizedList(new ArrayList<>());
            final Thread[] threads = new Thread[successors];
            for (int s = 0; s < successors; s++) {
                final String name = "node-" + (char) ('b' + s);
                threads[s] = new Thread(() -> {
                    try {
                        barrier.await();
                        fences.add(new NodeLogStore(store, root, NODE).fence(EPOCH, 0L, name, 2L));
                    } catch (Throwable t) {
                        errors.add(t);
                    }
                });
                threads[s].start();
            }
            for (Thread thread : threads) {
                thread.join(60_000);
                assertFalse(at + "a successor did not finish", thread.isAlive());
            }
            assertEquals(at + "no successor failed: " + errors, List.of(), errors);
            final long lowest = java.util.Collections.min(fences);
            final long highest = java.util.Collections.max(fences);
            if (liveWriter) {
                writing.join(60_000);
                assertFalse(at + "the writer must stop at the fence", writing.isAlive());
                assertNull(at + "the writer failed: " + writerFailure.get(), writerFailure.get());
                assertTrue(at + "refused by a fence: " + writer.refusal, writer.refusal instanceof FileAlreadyExistsException);
            }
            assertEquals(at + "the writer stopped at the lowest fence " + fences, lowest, writer.next);
            int fenceBlobs = 0;
            for (long ordinal = 0; ordinal <= highest + successors; ordinal++) {
                if (log.readEntry(EPOCH, ordinal).orElse(null) instanceof NodeLogBlob.Fence) {
                    fenceBlobs++;
                }
            }
            if (liveWriter == false) {
                assertEquals(at + "every successor got the same fence: " + fences, 1, new HashSet<>(fences).size());
                assertEquals(at + "one fence blob for all of them", 1, fenceBlobs);
            } else {
                logger.info("{}five successors racing a live writer landed {} fence blob(s) at {}", at, fenceBlobs, new HashSet<>(fences));
            }
            for (long fence : new HashSet<>(fences)) {
                ledger.assertRecovered(recoverBelow(log, fence), at + "a successor holding the fence at " + fence);
            }
        }
    }

    /** Both shards' records below one fence, as the successor holding that fence reads them. */
    private static Set<String> recoverBelow(NodeLogStore log, long fence) throws IOException {
        final Set<String> ids = new HashSet<>();
        final Optional<NodeLogBlob.Header> last = log.lastHeaderBelow(EPOCH, fence, 0L);
        if (last.isEmpty()) {
            return ids;
        }
        for (ShardKey shard : List.of(ALPHA, BETA)) {
            for (Range range : last.get().ordinalsOf(shard)) {
                for (long ordinal = range.from(); ordinal <= range.to(); ordinal++) {
                    final NodeLogBlob.Header header = (NodeLogBlob.Header) log.readEntry(EPOCH, ordinal).orElseThrow();
                    for (NodeLogBlob.Slice slice : header.slicesOf(shard)) {
                        for (WalRecord record : log.readSlice(EPOCH, header, slice)) {
                            ids.add(record.id());
                        }
                    }
                }
            }
        }
        return ids;
    }

    /** The store's request counts for the steps the design prices. */
    public void testRequestCountsMatchTheDesign() throws Exception {
        final BlobStore store = newStore();
        final BlobPath root = BlobPath.cleanPath().add("counts");
        final Ledger ledger = new Ledger();

        CrashPointBlobStore counted = new CrashPointBlobStore(store);
        final Writer writer = new Writer(new NodeLogStore(counted, root, NODE), EPOCH);
        for (int i = 0; i < 5; i++) {
            writer.window(Integer.toString(i), ledger);
        }
        assertEquals(
            "a window is one create-if-absent PUT",
            List.of(Kind.CREATE, Kind.CREATE, Kind.CREATE, Kind.CREATE, Kind.CREATE),
            kinds(counted)
        );

        counted = new CrashPointBlobStore(store);
        final long fence = new NodeLogStore(counted, root, NODE).fence(EPOCH, 4L, "node-b", 2L);
        assertEquals(5L, fence);
        assertEquals("a fence from a hint one behind is one PUT: " + counted.steps(), 1, count(counted, Kind.CREATE));
        assertEquals("and no listing: " + counted.steps(), 0, count(counted, Kind.LIST));
        assertTrue("a probe of at most three existence checks: " + counted.steps(), count(counted, Kind.EXISTS) <= 3);
        assertTrue("and at most one read of the slot below: " + counted.steps(), count(counted, Kind.READ) <= 1);

        counted = new CrashPointBlobStore(store);
        new NodeLogStore(counted, root, NODE).lastHeaderBelow(EPOCH, fence, 0L);
        assertEquals("the last header below a fence is one ranged GET", List.of(Kind.READ), kinds(counted));

        counted = new CrashPointBlobStore(store);
        new NodeLogStore(counted, root, NODE).deleteRange(EPOCH, 0L, 2_500L);
        assertEquals("deletes go 1,000 names to a request", List.of(Kind.DELETE, Kind.DELETE, Kind.DELETE), kinds(counted));
    }

    private static List<Kind> kinds(CrashPointBlobStore store) {
        return store.steps().stream().map(CrashPointBlobStore.Step::kind).toList();
    }

    private static long count(CrashPointBlobStore store, Kind kind) {
        return store.steps().stream().filter(s -> s.kind() == kind).count();
    }
}
