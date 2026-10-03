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
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.serverless.store.WalStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The log is fenced by occupying the predecessor's next slot, so the swap/seal window is closed rather than
 * narrowed: a record a predecessor lands is replayed and may be acknowledged, and nothing it tries after the
 * fence can land at all. See {@code serverless/rfc-fencing-closure.md}.
 *
 * <p>Each test states the two properties it pins -- nothing acknowledged is lost, nothing refused is replayed
 * -- in terms of what an append returned: an append that returned normally is acknowledged; one that threw is
 * refused. A subclass supplies the store, so the same history runs on a filesystem and on an S3 API.
 */
public abstract class LogFencingTestCase extends OpenSearchTestCase {

    /**
     * Returns a fresh, empty store.
     *
     * @return the store
     * @throws Exception if it cannot be made
     */
    protected abstract BlobStore newStore() throws Exception;

    private static final BlobPath SHARD = BlobPath.cleanPath().add("segments").add("alpha#uuid#0");

    private static WalRecord record(String id, long term) {
        return new WalRecord(id, "{\"msg\":\"" + id + "\"}", 0L, term, 1L);
    }

    private static Set<String> ids(List<WalRecord> records) {
        final Set<String> ids = new HashSet<>();
        for (WalRecord record : records) {
            ids.add(record.id());
        }
        return ids;
    }

    /**
     * The case the open item named: a predecessor appending after the swap and before the successor's seal.
     * Its append lands before the fence, so it is acknowledged and replayed; its next one hits the fence and is
     * refused, and is never replayed.
     */
    public void testAnAppendBeforeTheFenceIsKeptAndOneAfterItIsRefused() throws Exception {
        final BlobStore store = newStore();
        final WalStore predecessor = new WalStore(store, SHARD);
        predecessor.establish(1L, null);
        predecessor.append(1L, List.of(record("before-swap", 1L)));
        // The swap has happened; the successor has not yet fenced. The predecessor appends.
        predecessor.append(1L, List.of(record("inside-window", 1L)));

        final WalStore successor = new WalStore(store, SHARD);
        successor.fenceOlderTerms(2L);
        expectThrows(IOException.class, () -> predecessor.append(1L, List.of(record("after-fence", 1L))));

        final Set<String> replayed = ids(successor.replayableAfterFencing());
        assertTrue("acknowledged before the swap: replayed", replayed.contains("before-swap"));
        assertTrue(
            "acknowledged inside the window: replayed, where it used to be refused and replayed",
            replayed.contains("inside-window")
        );
        assertFalse("refused at the fence: never replayed", replayed.contains("after-fence"));
    }

    /**
     * A writer with two appends in flight, the first held unanswered while a successor fences its slot: the second must
     * not land past the fence and be acknowledged. A fleet run lost three acknowledged writes this way -- the record
     * past the fence was replayed by a later successor ahead of the newer term's write of the same sequence number,
     * which recovery then skipped, and that successor's publish trimmed it away.
     */
    public void testASecondAppendInFlightCannotLandPastAFence() throws Exception {
        final HookedBlobStore store = new HookedBlobStore(newStore());
        final WalStore predecessor = new WalStore(store, SHARD);
        predecessor.establish(1L, null);
        // establish fenced slot 1; this record is slot 2, the held one slot 3, and the one behind it slot 4.
        predecessor.append(1L, List.of(record("landed", 1L)));
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        store.holdNextWrite("t=1", "00000000000000000003", entered, release);
        final java.util.concurrent.atomic.AtomicReference<Exception> first = new java.util.concurrent.atomic.AtomicReference<>();
        final Thread held = new Thread(() -> {
            try {
                predecessor.append(1L, List.of(record("held", 1L)));
            } catch (Exception e) {
                first.set(e);
            }
        });
        held.start();
        assertTrue(entered.await(30, java.util.concurrent.TimeUnit.SECONDS));

        // The successor fences, while the first is unanswered: slot 3, which the held write has not filled.
        new WalStore(store, SHARD).fenceOlderTerms(2L);
        // Then the predecessor's next write.
        final java.util.concurrent.atomic.AtomicReference<Exception> second = new java.util.concurrent.atomic.AtomicReference<>();
        final java.util.concurrent.atomic.AtomicBoolean secondDone = new java.util.concurrent.atomic.AtomicBoolean();
        final Thread next = new Thread(() -> {
            try {
                predecessor.append(1L, List.of(record("second", 1L)));
            } catch (Exception e) {
                second.set(e);
            } finally {
                secondDone.set(true);
            }
        });
        next.start();
        release.countDown();
        held.join(30_000L);
        next.join(30_000L);
        assertTrue(secondDone.get());

        assertNotNull("the held write lost its slot to the fence", first.get());
        assertNotNull("so the one behind it is refused too, never acknowledged past the fence", second.get());
        assertFalse(ids(new WalStore(store, SHARD).replayableAfterFencing()).contains("second"));
    }

    /**
     * A writer that has begun its term and written nothing yet is fenced by its successor all the same. Its own fence
     * is the term's last blob, which a successor used to take for a fence of its own and skip -- leaving the writer
     * free to append after the successor had replayed.
     */
    public void testAWriterThatHasWrittenNothingIsStillFenced() throws Exception {
        final BlobStore store = newStore();
        final WalStore predecessor = new WalStore(store, SHARD);
        predecessor.establish(1L, null);
        new WalStore(store, SHARD).fenceOlderTerms(2L);
        expectThrows(IOException.class, () -> predecessor.append(1L, List.of(record("after-the-takeover", 1L))));
        assertFalse(ids(new WalStore(store, SHARD).replayableAfterFencing()).contains("after-the-takeover"));
    }

    /**
     * Once a publish has fenced and emptied every older term, later takeovers do not list those terms again, and lose
     * nothing by it. Each takeover used to list every term the shard had ever had, three times over; at forty-odd terms
     * a shard, taking a dead node's shards took twice as long.
     */
    public void testACompactedTermIsNotListedAgain() throws Exception {
        final HookedBlobStore store = new HookedBlobStore(newStore());
        final WalStore first = new WalStore(store, SHARD);
        first.establish(1L, null);
        first.append(1L, List.of(record("one", 1L)));
        final WalStore second = new WalStore(store, SHARD);
        second.fenceOlderTerms(2L);
        second.establish(2L, null);
        assertTrue(ids(second.replayableAfterFencing()).contains("one"));
        second.append(2L, List.of(record("two", 2L)));
        // Its first publish: term 1 is fenced, so its records are dropped and it is compacted.
        second.onPublished(2L);

        store.failListingsUnder("/t=1/");
        final WalStore third = new WalStore(store, SHARD);
        third.fenceOlderTerms(3L);
        third.establish(3L, null);
        assertTrue("term 2's record is still replayed", ids(third.replayableAfterFencing()).contains("two"));
        third.onPublished(3L);
    }

    /** A writer refused once stops for good, even where the next slot is free. */
    public void testAWriterRefusedOnceNeverWritesAgain() throws Exception {
        final BlobStore store = newStore();
        final WalStore predecessor = new WalStore(store, SHARD);
        predecessor.establish(1L, null);
        predecessor.append(1L, List.of(record("one", 1L)));
        new WalStore(store, SHARD).fenceOlderTerms(2L);
        expectThrows(IOException.class, () -> predecessor.append(1L, List.of(record("at-the-fence", 1L))));
        // The slot after the fence is free. A writer that tried it would land a record behind the fence.
        expectThrows(IOException.class, () -> predecessor.append(1L, List.of(record("past-the-fence", 1L))));
        assertFalse(ids(new WalStore(store, SHARD).replayableAfterFencing()).contains("past-the-fence"));
    }

    /**
     * Truncation cannot hide how far a writer got: after publishes that emptied everything else, the term's
     * last record stays, so the successor's fence lands on the predecessor's next slot rather than at 1.
     */
    public void testTruncationKeepsTheSlotAFenceNeeds() throws Exception {
        final BlobStore store = newStore();
        final WalStore predecessor = new WalStore(store, SHARD);
        predecessor.establish(1L, null);
        for (int i = 0; i < 5; i++) {
            predecessor.append(1L, List.of(record("r" + i, 1L)));
        }
        predecessor.onPublished(1L);
        predecessor.onPublished(1L);
        predecessor.onPublished(1L);

        new WalStore(store, SHARD).fenceOlderTerms(2L);
        expectThrows(
            IOException.class,
            "the predecessor's next append must hit the fence, not a slot past it",
            () -> predecessor.append(1L, List.of(record("after-truncation", 1L)))
        );
    }

    /** A fence outlives the successor's publish, which drops the older term's records: a stopped writer stays stopped. */
    public void testAFenceSurvivesTheOlderTermBeingDropped() throws Exception {
        final BlobStore store = newStore();
        final WalStore predecessor = new WalStore(store, SHARD);
        predecessor.establish(1L, null);
        predecessor.append(1L, List.of(record("old", 1L)));

        final WalStore successor = new WalStore(store, SHARD);
        successor.fenceOlderTerms(2L);
        successor.establish(2L, null);
        successor.append(2L, List.of(record("new", 2L)));
        successor.onPublished(2L);
        successor.onPublished(2L);

        final WalStore late = new WalStore(store, SHARD);
        // A writer of term 1 that re-derived its position would start past whatever is left; the fence is what
        // it must meet.
        expectThrows(IOException.class, () -> late.establish(1L, () -> false));
        expectThrows(IOException.class, () -> predecessor.append(1L, List.of(record("zombie", 1L))));
        assertFalse(ids(new WalStore(store, SHARD).replayableAfterFencing()).contains("zombie"));
    }

    /** A same-term reopen fences the instance it replaced, which is the second seal done the same way. */
    public void testASameTermReopenFencesTheInstanceItReplaced() throws Exception {
        final BlobStore store = newStore();
        final WalStore before = new WalStore(store, SHARD);
        before.establish(3L, null);
        before.append(3L, List.of(record("from-before", 3L)));

        final WalStore reopened = new WalStore(store, SHARD);
        reopened.establish(3L, null);
        expectThrows(IOException.class, () -> before.append(3L, List.of(record("stale", 3L))));
        reopened.append(3L, List.of(record("from-after", 3L)));

        final Set<String> replayed = ids(new WalStore(store, SHARD).replayableAfterFencing());
        assertTrue(replayed.contains("from-before"));
        assertTrue(replayed.contains("from-after"));
        assertFalse(replayed.contains("stale"));
    }

    /** A writer superseded before it wrote anything finds out after fencing its own term, and stops. */
    public void testAWriterSupersededBeforeItWroteStops() throws Exception {
        final BlobStore store = newStore();
        final WalStore superseded = new WalStore(store, SHARD);
        expectThrows(IOException.class, () -> superseded.establish(4L, () -> false));
        expectThrows(IOException.class, () -> superseded.append(4L, List.of(record("never", 4L))));
    }

    /**
     * Concurrent: a predecessor appending as fast as it can while two successors fence. Every append that
     * returned is replayed; none that threw is. Run several rounds, because the interesting interleavings are
     * a matter of timing.
     */
    public void testAConcurrentFenceLosesNothingAcknowledgedAndKeepsNothingRefused() throws Exception {
        for (int round = 0; round < 5; round++) {
            final BlobStore store = newStore();
            final BlobPath shard = SHARD.add("round-" + round);
            final WalStore predecessor = new WalStore(store, shard);
            predecessor.establish(1L, null);
            final List<String> acknowledged = Collections.synchronizedList(new ArrayList<>());
            final List<String> refused = Collections.synchronizedList(new ArrayList<>());
            final AtomicBoolean stop = new AtomicBoolean();
            final CountDownLatch writing = new CountDownLatch(1);
            final Thread writer = new Thread(() -> {
                for (int i = 0; stop.get() == false && i < 10_000; i++) {
                    final String id = "w" + i;
                    try {
                        predecessor.append(1L, List.of(record(id, 1L)));
                        acknowledged.add(id);
                    } catch (IOException e) {
                        refused.add(id);
                    }
                    if (i == 3) {
                        writing.countDown();
                    }
                }
            });
            writer.start();
            assertTrue(writing.await(60, java.util.concurrent.TimeUnit.SECONDS));
            final Thread[] takers = new Thread[2];
            final List<Exception> failures = Collections.synchronizedList(new ArrayList<>());
            for (int t = 0; t < takers.length; t++) {
                takers[t] = new Thread(() -> {
                    try {
                        new WalStore(store, shard).fenceOlderTerms(2L);
                    } catch (Exception e) {
                        failures.add(e);
                    }
                });
                takers[t].start();
            }
            for (Thread taker : takers) {
                taker.join(60_000);
            }
            // Past the fences the writer can land nothing more; let it find that out a few times.
            Thread.yield();
            stop.set(true);
            writer.join(60_000);
            assertEquals("fencing must not fail: " + failures, List.of(), failures);

            final Set<String> replayed = ids(new WalStore(store, shard).replayableAfterFencing());
            for (String id : acknowledged) {
                assertTrue("round " + round + ": acknowledged " + id + " was not replayed", replayed.contains(id));
            }
            for (String id : refused) {
                assertFalse("round " + round + ": refused " + id + " was replayed", replayed.contains(id));
            }
            // And whatever the writer managed before it stopped, its next append meets the fence.
            expectThrows(IOException.class, () -> predecessor.append(1L, List.of(record("after-the-fences", 1L))));
            assertFalse(ids(new WalStore(store, shard).replayableAfterFencing()).contains("after-the-fences"));
        }
    }
}
