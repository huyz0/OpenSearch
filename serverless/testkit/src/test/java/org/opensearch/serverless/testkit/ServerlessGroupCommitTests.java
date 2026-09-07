/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.store.WalGroupCommitter;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Group commit: many concurrent writes to one shard, one object-store PUT.
 *
 * <p>The saving this exists for is a single number — PUTs per second per shard bounded by the store's
 * write latency instead of by the write rate — and every other test here exists because that saving is
 * only worth having if it does not quietly weaken ordering, durability or the honesty of a refusal.
 *
 * <p><b>Why there is no timer to test.</b> Batching happens only while a PUT for the same shard is
 * already in flight, so the tests below create that condition explicitly, by blocking inside the flush,
 * rather than by waiting out a window. A quiet shard has nothing in flight and so batches nothing, which
 * is {@link #testAQuietShardPaysExactlyWhatItDidBefore}.
 */
public class ServerlessGroupCommitTests extends OpenSearchTestCase {

    private static final ShardId SHARD = new ShardId(new Index("alpha", "alpha-uuid"), 0);

    private static WalRecord record(String id) {
        return new WalRecord(id, "{\"msg\":\"" + id + "\"}");
    }

    /** A flush that records what it was handed, one list per group. */
    private static final class Recording implements WalGroupCommitter.Flush {
        private final List<List<WalRecord>> groups = new CopyOnWriteArrayList<>();

        @Override
        public void append(long term, List<WalRecord> records) {
            groups.add(List.copyOf(records));
        }

        private List<WalRecord> flattened() {
            final List<WalRecord> all = new ArrayList<>();
            for (List<WalRecord> group : groups) {
                all.addAll(group);
            }
            return all;
        }
    }

    /**
     * The property that keeps this from being a latency tax: with nothing in flight, a caller flushes its
     * own records immediately and the cost is exactly what it was before group commit existed.
     */
    public void testAQuietShardPaysExactlyWhatItDidBefore() throws Exception {
        final WalGroupCommitter committer = new WalGroupCommitter();
        final Recording flush = new Recording();
        for (int i = 0; i < 10; i++) {
            committer.commit(SHARD, 1L, List.of(record("d" + i)), flush);
        }
        assertEquals("a sequential writer must not batch: there is never anything in flight to batch with", 10, flush.groups.size());
        assertEquals(10, committer.groups());
        assertEquals(10, committer.recordsCommitted());
    }

    /**
     * The saving itself. One writer is held inside its PUT while the rest arrive; they share a single
     * blob, so the number of object-store writes is far below the number of documents.
     */
    public void testConcurrentWritesShareOnePut() throws Exception {
        final WalGroupCommitter committer = new WalGroupCommitter();
        final Recording recorded = new Recording();
        final CountDownLatch firstFlushEntered = new CountDownLatch(1);
        final CountDownLatch releaseFirstFlush = new CountDownLatch(1);
        final AtomicInteger flushes = new AtomicInteger();

        final WalGroupCommitter.Flush flush = (term, records) -> {
            if (flushes.getAndIncrement() == 0) {
                firstFlushEntered.countDown();
                try {
                    releaseFirstFlush.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted", e);
                }
            }
            recorded.append(term, records);
        };

        final int others = 32;
        final Thread leader = new Thread(() -> {
            try {
                committer.commit(SHARD, 1L, List.of(record("leader")), flush);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }, "leader");
        leader.start();
        assertTrue("the leader should have reached its flush", firstFlushEntered.await(30, java.util.concurrent.TimeUnit.SECONDS));

        final List<Thread> waiters = new ArrayList<>();
        for (int i = 0; i < others; i++) {
            final String id = "d" + i;
            final Thread thread = new Thread(() -> {
                try {
                    committer.commit(SHARD, 1L, List.of(record(id)), flush);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }, "waiter-" + i);
            waiters.add(thread);
            thread.start();
        }
        // Parked inside the committer, not merely started: only then is it certain they all joined the
        // group behind the in-flight PUT rather than racing its completion.
        assertBusy(() -> {
            for (Thread waiter : waiters) {
                final Thread.State state = waiter.getState();
                assertTrue(
                    "waiter " + waiter.getName() + " should be parked in the committer, was " + state,
                    state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING
                );
            }
        });

        releaseFirstFlush.countDown();
        leader.join(30_000);
        for (Thread waiter : waiters) {
            waiter.join(30_000);
        }

        assertEquals("every document must be logged", others + 1, recorded.flattened().size());
        // The claim, as a bound rather than an exact count: the leader's own PUT, and then the group that
        // accumulated behind it. What matters is that it is nothing like one per document.
        assertEquals("the leader's PUT and one for everyone who arrived during it", 2, recorded.groups.size());
        assertEquals(2, committer.groups());
    }

    /**
     * Batching must not reorder. Each writer's records stay in the order it submitted them, and every
     * record appears exactly once across the groups actually written.
     */
    public void testEveryRecordLandsExactlyOnceAndInOrder() throws Exception {
        final WalGroupCommitter committer = new WalGroupCommitter();
        final Recording recorded = new Recording();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger flushes = new AtomicInteger();
        final WalGroupCommitter.Flush flush = (term, records) -> {
            if (flushes.getAndIncrement() == 0) {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted", e);
                }
            }
            recorded.append(term, records);
        };

        final int writers = 16;
        final Thread leader = new Thread(() -> {
            try {
                committer.commit(SHARD, 1L, List.of(record("leader-0"), record("leader-1")), flush);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        leader.start();
        assertTrue(entered.await(30, java.util.concurrent.TimeUnit.SECONDS));

        final List<Thread> threads = new ArrayList<>();
        for (int w = 0; w < writers; w++) {
            final int which = w;
            final Thread thread = new Thread(() -> {
                try {
                    committer.commit(
                        SHARD,
                        1L,
                        List.of(record("w" + which + "-0"), record("w" + which + "-1"), record("w" + which + "-2")),
                        flush
                    );
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });
            threads.add(thread);
            thread.start();
        }
        assertBusy(() -> {
            for (Thread thread : threads) {
                final Thread.State state = thread.getState();
                assertTrue(state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING);
            }
        });
        release.countDown();
        leader.join(30_000);
        for (Thread thread : threads) {
            thread.join(30_000);
        }

        final List<String> ids = new ArrayList<>();
        for (WalRecord written : recorded.flattened()) {
            ids.add(written.id());
        }
        assertEquals("every record exactly once", 2 + writers * 3, ids.size());
        assertEquals("and no duplicates", ids.size(), Set.copyOf(ids).size());

        // One writer's records keep their relative order, which is what replay depends on: a later write
        // to the same id must not overtake an earlier one.
        for (int w = 0; w < writers; w++) {
            final int first = ids.indexOf("w" + w + "-0");
            final int second = ids.indexOf("w" + w + "-1");
            final int third = ids.indexOf("w" + w + "-2");
            assertTrue("writer " + w + " kept its order", first >= 0 && first < second && second < third);
        }
        assertTrue("the leader kept its order", ids.indexOf("leader-0") < ids.indexOf("leader-1"));
    }

    /**
     * A failed PUT fails every member of the group it carried, which is the only honest answer: those
     * operations are already in the engine, and the caller that is told the write failed must not find it
     * present. {@code ServerlessNode#appendOrRelease} turns each of these into a fenced shard.
     */
    public void testAFailedGroupFailsEveryMemberOfIt() throws Exception {
        final WalGroupCommitter committer = new WalGroupCommitter();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger flushes = new AtomicInteger();
        final WalGroupCommitter.Flush flush = (term, records) -> {
            if (flushes.getAndIncrement() == 0) {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return;
            }
            throw new IOException("the object store refused the group");
        };

        final Thread leader = new Thread(() -> {
            try {
                committer.commit(SHARD, 1L, List.of(record("leader")), flush);
            } catch (IOException e) {
                throw new AssertionError("the leader's own flush succeeded", e);
            }
        });
        leader.start();
        assertTrue(entered.await(30, java.util.concurrent.TimeUnit.SECONDS));

        final int others = 8;
        final List<Thread> threads = new ArrayList<>();
        final List<String> failures = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < others; i++) {
            final String id = "d" + i;
            final Thread thread = new Thread(() -> {
                try {
                    committer.commit(SHARD, 1L, List.of(record(id)), flush);
                    failures.add("NOT-FAILED-" + id);
                } catch (IOException e) {
                    failures.add(id);
                }
            });
            threads.add(thread);
            thread.start();
        }
        assertBusy(() -> {
            for (Thread thread : threads) {
                final Thread.State state = thread.getState();
                assertTrue(state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING);
            }
        });
        release.countDown();
        leader.join(30_000);
        for (Thread thread : threads) {
            thread.join(30_000);
        }

        assertEquals("every member of the failed group must see the failure", others, failures.size());
        for (String reported : failures) {
            assertFalse("no member may be told its write succeeded: " + reported, reported.startsWith("NOT-FAILED-"));
        }
    }

    /**
     * The off switch, and it is a value rather than a branch: a cap of one record per group is one PUT per
     * write, which is what this replaced.
     */
    public void testMaxRecordsOfOneRestoresOnePutPerWrite() throws Exception {
        final WalGroupCommitter committer = new WalGroupCommitter(1, WalGroupCommitter.DEFAULT_MAX_BYTES);
        final Recording flush = new Recording();
        for (int i = 0; i < 5; i++) {
            committer.commit(SHARD, 1L, List.of(record("d" + i)), flush);
        }
        assertEquals(5, flush.groups.size());
        for (List<WalRecord> group : flush.groups) {
            assertEquals("no group may carry more than the cap", 1, group.size());
        }
    }

    /**
     * A group is written at one term, so a term change starts a new one. In production the shard's fence
     * makes this unreachable — a release waits for every in-flight write — and it is enforced here anyway,
     * because a blob spanning two terms would be replayed under the wrong one.
     */
    public void testATermChangeStartsANewGroup() throws Exception {
        final WalGroupCommitter committer = new WalGroupCommitter();
        final List<Long> terms = new CopyOnWriteArrayList<>();
        final WalGroupCommitter.Flush flush = (term, records) -> terms.add(term);
        committer.commit(SHARD, 1L, List.of(record("a")), flush);
        committer.commit(SHARD, 2L, List.of(record("b")), flush);
        assertEquals(List.of(1L, 2L), terms);
    }

    /** A bulk whose items all belong to other shards leaves nothing for this one, and must not cost a PUT. */
    public void testAnEmptyBatchIsNotABlob() throws Exception {
        final WalGroupCommitter committer = new WalGroupCommitter();
        final Recording flush = new Recording();
        committer.commit(SHARD, 1L, List.of(), flush);
        assertEquals(0, flush.groups.size());
        assertEquals(0, committer.groups());
    }
}
