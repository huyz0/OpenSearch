/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * How a node learns about a descriptor another node wrote.
 *
 * <h2>The gap this closes</h2>
 *
 * A gated index has no cluster state entry, which is the point, and it means none of the machinery that
 * normally tells every node about an index applies. {@code DescriptorGate} appends each write to
 * {@link BlobDescriptorChangeLog} and applies it to the writing node's own name index, and its own comment
 * said what was missing: "the tailer that consumes it on those nodes is the piece still missing: today a
 * remote node learns of the change only through a rebuild."
 *
 * <p>So the log had a writer and no reader. Until this, the only bound on how stale another node's view
 * could be was the descriptor cache's freshness window, and the name index on a remote node simply never
 * heard about the change at all.
 *
 * <h2>What it does with a change</h2>
 *
 * Two things, and they are separate because they fail differently. The name index is told, so wildcards on
 * this node reflect indices created elsewhere. And the descriptor cache is invalidated for the name, so the
 * next point read goes to the store instead of answering from a copy taken before the change.
 *
 * <p>Both are idempotent, which is what lets this be a poll rather than a subscription. Re-applying a change
 * already seen costs an invalidation of something already invalid.
 *
 * <h2>Why it re-reads the bucket it stopped in, and one before that</h2>
 *
 * The log's buckets are ordered and entries within a bucket are not, so there is no cursor finer than a
 * bucket. Resuming from the bucket last consumed re-reads entries already applied, which idempotency makes
 * harmless, and resuming from the <em>next</em> one would skip anything written to that bucket after the
 * read. Given the choice between repeating work and losing a change, this repeats.
 *
 * <p>The cursor is additionally held a fixed lag behind the current bucket, because the bucket an entry
 * lands in is decided by the appender's wall clock and the bucket this cursor names is decided by this
 * node's. See {@link #CURSOR_LAG_MILLIS}. Re-reading is bounded by the already-consumed set; not reading is
 * not bounded by anything.
 *
 * <h2>What it is not</h2>
 *
 * Not a durability mechanism and not a source of truth. A change log entry that never lands, or a tailer
 * that never runs, costs freshness: the descriptor cache still expires on its own window, and D2's sweep
 * still re-derives which gated indices are gone. That is what makes it safe for this to be best effort
 * and to swallow its own failures.
 */
public final class DescriptorChangeTailer {

    private static final Logger logger = LogManager.getLogger(DescriptorChangeTailer.class);

    private final BlobDescriptorChangeLog changeLog;
    private final DescriptorBackend backend;

    /**
     * The bucket to resume from, set at construction to the bucket current at start-up less the cursor lag.
     *
     * <p>This used to start at null, so the first pass read the whole log back to the beginning of time.
     * That was right when the tailer fed a per-node name index, which had to be built from the history
     * before it could answer anything. It has not been right since that index was removed, and it was
     * unbounded: the log is never pruned, so a node joining a year-old cluster read a year of changes.
     *
     * <p>What the tailer does now is invalidate cache entries and release shards of deleted gated indices.
     * A starting node has an empty descriptor cache and an empty {@code openedOnDemand} set, both being
     * plain in-memory structures, so there is nothing for any of that history to act on. Every entry it
     * used to read was applied to nothing.
     */
    private final AtomicReference<String> resumeFrom;

    /**
     * Keys already consumed, per bucket the cursor can still revisit, so a revisit does not redeliver them.
     *
     * <p>Bounded by what the cluster writes in the buckets at or after the cursor, and pruned to exactly
     * that on every pass: a bucket the cursor has passed is never read again, so remembering its keys would
     * grow without bound for no benefit. Entry names are random, so there is no "everything after key K" to
     * resume from and the individual keys have to be named.
     *
     * <p>Keyed by bucket rather than a flat set, which it was while the cursor could only sit in the one
     * bucket the last pass started in. The cursor now lags deliberately (see {@link #CURSOR_LAG_MILLIS}),
     * so it spans two, and a flat set could not be pruned without knowing which bucket each key came from.
     * Dropping keys that are still in range is not a leak but a redelivery, and a redelivery invalidates a
     * cache entry that did not need invalidating -- the cost this exclusion exists to avoid.
     */
    private final AtomicReference<Map<String, Set<String>>> consumed = new AtomicReference<>(Map.of());

    /**
     * How far behind the current bucket the cursor is held, to absorb clock skew between nodes.
     *
     * <p>An appender buckets by its own wall clock and this reader buckets by its own. With no lag, a
     * reader whose clock is a few seconds ahead across a bucket boundary sets its cursor to bucket N while
     * an appender at the same instant is still writing into N-1 -- which the cursor has already passed. That
     * entry is not read late, it is never read at all, and for a gated index there is no second mechanism
     * that would mention it. Thirty seconds is less than one bucket, so it costs one extra bucket listing
     * per pass whose entries the exclusion set filters out, and it is comfortably more than the skew a
     * cluster with working time sync has.
     */
    static final long CURSOR_LAG_MILLIS = java.util.concurrent.TimeUnit.SECONDS.toMillis(30);

    /**
     * How many consecutive passes one bucket may hold the cursor before it is stepped over.
     *
     * <p>Holding the cursor at a bucket that could not be fully read is the point of reporting
     * incompleteness at all, and it is right for the failure that motivates it: an entry listed before its
     * body is complete, or a bucket the store briefly could not serve, both resolve on the next pass.
     *
     * <p>An entry that <em>never</em> becomes readable is the other case, and holding forever for it is a
     * failure of its own rather than caution. Nothing is lost while the cursor is held -- later buckets are
     * still read and applied -- but the cost grows without bound: every pass re-lists from that bucket, and
     * the already-consumed set keeps every key at or after it rather than one bucket's worth. It only ends
     * when the retention window prunes the bucket, which is hours.
     *
     * <p>So the hold is bounded, and the bound is the only signal available for telling the two apart:
     * whether it resolves. Twelve passes is a minute at the default five second interval, which is many
     * times over what a partial write needs and far short of a retention window. Stepping over is logged at
     * error and counted, because it is the one case here where a change really is lost to this node -- the
     * descriptor cache's own freshness window is what remains behind it.
     */
    static final int MAX_PASSES_HELD_BY_ONE_BUCKET = 12;

    /** The bucket currently holding the cursor back, and for how many consecutive passes. */
    private final AtomicReference<String> holdingBucket = new AtomicReference<>();
    private final java.util.concurrent.atomic.AtomicInteger passesHeld = new java.util.concurrent.atomic.AtomicInteger();

    private final AtomicLong appliedCount = new AtomicLong();
    private final AtomicLong passCount = new AtomicLong();
    private final AtomicLong incompleteReads = new AtomicLong();
    private final AtomicLong steppedOverBuckets = new AtomicLong();

    public DescriptorChangeTailer(BlobDescriptorChangeLog changeLog, DescriptorBackend backend) {
        this.changeLog = changeLog;
        this.backend = backend;
        // Captured now rather than on the first pass, so a change written between start-up and that pass is
        // still seen: the cursor names the bucket it starts in and entries are filtered by key, not skipped
        // wholesale.
        this.resumeFrom = new AtomicReference<>(changeLog.bucketAtOrBefore(CURSOR_LAG_MILLIS));
    }

    /**
     * Reads everything since the last pass and applies it locally.
     *
     * <p>Blocking, because reading a change log is object-store I/O. The caller decides where that runs, and
     * it must not be a transport or cluster state thread.
     *
     * @return how many changes were applied on this pass
     */
    public int tailOnce() {
        passCount.incrementAndGet();
        String from = resumeFrom.get();
        // Captured before the read, not after. A bucket that rolls over mid-read would otherwise have its
        // early entries skipped next time: the reader would resume from the new bucket having never seen
        // what was written to the old one after the listing. Held back by the cursor lag for the separate
        // reason CURSOR_LAG_MILLIS gives.
        String nextResume = changeLog.bucketAtOrBefore(CURSOR_LAG_MILLIS);

        BlobDescriptorChangeLog.ChangeBatch batch;
        try {
            batch = changeLog.readSince(from, consumedKeys());
        } catch (RuntimeException e) {
            // Left un-advanced on purpose, so the next pass retries the same range rather than stepping
            // over changes it never read.
            logger.warn("could not read the descriptor change log from bucket [{}]; will retry: {}", from, e);
            return 0;
        }

        // The cursor never moves past a bucket the read could not finish, which is what the claim above
        // asserts and what was not actually true: the read used to swallow an IOException mid-iteration and
        // return a short list, and this then advanced over the buckets it had never listed. Permanently --
        // they are behind the cursor from then on, and for a gated index nothing else ever mentions the
        // changes in them.
        if (batch.complete() == false) {
            incompleteReads.incrementAndGet();
            String unread = batch.firstUnreadBucket();
            int held = unread.equals(holdingBucket.get()) ? passesHeld.incrementAndGet() : startHolding(unread);
            if (held <= MAX_PASSES_HELD_BY_ONE_BUCKET) {
                if (unread.compareTo(nextResume) < 0) {
                    nextResume = unread;
                }
                logger.warn(
                    "the descriptor change log was only partly readable from bucket [{}]; holding the cursor at [{}] (pass {} of {})",
                    from,
                    nextResume,
                    held,
                    MAX_PASSES_HELD_BY_ONE_BUCKET
                );
            } else {
                // See MAX_PASSES_HELD_BY_ONE_BUCKET. Loud, because this is the one place a change is
                // genuinely lost to this node rather than merely late.
                steppedOverBuckets.incrementAndGet();
                logger.error(
                    "change log bucket [{}] has been unreadable for {} consecutive passes; stepping over it. Whatever it "
                        + "holds will not be applied on this node, so a descriptor changed elsewhere stays cached here until "
                        + "its freshness window expires",
                    unread,
                    held
                );
                stopHolding();
            }
        } else {
            stopHolding();
        }

        List<BlobDescriptorChangeLog.LoggedChange> entries = batch.entries();
        if (entries.isEmpty()) {
            advanceTo(nextResume, entries);
            return 0;
        }

        List<DescriptorChange> changes = new ArrayList<>(entries.size());
        for (BlobDescriptorChangeLog.LoggedChange entry : entries) {
            changes.add(entry.change());
        }

        // Invalidate every name including deletes: a
        // cached live descriptor for a name deleted elsewhere is the dangerous direction, because a write
        // routed on it lands against a shard the cluster no longer believes in.
        Set<String> touched = new LinkedHashSet<>();
        for (DescriptorChange change : changes) {
            touched.add(change.name());
        }
        for (String name : touched) {
            try {
                backend.invalidate(name);
            } catch (RuntimeException e) {
                logger.debug("could not invalidate the cached descriptor for [{}]: {}", name, e);
            }
        }

        // A delete and a close are the changes with a shard behind them. Nothing else tells a node that a
        // gated index no longer has one, because neither produces a cluster state diff, so without this the
        // shard stays open until D2's sweep re-derives the same conclusion on its next tick. The sweep stays
        // as the backstop: a push arrives once and can be missed, and re-deriving from scratch converges
        // regardless.
        //
        // Asked as releasesShard() rather than as !live(), because a closed index is still live -- its name
        // resolves, it simply has no shard. Conflating the two is what let a closed gated index go on
        // serving writes with its writer lease renewing underneath.
        //
        // By uuid as well as name, because a name alone cannot tell a deleted index from a new one created
        // with the same name moments later, and closing the wrong one would take down a live shard.
        for (DescriptorChange change : changes) {
            if (change.releasesShard()) {
                org.opensearch.cluster.metadata.GatedIndexRelease.release(
                    new org.opensearch.core.index.Index(change.name(), change.uuid())
                );
            }
        }

        // Counted as changes consumed rather than name-index entries written. This used to return what
        // NameIndexService.apply reported, which meant a tailer that invalidated caches and released
        // shards correctly still reported zero once the name index was gone.
        int applied = changes.size();

        advanceTo(nextResume, entries);
        appliedCount.addAndGet(applied);
        logger.debug("tailed [{}] descriptor changes over [{}] names from bucket [{}]", changes.size(), touched.size(), from);
        return applied;
    }

    /** Starts holding at a bucket, reporting that this is the first pass it has held. */
    private int startHolding(String bucket) {
        holdingBucket.set(bucket);
        passesHeld.set(1);
        return 1;
    }

    private void stopHolding() {
        holdingBucket.set(null);
        passesHeld.set(0);
    }

    /** Every key the next read must skip, flattened because entry names are unique across buckets. */
    private Set<String> consumedKeys() {
        Set<String> keys = new HashSet<>();
        for (Set<String> perBucket : consumed.get().values()) {
            keys.addAll(perBucket);
        }
        return keys;
    }

    /**
     * Moves the cursor, carrying forward the consumed keys of every bucket that can still be revisited.
     *
     * <p>Only those buckets' keys are kept. Anything older is behind the cursor and will never be listed
     * again, so remembering it would grow without bound for no benefit.
     *
     * <p>The previous set is carried unconditionally rather than only when the cursor stands still, which
     * is what it used to do. That was correct while everything at or after the cursor was re-read on every
     * pass and therefore re-derivable from {@code justRead}; it stops being correct as soon as a key can be
     * excluded from the read that would have re-derived it, which is the case for any bucket the cursor
     * revisits more than once. Dropping it would redeliver the entry, and a redelivery invalidates a live
     * cache entry for nothing.
     */
    private void advanceTo(String nextResume, List<BlobDescriptorChangeLog.LoggedChange> justRead) {
        Map<String, Set<String>> carried = new HashMap<>();
        for (Map.Entry<String, Set<String>> bucket : consumed.get().entrySet()) {
            if (nextResume == null || bucket.getKey().compareTo(nextResume) >= 0) {
                carried.put(bucket.getKey(), new HashSet<>(bucket.getValue()));
            }
        }
        for (BlobDescriptorChangeLog.LoggedChange entry : justRead) {
            if (nextResume == null || entry.bucket().compareTo(nextResume) >= 0) {
                carried.computeIfAbsent(entry.bucket(), ignored -> new HashSet<>()).add(entry.key());
            }
        }
        resumeFrom.set(nextResume);
        consumed.set(Map.copyOf(carried));
    }

    /** How many changes this tailer has applied, which a test asserts rather than infers from timing. */
    public long appliedCount() {
        return appliedCount.get();
    }

    /** How many passes have run, so a test can tell "did nothing" from "never ran". */
    public long passCount() {
        return passCount.get();
    }

    /**
     * How many passes read only part of the log, which is the difference between a quiet feed and a broken
     * one.
     *
     * <p>Exposed for the same reason {@code BlobDescriptorChangeLog.failedAppendCount} is: the failure is
     * survivable, so it is logged and continued from, and a survivable failure with no counter behind it is
     * invisible. A cursor that stops advancing because a bucket cannot be listed looks exactly like a
     * cluster where nothing is happening.
     */
    public long incompleteReadCount() {
        return incompleteReads.get();
    }

    /**
     * How many buckets this tailer gave up on and stepped over.
     *
     * <p>The number that should be zero. Unlike {@link #incompleteReadCount}, which counts a delay, this
     * counts a change this node will never see: whatever was in that bucket is not applied here, and the
     * descriptor cache's freshness window is all that remains behind it.
     */
    public long steppedOverBucketCount() {
        return steppedOverBuckets.get();
    }

    /** The bucket the next pass will resume from, exposed so a test can assert the cursor advances. */
    public String resumeFrom() {
        return resumeFrom.get();
    }
}
