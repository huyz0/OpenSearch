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
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * An append-only log of descriptor changes in object storage, for the two readers that cannot be served by
 * the descriptor store itself.
 *
 * <h2>Why it exists twice over</h2>
 *
 * The name index needs a feed. It listens to cluster state today, which is the one place a gated index
 * never appears, so it is blind to exactly the indices it exists to resolve.
 *
 * <p>And the descriptor cache needs invalidation. T3 found its window is one second, which is free against
 * a local index and is a round trip per active tenant per second per node against an object store. The fix
 * is a long window with explicit invalidation rather than a shorter window, and this is what drives it.
 * Either reason alone would justify the log; together they move it ahead of switching the blob backend on
 * rather than after.
 *
 * <h2>One object per entry, and why anything else fails</h2>
 *
 * The obvious design is a single object appended under compare-and-swap. That serialises every descriptor
 * write in the cluster behind one key, at roughly one write per round trip, which is worse than the
 * cluster manager this whole design exists to remove from the creation path. So every entry is its own
 * object, written independently, and nothing coordinates.
 *
 * <p>Keys are {@code changelog/<bucket>/<random>}. The bucket is a coarse time slice so a reader can skip
 * the past without listing everything; the random suffix is what lets concurrent appenders avoid each
 * other without agreeing on a sequence number.
 *
 * <h2>There is no total order, and callers must not want one</h2>
 *
 * This is the cost of the above, and it is worth stating plainly because cluster state did provide a total
 * order and things were built on it. Two changes in the same bucket have no defined order, wall clocks
 * across nodes disagree, and a slow appender can land an entry after a later one. So:
 *
 * <ul>
 *   <li>Nothing may use a log position as a version cursor.</li>
 *   <li>A consumer must be idempotent and order-insensitive within a bucket. Both real consumers are: the
 *       cache just drops an entry, and the name index applies name plus uuid plus liveness, which is
 *       last-writer-wins on a key rather than a sequence.</li>
 *   <li>A reader that needs authoritative state reads the descriptor store, which is what it is for. The
 *       log says something changed, not what is currently true.</li>
 * </ul>
 */
public final class BlobDescriptorChangeLog {

    private static final Logger logger = LogManager.getLogger(BlobDescriptorChangeLog.class);

    /** Where the log lives, beside descriptors and tombstones rather than mixed in with either. */
    public static final String CHANGELOG_PREFIX = "changelog/";

    /**
     * How much time one bucket covers.
     *
     * <p>A minute, which is a trade between two costs rather than a tuned value. Smaller buckets mean more
     * prefixes to list when catching up; larger ones mean a reader that wants the last few seconds has to
     * page through everything since the bucket started. A minute keeps a bucket to whatever one cluster
     * writes in a minute, which at the creation rates this design targets is large but listable.
     */
    static final long BUCKET_MILLIS = TimeUnit.MINUTES.toMillis(1);

    /**
     * How many prefixes one bucket's appends are spread over.
     *
     * <p><b>Why a bucket cannot be one prefix.</b> Appends never contend with each other -- each writes its
     * own blob under its own name -- so this was written as though the append rate were unbounded. It is
     * not: an object store rate-limits by key prefix, and S3's documented figure is 3,500 writes per second
     * per prefix. Every append in a minute landing under one bucket makes that bucket the ceiling on how
     * fast the whole cluster can create indices, at about a quarter of the rate 100M indices in two hours
     * needs -- and it is the worst shape for the mitigation S3 does have, which is to partition a prefix
     * that stays hot, because a new bucket every minute is a cold prefix every minute.
     *
     * <p>Sixteen puts the ceiling at about 56,000 appends per second, which is four times that target. It
     * is not a tuned number and does not need to be: the cost of raising it is paid by readers, in one
     * extra listing per shard that actually has entries, and the cost of it being too low is a ceiling
     * nobody can see until they hit it.
     *
     * <p><b>Readers do not know this number and must not.</b> They discover shards by listing the bucket's
     * children, so this can change in either direction without a migration and without a mixed-version
     * cluster disagreeing about where to look.
     */
    static final int APPEND_SHARDS = 16;

    private final Function<BlobPath, BlobContainer> containers;
    private final BlobPath basePath;
    private final LongSupplier clock;

    /**
     * Takes a store and a base path rather than a container, because a bucket is a container of its own.
     *
     * <p>Nesting through {@link BlobPath} is the contract every implementation honours. Putting a path
     * separator inside a blob name is not: it works on S3, where keys are flat strings, and fails on a
     * filesystem repository, where nothing creates the intermediate directory. That divergence has already
     * bitten this branch three times in the other direction, and building a fourth instance of it into a
     * new component would have been careless.
     */
    public BlobDescriptorChangeLog(Function<BlobPath, BlobContainer> containers, BlobPath basePath) {
        this(containers, basePath, System::currentTimeMillis);
    }

    /** Test seam for the clock, so bucketing can be exercised without waiting a minute. */
    BlobDescriptorChangeLog(Function<BlobPath, BlobContainer> containers, BlobPath basePath, LongSupplier clock) {
        this.containers = containers;
        this.basePath = basePath;
        this.clock = clock;
    }

    /** Appends that failed, so a caller can tell a quiet feed from a broken one. */
    private final java.util.concurrent.atomic.AtomicLong failedAppends = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Appends one change. Independent of every other appender, so this never contends.
     *
     * <p>A failure here is logged rather than thrown. The log is a derived feed: losing an entry costs a
     * stale cache entry until its window expires and a name index that needs a rebuild to notice, and
     * neither is worth failing the descriptor write that already succeeded. Turning a log append into a
     * creation failure would make the derived thing able to break the authoritative one.
     */
    public void append(DescriptorChange change) {
        try {
            BytesReference bytes = encode(change);
            // The name decides the shard, so the spread is the UUID's and there is no second source of
            // randomness to reason about. Entry names stay unique across shards within a bucket, which is
            // what a tailer's already-consumed set is keyed on.
            String name = UUIDs.randomBase64UUID();
            shardContainer(bucketOf(clock.getAsLong()), shardOf(name)).writeBlob(name, bytes.streamInput(), bytes.length(), true);
        } catch (IOException | RuntimeException e) {
            failedAppends.incrementAndGet();
            logger.warn("could not append a change log entry for [{}]; the feed will need a rebuild to notice: {}", change.name(), e);
        }
    }

    /**
     * How many appends have failed.
     *
     * <p>Exposed because swallowing the failure is right and invisible failure is not. A log that silently
     * appends nothing reads exactly like a log with nothing to say, and this counter is the difference.
     * Writing this component without it cost one debugging cycle: four tests read back empty and the
     * reason had already been logged and discarded.
     */
    public long failedAppendCount() {
        return failedAppends.get();
    }

    /**
     * Every change in the buckets at or after {@code fromBucket}, oldest bucket first.
     *
     * <p>Bucket ordering is real and entry ordering within a bucket is not, which is why the return is a
     * flat list rather than anything that looks like a sequence. A consumer that treats the index of an
     * entry as meaningful is relying on something this cannot provide.
     */
    public List<DescriptorChange> since(String fromBucket) {
        List<LoggedChange> entries = readSince(fromBucket, java.util.Set.of()).entries();
        List<DescriptorChange> changes = new ArrayList<>(entries.size());
        for (LoggedChange entry : entries) {
            changes.add(entry.change());
        }
        return changes;
    }

    /**
     * Deletes whole buckets older than {@code retentionMillis}, and reports how many went.
     *
     * <p><b>The path structure is the worklist, which is what makes this leak-proof.</b> There is no index
     * of what to delete and nothing to keep in step: the set of buckets is the set of things that exist, so
     * a bucket cannot become invisible to this the way an entry can become invisible to a secondary index.
     * Whatever was written is either inside a bucket this will eventually reach, or inside one too young to
     * reach yet.
     *
     * <p>Safe to run because nothing reads history any more. The tailer starts at the bucket its node
     * started in and never falls further behind than the cursor lag it holds itself back by, which is less
     * than one bucket, so a bucket older than the retention window has no reader by construction as long as
     * the window is more than a couple of buckets wide -- which the smallest sane retention already is.
     * That was not true while the log fed a name index built by replaying it from the beginning, and
     * pruning then would have silently broken a joining node.
     *
     * <p>The bucket being written to is never deleted, and that falls out of the arithmetic rather than
     * needing a guard: the cutoff is {@code bucketOf(now - retention)} and the live bucket is
     * {@code bucketOf(now)}, so for any non-negative window the cutoff is at or before it and the
     * at-or-after test below already spares it. A zero window prunes nothing at all for the same reason.
     *
     * <p>The clock is read once for that reason. Deriving the cutoff and the live bucket from two separate
     * reads would let a backwards clock jump between them put the live bucket before the cutoff, and the
     * bucket every tailer's cursor names would be deleted underneath it.
     *
     * <p>Failures are logged and swallowed per bucket rather than aborting the pass. A bucket that resists
     * deletion is retried on the next pass, and one that half-deletes leaves entries the tailer will not
     * read anyway because the whole bucket is behind every cursor.
     */
    public int pruneOlderThan(long retentionMillis) {
        long nowMillis = clock.getAsLong();
        String cutoff = bucketOf(nowMillis - retentionMillis);
        int pruned = 0;
        try {
            BlobContainer root = containers.apply(changelogPath());
            for (Map.Entry<String, BlobContainer> bucket : new TreeMap<>(root.children()).entrySet()) {
                if (bucket.getKey().compareTo(cutoff) >= 0) {
                    continue;
                }
                try {
                    bucket.getValue().delete();
                    pruned++;
                } catch (IOException | RuntimeException e) {
                    logger.warn("could not prune change log bucket [{}]; will retry on the next pass: {}", bucket.getKey(), e);
                }
            }
        } catch (IOException | RuntimeException e) {
            logger.warn("could not list change log buckets to prune", e);
        }
        if (pruned > 0) {
            logger.debug("pruned [{}] change log buckets older than [{}]", pruned, cutoff);
        }
        return pruned;
    }

    /**
     * One entry as it sits in the log, so a reader can remember what it has already consumed.
     *
     * <p>The key is needed because entry names are random. A reader resuming inside a bucket cannot say
     * "everything after key K" the way it could with an ordered name, so it has to name the individual
     * entries it has seen.
     */
    public record LoggedChange(String bucket, String key, DescriptorChange change) {
    }

    /**
     * What one read of the log returned, and whether it is all of it.
     *
     * <p><b>The second field is the whole point of this type.</b> The read used to catch its own
     * {@link IOException} mid-iteration and return whatever it had reached so far, which is
     * indistinguishable from a complete read of a shorter log. Its one real caller then advanced its cursor
     * past the buckets it had never managed to list, permanently -- exactly what {@code
     * DescriptorChangeTailer}'s "left un-advanced on purpose, so the next pass retries the same range
     * rather than stepping over changes it never read" comment claimed could not happen. A partial success
     * that looks like a success is worse here than a failure, because the entries lost are cache
     * invalidations and shard releases for indices cluster state does not mention, and nothing else will
     * ever mention them either.
     *
     * @param entries          what was read, oldest bucket first
     * @param firstUnreadBucket the earliest bucket this read could not finish, or null when it finished all
     *                          of them. A reader must not move its cursor past this.
     */
    public record ChangeBatch(List<LoggedChange> entries, String firstUnreadBucket) {
        /** Whether everything from the requested bucket onward was actually read. */
        public boolean complete() {
            return firstUnreadBucket == null;
        }
    }

    /**
     * Everything from {@code fromBucket} onward except the entries in {@code alreadyConsumed}, and a note of
     * whatever could not be read.
     *
     * <p>{@code alreadyConsumed} holds keys from any bucket at or after {@code fromBucket}, which is the
     * range a reader can revisit: the cursor is set at or behind the bucket the read started in rather than
     * past it, so entries written after the listing are not missed. Without the exclusion those entries are
     * delivered again on every pass until the cursor moves beyond their bucket, which at a one minute bucket
     * and a five second interval is up to twelve times or more, and each redelivery invalidates a cache
     * entry that did not need invalidating.
     *
     * <p>It used to hold keys from {@code fromBucket} alone, because the cursor could only ever sit in the
     * bucket the last read started in. It can now sit one bucket behind that, to absorb clock skew between
     * an appender bucketing by its own clock and a reader bucketing by its own, so the exclusion set has to
     * span the same range the read does. Entry names are UUIDs, so a key identifies an entry without its
     * bucket.
     */
    public ChangeBatch readSince(String fromBucket, java.util.Set<String> alreadyConsumed) {
        List<LoggedChange> changes = new ArrayList<>();
        String firstUnreadBucket = null;
        try {
            // Sorted, because children() gives no order and the buckets are the only ordering there is.
            Map<String, BlobContainer> buckets = new TreeMap<>(containers.apply(changelogPath()).children());
            for (Map.Entry<String, BlobContainer> bucket : buckets.entrySet()) {
                if (fromBucket != null && bucket.getKey().compareTo(fromBucket) < 0) {
                    continue;
                }
                boolean complete;
                try {
                    // Directly under the bucket first, which is where appends landed before they were
                    // sharded. Costs one listing that is empty on anything written since, and means a log
                    // written by an older node is not silently skipped by a newer one.
                    complete = readEntriesInto(changes, bucket.getKey(), bucket.getValue(), alreadyConsumed);
                    // Then each shard that exists. Discovered rather than enumerated, so a reader never
                    // needs to know APPEND_SHARDS, an empty shard costs nothing, and changing the count is
                    // not a migration.
                    //
                    // Non-short-circuiting on purpose. Every shard is read whatever the ones before it did,
                    // because an entry that could not be read says nothing about the entries beside it.
                    for (BlobContainer shard : new TreeMap<>(bucket.getValue().children()).values()) {
                        complete &= readEntriesInto(changes, bucket.getKey(), shard, alreadyConsumed);
                    }
                } catch (IOException | RuntimeException e) {
                    // A *listing* failure, which is different from an unreadable entry: nothing is known
                    // about what this bucket holds, so there is nothing to deliver and the whole bucket has
                    // to be retried. Per bucket rather than abandoning the pass, so a later bucket that is
                    // readable is still delivered.
                    logger.warn("could not list change log bucket [{}]; it will be retried: {}", bucket.getKey(), e);
                    complete = false;
                }
                if (complete == false && (firstUnreadBucket == null || bucket.getKey().compareTo(firstUnreadBucket) < 0)) {
                    // What must not happen is the caller treating this as read: the earliest incomplete
                    // bucket is reported so the cursor stays at or before it.
                    firstUnreadBucket = bucket.getKey();
                }
            }
        } catch (IOException | RuntimeException e) {
            // The bucket listing itself failed, so nothing at all is known about this range. Reporting
            // fromBucket as unread is what keeps the cursor where it is.
            logger.warn("could not list the change log from bucket [{}]: {}", fromBucket, e);
            return new ChangeBatch(List.copyOf(changes), fromBucket == null ? bucketOf(0L) : fromBucket);
        }
        return new ChangeBatch(List.copyOf(changes), firstUnreadBucket);
    }

    /** The bucket a reader should resume from next time, given it has consumed everything up to now. */
    public String currentBucket() {
        return bucketOf(clock.getAsLong());
    }

    /**
     * The bucket that was current {@code lagMillis} ago, which is where a cursor belongs rather than at
     * {@link #currentBucket}.
     *
     * <p>Appenders bucket by their own wall clock and a reader bucketing by its own; the two are only as
     * close as NTP keeps them. A reader whose clock is a few seconds ahead, crossing a bucket boundary,
     * sets its cursor to bucket N while an appender still writing entries by the same instant puts them in
     * N-1 -- and the cursor is already past N-1, so those entries are never read by that node. Not
     * eventually: never. Holding the cursor back by more than the skew makes that impossible for skew up to
     * the lag, at the cost of re-listing one extra bucket per pass, whose entries are filtered out by the
     * already-consumed set anyway.
     */
    public String bucketAtOrBefore(long lagMillis) {
        return bucketOf(clock.getAsLong() - lagMillis);
    }

    /**
     * Zero-padded so lexicographic order is chronological order.
     *
     * <p>Not cosmetic. Buckets are compared as strings, by both the {@link TreeMap} above and by any
     * prefix listing over the store, so an unpadded bucket 9 would sort after bucket 10 and a reader
     * resuming from it would silently skip everything in between.
     */
    static String bucketOf(long epochMillis) {
        return String.format(java.util.Locale.ROOT, "%019d", epochMillis / BUCKET_MILLIS);
    }

    private BlobPath changelogPath() {
        return basePath.add(CHANGELOG_PREFIX.substring(0, CHANGELOG_PREFIX.length() - 1));
    }

    /**
     * Reads every entry in one container into {@code changes}, skipping what the reader already had.
     *
     * <p>Entries are identified by name alone rather than by shard and name, because the names are UUIDs
     * and a tailer's cursor was keyed that way before sharding existed. Qualifying them now would make
     * every cursor written by a running node stop matching.
     *
     * <p><b>A listing failure propagates and an unreadable entry does not, and that asymmetry is the whole
     * of this method's contract.</b> A bucket that could not be listed holds an unknown set of entries, so
     * there is nothing to deliver from it. An entry that could not be read is one entry: the ones beside it
     * were listed, are readable, and must still be delivered.
     *
     * <p>Getting that backwards cost a run of green tests. Throwing on an unreadable entry aborted the
     * caller's loop over the bucket's shards, so a single unparseable object under a bucket hid every real
     * entry in it -- and Lucene's {@code ExtrasFS}, which drops an empty {@code extra0} file into every
     * directory it creates on about one seed in four, produces exactly that object. Three suites read back
     * empty while reporting that no append had failed, which is indistinguishable from a cluster where
     * nothing happened.
     *
     * <p>The entry is still not <em>forgotten</em>: this reports the bucket as incomplete, which is what
     * stops a reader's cursor stepping over an entry it never read. An entry can be listed between its key
     * appearing and its body being complete on a store that does not make writes atomic, and that resolves
     * on the next pass. One that never resolves is bounded by the reader rather than here -- see
     * {@code DescriptorChangeTailer.MAX_PASSES_HELD_BY_ONE_BUCKET}.
     *
     * @return whether every listed entry was read, so the caller can tell a complete bucket from a partial
     *         one without losing what the partial one did yield
     */
    private boolean readEntriesInto(
        List<LoggedChange> changes,
        String bucket,
        BlobContainer container,
        java.util.Set<String> alreadyConsumed
    ) throws IOException {
        boolean complete = true;
        for (String entry : container.listBlobs().keySet()) {
            if (alreadyConsumed.contains(entry)) {
                continue;
            }
            try (InputStream stream = container.readBlob(entry); StreamInput in = StreamInput.wrap(stream.readAllBytes())) {
                changes.add(new LoggedChange(bucket, entry, new DescriptorChange(in)));
            } catch (IOException e) {
                // The loop continues, so one unreadable entry costs that entry rather than the rest of the
                // catch-up. Reported rather than swallowed, so the cursor holds.
                logger.debug("could not read change log entry [{}/{}] yet: {}", bucket, entry, e);
                complete = false;
            }
        }
        return complete;
    }

    /** Which of a bucket's prefixes an entry belongs in, derived from its own name. */
    static String shardOf(String entryName) {
        return String.format(java.util.Locale.ROOT, "%02d", Math.floorMod(entryName.hashCode(), APPEND_SHARDS));
    }

    private BlobContainer shardContainer(String bucket, String shard) {
        return containers.apply(changelogPath().add(bucket).add(shard));
    }

    private BlobContainer bucketContainer(String bucket) {
        return containers.apply(changelogPath().add(bucket));
    }

    private static BytesReference encode(DescriptorChange change) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            change.writeTo(out);
            return out.bytes();
        }
    }

}
