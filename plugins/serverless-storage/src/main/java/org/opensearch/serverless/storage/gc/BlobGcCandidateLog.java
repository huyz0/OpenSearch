/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * An append-only, fleet-wide log of {@link GcCandidate}s in object storage -- the discovery mechanism
 * {@link GcCandidateTailer} reads instead of relisting every warm shard's own container on a fixed clock.
 *
 * <h2>Deliberately the same shape as {@code BlobDescriptorChangeLog}, not a generalisation of it</h2>
 *
 * Time-bucketed, sharded-within-a-bucket appends, one object per entry: the identical layout that log
 * already uses, and for the identical reason -- see that class's own javadoc for why one object per entry
 * beats a single compare-and-swapped object (serialises every append behind one key), and why a bucket
 * cannot be one prefix (S3's documented per-prefix write ceiling). Built as a second, parallel class rather
 * than a generic parameter on the existing one: that class is depended on by {@code DescriptorChangeTailer}
 * and {@code DescriptorGate} today, both load-bearing, and widening its signature to carry an unrelated
 * payload type risks both for a saving -- one shared class instead of two similarly-shaped ones -- that
 * matters far less than not destabilising either.
 *
 * <h2>Where this genuinely differs, and why</h2>
 *
 * {@code BlobDescriptorChangeLog} has no total order and is read by advancing a cursor forward through
 * buckets, never revisiting one once passed -- correct for its own two consumers, which apply an entry once
 * and are done with it (a cache invalidation, a last-writer-wins name-index update). A GC candidate is not
 * like that: it may need to be <em>seen</em> now and <em>acted on</em> only once its retention window has
 * elapsed, possibly tens of minutes later, and a cursor that never revisits a bucket would let exactly that
 * candidate age out of reach before it became eligible.
 *
 * <p>So retirement here is not "advance past it" but "delete it": an entry's mere existence <em>is</em> the
 * fact that it is still pending, and nothing else needs to track that separately -- the same principle
 * {@link #pruneOlderThan} already uses for whole buckets ("the set of buckets is the set of things that
 * exist"), applied one level down to individual entries. A reader lists everything from a bounded lookback
 * window on every pass (bounded by the caller's own retention window, not by fleet size -- see {@link
 * GcCandidateTailer}), evaluates each entry it finds, and deletes the ones it resolves. One not yet eligible
 * is simply left for the next pass to find again, at the position it has always been at.
 */
public final class BlobGcCandidateLog {

    private static final Logger logger = LogManager.getLogger(BlobGcCandidateLog.class);

    /** Where the log lives, mirroring {@code BlobDescriptorChangeLog#CHANGELOG_PREFIX}'s own naming. */
    public static final String GC_CANDIDATES_PREFIX = "gc-candidates/";

    /** Same trade as {@code BlobDescriptorChangeLog#BUCKET_MILLIS}, for the identical reason. */
    static final long BUCKET_MILLIS = TimeUnit.MINUTES.toMillis(1);

    /** Same per-prefix write-throttling mitigation as {@code BlobDescriptorChangeLog#APPEND_SHARDS}. */
    static final int APPEND_SHARDS = 16;

    private final Function<BlobPath, BlobContainer> containers;
    private final BlobPath basePath;
    private final LongSupplier clock;

    /** See {@code BlobDescriptorChangeLog}'s own constructor javadoc for why a path, not a container. */
    public BlobGcCandidateLog(Function<BlobPath, BlobContainer> containers, BlobPath basePath) {
        this(containers, basePath, System::currentTimeMillis);
    }

    /** Test seam for the clock, so bucketing can be exercised without waiting a minute. */
    BlobGcCandidateLog(Function<BlobPath, BlobContainer> containers, BlobPath basePath, LongSupplier clock) {
        this.containers = containers;
        this.basePath = basePath;
        this.clock = clock;
    }

    /** Appends that failed, so a caller can tell a quiet feed from a broken one -- same purpose as the descriptor log's own. */
    private final java.util.concurrent.atomic.AtomicLong failedAppends = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Records that {@code candidate} was just superseded. Independent of every other appender; never
     * contends.
     *
     * <p>Named deterministically via {@link GcCandidate#entryName()} rather than a random suffix -- unlike a
     * descriptor change, which is genuinely a new fact every time, two appends of the same candidate really
     * are the same fact, and landing them on the same key makes a caller-level retry idempotent for free
     * rather than leaving a duplicate entry for the tailer to resolve twice.
     *
     * <p>A failure here is logged rather than thrown, for the same reason {@code BlobDescriptorChangeLog
     * #append} does not throw: this runs inline in {@code ObjectStoreCommitHeadPublisher#publishCommitAsHead},
     * on the hottest path in this system, right after a publish has already durably succeeded. Turning a
     * best-effort discovery hint into a publish failure would let this break the thing it is only meant to
     * help clean up after. A lost entry costs a delayed collection, recovered by {@code GcSchedulerTask}'s
     * own sweep, which keeps running underneath this unconditionally for exactly this reason.
     */
    public void append(GcCandidate candidate) {
        try {
            BytesReference bytes = encode(candidate);
            String name = candidate.entryName();
            shardContainer(bucketOf(clock.getAsLong()), shardOf(name)).writeBlob(name, bytes.streamInput(), bytes.length(), false);
        } catch (IOException | RuntimeException e) {
            failedAppends.incrementAndGet();
            logger.warn(
                "could not append a GC candidate for [{}/{} {}/{}]; it will be found by the periodic sweep instead: {}",
                candidate.indexUuid(),
                candidate.shardId(),
                candidate.primaryTerm(),
                candidate.generation(),
                e
            );
        }
    }

    /** How many appends have failed -- same purpose as {@code BlobDescriptorChangeLog#failedAppendCount}. */
    public long failedAppendCount() {
        return failedAppends.get();
    }

    /**
     * One candidate as it sits in the log, carrying enough to delete it again once resolved.
     */
    public record LoggedCandidate(String bucket, String shard, String key, GcCandidate candidate) {
    }

    /**
     * Every still-pending candidate in buckets at or after {@code fromBucket}.
     *
     * <p>Bounded by the caller's own lookback, not by fleet size: a candidate this old and still present is
     * either not yet past its retention window (the common case) or stuck for some other reason a human
     * should look at, but either way the number of <em>buckets</em> examined is fixed by how far back the
     * caller asks, never by how many shards exist or how many of them are currently warm.
     */
    public List<LoggedCandidate> entriesSince(String fromBucket) {
        List<LoggedCandidate> pending = new ArrayList<>();
        try {
            Map<String, BlobContainer> buckets = new TreeMap<>(containers.apply(gcCandidatesPath()).children());
            for (Map.Entry<String, BlobContainer> bucket : buckets.entrySet()) {
                if (fromBucket != null && bucket.getKey().compareTo(fromBucket) < 0) {
                    continue;
                }
                for (Map.Entry<String, BlobContainer> shard : new TreeMap<>(bucket.getValue().children()).entrySet()) {
                    readEntriesInto(pending, bucket.getKey(), shard.getKey(), shard.getValue());
                }
            }
        } catch (IOException e) {
            logger.warn("could not read the GC candidate log from bucket [{}]: {}", fromBucket, e);
        }
        return pending;
    }

    /**
     * Retires one candidate: it was either acted on (the manifest is gone) or found to no longer apply (it
     * is durably pinned). Either way, deleting the entry is what "resolved" means here -- see this class's
     * own javadoc for why deletion, not a cursor, is the retirement mechanism.
     *
     * <p>Ignores an already-absent entry rather than treating it as an error: two tailer passes racing (this
     * log is read, and safely can be read, by more than one node) resolving the same candidate is the
     * ordinary outcome of no single node owning this work, not a bug.
     */
    public void resolve(LoggedCandidate logged) {
        try {
            shardContainer(logged.bucket(), logged.shard()).deleteBlobsIgnoringIfNotExists(List.of(logged.key()));
        } catch (IOException | RuntimeException e) {
            logger.warn(
                "could not resolve GC candidate [{}/{}]; it will be re-evaluated on the next pass: {}",
                logged.bucket(),
                logged.key(),
                e
            );
        }
    }

    /** The bucket a fresh tailer pass should look back to, given it wants to see anything up to {@code lookbackMillis} old. */
    public String bucketAtOrBefore(long lookbackMillis) {
        return bucketOf(clock.getAsLong() - lookbackMillis);
    }

    /**
     * Deletes whole buckets older than {@code retentionMillis} -- the backstop against a candidate that,
     * for whatever reason, was never resolved by {@link GcCandidateTailer}: a permanently durably-pinned
     * generation, say, whose pin release does not currently re-trigger anything here (a follow-up, not this
     * delivery). {@code retentionMillis} should comfortably exceed the longest retention window this cluster
     * runs, so a healthy candidate never reaches this path; if this ever prunes something, that is worth
     * looking at, not just quietly true-up.
     *
     * <p>Same mechanics as {@code BlobDescriptorChangeLog#pruneOlderThan}: one clock read, whole-bucket
     * deletion, failures logged and retried next pass. See that method's own javadoc for the reasoning,
     * which applies here unchanged.
     */
    public int pruneOlderThan(long retentionMillis) {
        long nowMillis = clock.getAsLong();
        String cutoff = bucketOf(nowMillis - retentionMillis);
        int pruned = 0;
        try {
            BlobContainer root = containers.apply(gcCandidatesPath());
            for (Map.Entry<String, BlobContainer> bucket : new TreeMap<>(root.children()).entrySet()) {
                if (bucket.getKey().compareTo(cutoff) >= 0) {
                    continue;
                }
                try {
                    bucket.getValue().delete();
                    pruned++;
                } catch (IOException | RuntimeException e) {
                    logger.warn("could not prune GC candidate bucket [{}]; will retry on the next pass: {}", bucket.getKey(), e);
                }
            }
        } catch (IOException | RuntimeException e) {
            logger.warn("could not list GC candidate buckets to prune", e);
        }
        if (pruned > 0) {
            // Not a routine number: see this method's own javadoc for why a healthy cluster should never
            // reach this branch at all.
            logger.warn("pruned [{}] GC candidate buckets that were never resolved by the tailer -- worth investigating", pruned);
        }
        return pruned;
    }

    /** Same zero-padding reasoning as {@code BlobDescriptorChangeLog#bucketOf}. */
    static String bucketOf(long epochMillis) {
        return String.format(java.util.Locale.ROOT, "%019d", epochMillis / BUCKET_MILLIS);
    }

    /** Same per-prefix throttling mitigation as {@code BlobDescriptorChangeLog#shardOf}. */
    static String shardOf(String entryName) {
        return String.format(java.util.Locale.ROOT, "%02d", Math.floorMod(entryName.hashCode(), APPEND_SHARDS));
    }

    private void readEntriesInto(List<LoggedCandidate> pending, String bucket, String shard, BlobContainer container) throws IOException {
        for (String entry : container.listBlobs().keySet()) {
            try (InputStream stream = container.readBlob(entry); StreamInput in = StreamInput.wrap(stream.readAllBytes())) {
                pending.add(new LoggedCandidate(bucket, shard, entry, new GcCandidate(in)));
            } catch (NoSuchFileException e) {
                // Resolved by another node's tailer pass between the listing above and this read -- the
                // ordinary outcome of more than one node safely reading this log, not a problem.
            } catch (IOException e) {
                // One unreadable entry must not lose the rest of the pass -- same tolerance
                // BlobDescriptorChangeLog#readEntriesInto already has for its own entries.
                logger.debug("skipping an unreadable GC candidate entry [{}/{}/{}]: {}", bucket, shard, entry, e);
            }
        }
    }

    private BlobPath gcCandidatesPath() {
        return basePath.add(GC_CANDIDATES_PREFIX.substring(0, GC_CANDIDATES_PREFIX.length() - 1));
    }

    private BlobContainer shardContainer(String bucket, String shard) {
        return containers.apply(gcCandidatesPath().add(bucket).add(shard));
    }

    private static BytesReference encode(GcCandidate candidate) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            candidate.writeTo(out);
            return out.bytes();
        }
    }
}
