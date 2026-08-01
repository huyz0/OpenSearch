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
            bucketContainer(bucketOf(clock.getAsLong())).writeBlob(UUIDs.randomBase64UUID(), bytes.streamInput(), bytes.length(), true);
        } catch (IOException | RuntimeException e) {
            failedAppends.incrementAndGet();
            logger.warn("could not append a change log entry for [{}]; the feed will need a rebuild to notice", change.name(), e);
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
        List<DescriptorChange> changes = new ArrayList<>();
        try {
            // Sorted, because children() gives no order and the buckets are the only ordering there is.
            Map<String, BlobContainer> buckets = new TreeMap<>(containers.apply(changelogPath()).children());
            for (Map.Entry<String, BlobContainer> bucket : buckets.entrySet()) {
                if (fromBucket != null && bucket.getKey().compareTo(fromBucket) < 0) {
                    continue;
                }
                for (String entry : bucket.getValue().listBlobs().keySet()) {
                    try (InputStream stream = bucket.getValue().readBlob(entry); StreamInput in = StreamInput.wrap(stream.readAllBytes())) {
                        changes.add(new DescriptorChange(in));
                    } catch (IOException e) {
                        // One unreadable entry is not a reason to lose the rest of the catch-up. It is also
                        // expected transiently: an entry can be listed between its key appearing and its
                        // body being complete, on a store that does not make writes atomic.
                        logger.debug("skipping an unreadable change log entry [{}/{}]", bucket.getKey(), entry, e);
                    }
                }
            }
        } catch (IOException e) {
            logger.warn("could not read the change log from bucket [{}]", fromBucket, e);
        }
        return changes;
    }

    /** The bucket a reader should resume from next time, given it has consumed everything up to now. */
    public String currentBucket() {
        return bucketOf(clock.getAsLong());
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
