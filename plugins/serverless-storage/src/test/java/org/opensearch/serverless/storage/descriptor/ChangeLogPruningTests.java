/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The change log has a bound, and the bound cannot be missed.
 *
 * <h2>Why the path structure matters here</h2>
 *
 * The log had no retention and grew with every descriptor write, forever. Bounding it could have been done
 * with a secondary index of what to delete, and that would have been the wrong shape: an index can fall out
 * of step with what it indexes, and anything it loses track of is invisible and therefore permanent.
 *
 * <p>Buckets are already a time-ordered path structure, so the set of things to delete <em>is</em> the set of
 * things that exist. There is nothing to keep in step and nothing to lose track of. A bucket is either old
 * enough to remove or too young, and both are decided by its own name.
 *
 * <h2>Why this is safe now and would not have been before</h2>
 *
 * Pruning history is only safe if nothing reads history. Until recently the tailer started its cursor at null
 * and replayed the log from the beginning, because it fed a per-node name index that had to be built that
 * way. Pruning then would have silently broken a joining node. That index is gone and the cursor now starts
 * at the bucket its node started in, so a bucket past the retention window has no reader by construction.
 */
public class ChangeLogPruningTests extends OpenSearchTestCase {

    private static final long MINUTE = TimeUnit.MINUTES.toMillis(1);

    private BlobStore store;
    private long now;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        store = new FsBlobStore(1024, createTempDir(), false);
        now = TimeUnit.HOURS.toMillis(10_000);
    }

    private BlobDescriptorChangeLog log() {
        return new BlobDescriptorChangeLog(store::blobContainer, BlobPath.cleanPath(), () -> now);
    }

    private static DescriptorChange change(String name) {
        return new DescriptorChange(name, name + "-uuid", DescriptorChange.Kind.UPDATED, 0L);
    }

    /** Buckets past the window go; buckets inside it stay. */
    public void testOldBucketsArePrunedAndRecentOnesAreNot() {
        log().append(change("tenant-ancient"));

        now += 30 * MINUTE;
        log().append(change("tenant-recent"));

        now += MINUTE;
        assertEquals("only the bucket past the window", 1, log().pruneOlderThan(20 * MINUTE));

        List<DescriptorChange> left = log().since(null);
        assertEquals(1, left.size());
        assertEquals("the recent one survives", "tenant-recent", left.get(0).name());
    }

    /**
     * The bucket being written to is never pruned, whatever the window says.
     *
     * <p>It is the bucket every tailer's cursor names, so removing it would strand every reader on a bucket
     * that no longer exists.
     *
     * <p>No guard enforces this and none is needed, which is worth stating because an explicit one was
     * written first and mutation testing showed it could never fire. The cutoff is
     * {@code bucketOf(now - retention)} and the live bucket is {@code bucketOf(now)}, so for any
     * non-negative window the cutoff is at or before the live bucket and the at-or-after test spares it.
     * The clock is read once so that stays true across a backwards jump.
     */
    public void testTheCurrentBucketIsNeverPruned() {
        log().append(change("tenant-now"));

        assertEquals("a zero window must still not touch the live bucket", 0, log().pruneOlderThan(0));
        assertEquals(1, log().since(null).size());
    }

    /** Pruning is idempotent, because a poll that runs twice must not be a different thing from one that runs once. */
    public void testPruningTwiceIsTheSameAsOnce() {
        log().append(change("tenant-old"));
        now += 30 * MINUTE;

        assertEquals(1, log().pruneOlderThan(5 * MINUTE));
        assertEquals("nothing left to do the second time", 0, log().pruneOlderThan(5 * MINUTE));
    }

    /**
     * A pruned log does not break the tailer, which is the thing pruning could plausibly break.
     *
     * <p>The tailer starts at the bucket its node started in, so history disappearing underneath it is not
     * something it can observe. Asserted rather than argued, because "nothing reads it" is exactly the kind
     * of claim that is true until someone adds a reader.
     */
    public void testATailerIsUnaffectedByPrunedHistory() throws Exception {
        log().append(change("tenant-history"));
        now += 30 * MINUTE;

        Path backing = createTempDir();
        DescriptorChangeTailer tailer = new DescriptorChangeTailer(
            log(),
            new BlobDescriptorBackend(new FsBlobStore(1024, backing, false).blobContainer(BlobPath.cleanPath()))
        );

        log().pruneOlderThan(5 * MINUTE);

        assertEquals("history is not this tailer's concern either way", 0, tailer.tailOnce());
        log().append(change("tenant-new"));
        assertEquals("and it still sees what arrives after it started", 1, tailer.tailOnce());
    }
}
