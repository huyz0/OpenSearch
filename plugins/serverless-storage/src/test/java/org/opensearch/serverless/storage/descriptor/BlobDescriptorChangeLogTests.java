/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class BlobDescriptorChangeLogTests extends OpenSearchTestCase {

    private FsBlobStore storeOver(Path directory) throws Exception {
        return new FsBlobStore(1024, directory, false);
    }

    private BlobDescriptorChangeLog logOver(Path directory) throws Exception {
        return new BlobDescriptorChangeLog(storeOver(directory)::blobContainer, BlobPath.cleanPath());
    }

    private static DescriptorChange change(String name, DescriptorChange.Kind kind) {
        return new DescriptorChange(name, java.util.UUID.randomUUID().toString(), kind, 0L);
    }

    /**
     * The ceiling this component had, and did not look like it had.
     *
     * <p>Appends never contend, so the rate was assumed unbounded. An object store rate-limits by key
     * prefix instead, and every append in a minute went under that minute's bucket -- one prefix, about
     * 3,500 writes per second on S3, which is roughly a quarter of what 100M indices in two hours needs and
     * the whole cluster's creation ceiling regardless of how many nodes are creating. Nothing about a
     * passing test would have shown it: a single prefix is exactly as correct as sixteen, and only slower
     * somewhere no test runs.
     *
     * <p>Asserted on the layout rather than on a rate, because the rate belongs to the store rather than to
     * this code. What this owns is that appends land under more than one prefix.
     */
    public void testAppendsAreSpreadOverPrefixesRatherThanOneBucketWideOne() throws Exception {
        Path directory = createTempDir();
        BlobDescriptorChangeLog log = logOver(directory);
        for (int i = 0; i < 200; i++) {
            log.append(change("tenant-" + i, DescriptorChange.Kind.CREATED));
        }
        assertEquals("no append may have failed silently", 0, log.failedAppendCount());

        BlobContainer changelog = storeOver(directory).blobContainer(BlobPath.cleanPath().add("changelog"));
        Map<String, BlobContainer> buckets = changelog.children();
        assertFalse("the appends have to be somewhere", buckets.isEmpty());
        int prefixes = 0;
        int entriesDirectlyInABucket = 0;
        for (BlobContainer bucket : buckets.values()) {
            prefixes += bucket.children().size();
            entriesDirectlyInABucket += bucket.listBlobs().size();
        }
        assertEquals("an append under the bucket itself is the layout that had the ceiling", 0, entriesDirectlyInABucket);
        assertThat(
            "200 appends over one prefix is the ceiling; they must be spread over several",
            prefixes,
            org.hamcrest.Matchers.greaterThan(1)
        );
        assertEquals("and every one of them must still be readable", 200, log.since(null).size());
    }

    /**
     * An entry written before the appends were sharded is still read.
     *
     * <p>A running node's log has entries directly under the bucket, and a node that upgraded and stopped
     * looking there would skip every one of them -- silently, since a change log that reads short is
     * indistinguishable from a quiet one.
     */
    public void testEntriesWrittenBeforeShardingAreStillRead() throws Exception {
        Path directory = createTempDir();
        BlobDescriptorChangeLog log = logOver(directory);
        log.append(change("sharded", DescriptorChange.Kind.CREATED));

        // The pre-sharding layout, written by hand into the bucket the append just created.
        BlobContainer changelog = storeOver(directory).blobContainer(BlobPath.cleanPath().add("changelog"));
        String bucket = changelog.children().keySet().iterator().next();
        BytesStreamOutput out = new BytesStreamOutput();
        change("written-the-old-way", DescriptorChange.Kind.CREATED).writeTo(out);
        BytesReference bytes = out.bytes();
        changelog.children().get(bucket).writeBlob("an-entry-from-before-sharding", bytes.streamInput(), bytes.length(), true);

        assertEquals(
            Set.of("sharded", "written-the-old-way"),
            log.since(null).stream().map(DescriptorChange::name).collect(Collectors.toSet())
        );
    }

    public void testAnUnwrittenLogReadsEmptyRatherThanFailing() throws Exception {
        assertTrue(logOver(createTempDir()).since(null).isEmpty());
    }

    public void testAppendedChangesComeBack() throws Exception {
        BlobDescriptorChangeLog log = logOver(createTempDir());
        log.append(change("tenant-a", DescriptorChange.Kind.CREATED));
        log.append(change("tenant-b", DescriptorChange.Kind.CREATED));
        log.append(change("tenant-a", DescriptorChange.Kind.DELETED));

        List<DescriptorChange> read = log.since(null);
        assertEquals("no append may have failed silently", 0, log.failedAppendCount());
        assertEquals(3, read.size());
        assertEquals(Set.of("tenant-a", "tenant-b"), read.stream().map(DescriptorChange::name).collect(Collectors.toSet()));
        assertEquals(1, read.stream().filter(c -> c.live() == false).count());
    }

    /**
     * The property that makes this affordable at all. A single appended object under compare-and-swap
     * would serialise every descriptor write in the cluster behind one key, at roughly one write per round
     * trip, which is worse than the cluster manager this design removes from the creation path.
     */
    public void testConcurrentAppendersDoNotContendOrLoseEntries() throws Exception {
        BlobDescriptorChangeLog log = logOver(createTempDir());
        int appenders = 16;
        int each = 8;
        ExecutorService executor = Executors.newFixedThreadPool(appenders);
        CountDownLatch startLine = new CountDownLatch(1);

        try {
            List<Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < appenders; i++) {
                final int appender = i;
                futures.add(executor.submit(() -> {
                    try {
                        startLine.await();
                        for (int j = 0; j < each; j++) {
                            log.append(change("tenant-" + appender + "-" + j, DescriptorChange.Kind.CREATED));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            startLine.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }

        assertEquals("no append may have failed silently", 0, log.failedAppendCount());
        assertEquals("every append must survive", appenders * each, log.since(null).size());
    }

    /** Buckets order chronologically, and a reader resuming from one skips only what precedes it. */
    public void testResumingFromABucketSkipsOnlyEarlierBuckets() throws Exception {
        AtomicLong clock = new AtomicLong(0);
        BlobDescriptorChangeLog log = new BlobDescriptorChangeLog(
            storeOver(createTempDir())::blobContainer,
            BlobPath.cleanPath(),
            clock::get
        );

        log.append(change("early", DescriptorChange.Kind.CREATED));
        String secondBucket = BlobDescriptorChangeLog.bucketOf(BlobDescriptorChangeLog.BUCKET_MILLIS);
        clock.set(BlobDescriptorChangeLog.BUCKET_MILLIS);
        log.append(change("late", DescriptorChange.Kind.CREATED));

        assertEquals(2, log.since(null).size());
        List<DescriptorChange> tail = log.since(secondBucket);
        assertEquals(1, tail.size());
        assertEquals("late", tail.get(0).name());
    }

    /**
     * Zero padding is load-bearing rather than cosmetic: buckets are compared as strings, so an unpadded
     * bucket 9 would sort after bucket 10 and a reader resuming from it would silently skip the gap.
     */
    public void testBucketsSortChronologicallyAsStrings() {
        String ninth = BlobDescriptorChangeLog.bucketOf(9 * BlobDescriptorChangeLog.BUCKET_MILLIS);
        String tenth = BlobDescriptorChangeLog.bucketOf(10 * BlobDescriptorChangeLog.BUCKET_MILLIS);
        assertTrue("bucket 9 must sort before bucket 10, which is only true when padded", ninth.compareTo(tenth) < 0);
    }

    /**
     * A change carries the uuid, which is what tells a delete apart from the recreate that follows it.
     * Without it a consumer replaying a bucket in an arbitrary order cannot tell whether the name is live.
     */
    public void testAChangeCarriesTheUuidItAppliedTo() throws Exception {
        BlobDescriptorChangeLog log = logOver(createTempDir());
        DescriptorChange deleted = change("tenant-a", DescriptorChange.Kind.DELETED);
        DescriptorChange recreated = change("tenant-a", DescriptorChange.Kind.CREATED);
        log.append(deleted);
        log.append(recreated);

        Set<String> uuids = log.since(null).stream().map(DescriptorChange::uuid).collect(Collectors.toSet());
        assertEquals(Set.of(deleted.uuid(), recreated.uuid()), uuids);
    }
}
