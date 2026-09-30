/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.serverless.metadata.DescriptorStore;
import org.opensearch.serverless.metadata.DigestRollups;
import org.opensearch.serverless.store.PruningDigest;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** The rollup's rules: it only widens within an incarnation, writers keep a shard unskippable, and nothing unknown is ruled out. */
public class DigestRollupsTests extends OpenSearchTestCase {

    private static final long NOW = 1_800_000_000_000L;
    private static final DescriptorStore.Reads IN_SEQUENCE = new DescriptorStore.Reads() {
        @Override
        public <T> List<T> runAll(List<Callable<T>> tasks) {
            final List<T> out = new ArrayList<>();
            for (Callable<T> task : tasks) {
                try {
                    out.add(task.call());
                } catch (Exception e) {
                    out.add(null);
                }
            }
            return out;
        }
    };

    private static PruningDigest n(long min, long max) {
        return new PruningDigest(Map.of("n", new PruningDigest.FieldRange("long", min, max, null, null)));
    }

    private DigestRollups rollups() throws java.io.IOException {
        return new DigestRollups(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath().add("rollups"));
    }

    private static DigestRollups.Entry entry(DigestRollups rollups, String name) throws Exception {
        return rollups.readGroup(DigestRollups.group(name), IN_SEQUENCE).get(name);
    }

    public void testAPublishedShardIsRuledOutOnlyByWhatItsDigestExcludes() throws Exception {
        final DigestRollups rollups = rollups();
        rollups.widen("logs-a", "uuid-a", 1, 0, n(100, 200));
        final DigestRollups.Entry entry = entry(rollups, "logs-a");
        assertTrue(entry.rulesOut(QueryBuilders.rangeQuery("n").gte(10_000), NOW));
        assertFalse(entry.rulesOut(QueryBuilders.rangeQuery("n").gte(150), NOW));
        assertFalse("an undigested field is a maybe", entry.rulesOut(QueryBuilders.rangeQuery("other").gte(10_000), NOW));
    }

    public void testADigestOnlyWidensWithinAnIncarnation() throws Exception {
        final DigestRollups rollups = rollups();
        rollups.widen("logs-a", "uuid-a", 1, 0, n(100, 200));
        rollups.widen("logs-a", "uuid-a", 1, 0, n(150, 160));
        final DigestRollups.Entry entry = entry(rollups, "logs-a");
        assertFalse("a later, narrower commit does not narrow the record", entry.rulesOut(QueryBuilders.rangeQuery("n").lte(120), NOW));
        rollups.widen("logs-a", "uuid-a", 1, 0, n(5_000, 6_000));
        assertFalse(entry(rollups, "logs-a").rulesOut(QueryBuilders.rangeQuery("n").gte(5_500), NOW));
        assertFalse(entry(rollups, "logs-a").rulesOut(QueryBuilders.rangeQuery("n").lte(120), NOW));
    }

    public void testANewIncarnationReplacesTheEntry() throws Exception {
        final DigestRollups rollups = rollups();
        rollups.widen("logs-a", "uuid-old", 1, 0, n(100, 200));
        rollups.widen("logs-a", "uuid-new", 1, 0, n(9_000, 9_100));
        final DigestRollups.Entry entry = entry(rollups, "logs-a");
        assertEquals("uuid-new", entry.uuid());
        assertTrue("the deleted incarnation's range is gone with it", entry.rulesOut(QueryBuilders.rangeQuery("n").lte(500), NOW));
    }

    /** A shard held by a writer is never ruled out, and a predecessor's late clear cannot unmark its successor. */
    public void testAWriterKeepsItsShardAndOnlyItsOwnClearReleasesIt() throws Exception {
        final DigestRollups rollups = rollups();
        rollups.widen("logs-a", "uuid-a", 1, 0, n(100, 200));
        rollups.markOwned("logs-a", "uuid-a", 1, 0, 3L);
        assertFalse(entry(rollups, "logs-a").rulesOut(QueryBuilders.rangeQuery("n").gte(10_000), NOW));
        // The successor takes it at term 4; the predecessor's clear at term 3 arrives after.
        rollups.markOwned("logs-a", "uuid-a", 1, 0, 4L);
        rollups.clearOwned("logs-a", "uuid-a", 0, 3L);
        assertFalse("still held by term 4", entry(rollups, "logs-a").rulesOut(QueryBuilders.rangeQuery("n").gte(10_000), NOW));
        rollups.clearOwned("logs-a", "uuid-a", 0, 4L);
        assertTrue(entry(rollups, "logs-a").rulesOut(QueryBuilders.rangeQuery("n").gte(10_000), NOW));
    }

    /** One shard's publish makes an entry; the shards that have not published are unknown, and unknown is never ruled out. */
    public void testAShardThatHasNotPublishedIsNeverRuledOut() throws Exception {
        final DigestRollups rollups = rollups();
        rollups.widen("logs-b", "uuid-b", 3, 1, n(100, 200));
        assertFalse(entry(rollups, "logs-b").rulesOut(QueryBuilders.rangeQuery("n").gte(10_000), NOW));
        rollups.widen("logs-b", "uuid-b", 3, 0, n(100, 200));
        rollups.widen("logs-b", "uuid-b", 3, 2, n(100, 200));
        assertTrue(entry(rollups, "logs-b").rulesOut(QueryBuilders.rangeQuery("n").gte(10_000), NOW));
    }

    /** Concurrent publishes to indices sharing a bucket: every one's range survives the swaps. */
    public void testConcurrentWidensAllLand() throws Exception {
        final DigestRollups shared = rollups();
        final int writers = 8;
        final CyclicBarrier together = new CyclicBarrier(writers);
        final ExecutorService pool = Executors.newFixedThreadPool(writers);
        try {
            final List<Future<?>> done = new ArrayList<>();
            for (int w = 0; w < writers; w++) {
                final int writer = w;
                done.add(pool.submit(() -> {
                    together.await(10, TimeUnit.SECONDS);
                    for (int i = 0; i < 5; i++) {
                        shared.widen("logs-" + writer, "uuid-" + writer, 1, 0, n(writer * 1_000L + i, writer * 1_000L + i));
                    }
                    return null;
                }));
            }
            for (Future<?> f : done) {
                f.get(60, TimeUnit.SECONDS);
            }
        } finally {
            terminate(pool);
        }
        final Map<String, DigestRollups.Entry> all = shared.readGroup("logs-", IN_SEQUENCE);
        for (int w = 0; w < writers; w++) {
            final DigestRollups.Entry entry = all.get("logs-" + w);
            assertNotNull("logs-" + w + " lost its entry", entry);
            assertFalse(entry.rulesOut(QueryBuilders.rangeQuery("n").gte(w * 1_000L).lte(w * 1_000L + 4), NOW));
        }
    }

    public void testCoarseningOnlyWidens() {
        final PruningDigest exact = new PruningDigest(
            Map.of(
                "@timestamp",
                new PruningDigest.FieldRange(
                    "date",
                    1_790_000_123_456L,
                    1_790_000_987_654L,
                    "strict_date_optional_time||epoch_millis",
                    "und"
                ),
                "n",
                new PruningDigest.FieldRange("long", 1_000L, 1_999L, null, null)
            )
        );
        final PruningDigest coarse = exact.coarsened();
        for (String field : List.of("@timestamp", "n")) {
            assertTrue(coarse.fields().get(field).min().longValue() <= exact.fields().get(field).min().longValue());
            assertTrue(coarse.fields().get(field).max().longValue() >= exact.fields().get(field).max().longValue());
        }
        assertEquals("to the hour", 0L, coarse.fields().get("@timestamp").min().longValue() % 3_600_000L);
        assertEquals(3_600_000L - 1, coarse.fields().get("@timestamp").max().longValue() % 3_600_000L);
        final PruningDigest extreme = new PruningDigest(
            Map.of("n", new PruningDigest.FieldRange("long", Long.MIN_VALUE + 1, Long.MAX_VALUE - 1, null, null))
        ).coarsened();
        assertTrue(extreme.fields().get("n").min().longValue() <= Long.MIN_VALUE + 1);
        assertTrue(extreme.fields().get("n").max().longValue() >= Long.MAX_VALUE - 1);
    }
}
