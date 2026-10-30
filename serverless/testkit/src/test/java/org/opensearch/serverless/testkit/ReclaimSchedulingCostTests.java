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
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.ReclaimQueue;
import org.opensearch.serverless.reconcile.GarbageCollector;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicLong;

/** Empty and not-yet-due cleanup queues need no membership refresh. */
public class ReclaimSchedulingCostTests extends OpenSearchTestCase {
    public void testNoMembershipReadsWithoutDueReclaims() throws Exception {
        for (boolean future : new boolean[] { false, true }) {
            final var clock = new AtomicLong(1_000L);
            try (var counter = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
                final var metadata = new MetadataPlane(counter, BlobPath.cleanPath(), clock::get, 30_000L);
                if (future) {
                    metadata.reclaimQueue().enqueue(new ReclaimQueue.Intent("old-index", "uuid-old", 1, false), clock.get());
                }
                counter.reset();
                assertEquals(0, new GarbageCollector(counter, BlobPath.cleanPath()).collectReclaims(metadata, clock.get(), "self"));
                assertEquals(counter.breakdown(), 0L, counter.registerReads());
                assertEquals("only discovery of due buckets is needed: " + counter.breakdown(), 1L, counter.reads());
            }
        }
    }

    public void testDueBucketsStillUseMembershipAndAreDrained() throws Exception {
        final var clock = new AtomicLong(1_000L);
        try (var counter = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var metadata = new MetadataPlane(counter, BlobPath.cleanPath(), clock::get, 30_000L);
            metadata.reclaimQueue().enqueue(new ReclaimQueue.Intent("old-one", "uuid-one", 1, false), clock.get());
            clock.addAndGet(60_000L);
            metadata.reclaimQueue().enqueue(new ReclaimQueue.Intent("old-two", "uuid-two", 1, false), clock.get());
            clock.addAndGet(ReclaimQueue.DEFAULT_DELAY_MILLIS + 120_000L);
            counter.reset();
            assertTrue(new GarbageCollector(counter, BlobPath.cleanPath()).collectReclaims(metadata, clock.get(), "self") >= 2);
            // One membership read, plus one descriptor and one shard-head read for each absent old index.
            assertEquals(counter.breakdown(), 5L, counter.registerReads());
        }
    }
}
