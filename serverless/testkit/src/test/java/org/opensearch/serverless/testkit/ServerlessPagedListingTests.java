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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.DescriptorStore;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.rest.Fanout;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/** Walking descriptors a page at a time: every live index once, in order, and what a walk does not promise. */
public class ServerlessPagedListingTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private MetadataPlane plane(CountingBlobStore store) {
        final AtomicLong clock = new AtomicLong(1_000L);
        return new MetadataPlane(store, BlobPath.cleanPath(), clock::get, 30_000L);
    }

    private static String name(int i) {
        return String.format(Locale.ROOT, "logs-%04d", i);
    }

    /**
     * Tombstones between live names, at every page size: each live index once, none of the deleted, and pages
     * that are full whenever more follows. Read through a real pool so the concurrent path is the one tested.
     */
    public void testAWalkReturnsEveryLiveIndexOnceWhateverThePageSize() throws Exception {
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = plane(store);
        plane.descriptors().setConditionalDelete(false); // so deletes leave tombstones among the names
        final TreeSet<String> live = new TreeSet<>();
        for (int i = 0; i < 60; i++) {
            plane.createIndex(new IndexDescriptor(name(i), "uuid-" + i, 1, MAPPING, null));
            live.add(name(i));
        }
        for (int i = 0; i < 60; i += randomIntBetween(1, 4)) {
            assertTrue(plane.deleteIndex(name(i)));
            live.remove(name(i));
        }
        plane.createIndex(new IndexDescriptor("other", "uuid-other", 1, MAPPING, null));

        final ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            final DescriptorStore.Reads reads = new DescriptorStore.Reads() {
                @Override
                public <T> List<T> runAll(List<Callable<T>> tasks) throws InterruptedException {
                    return Fanout.run(pool, 4, tasks);
                }
            };
            for (int size : new int[] { 1, 2, 3, 7, live.size() - 1, live.size(), live.size() + 1, randomIntBetween(1, 70) }) {
                final List<String> walked = new ArrayList<>();
                String after = null;
                int pages = 0;
                do {
                    final DescriptorStore.Page page = plane.descriptors().listPage("logs-", after, size, reads);
                    assertTrue("a page holds at most its size", page.descriptors().size() <= size);
                    if (page.hasMore()) {
                        assertEquals("a page with more after it is full", size, page.descriptors().size());
                    }
                    walked.addAll(page.descriptors().keySet());
                    after = page.nextAfter();
                    assertTrue("too many pages at size " + size, ++pages <= 61);
                } while (after != null);
                assertEquals("page size " + size, new ArrayList<>(live), walked);
            }
        } finally {
            terminate(pool);
        }
    }

    /** A page resumes where the last stopped: the listing it pays for does not grow with the pages before it. */
    public void testALatePageCostsWhatTheFirstDid() throws Exception {
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = plane(store);
        for (int i = 0; i < 200; i++) {
            plane.createIndex(new IndexDescriptor(name(i), "uuid-" + i, 1, MAPPING, null));
        }
        store.reset();
        final DescriptorStore.Page first = plane.descriptors().listPage("logs-", null, 10);
        final long firstCost = store.impliedS3Requests();
        store.reset();
        final DescriptorStore.Page late = plane.descriptors().listPage("logs-", name(180), 10);
        final long lateCost = store.impliedS3Requests();
        assertEquals(10, first.descriptors().size());
        assertEquals(List.of(name(181), name(182)), new ArrayList<>(late.descriptors().keySet()).subList(0, 2));
        assertEquals("one listing and ten reads, early or late", firstCost, lateCost);
        assertEquals(11, lateCost);
    }

    /**
     * A create or delete between two pages. The walk is not a snapshot: behind the cursor a change is not seen,
     * ahead of it it is; an index that exists for the whole walk is returned exactly once.
     */
    public void testAChangeBetweenPagesIsSeenOnlyAheadOfTheCursor() throws Exception {
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = plane(store);
        for (int i = 0; i < 20; i += 2) {
            plane.createIndex(new IndexDescriptor(name(i), "uuid-" + i, 1, MAPPING, null));
        }
        final DescriptorStore.Page first = plane.descriptors().listPage("logs-", null, 4);
        assertEquals(List.of(name(0), name(2), name(4), name(6)), new ArrayList<>(first.descriptors().keySet()));

        plane.createIndex(new IndexDescriptor(name(1), "uuid-1", 1, MAPPING, null)); // behind
        plane.createIndex(new IndexDescriptor(name(9), "uuid-9", 1, MAPPING, null)); // ahead
        assertTrue(plane.deleteIndex(name(2))); // behind: already returned
        assertTrue(plane.deleteIndex(name(12))); // ahead: never returned

        final List<String> rest = new ArrayList<>();
        String after = first.nextAfter();
        while (after != null) {
            final DescriptorStore.Page page = plane.descriptors().listPage("logs-", after, 4);
            rest.addAll(page.descriptors().keySet());
            after = page.nextAfter();
        }
        assertEquals(List.of(name(8), name(9), name(10), name(14), name(16), name(18)), rest);
    }
}
