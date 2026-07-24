/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.manifest;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class BlobContainerManifestStoreTests extends OpenSearchTestCase {

    private BlobContainer newFsBlobContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    public void testWriteThenReadManifestRoundTrips() throws Exception {
        BlobContainerManifestStore store = new BlobContainerManifestStore(newFsBlobContainer());

        CommitManifest manifest = new CommitManifest(
            "index-uuid",
            0,
            1,
            3,
            "segments_3",
            Map.of("segments_3", new FileReference("bundle-1-3", 0, 100, 1L)),
            10,
            10,
            new WalPosition("epoch-1", 42),
            0,
            PruningStats.empty(),
            123456789L
        );

        store.writeManifest(manifest);
        CommitManifest read = store.readManifest(1, 3);

        assertEquals(manifest, read);
    }

    public void testManifestBlobNameMatchesCanonicalNaming() throws Exception {
        BlobContainer blobContainer = newFsBlobContainer();
        BlobContainerManifestStore store = new BlobContainerManifestStore(blobContainer);

        CommitManifest manifest = new CommitManifest(
            "idx",
            0,
            7,
            42,
            "segments_42",
            Map.of("segments_42", new FileReference("bundle-7-42", 0, 10, 1L)),
            0,
            0,
            null,
            0,
            PruningStats.empty(),
            0L
        );
        store.writeManifest(manifest);

        assertTrue(blobContainer.blobExists("manifest-7-42"));
    }

    private static CommitManifest manifest(long term, long generation) {
        return new CommitManifest(
            "idx",
            0,
            term,
            generation,
            "segments_" + generation,
            Map.of("segments_" + generation, new FileReference("bundle-" + term + "-" + generation, 0, 10, 1L)),
            0,
            0,
            null,
            0,
            PruningStats.empty(),
            generation * 1000L
        );
    }

    public void testListManifestsOnAnEmptyContainerReturnsNothing() throws Exception {
        BlobContainerManifestStore store = new BlobContainerManifestStore(newFsBlobContainer());
        assertEquals(List.of(), store.listManifests());
    }

    public void testListManifestsReturnsEveryWrittenManifest() throws Exception {
        BlobContainerManifestStore store = new BlobContainerManifestStore(newFsBlobContainer());
        CommitManifest gen0 = manifest(1, 0);
        CommitManifest gen1 = manifest(1, 1);
        CommitManifest gen2 = manifest(1, 2);
        store.writeManifest(gen0);
        store.writeManifest(gen1);
        store.writeManifest(gen2);

        List<CommitManifest> listed = store.listManifests();

        assertEquals(3, listed.size());
        assertEquals(Set.of(gen0, gen1, gen2), listed.stream().collect(Collectors.toSet()));
    }

    public void testListManifestsAcrossMultipleTermsReturnsAllOfThem() throws Exception {
        BlobContainerManifestStore store = new BlobContainerManifestStore(newFsBlobContainer());
        CommitManifest term1Gen0 = manifest(1, 0);
        CommitManifest term2Gen0 = manifest(2, 0); // same generation, different term -- distinct blob name
        store.writeManifest(term1Gen0);
        store.writeManifest(term2Gen0);

        List<CommitManifest> listed = store.listManifests();

        assertEquals(Set.of(term1Gen0, term2Gen0), listed.stream().collect(Collectors.toSet()));
    }

    /** Wraps a container, counting every {@link BlobContainer#readBlob(String)} call. */
    private static final class ReadCountingBlobContainer extends FilterBlobContainer {
        final AtomicInteger readCount = new AtomicInteger();

        ReadCountingBlobContainer(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            readCount.incrementAndGet();
            return super.readBlob(blobName);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new ReadCountingBlobContainer(child);
        }
    }

    public void testListManifestsWithCacheReturnsTheSameResultAsWithoutOne() throws Exception {
        BlobContainer blobContainer = newFsBlobContainer();
        BlobContainerManifestStore store = new BlobContainerManifestStore(blobContainer);
        CommitManifest gen0 = manifest(1, 0);
        CommitManifest gen1 = manifest(1, 1);
        store.writeManifest(gen0);
        store.writeManifest(gen1);

        List<CommitManifest> uncached = store.listManifests();
        List<CommitManifest> cached = store.listManifests(new HashMap<>());

        assertEquals(Set.copyOf(uncached), Set.copyOf(cached));
        assertEquals(2, cached.size());
    }

    public void testListManifestsWithCacheDoesNotReReadAnAlreadySeenManifestOnANoOpTick() throws Exception {
        ReadCountingBlobContainer countingContainer = new ReadCountingBlobContainer(newFsBlobContainer());
        BlobContainerManifestStore store = new BlobContainerManifestStore(countingContainer);
        store.writeManifest(manifest(1, 0));
        store.writeManifest(manifest(1, 1));

        Map<String, CommitManifest> cache = new HashMap<>();
        List<CommitManifest> firstCall = store.listManifests(cache);
        assertEquals(2, firstCall.size());
        assertEquals("first call must read both manifests fresh", 2, countingContainer.readCount.get());

        List<CommitManifest> secondCall = store.listManifests(cache);
        assertEquals(2, secondCall.size());
        assertEquals(
            "a second call against an unchanged container must serve both manifests from the cache, reading neither again",
            2,
            countingContainer.readCount.get()
        );
        assertEquals(Set.copyOf(firstCall), Set.copyOf(secondCall));
    }

    public void testListManifestsWithCacheOnlyReadsTheManifestNewlyWrittenSinceTheLastCall() throws Exception {
        ReadCountingBlobContainer countingContainer = new ReadCountingBlobContainer(newFsBlobContainer());
        BlobContainerManifestStore store = new BlobContainerManifestStore(countingContainer);
        store.writeManifest(manifest(1, 0));

        Map<String, CommitManifest> cache = new HashMap<>();
        store.listManifests(cache);
        assertEquals(1, countingContainer.readCount.get());

        store.writeManifest(manifest(1, 1));
        List<CommitManifest> secondCall = store.listManifests(cache);

        assertEquals(2, secondCall.size());
        assertEquals(
            "only the newly written manifest should have been read; the first one must still come from the cache",
            2,
            countingContainer.readCount.get()
        );
    }

    public void testListManifestsWithCacheEvictsEntriesForManifestsNoLongerPresent() throws Exception {
        BlobContainer blobContainer = newFsBlobContainer();
        BlobContainerManifestStore store = new BlobContainerManifestStore(blobContainer);
        CommitManifest gen0 = manifest(1, 0);
        CommitManifest gen1 = manifest(1, 1);
        store.writeManifest(gen0);
        store.writeManifest(gen1);

        Map<String, CommitManifest> cache = new HashMap<>();
        store.listManifests(cache);
        assertEquals(2, cache.size());

        store.deleteManifests(List.of(gen0));
        List<CommitManifest> afterDelete = store.listManifests(cache);

        assertEquals(List.of(gen1), afterDelete);
        assertEquals(
            "the cache must not keep growing to hold every manifest ever seen -- a deleted manifest's entry must be evicted",
            1,
            cache.size()
        );
        assertFalse(cache.containsKey(gen0.manifestName()));
    }
}
