/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Covers the node-wide half of the local disk cache bound: the per-shard budget these tests
 * deliberately leave at zero (unbounded) throughout, so anything actually deleted here can only have
 * been deleted by the governor sweeping across shard directories.
 */
public class DiskCacheSpaceGovernorTests extends OpenSearchTestCase {

    private static final class CountingBundleFileReader implements BundleFileReader {
        private final BundleFileReader delegate;
        final AtomicInteger callCount = new AtomicInteger();

        CountingBundleFileReader(BundleFileReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
            callCount.incrementAndGet();
            return delegate.readFile(bundleName, entry);
        }
    }

    private static SegmentBundle threeTenByteFiles() {
        return BundleWriter.write(
            List.of(
                new BundleFileContent("a.bin", "aaaaaaaaaa".getBytes(StandardCharsets.UTF_8)),
                new BundleFileContent("b.bin", "bbbbbbbbbb".getBytes(StandardCharsets.UTF_8)),
                new BundleFileContent("c.bin", "cccccccccc".getBytes(StandardCharsets.UTF_8))
            )
        );
    }

    private static BundleFileReader inMemoryReader(SegmentBundle bundle) {
        return (bundleName, entry) -> BundleReader.extractFile(bundle.bytes(), entry);
    }

    private static Path theSingleFileIn(Path directory) throws IOException {
        try (var stream = Files.list(directory)) {
            List<Path> files = stream.filter(Files::isRegularFile).toList();
            assertEquals("expected exactly one cache file in " + directory, 1, files.size());
            return files.get(0);
        }
    }

    /**
     * The gap this whole change exists to close: two shards, each individually unbounded, whose
     * combined footprint is what actually fills the node's disk. Before the governor, nothing in the
     * system could see across those two directories at all, so this could grow without limit.
     */
    public void testTheGovernorBoundsTheWholeTreeAcrossTwoShardDirectoriesAndEvictsTheGloballyOldest() throws Exception {
        SegmentBundle bundle = threeTenByteFiles();
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));
        Path cacheRoot = createTempDir().resolve("serverless_storage_cache");
        // 25 bytes across the whole tree fits two 10-byte entries but not three, exactly as the
        // per-shard test uses 25 for one directory -- the point being that here the three entries
        // are split across two shard directories neither of which has a budget of its own.
        DiskCacheSpaceGovernor governor = new DiskCacheSpaceGovernor(cacheRoot, 25L);

        Path shardZeroDir = cacheRoot.resolve("index-uuid").resolve("0");
        Path shardOneDir = cacheRoot.resolve("index-uuid").resolve("1");
        LocalDiskCachingBundleStore shardZero = new LocalDiskCachingBundleStore(counting, shardZeroDir, null, 0L, governor);
        LocalDiskCachingBundleStore shardOne = new LocalDiskCachingBundleStore(counting, shardOneDir, null, 0L, governor);

        shardZero.readFile("bundle-1", bundle.entries().get("a.bin"));
        Path fileA = theSingleFileIn(shardZeroDir);
        Files.setLastModifiedTime(fileA, FileTime.from(Instant.now().minusSeconds(30)));

        shardOne.readFile("bundle-1", bundle.entries().get("b.bin"));
        Path fileB = theSingleFileIn(shardOneDir);
        Files.setLastModifiedTime(fileB, FileTime.from(Instant.now().minusSeconds(20)));

        assertEquals("nothing may be evicted while the tree is still inside its budget", 0, governor.evictedCount());

        // The third entry takes the tree to 30 > 25 and triggers a sweep down to floor(25*0.9)=22:
        // A, the globally oldest, goes -- and it lives in a DIFFERENT shard directory from the write
        // that triggered the sweep, which is the whole property under test.
        shardOne.readFile("bundle-1", bundle.entries().get("c.bin"));

        assertEquals(1, governor.evictedCount());
        assertFalse("the globally oldest file must be the one deleted, whichever shard owns it", Files.exists(fileA));
        assertTrue("the newer file in the same shard as the oldest must survive", Files.exists(fileB));
        try (var shardOneFiles = Files.list(shardOneDir)) {
            assertEquals("the surviving shard-1 directory must still hold both its entries", 2, shardOneFiles.count());
        }

        // And the eviction is real, not just a counter: re-reading the evicted entry must go back to
        // the delegate, while the survivor must not.
        int callsBefore = counting.callCount.get();
        shardOne.readFile("bundle-1", bundle.entries().get("b.bin"));
        assertEquals("a surviving entry must still be a disk-cache hit", callsBefore, counting.callCount.get());
        shardZero.readFile("bundle-1", bundle.entries().get("a.bin"));
        assertEquals("the evicted entry must re-fetch from the delegate", callsBefore + 1, counting.callCount.get());
    }

    /** A governor with a non-positive budget must leave the tree exactly as unbounded as it was before this class existed. */
    public void testAZeroBudgetGovernorNeverEvictsAnything() throws Exception {
        SegmentBundle bundle = threeTenByteFiles();
        CountingBundleFileReader counting = new CountingBundleFileReader(inMemoryReader(bundle));
        Path cacheRoot = createTempDir().resolve("serverless_storage_cache");
        DiskCacheSpaceGovernor governor = new DiskCacheSpaceGovernor(cacheRoot, 0L);
        LocalDiskCachingBundleStore store = new LocalDiskCachingBundleStore(
            counting,
            cacheRoot.resolve("index-uuid").resolve("0"),
            null,
            0L,
            governor
        );

        for (String name : List.of("a.bin", "b.bin", "c.bin")) {
            store.readFile("bundle-1", bundle.entries().get(name));
        }

        assertEquals(0, governor.evictedCount());
        assertEquals(0, governor.sweepCountForTesting());
        try (var cachedFiles = Files.list(cacheRoot.resolve("index-uuid").resolve("0"))) {
            assertEquals(3, cachedFiles.count());
        }
    }

    /** A tree a previous process already filled must be measured at construction, not discovered only once new writes accumulate. */
    public void testTheRunningTotalIsSeededByWalkingAnExistingTree() throws Exception {
        Path cacheRoot = createTempDir().resolve("serverless_storage_cache");
        Files.createDirectories(cacheRoot.resolve("uuid-a").resolve("0"));
        Files.write(cacheRoot.resolve("uuid-a").resolve("0").resolve("entry"), new byte[40]);
        // A half-written temp file is skipped exactly as the per-shard listing skips it: it belongs
        // to a write still in flight, and neither counting nor deleting it is safe.
        Files.write(cacheRoot.resolve("uuid-a").resolve("0").resolve("entry.tmp-7"), new byte[1000]);

        DiskCacheSpaceGovernor governor = new DiskCacheSpaceGovernor(cacheRoot, 1024L);

        assertEquals(40L, governor.currentTotalBytesForTesting());
    }

    // -------- the settings decision --------

    public void testAnExplicitNodeWideBudgetWinsOverTheDerivedDefault() {
        Path cacheRoot = createTempDir().resolve("serverless_storage_cache");
        Settings settings = Settings.builder().put("serverless_storage.local_cache.max_bytes", "128mb").build();

        assertEquals(128L * 1024 * 1024, ServerlessStoragePlugin.resolveNodeWideLocalCacheBudgetBytes(settings, cacheRoot));
    }

    /**
     * The behaviour this whole setting exists for: unset must NOT mean unbounded, because the disk
     * cache is created for every reader shard on every node with a data path, so unset previously
     * meant "an untuned deployment fills its disk". Deliberately asserts only the floor and the
     * shape, not the exact number -- the derived value is a fraction of whatever filesystem the test
     * happens to run on.
     */
    public void testAnUnsetNodeWideBudgetDerivesABoundedDefaultRatherThanUnbounded() {
        Path cacheRoot = createTempDir().resolve("serverless_storage_cache");

        long derived = ServerlessStoragePlugin.resolveNodeWideLocalCacheBudgetBytes(Settings.EMPTY, cacheRoot);

        assertTrue("an unset budget must derive a real bound, not 0/unbounded, got " + derived, derived > 0);
        assertTrue("the derived budget must respect its 64MB floor, got " + derived, derived >= 64L * 1024 * 1024);
    }

    /** An operator who genuinely wants the old unbounded behaviour must be able to ask for it, and zero is how. */
    public void testAnExplicitZeroStillMeansUnbounded() {
        Path cacheRoot = createTempDir().resolve("serverless_storage_cache");
        Settings settings = Settings.builder().put("serverless_storage.local_cache.max_bytes", "0b").build();

        assertEquals(0L, ServerlessStoragePlugin.resolveNodeWideLocalCacheBudgetBytes(settings, cacheRoot));
    }

    // -------- directory removal --------

    private static Path seedCacheDir(Path cacheRoot, String indexUuid, int shardId) throws IOException {
        Path dir = cacheRoot.resolve(indexUuid).resolve(String.valueOf(shardId));
        Files.createDirectories(dir);
        Files.write(dir.resolve("entry"), new byte[8]);
        return dir;
    }

    public void testDeletingAShardRemovesExactlyThatShardsDirectoryAndLeavesItsSibling() throws Exception {
        Path cacheRoot = createTempDir().resolve("serverless_storage_cache");
        Path shardZero = seedCacheDir(cacheRoot, "uuid-a", 0);
        Path shardOne = seedCacheDir(cacheRoot, "uuid-a", 1);
        Path otherIndexShardZero = seedCacheDir(cacheRoot, "uuid-b", 0);

        assertTrue(DiskCacheSpaceGovernor.deleteShardCache(cacheRoot, "uuid-a", 0));

        assertFalse("the deleted shard's own directory must be gone", Files.exists(shardZero));
        assertTrue("a sibling shard of the same index must keep its cache", Files.exists(shardOne));
        assertTrue("another index's cache must be untouched", Files.exists(otherIndexShardZero));
        assertTrue(
            "the parent index directory must survive while a sibling shard still uses it",
            Files.exists(cacheRoot.resolve("uuid-a"))
        );

        // Idempotent: the callback can fire for a shard whose cache was never created on this node.
        assertFalse(DiskCacheSpaceGovernor.deleteShardCache(cacheRoot, "uuid-a", 7));
    }

    public void testDeletingAnIndexRemovesItsWholeSubtreeAndNothingElse() throws Exception {
        Path cacheRoot = createTempDir().resolve("serverless_storage_cache");
        seedCacheDir(cacheRoot, "uuid-a", 0);
        seedCacheDir(cacheRoot, "uuid-a", 1);
        Path survivor = seedCacheDir(cacheRoot, "uuid-b", 0);

        assertTrue(DiskCacheSpaceGovernor.deleteIndexCache(cacheRoot, "uuid-a"));

        assertFalse(Files.exists(cacheRoot.resolve("uuid-a")));
        assertTrue(Files.exists(survivor));
    }

    // -------- the orphan sweep decision --------

    public void testTheOrphanSweepKeepsLiveUuidsAndRemovesTheRest() throws Exception {
        Path cacheRoot = createTempDir().resolve("serverless_storage_cache");
        seedCacheDir(cacheRoot, "live-uuid", 0);
        seedCacheDir(cacheRoot, "orphan-uuid-1", 0);
        seedCacheDir(cacheRoot, "orphan-uuid-2", 3);

        int removed = DiskCacheSpaceGovernor.deleteOrphanedIndexCaches(cacheRoot, Set.of("live-uuid", "an-index-never-cached-here"));

        assertEquals(2, removed);
        assertTrue("an index still in the cluster metadata must keep its cache", Files.exists(cacheRoot.resolve("live-uuid")));
        assertFalse(Files.exists(cacheRoot.resolve("orphan-uuid-1")));
        assertFalse(Files.exists(cacheRoot.resolve("orphan-uuid-2")));
    }

    /**
     * "No live indices" and "the metadata was not available" are indistinguishable from inside this
     * function, and the two call for opposite actions -- so it must take the side that cannot wipe
     * every warm cache on the node.
     */
    public void testTheOrphanSweepDeletesNothingWhenTheLiveSetIsEmpty() throws Exception {
        Path cacheRoot = createTempDir().resolve("serverless_storage_cache");
        seedCacheDir(cacheRoot, "uuid-a", 0);
        seedCacheDir(cacheRoot, "uuid-b", 0);

        assertEquals(0, DiskCacheSpaceGovernor.deleteOrphanedIndexCaches(cacheRoot, Set.of()));

        assertTrue(Files.exists(cacheRoot.resolve("uuid-a")));
        assertTrue(Files.exists(cacheRoot.resolve("uuid-b")));
    }

    /** A root that was never created (no reader shard ever opened here) is not an error to sweep. */
    public void testTheOrphanSweepToleratesAMissingRoot() {
        Path cacheRoot = createTempDir().resolve("never-created");

        assertEquals(0, DiskCacheSpaceGovernor.deleteOrphanedIndexCaches(cacheRoot, Set.of("uuid-a")));
    }
}
