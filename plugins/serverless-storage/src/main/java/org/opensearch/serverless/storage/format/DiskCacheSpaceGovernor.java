/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.util.io.IOUtils;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * The node-wide bound on {@code <data path>/serverless_storage_cache}, the one directory tree every
 * reader shard's {@link LocalDiskCachingBundleStore} caches into.
 *
 * <p>Why this exists separately from that class's own {@link LocalDiskCachingBundleStore
 * #maxBytesOnDisk}: that budget is <em>per shard</em>, and a per-shard budget does not bound a node.
 * N reader shards times B bytes is what actually lands on the disk, and N is decided by the cluster
 * manager's allocation, not by the operator who set B. This governor is the bound with the shape the
 * disk actually has -- one budget over the whole tree, enforced no matter how the shards underneath
 * it are distributed. The per-shard budget stays exactly as it was, as an optional extra cap on any
 * single shard's share; the two are independent and either can be off.
 *
 * <p>The mechanism is deliberately the same one {@code LocalDiskCachingBundleStore#evictIfOverBudget}
 * already uses, one level up the tree: an in-memory running total seeded once by walking the root, a
 * cheap O(1) bail-out on that total so the common case never touches the filesystem, a CAS guard so
 * only one sweep runs at a time, and a sweep that lists every cache file under the root, sorts by
 * {@code lastModifiedTime} and deletes the globally oldest until the tree is under {@link
 * LocalDiskCachingBundleStore#EVICTION_TARGET_FRACTION} of the budget -- the same fraction, and for
 * the same reason: a write sitting right at the line must not immediately re-trigger another sweep.
 * "Globally oldest" is the whole point of doing it here rather than per shard: the file that should
 * go when the node is out of disk is the coldest file on the node, which may well belong to a shard
 * that is nowhere near its own per-shard budget.
 *
 * <p>Same first-pass caveat as the per-shard policy: plain LRU-by-mtime over a directory walk,
 * evaluated synchronously on whichever request's write happened to cross the budget, not a tuned
 * admission-control policy and not a background schedule. A sweep failing must never fail the read
 * or write that triggered it -- the tree just stays over budget until the next successful sweep.
 */
public final class DiskCacheSpaceGovernor {

    private static final Logger logger = LogManager.getLogger(DiskCacheSpaceGovernor.class);

    private final Path cacheRoot;
    /** {@code <= 0} means unbounded, which is what an explicitly configured zero asks for. */
    private final long maxTotalBytes;
    // Running total of on-disk bytes across the whole tree, maintained incrementally for exactly the
    // reason LocalDiskCachingBundleStore#currentTotalBytes is: re-deriving it from a fresh walk on
    // every read would turn "eviction is enabled" into "every read walks the node's cache tree".
    // Seeded once here by a real walk (so a tree a previous process already populated is accounted
    // for), then kept up to date purely incrementally by the stores that write into it. It can drift
    // HIGH -- see this class's contract with those stores in recordBytesAdded -- and that direction
    // is harmless: sweepIfOverBudget re-derives the real total from a real listing before deleting
    // anything, so an inflated counter costs a wasted walk, never an incorrect deletion.
    private final AtomicLong currentTotalBytes;
    // Only one sweep at a time; a caller that loses the race just leaves its overage for the next
    // one to catch, the same best-effort shape as the per-shard sweep's own guard.
    private final AtomicBoolean sweepInProgress = new AtomicBoolean(false);
    private final AtomicLong evictedCount = new AtomicLong();
    private final AtomicLong sweepCount = new AtomicLong();

    /**
     * Creates a governor over {@code cacheRoot}, seeding its running total by walking whatever is
     * already there. A root that does not exist yet (the ordinary case on a fresh node -- shard
     * directories are created at reader-shard open) simply seeds zero.
     *
     * @param cacheRoot the shared cache root, {@code <data path>/serverless_storage_cache}.
     * @param maxTotalBytes the byte budget for the whole tree; {@code <= 0} leaves it unbounded.
     */
    public DiskCacheSpaceGovernor(Path cacheRoot, long maxTotalBytes) {
        this.cacheRoot = cacheRoot;
        this.maxTotalBytes = maxTotalBytes;
        this.currentTotalBytes = new AtomicLong(maxTotalBytes > 0 ? seedTotalBytes(cacheRoot) : 0L);
    }

    private static long seedTotalBytes(Path cacheRoot) {
        try {
            long total = 0;
            for (CacheFile file : listCacheFiles(cacheRoot)) {
                total += file.sizeBytes;
            }
            return total;
        } catch (IOException e) {
            // A failed seed is a missed head start, not a broken bound: the first sweep this
            // governor does run re-derives the real total from a real listing anyway. Starting at
            // zero only delays the first sweep until enough newly-written bytes are recorded.
            logger.warn("could not measure the existing local disk cache tree at [" + cacheRoot + "]; starting its total at zero", e);
            return 0L;
        }
    }

    /**
     * Records bytes a store just landed on disk and sweeps if that pushed the tree over budget.
     *
     * <p>Callers pass the <em>net</em> change (a write replacing an existing file at the same path
     * passes the difference), so the counter tracks the tree rather than the write volume. Bytes
     * this governor's own sweep deletes are netted out by the sweep itself; bytes a store's own
     * per-shard eviction deletes are reported through {@link #recordBytesRemoved}.
     *
     * @param bytes net on-disk bytes added; non-positive values are still recorded but never sweep.
     */
    public void recordBytesAdded(long bytes) {
        if (maxTotalBytes <= 0) {
            return;
        }
        currentTotalBytes.addAndGet(bytes);
        maybeSweep();
    }

    /**
     * Records bytes some other mechanism removed from the tree -- a store's own per-shard eviction,
     * today. Purely a counter correction: the tree only got smaller, so this never sweeps.
     *
     * @param bytes on-disk bytes removed.
     */
    public void recordBytesRemoved(long bytes) {
        if (maxTotalBytes <= 0) {
            return;
        }
        currentTotalBytes.addAndGet(-bytes);
    }

    private void maybeSweep() {
        // The O(1) bail-out that keeps "bounded" from meaning "walk the tree on every read".
        if (currentTotalBytes.get() <= maxTotalBytes) {
            return;
        }
        if (sweepInProgress.compareAndSet(false, true) == false) {
            return;
        }
        try {
            sweepIfOverBudget();
        } catch (IOException e) {
            // Never fail the read or write that triggered this -- the tree stays over budget until
            // the next successful sweep, exactly as the per-shard sweep behaves.
            logger.warn("failed to sweep the node-wide local disk cache tree [" + cacheRoot + "]", e);
        } finally {
            sweepInProgress.set(false);
        }
    }

    private void sweepIfOverBudget() throws IOException {
        sweepCount.incrementAndGet();
        List<CacheFile> files = listCacheFiles(cacheRoot);
        long totalBytes = 0;
        for (CacheFile file : files) {
            totalBytes += file.sizeBytes;
        }
        if (totalBytes <= maxTotalBytes) {
            return;
        }
        long targetBytes = (long) (maxTotalBytes * LocalDiskCachingBundleStore.EVICTION_TARGET_FRACTION);
        files.sort(Comparator.comparing(f -> f.lastModifiedTime));
        for (CacheFile file : files) {
            if (totalBytes <= targetBytes) {
                break;
            }
            try {
                // Deliberately NOT taking the owning store's per-path lock: this governor has no
                // handle on the stores whose files it deletes (they come and go with shard
                // allocation), and taking a lock it cannot see is not an option. Deleting a file a
                // store is concurrently writing is safe anyway -- writeAtomically writes a ".tmp-"
                // sibling and renames over the target, so the worst outcome is that this delete
                // races a rename and the entry survives, or that a just-written entry is removed and
                // the next read for it is a correct re-fetch. What is NOT safe is deleting the temp
                // file mid-write, which is why those are skipped in listCacheFiles, exactly as
                // LocalDiskCachingBundleStore#listCacheEntries skips them.
                long actualSize = Files.exists(file.path) ? Files.size(file.path) : 0L;
                Files.deleteIfExists(file.path);
                totalBytes -= file.sizeBytes;
                currentTotalBytes.addAndGet(-actualSize);
                evictedCount.incrementAndGet();
            } catch (IOException e) {
                // Raced by a concurrent write, or already gone -- move to the next candidate rather
                // than abandoning the whole sweep.
                logger.debug("failed to evict node-wide cache entry [" + file.path + "]", e);
            }
        }
    }

    /**
     * Every cache file under {@code cacheRoot}, at any depth -- the layout is
     * {@code <root>/<index uuid>/<shard id>/<entry>}, but nothing here depends on that.
     * Skips ".tmp-" files exactly as {@code LocalDiskCachingBundleStore#listCacheEntries} does:
     * those are half-written entries some thread is about to rename into place, and deleting one is
     * the single way this sweep could actually break a concurrent write rather than merely undo it.
     */
    private static List<CacheFile> listCacheFiles(Path cacheRoot) throws IOException {
        List<CacheFile> files = new ArrayList<>();
        if (Files.isDirectory(cacheRoot) == false) {
            return files;
        }
        try (Stream<Path> stream = Files.walk(cacheRoot)) {
            for (Path path : (Iterable<Path>) stream::iterator) {
                if (path.getFileName().toString().contains(".tmp-")) {
                    continue;
                }
                try {
                    if (Files.isRegularFile(path) == false) {
                        continue;
                    }
                    files.add(new CacheFile(path, Files.size(path), Files.getLastModifiedTime(path)));
                } catch (IOException e) {
                    // Deleted or replaced between the walk and this stat -- not a candidate either way.
                }
            }
        }
        return files;
    }

    /** Number of cache files this node-wide sweep has deleted so far. */
    public long evictedCount() {
        return evictedCount.get();
    }

    /**
     * Number of full sweeps run so far, whether or not any deleted anything -- the same test-only
     * signal {@code LocalDiskCachingBundleStore#sweepCountForTesting} provides for the per-shard
     * sweep, for the same reason: the running total staying accurate is a pure efficiency property,
     * observable only as sweeps that find nothing to do.
     */
    public long sweepCountForTesting() {
        return sweepCount.get();
    }

    /** Current running total of on-disk bytes across the tree, as this governor believes it to be. */
    public long currentTotalBytesForTesting() {
        return currentTotalBytes.get();
    }

    private record CacheFile(Path path, long sizeBytes, FileTime lastModifiedTime) {
    }

    /**
     * Removes one shard's cache directory, {@code <cacheRoot>/<index uuid>/<shard id>}, and nothing
     * else -- a sibling shard of the same index keeps its cache. Never throws: every caller is a
     * lifecycle callback where an exception would break a shard-removal sequence that has nothing to
     * do with a cache, and a directory that survives is wasted disk the next deletion or the orphan
     * sweep gets another try at.
     *
     * @param cacheRoot the shared cache root.
     * @param indexUuid the uuid of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @return {@code true} if the directory existed and is now gone.
     */
    public static boolean deleteShardCache(Path cacheRoot, String indexUuid, int shardId) {
        return deleteRecursively(cacheRoot.resolve(indexUuid).resolve(String.valueOf(shardId)), "shard");
    }

    /** The index-wide counterpart of {@link #deleteShardCache}, with the same never-throw contract.
     *
     * @param cacheRoot the shared cache root.
     * @param indexUuid the uuid of the deleted index.
     * @return {@code true} if the directory existed and is now gone.
     */
    public static boolean deleteIndexCache(Path cacheRoot, String indexUuid) {
        return deleteRecursively(cacheRoot.resolve(indexUuid), "index");
    }

    private static boolean deleteRecursively(Path directory, String what) {
        if (Files.exists(directory) == false) {
            return false;
        }
        try {
            IOUtils.rm(directory);
            return true;
        } catch (IOException | RuntimeException e) {
            logger.warn("failed to remove the local disk cache directory [" + directory + "] of a deleted " + what, e);
            return false;
        }
    }

    /**
     * Deletes every {@code <cacheRoot>/<index uuid>} directory whose uuid is not in
     * {@code liveIndexUuids} -- the cache tree's answer to an index that was deleted while this node
     * was down, which no shard- or index-level lifecycle callback on this node will ever fire for.
     *
     * <p>Kept a pure function of (root, live uuid set) precisely so the decision -- which is the part
     * that can delete a live index's cache if it is wrong -- is unit-testable without a cluster; the
     * {@code ClusterStateListener} that calls it after the first recovered cluster state is a thin
     * wrapper that only supplies those two arguments. Public rather than package-private only
     * because that wrapper lives in the plugin's own package.
     *
     * <p><b>An empty {@code liveIndexUuids} deletes nothing.</b> "No live indices" and "the metadata
     * was not available" are indistinguishable from here, and the two call for opposite actions, so
     * this takes the side that cannot destroy a warm cache. The cost of being wrong in this
     * direction is one leaked directory until the next real deletion; the cost in the other
     * direction is every reader shard on the node re-fetching its whole working set.
     *
     * @param cacheRoot the shared cache root to sweep.
     * @param liveIndexUuids uuids of every index still in the cluster metadata; empty means "do nothing".
     * @return the number of orphaned index cache directories actually removed.
     */
    public static int deleteOrphanedIndexCaches(Path cacheRoot, Set<String> liveIndexUuids) {
        if (liveIndexUuids == null || liveIndexUuids.isEmpty()) {
            return 0;
        }
        if (Files.isDirectory(cacheRoot) == false) {
            return 0;
        }
        int removed = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(cacheRoot)) {
            for (Path indexCacheDir : stream) {
                if (Files.isDirectory(indexCacheDir) == false) {
                    continue;
                }
                if (liveIndexUuids.contains(indexCacheDir.getFileName().toString())) {
                    continue;
                }
                try {
                    IOUtils.rm(indexCacheDir);
                    removed++;
                } catch (IOException e) {
                    // Best-effort, like every other cache deletion here: a directory that survives
                    // this pass is wasted disk, not a correctness problem, and the next node start
                    // gets another try at it.
                    logger.warn("failed to remove orphaned local disk cache directory [" + indexCacheDir + "]", e);
                }
            }
        } catch (IOException e) {
            logger.warn("failed to list the local disk cache root [" + cacheRoot + "] for orphaned index caches", e);
        }
        return removed;
    }
}
