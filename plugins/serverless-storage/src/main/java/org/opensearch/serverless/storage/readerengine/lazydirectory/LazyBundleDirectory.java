/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.lazydirectory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.NoLockFactory;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * A read-only Lucene {@link Directory} whose "files" are {@code (bundle, offset, length)} ranges
 * resolved lazily through a {@link TransferManager}-backed block cache, exactly as
 * rfc-serverless-opensearch.md &sect;7.2 describes: {@code createOutput}/{@code deleteFile} throw,
 * the directory holds a file map (from a {@link CommitManifest}), not file bytes. Opening one
 * requires no upfront I/O at all -- contrast {@code ObjectStoreCommitMaterializer}, which fetches
 * every referenced file in full before a directory becomes usable; this class defers every fetch
 * until Lucene actually asks for the bytes, and only for the specific blocks it asks for.
 *
 * <p>Every read genuinely defers to and is cached by core's own searchable-snapshots
 * {@link org.opensearch.index.store.remote.filecache.FileCache}/{@link TransferManager}/{@link
 * org.opensearch.index.store.remote.file.AbstractBlockIndexInput} machinery -- reused directly,
 * not reimplemented, since it already solves exactly this "lazy block-cached remote file" problem
 * for a different caller (searchable snapshots' own {@code RemoteSnapshotDirectory}).
 *
 * <p><b>{@link #advanceToManifest} rebinds, and this class used to claim it never did.</b> The
 * previous javadoc here asserted "a file already listed is never removed or replaced (immutable
 * manifests/bundles mean an existing {@link FileReference} entry is always still correct)". That
 * is false, and the way it is false is the interesting part: manifests and bundles are indeed
 * immutable, but a Lucene <em>file name</em> is not a stable identifier across them. A compaction
 * publishes its merged commit with a fresh Lucene generation and fresh segment names, and Lucene
 * recycles both -- so a later manifest can legitimately bind {@code segments_1}, or {@code _0.si},
 * to completely different bytes in a completely different bundle. {@code putAll} replaced those
 * entries all along; only the documentation disagreed.
 *
 * <p>What <em>is</em> true, and is what the old sentence was reaching for, is that a rebind never
 * disturbs a read already in progress: {@link #openInput} resolves a name to a {@link
 * FileReference} once and hands that immutable triple to the {@code IndexInput}, which never
 * consults this map again. A concurrent advance therefore cannot move an in-flight reader's bytes.
 * The thing that genuinely was unsafe -- the shared block cache underneath those inputs being
 * keyed by file name alone, so a rebound name served the previous file's blocks -- is fixed in
 * {@link LazyBundleIndexInput}, by making the cache key content-addressed. A rebind is logged here
 * rather than silently applied, because on a shard with no compaction it should never happen and is
 * worth seeing if it does.
 *
 * <p>The file map is pruned to the union of the current and immediately-previous manifests' files
 * on every advance, rather than growing forever. A reader shard has no {@code IndexWriter} and so
 * no {@code IndexFileDeleter}; without this, {@link #listAll} returned every file name the shard
 * had ever published, which is both an unbounded leak and (on the eager path's equivalent) part of
 * what made a superseded commit selectable at all. One generation of slack is kept deliberately, so
 * a searcher acquired just before an advance can still resolve anything its own commit references.
 */
public final class LazyBundleDirectory extends Directory {

    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(LazyBundleDirectory.class);

    private final Map<String, FileReference> filesByName;
    private final FSDirectory cacheDirectory;
    /**
     * {@code cacheDirectory}'s path, captured at construction.
     *
     * <p>Not read from {@code cacheDirectory.getDirectory()} inside {@link #close()}: {@code
     * FSDirectory} throws {@link org.apache.lucene.store.AlreadyClosedException} from that accessor
     * once it has been closed, and this directory is legitimately closed more than once -- {@code
     * Store} wraps and closes it, a test may close it as well, and Lucene's own {@code
     * FSDirectory#close} is documented as idempotent precisely because callers do this. Asking a
     * closed directory where it lives turned a second, harmless close into a thrown exception.
     */
    private final Path cachePath;
    private final TransferManager transferManager;
    /**
     * The node-shared block cache {@code transferManager} writes into, or {@code null} when this
     * directory was built without one (tests, and any caller that owns cleanup itself). Held only
     * so {@link #close()} can prune this shard's entries out of it -- see that method.
     */
    private final org.opensearch.index.store.remote.filecache.FileCache fileCache;
    private final Lock noOpLock = NoLockFactory.INSTANCE.obtainLock(null, null);
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean(false);
    private volatile CommitManifest currentManifest;
    /** The manifest before {@link #currentManifest}, whose files are retained for one more generation -- see this class's javadoc. */
    private volatile CommitManifest previousManifest;

    /**
     * Builds a directory whose file map starts from {@code manifest}'s files, with no handle on the
     * node-shared block cache -- so {@link #close()} deletes this shard's on-disk block files but
     * cannot prune the in-memory cache entries naming them. Only appropriate when the caller owns
     * that cleanup itself.
     *
     * @param manifest the commit manifest whose files seed this directory's file map
     * @param cacheDirectory local on-disk directory {@link TransferManager} writes fetched blocks into
     * @param transferManager fetches and caches blocks on demand
     */
    public LazyBundleDirectory(CommitManifest manifest, FSDirectory cacheDirectory, TransferManager transferManager) {
        this(manifest, cacheDirectory, transferManager, null);
    }

    /**
     * Builds a directory whose file map starts from {@code manifest}'s files.
     *
     * @param manifest the commit manifest whose files seed this directory's file map
     * @param cacheDirectory local on-disk directory {@link TransferManager} writes fetched blocks into
     * @param transferManager fetches and caches blocks on demand
     * @param fileCache the node-shared block cache backing {@code transferManager}, so {@link
     *                  #close()} can prune this shard's entries out of it; {@code null} to skip that.
     */
    public LazyBundleDirectory(
        CommitManifest manifest,
        FSDirectory cacheDirectory,
        TransferManager transferManager,
        org.opensearch.index.store.remote.filecache.FileCache fileCache
    ) {
        this.filesByName = new ConcurrentHashMap<>(manifest.files());
        this.cacheDirectory = cacheDirectory;
        this.cachePath = cacheDirectory.getDirectory();
        this.transferManager = transferManager;
        this.fileCache = fileCache;
        this.currentManifest = manifest;
        this.previousManifest = manifest;
    }

    /**
     * The most recent manifest this directory was built or advanced with -- lets a caller that
     * already holds a reference to this directory (e.g. {@code ReaderEngineFactory#newReadWriteEngine},
     * moments after {@code ServerlessStorageLazyDirectoryFactory#newDirectory} constructed it for the
     * exact same shard open) reuse it instead of independently re-reading the shard's head and
     * manifest from the object store -- one redundant head read plus one redundant manifest GET
     * avoided per lazy-directory reader-shard open. No staler than the two independent reads it
     * replaces would have been relative to each other anyway: both this field and an independent
     * fresh read are subject to the exact same "a newer commit could land in between" race, which
     * {@code ObjectStoreReaderEngine}'s own poll-and-catch-up design already tolerates by
     * construction, so reusing this field introduces no new consistency risk.
     */
    public CommitManifest currentManifest() {
        return currentManifest;
    }

    /**
     * Adds every file referenced by {@code manifest} that isn't already known to this directory's
     * file map -- see this class's own javadoc for why this is safe to call concurrently with
     * in-flight reads of already-known files. Callers still need to trigger a real Lucene reader
     * reopen ({@code DirectoryReader#openIfChanged}, exactly as the eager materialization path's
     * refresh already does) for a newly-added {@code segments_N} file to actually become visible
     * to search -- adding the entry here alone does not do that.
     *
     * @param manifest the newer manifest generation whose files should be merged into this directory
     */
    public void advanceToManifest(CommitManifest manifest) {
        for (Map.Entry<String, FileReference> entry : manifest.files().entrySet()) {
            FileReference previous = filesByName.put(entry.getKey(), entry.getValue());
            if (previous != null && previous.equals(entry.getValue()) == false) {
                // Not an error -- the new manifest is authoritative and this is exactly what a
                // compaction's recycled segment names look like -- but it is the condition that
                // used to serve the OLD file's cached blocks under the new name, so it is worth
                // being visible in a log rather than being the silent thing this class's javadoc
                // used to claim could not happen at all.
                logger.info(
                    "manifest generation {} rebinds file [{}] from {} to {}; block cache keys are content-addressed so"
                        + " the new bytes are what will be read",
                    manifest.generation(),
                    entry.getKey(),
                    previous,
                    entry.getValue()
                );
            }
        }
        CommitManifest supersededTwiceOver = previousManifest;
        previousManifest = currentManifest;
        currentManifest = manifest;
        // Anything referenced only by a manifest that is now two generations old can go: no reader
        // this directory serves can still be resolving it (see this class's own javadoc for why one
        // generation of slack is kept and no more). Purely a memory/listAll bound -- the block cache
        // underneath is bounded separately and content-addressed, so nothing here invalidates it.
        if (supersededTwiceOver != currentManifest && supersededTwiceOver != previousManifest) {
            Set<String> retained = new java.util.HashSet<>(currentManifest.files().keySet());
            retained.addAll(previousManifest.files().keySet());
            filesByName.keySet().retainAll(retained);
        }
    }

    /**
     * The most files' prefetches this directory will ever have simultaneously in flight, across
     * one {@link #prefetchBootSet} call -- see that method's own javadoc for why an unbounded
     * fan-out (one task per file, all submitted at once) is unsafe: {@code executor} is a shared,
     * node-wide pool, and many reader shards opening around the same time (a node restart, a scale-
     * up event) would otherwise each contribute their own unbounded burst on top of each other,
     * competing for the same pool and the same object-store connections as every other shard's
     * legitimate, non-prefetch work (manifest polls, directory refreshes, actual query reads).
     */
    private static final int MAX_CONCURRENT_BOOT_SET_PREFETCHES = 8;

    /**
     * Illustrative "boot-set" prefetch (rfc-serverless-opensearch.md &sect;18 risk #2, "cold-query
     * latency... mitigation: boot-set prefetch"): warms this directory's local block cache by
     * fetching just the first block of every currently-known file, concurrently but bounded to at
     * most {@link #MAX_CONCURRENT_BOOT_SET_PREFETCHES} in flight at once, on {@code executor} --
     * <em>not</em> on the calling thread, so this method itself returns immediately and never
     * reintroduces the upfront-I/O cost this whole directory exists to avoid (see this class's own
     * javadoc). The actual win is reordering, not reducing, request count: Lucene's own {@code
     * DirectoryReader}/{@code SegmentInfos} open sequence would fetch these same first blocks
     * anyway, one at a time, serially, as it opens each file in turn -- this fires a bounded number
     * of them concurrently instead, so by the time Lucene actually asks, the fetch is already in
     * flight or done rather than starting cold, without unboundedly bursting every file's fetch at
     * once (a real shard can easily have far more than a handful of segment files).
     *
     * <p>Implemented as a small fixed-size worker pool pulling from one shared queue, rather than
     * submitting every file's task upfront and having most of them immediately block on a permit:
     * that would still tie up {@code executor}'s own threads/queue slots one-for-one with file
     * count, defeating the point of bounding concurrency at all.
     *
     * <p>Deliberately best-effort: a failed or slow prefetch of one file must never block or fail
     * this method, nor the real read that follows -- that real read (via the ordinary {@link
     * #openInput} path) still correctly (re)fetches on its own if the prefetch never completed or
     * hit a transient fault, exactly the same fault-tolerance the on-demand path always had.
     *
     * @param executor runs each worker's prefetch loop; must not run tasks synchronously on the
     *                  calling thread (see above).
     */
    public void prefetchBootSet(Executor executor) {
        Deque<String> queue = new ArrayDeque<>(filesByName.keySet());
        int workerCount = Math.min(MAX_CONCURRENT_BOOT_SET_PREFETCHES, queue.size());
        for (int i = 0; i < workerCount; i++) {
            executor.execute(() -> drainBootSetPrefetchQueue(queue));
        }
    }

    private void drainBootSetPrefetchQueue(Deque<String> queue) {
        for (;;) {
            String name;
            synchronized (queue) {
                name = queue.poll();
            }
            if (name == null) {
                return;
            }
            try (IndexInput input = openInput(name, IOContext.READONCE)) {
                if (input.length() > 0) {
                    input.readByte();
                }
            } catch (IOException | RuntimeException prefetchFailure) {
                // Best-effort only -- the real read still happens (and still correctly fetches)
                // through the ordinary openInput path regardless of this outcome.
            }
        }
    }

    private FileReference resolve(String name) throws IOException {
        FileReference ref = filesByName.get(name);
        if (ref == null) {
            throw new java.io.FileNotFoundException(name);
        }
        return ref;
    }

    @Override
    public String[] listAll() {
        String[] names = filesByName.keySet().toArray(new String[0]);
        java.util.Arrays.sort(names);
        return names;
    }

    @Override
    public long fileLength(String name) throws IOException {
        return resolve(name).length();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        FileReference ref = resolve(name);
        return new LazyBundleIndexInput(
            "LazyBundleIndexInput(bundle=\"" + ref.bundleName() + "\", file=\"" + name + "\")",
            ref.bundleName(),
            name,
            ref.offset(),
            ref.length(),
            ref.checksum(),
            cacheDirectory,
            transferManager
        );
    }

    @Override
    public Lock obtainLock(String name) {
        // Read-only: nothing ever writes here, so there is nothing to actually lock -- same
        // no-op-lock shape ReadOnlyEngine's own materialized directories already tolerate for
        // non-writing use.
        return noOpLock;
    }

    @Override
    public void close() throws IOException {
        // The shared node-wide FileCache backing transferManager outlives any single
        // LazyBundleDirectory and is closed by whoever constructed it, not by this class. But
        // cacheDirectory -- the local on-disk location TransferManager writes fetched blocks
        // into -- is constructed fresh per shard specifically for this directory (see
        // ServerlessStorageLazyDirectoryFactory), so this class is what owns and must close it;
        // nothing else ever will. Lucene's own FSDirectory#close is idempotent, so a caller that
        // also happens to hold and close the same cacheDirectory instance separately (e.g. a test)
        // is safe.
        //
        // Closing it is not enough, though, and that gap was a real correctness bug rather than a
        // disk-space one: this directory used to close the FSDirectory and delete nothing, leaving
        // every fetched block file sitting under <shardPath>/lazy_directory_cache. On the next open
        // of this shard on this node -- a relocation back, a node restart -- the in-memory FileCache
        // starts empty but those files are still on disk, and core's TransferManager serves any file
        // already present at the request path without validating it. Every block read of the new
        // incarnation therefore short-circuited on the previous incarnation's bytes. Keying blocks
        // by content (see LazyBundleIndexInput) makes that survivable, but leaving a whole shard's
        // worth of stale blocks behind on every close is still not something this class should do
        // when it is the sole owner of the directory.
        if (closed.compareAndSet(false, true) == false) {
            // Idempotent, like the FSDirectory underneath it. A second close must not re-prune a
            // cache it already pruned, and must not fail the caller for tidying up twice.
            return;
        }
        try {
            cacheDirectory.close();
        } finally {
            if (fileCache != null) {
                // Drop the in-memory entries naming files about to be deleted, so the cache's own
                // accounting does not keep charging this node for bytes that no longer exist.
                try {
                    fileCache.prune(path -> path.startsWith(cachePath));
                } catch (RuntimeException pruneFailure) {
                    logger.warn("failed to prune the node block cache for [" + cachePath + "] on close", pruneFailure);
                }
            }
            try {
                org.opensearch.common.util.io.IOUtils.rm(cachePath);
            } catch (IOException | RuntimeException removalFailure) {
                // Best-effort: a leftover cache directory now only wastes disk, since the block keys
                // are content-addressed. Never worth failing a shard close over.
                logger.warn("failed to remove the lazy-directory block cache at [" + cachePath + "]", removalFailure);
            }
        }
    }

    @Override
    public Set<String> getPendingDeletions() {
        return Set.of();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        throw new IOException("LazyBundleDirectory is read-only: createOutput(" + name + ") is not supported");
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        throw new IOException("LazyBundleDirectory is read-only: createTempOutput is not supported");
    }

    @Override
    public void deleteFile(String name) throws IOException {
        throw new IOException("LazyBundleDirectory is read-only: deleteFile(" + name + ") is not supported");
    }

    @Override
    public void sync(Collection<String> names) {
        // Nothing was ever written locally, so there is nothing to fsync -- a genuine no-op, not
        // an unsupported operation, matching how a read-only directory's own callers (e.g. a
        // DirectoryReader open) never need this to do anything real.
    }

    @Override
    public void syncMetaData() {
        // See sync(Collection) above.
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        throw new IOException("LazyBundleDirectory is read-only: rename is not supported");
    }
}
