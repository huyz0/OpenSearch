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
 * <p>{@link #advanceToManifest} lets this directory's file map grow additively as newer manifest
 * generations are published, the same additive semantics {@code ObjectStoreCommitMaterializer}
 * already has for the eager, fully-materialized directory this class is an alternative to -- a
 * file already listed is never removed or replaced (immutable manifests/bundles mean an existing
 * {@link FileReference} entry is always still correct), only new entries are added, so an
 * in-flight {@code IndexInput} reading an older file is never disturbed by a concurrent advance.
 */
public final class LazyBundleDirectory extends Directory {

    private final Map<String, FileReference> filesByName;
    private final FSDirectory cacheDirectory;
    private final TransferManager transferManager;
    private final Lock noOpLock = NoLockFactory.INSTANCE.obtainLock(null, null);
    private volatile CommitManifest currentManifest;

    /**
     * Builds a directory whose file map starts from {@code manifest}'s files.
     *
     * @param manifest the commit manifest whose files seed this directory's file map
     * @param cacheDirectory local on-disk directory {@link TransferManager} writes fetched blocks into
     * @param transferManager fetches and caches blocks on demand
     */
    public LazyBundleDirectory(CommitManifest manifest, FSDirectory cacheDirectory, TransferManager transferManager) {
        this.filesByName = new ConcurrentHashMap<>(manifest.files());
        this.cacheDirectory = cacheDirectory;
        this.transferManager = transferManager;
        this.currentManifest = manifest;
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
        filesByName.putAll(manifest.files());
        currentManifest = manifest;
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
        cacheDirectory.close();
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
