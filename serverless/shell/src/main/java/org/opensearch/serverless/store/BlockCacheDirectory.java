/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * A shard's directory: published segments read lazily from the object store, everything else local.
 *
 * <p>Hybrid rather than read-only, and the reason is concrete. Opening a restored shard runs
 * {@code Store#bootstrapNewHistory}, which writes a fresh {@code segments_N}; a purely remote directory
 * would fail that write, and a reader that cannot bootstrap cannot open at all. So local disk holds
 * what this node writes and the object store holds what was published — which is also the honest
 * description of the architecture, rather than a workaround for it.
 *
 * <p>Lookups prefer local. A file that exists in both is one this node produced, and its own copy is
 * both cheaper and more current.
 *
 * <p><b>Deletes of remote files are ignored.</b> Lucene removes superseded segments as it commits, but
 * a published blob is not this node's to remove — other readers may be serving the same commit, and
 * reclaiming it belongs to {@code GarbageCollector}, which knows what the manifest still references.
 * Deleting here would be a reader silently destroying shared truth.
 */
public final class BlockCacheDirectory extends FilterDirectory {

    private final Map<String, RemoteFile> remote;
    private final BlockCache cache;
    private final String cacheScope;

    private record RemoteFile(BlobContainer container, String blobName, long length, String termDir, long blobOffset) {
    }

    private BlockCacheDirectory(Directory local, Map<String, RemoteFile> remote, BlockCache cache, String cacheScope) {
        super(local);
        this.remote = remote;
        this.cache = cache;
        this.cacheScope = cacheScope;
    }

    /**
     * Builds a directory for one shard from its published manifest.
     *
     * @param local the shard's local directory, used for writes and as a cache of its own output
     * @param blobStore the backing store
     * @param shardBase the shard's base path in the object store
     * @param cache the shared block cache
     * @param cacheScope a prefix making cache keys unique across shards
     * @return the directory
     * @throws IOException if the manifest or the term containers cannot be listed
     */
    public static BlockCacheDirectory create(Directory local, BlobStore blobStore, BlobPath shardBase, BlockCache cache, String cacheScope)
        throws IOException {
        return create(
            local,
            blobStore,
            shardBase,
            cache,
            cacheScope,
            new SegmentPublisher(blobStore, shardBase).readManifest().orElse(null)
        );
    }

    /**
     * Opens a directory over a commit chosen by the caller rather than the current one.
     *
     * <p>For a frozen view, which exists precisely because the current commit has moved on. Passing the
     * manifest in rather than reading it is the whole difference: reading would read the one this is meant
     * not to see.
     *
     * @param local the shard's local directory
     * @param blobStore the object store
     * @param shardBase the shard's container
     * @param cache the block cache
     * @param cacheScope a key distinguishing this shard's blocks from another's
     * @param manifest the commit to expose, or null for an empty one
     * @return the directory
     * @throws java.io.IOException if it cannot be built
     */
    public static BlockCacheDirectory create(
        Directory local,
        BlobStore blobStore,
        BlobPath shardBase,
        BlockCache cache,
        String cacheScope,
        org.opensearch.serverless.store.CommitManifest manifest
    ) throws java.io.IOException {
        final var present = java.util.Optional.ofNullable(manifest);
        final Map<String, RemoteFile> files = new LinkedHashMap<>();

        if (present.isPresent()) {
            // The manifest's own lengths first, and a listing only for a file it does not record: a
            // manifest written before lengths were recorded, or a file inherited from one. Lucene needs
            // the length before it will read a byte, and an open used to list every term container.
            final Map<String, Map<String, Long>> lengthsByTerm = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : present.get().files().entrySet()) {
                final FileLocation location = present.get().location(entry.getKey());
                final String termDir = location.termDir();
                Long length = present.get().lengthOf(entry.getKey());
                if (length == null) {
                    final Map<String, Long> lengths = lengthsByTerm.computeIfAbsent(termDir, dir -> {
                        final Map<String, Long> found = new LinkedHashMap<>();
                        try {
                            blobStore.blobContainer(shardBase.add(dir)).listBlobs().forEach((name, meta) -> found.put(name, meta.length()));
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                        return found;
                    });
                    length = lengths.get(entry.getKey());
                }
                if (length != null) {
                    files.put(
                        entry.getKey(),
                        new RemoteFile(
                            blobStore.blobContainer(location.containerPath(shardBase)),
                            location.blob(),
                            length,
                            // A pack's name, not only its term: a cache keyed on the term and the file's name alone
                            // could serve another pack's bytes for a same-named file.
                            location.isPacked() ? location.referencedBlob() : termDir,
                            location.offset()
                        )
                    );
                }
            }
        }
        final java.util.concurrent.Executor prefetch = cache.prefetchExecutor();
        if (prefetch != null && files.isEmpty() == false) {
            // A file this node holds locally is read from there, so fetching it would be a request for nothing.
            final Set<String> held = new LinkedHashSet<>(java.util.Arrays.asList(local.listAll()));
            final Map<String, OpenPrefetch.Target> targets = new LinkedHashMap<>();
            for (Map.Entry<String, RemoteFile> entry : files.entrySet()) {
                if (held.contains(entry.getKey())) {
                    continue;
                }
                final RemoteFile file = entry.getValue();
                targets.put(
                    entry.getKey(),
                    new OpenPrefetch.Target(
                        file.container(),
                        file.blobName(),
                        keyPrefix(cacheScope, file.termDir(), entry.getKey()),
                        file.length(),
                        file.blobOffset()
                    )
                );
            }
            // Before the directory is handed out, so the open that follows reads from memory.
            OpenPrefetch.run(targets, cache, prefetch);
        }
        return new BlockCacheDirectory(local, files, cache, cacheScope);
    }

    /**
     * Returns the cache key prefix of every published file this directory reads from the object store: the files of
     * the commit it is open on, so a block under any other prefix belongs to a file merged away or deleted since.
     *
     * @return the prefixes, each a key up to its {@code #}
     */
    public Set<String> keyPrefixes() {
        final Set<String> prefixes = new LinkedHashSet<>();
        for (Map.Entry<String, RemoteFile> entry : remote.entrySet()) {
            prefixes.add(keyPrefix(cacheScope, entry.getValue().termDir(), entry.getKey()));
        }
        return prefixes;
    }

    /**
     * Fetches those of some blocks that belong to this shard's published files and are not held, in coalesced ranged
     * reads. Keys of other shards, of files no longer in this commit, or of files held locally are ignored.
     *
     * @param keys block cache keys
     * @param executor where the helper reads run
     * @return how many ranged reads were made
     * @throws IOException if the local files cannot be listed
     */
    public int fetchMissing(java.util.Collection<String> keys, java.util.concurrent.Executor executor) throws IOException {
        final Map<String, String> fileByPrefix = new java.util.HashMap<>();
        for (Map.Entry<String, RemoteFile> entry : remote.entrySet()) {
            fileByPrefix.put(keyPrefix(cacheScope, entry.getValue().termDir(), entry.getKey()), entry.getKey());
        }
        final Set<String> held = new LinkedHashSet<>(java.util.Arrays.asList(in.listAll()));
        final Map<OpenPrefetch.Target, java.util.TreeSet<Long>> wanted = new LinkedHashMap<>();
        final Map<String, OpenPrefetch.Target> targets = new java.util.HashMap<>();
        for (String key : keys) {
            final int hash = key.lastIndexOf('#');
            if (hash < 0) {
                continue;
            }
            final String name = fileByPrefix.get(key.substring(0, hash));
            if (name == null || held.contains(name)) {
                continue;
            }
            final long block;
            try {
                block = Long.parseLong(key.substring(hash + 1));
            } catch (NumberFormatException e) {
                continue;
            }
            final RemoteFile file = remote.get(name);
            final OpenPrefetch.Target target = targets.computeIfAbsent(
                name,
                n -> new OpenPrefetch.Target(file.container(), file.blobName(), key.substring(0, hash), file.length(), file.blobOffset())
            );
            if (block >= 0 && block * cache.blockSize() < file.length()) {
                wanted.computeIfAbsent(target, t -> new java.util.TreeSet<>()).add(block);
            }
        }
        return OpenPrefetch.fetchBlocks(wanted, cache, executor);
    }

    /**
     * Puts the small files of a commit this node just published into the block cache, from its own local copies, so
     * the next open of the commit on this node -- a hibernated writer waking, a writer taken back after release, a
     * reader -- reads them from memory rather than the object store.
     *
     * <p>Only files held locally, which are this node's own output: Lucene never writes a file name twice in a shard's
     * history, and the publisher uploaded exactly these bytes. A local file whose length is not the one the commit
     * records is skipped rather than trusted. Smallest first, up to a budget, so a large segment does not push the
     * metadata an open reads first out of the cache.
     *
     * @param manifest the commit just published from this directory
     * @param maxBytes the most to cache for it
     * @return the bytes put into the cache
     * @throws IOException if a local file cannot be read
     */
    public long warmFromLocal(org.opensearch.serverless.store.CommitManifest manifest, long maxBytes) throws IOException {
        // Only into room the cache has free: a block put into a full cache pushes out one somebody may be searching,
        // for a file that is read only if this shard opens again. Free room is the cache's own bound, so no more heap.
        final long free = (long) cache.maxBlocks() * cache.blockSize() - cache.bytesResident();
        maxBytes = Math.min(maxBytes, free);
        long cached = 0;
        for (Map.Entry<String, byte[]> block : localBlocks(manifest, manifest.files().keySet(), maxBytes).entrySet()) {
            cache.put(block.getKey(), block.getValue());
            cached += block.getValue().length;
        }
        return cached;
    }

    /**
     * Reads some files of a commit this node just published from its own local copies, as the block cache keys and
     * holds them on any node that opens the commit: a cache key names the shard's storage and the file, not the node.
     *
     * <p>Only files held locally, which are this node's own output: Lucene never writes a file name twice in a shard's
     * history, and the publisher uploaded exactly these bytes. A local file whose length is not the one the commit
     * records is skipped rather than trusted. Smallest first, whole files only, up to a budget.
     *
     * @param manifest the commit just published from this directory
     * @param names the files of it wanted
     * @param maxBytes the most bytes to read
     * @return the blocks read, by cache key, smallest file first
     * @throws IOException if a local file cannot be read
     */
    public Map<String, byte[]> localBlocks(
        org.opensearch.serverless.store.CommitManifest manifest,
        java.util.Collection<String> names,
        long maxBytes
    ) throws IOException {
        final Map<String, byte[]> blocks = new LinkedHashMap<>();
        if (maxBytes <= 0 || names.isEmpty()) {
            return blocks;
        }
        final Set<String> held = new LinkedHashSet<>(java.util.Arrays.asList(in.listAll()));
        final java.util.List<Map.Entry<String, Long>> candidates = new java.util.ArrayList<>();
        for (String name : names) {
            final Long length = manifest.lengthOf(name);
            if (length == null
                || manifest.files().containsKey(name) == false
                || held.contains(name) == false
                || in.fileLength(name) != length) {
                continue;
            }
            candidates.add(Map.entry(name, length));
        }
        candidates.sort(Map.Entry.comparingByValue());
        final int blockSize = cache.blockSize();
        long read = 0;
        for (Map.Entry<String, Long> candidate : candidates) {
            if (read + candidate.getValue() > maxBytes) {
                break;
            }
            final FileLocation location = manifest.location(candidate.getKey());
            final String prefix = keyPrefix(
                cacheScope,
                location.isPacked() ? location.referencedBlob() : location.termDir(),
                candidate.getKey()
            );
            try (IndexInput input = in.openInput(candidate.getKey(), IOContext.READONCE)) {
                final long length = candidate.getValue();
                for (long block = 0; block * blockSize < length; block++) {
                    final byte[] bytes = new byte[(int) Math.min(blockSize, length - block * blockSize)];
                    input.readBytes(bytes, 0, bytes.length);
                    blocks.put(prefix + "#" + block, bytes);
                }
            }
            read += candidate.getValue();
        }
        return blocks;
    }

    /**
     * Finds the block cache directory under a store's wrappers, or null if the store does not read through one.
     *
     * @param directory a shard store's directory
     * @return the block cache directory beneath it, or null
     */
    public static BlockCacheDirectory unwrap(Directory directory) {
        Directory current = directory;
        while (current != null) {
            if (current instanceof BlockCacheDirectory found) {
                return found;
            }
            current = current instanceof FilterDirectory filter ? filter.getDelegate() : null;
        }
        return null;
    }

    /**
     * The prefix of every cache key of one published file. The term directory is part of it: a same-named
     * file from another term is other bytes, and a cache that keyed on the name alone served them as this file's.
     */
    private static String keyPrefix(String cacheScope, String termDir, String name) {
        return cacheScope + "/" + termDir + "/" + name;
    }

    @Override
    public String[] listAll() throws IOException {
        final Set<String> all = new LinkedHashSet<>(java.util.Arrays.asList(in.listAll()));
        all.addAll(remote.keySet());
        final String[] names = all.toArray(new String[0]);
        java.util.Arrays.sort(names);
        return names;
    }

    @Override
    public long fileLength(String name) throws IOException {
        try {
            return in.fileLength(name);
        } catch (FileNotFoundException | java.nio.file.NoSuchFileException e) {
            final RemoteFile file = remote.get(name);
            if (file == null) {
                throw e;
            }
            return file.length();
        }
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        try {
            return in.openInput(name, context);
        } catch (FileNotFoundException | java.nio.file.NoSuchFileException e) {
            final RemoteFile file = remote.get(name);
            if (file == null) {
                throw e;
            }
            return new ObjectStoreIndexInput(
                "block-cache(" + cacheScope + "/" + name + ")",
                context,
                file.container(),
                cache,
                file.blobName(),
                keyPrefix(cacheScope, file.termDir(), name),
                file.length(),
                file.blobOffset()
            );
        }
    }

    @Override
    public void deleteFile(String name) throws IOException {
        if (remote.containsKey(name) && contains(in.listAll(), name) == false) {
            // Published, and not ours to remove. See this class's documentation.
            return;
        }
        in.deleteFile(name);
    }

    /**
     * Does nothing: the local directory is a cache, and nothing it holds is ever trusted after a crash.
     *
     * <p>What is durable is in the object store -- the log before a write is acknowledged, the segments before their
     * manifest -- and a shard opens from there, after deleting every local commit point and every file the manifest
     * names (see {@code ShardReconciler#dropLocalFilesNamedBy}). An fsync here bought nothing, and it was the most
     * expensive thing a publish did while holding the shard's exclusive guard: a fleet's thread dumps had a hundred
     * publish threads in fsync and every write to those shards queued behind them, at three seconds' median latency on
     * a store answering in a tenth of one.
     */
    @Override
    public void sync(java.util.Collection<String> names) {}

    /** Does nothing, for the reason {@link #sync} does nothing. */
    @Override
    public void syncMetaData() {}

    private static boolean contains(String[] names, String name) {
        for (String candidate : names) {
            if (candidate.equals(name)) {
                return true;
            }
        }
        return false;
    }

    /** Wraps a checked listing failure so it can escape a lambda. */
    private static final class UncheckedIOException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        UncheckedIOException(IOException cause) {
            super(cause);
        }
    }
}
