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

    private record RemoteFile(BlobContainer container, String blobName, long length) {
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
            // One listing per term container rather than a length lookup per file: the manifest records
            // where each file lives but not how long it is, and Lucene needs the length before it will
            // read a byte.
            final Map<String, Map<String, Long>> lengthsByTerm = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : present.get().files().entrySet()) {
                final String termDir = entry.getValue();
                final Map<String, Long> lengths = lengthsByTerm.computeIfAbsent(termDir, dir -> {
                    final Map<String, Long> found = new LinkedHashMap<>();
                    try {
                        blobStore.blobContainer(shardBase.add(dir)).listBlobs().forEach((name, meta) -> found.put(name, meta.length()));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return found;
                });
                final Long length = lengths.get(entry.getKey());
                if (length != null) {
                    files.put(entry.getKey(), new RemoteFile(blobStore.blobContainer(shardBase.add(termDir)), entry.getKey(), length));
                }
            }
        }
        return new BlockCacheDirectory(local, files, cache, cacheScope);
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
                cacheScope + "/" + name,
                file.length()
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

    @Override
    public void sync(java.util.Collection<String> names) throws IOException {
        final Set<String> local = new LinkedHashSet<>(java.util.Arrays.asList(in.listAll()));
        local.retainAll(names);
        in.sync(local);
    }

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
