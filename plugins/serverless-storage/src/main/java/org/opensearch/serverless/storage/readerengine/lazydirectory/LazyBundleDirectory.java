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
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * A read-only Lucene {@link Directory} whose "files" are {@code (bundle, offset, length)} ranges
 * resolved lazily through a {@link TransferManager}-backed block cache, exactly as
 * rfc-serverless-opensearch.md &sect;7.2 describes: {@code createOutput}/{@code deleteFile} throw,
 * the directory holds a file map (from a {@link CommitManifest}), not file bytes. Opening one
 * requires no upfront I/O at all -- contrast {@code ObjectStoreCommitMaterializer}, which fetches
 * every referenced file in full before a directory becomes usable; this class defers every fetch
 * until Lucene actually asks for the bytes, and only for the specific blocks it asks for.
 *
 * <p>Standalone first slice, not yet wired into {@code ObjectStoreReaderEngine}'s {@code open()}
 * path (which still fully materializes) -- see rfc-serverless-opensearch.md &sect;7.2's status
 * note. Every read genuinely defers to and is cached by core's own searchable-snapshots
 * {@link org.opensearch.index.store.remote.filecache.FileCache}/{@link TransferManager}/{@link
 * org.opensearch.index.store.remote.file.AbstractBlockIndexInput} machinery -- reused directly,
 * not reimplemented, since it already solves exactly this "lazy block-cached remote file" problem
 * for a different caller (searchable snapshots' own {@code RemoteSnapshotDirectory}).
 */
public final class LazyBundleDirectory extends Directory {

    private final Map<String, FileReference> filesByName;
    private final FSDirectory cacheDirectory;
    private final TransferManager transferManager;
    private final Lock noOpLock = NoLockFactory.INSTANCE.obtainLock(null, null);

    public LazyBundleDirectory(CommitManifest manifest, FSDirectory cacheDirectory, TransferManager transferManager) {
        this.filesByName = manifest.files();
        this.cacheDirectory = cacheDirectory;
        this.transferManager = transferManager;
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
    public void close() {
        // Nothing local to this directory owns closeable resources -- the shared FileCache and
        // TransferManager outlive any single LazyBundleDirectory and are closed by whoever
        // constructed them, not by this class.
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
