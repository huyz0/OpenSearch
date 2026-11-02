/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.IOUtils;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Disposable local writes over the exact files of a pinned immutable Lucene commit. The supplied
 * pin must keep both the commit and its source directory alive. Ownership of the local directory
 * and pin transfers only on successful construction. Close requires all private native work to drain.
 * This is not a durable recovery base: its private deletions exist only in this process.
 */
final class PreparationOverlayDirectory extends FilterDirectory {
    /** Host-admitted bounds for retained file metadata and the disk bytes held by the source pin. */
    record Limits(int maximumFiles, long maximumMetadataBytes, long maximumPinnedBytes) {
        Limits {
            if (maximumFiles <= 0 || maximumMetadataBytes <= 0 || maximumPinnedBytes <= 0) {
                throw new IllegalArgumentException("invalid preparation snapshot limits");
            }
        }
    }

    private final Directory source;
    private final Set<String> baseFiles;
    private final Set<String> hiddenBaseFiles = ConcurrentHashMap.newKeySet();
    private final Closeable pin;
    private final AtomicBoolean closed = new AtomicBoolean();

    PreparationOverlayDirectory(Directory local, IndexCommit commit, Closeable pin, Limits limits) throws IOException {
        super(java.util.Objects.requireNonNull(local));
        java.util.Objects.requireNonNull(pin);
        java.util.Objects.requireNonNull(limits);
        source = java.util.Objects.requireNonNull(commit).getDirectory();
        if (FilterDirectory.unwrap(local) == FilterDirectory.unwrap(source)) {
            throw new IllegalArgumentException("preparation writes require a separate directory");
        }
        if (local.listAll().length != 0) {
            throw new IOException("preparation overlay requires an empty local directory");
        }
        final Collection<String> names = commit.getFileNames();
        if (names.size() > limits.maximumFiles()) {
            throw new IOException("preparation commit exceeds file count budget");
        }
        long metadataBytes = 128;
        long pinnedBytes = 0;
        // Check every bound before allocating retained name sets or copying any file data.
        for (String name : names) {
            final long charge = 128L + 2L * name.length();
            if (charge > limits.maximumMetadataBytes() - metadataBytes) {
                throw new IOException("preparation commit exceeds metadata budget");
            }
            metadataBytes += charge;
            final long length = source.fileLength(name);
            if (length < 0 || length > limits.maximumPinnedBytes() - pinnedBytes) {
                throw new IOException("preparation commit exceeds pinned disk budget");
            }
            pinnedBytes += length;
        }
        baseFiles = Set.copyOf(names);
        this.pin = pin;
    }

    @Override
    public String[] listAll() throws IOException {
        checkOpen();
        final var names = new TreeSet<String>();
        for (String name : baseFiles) {
            if (hiddenBaseFiles.contains(name) == false) {
                names.add(name);
            }
        }
        java.util.Collections.addAll(names, in.listAll());
        return names.toArray(String[]::new);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        checkOpen();
        try {
            return in.openInput(name, context);
        } catch (FileNotFoundException | NoSuchFileException missing) {
            requireVisibleBase(name, missing);
            return source.openInput(name, context);
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        checkOpen();
        try {
            return in.fileLength(name);
        } catch (FileNotFoundException | NoSuchFileException missing) {
            requireVisibleBase(name, missing);
            return source.fileLength(name);
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        checkOpen();
        final var output = in.createOutput(name, context);
        hideBase(name);
        return output;
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        checkOpen();
        // Delegate temp-name allocation/locking to the private directory, but never shadow a base file.
        for (int attempt = 0; attempt <= baseFiles.size(); attempt++) {
            final var output = in.createTempOutput(prefix, suffix, context);
            if (baseFiles.contains(output.getName()) == false) {
                return output;
            }
            final String name = output.getName();
            output.close();
            in.deleteFile(name);
        }
        throw new IOException("private temp allocation repeatedly collides with the pinned commit");
    }

    @Override
    public void deleteFile(String name) throws IOException {
        checkOpen();
        try {
            in.deleteFile(name);
        } catch (FileNotFoundException | NoSuchFileException missing) {
            requireVisibleBase(name, missing);
        }
        hideBase(name);
    }

    @Override
    public void rename(String sourceName, String destinationName) throws IOException {
        checkOpen();
        // Native writers rename their own outputs. Renaming a source commit file would require copying it.
        if (baseFiles.contains(destinationName) && hiddenBaseFiles.contains(destinationName) == false) {
            throw new java.nio.file.FileAlreadyExistsException(destinationName);
        }
        in.rename(sourceName, destinationName);
        hideBase(sourceName);
        hideBase(destinationName);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        checkOpen();
        final var local = new ArrayList<String>();
        for (String name : names) {
            try {
                in.fileLength(name);
                local.add(name);
            } catch (FileNotFoundException | NoSuchFileException missing) {
                requireVisibleBase(name, missing);
            }
        }
        in.sync(local);
    }

    @Override
    public void syncMetaData() throws IOException {
        checkOpen();
        in.syncMetaData();
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        checkOpen();
        return in.obtainLock(name);
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        checkOpen();
        return in.getPendingDeletions();
    }

    private void requireVisibleBase(String name, IOException missing) throws IOException {
        if (baseFiles.contains(name) == false || hiddenBaseFiles.contains(name)) {
            throw missing;
        }
    }

    private void hideBase(String name) {
        if (baseFiles.contains(name)) {
            hiddenBaseFiles.add(name);
        }
    }

    private void checkOpen() {
        if (closed.get()) {
            throw new AlreadyClosedException("preparation overlay is closed");
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            IOUtils.close(in, pin);
        }
    }
}
