/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone;

import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.List;

/**
 * The lazy-directory (reader-shard) counterpart of {@link FallbackBundleFileReader}, for the same
 * reason: a cloned shard's own {@code BlobContainer} cannot see bundles that still physically live
 * in the source shard's container. {@link org.opensearch.index.store.remote.utils.TransferManager}
 * is constructed around a single {@link TransferManager.StreamReader} function rather than an
 * object implementing an interface with multiple methods, so this wraps two of them (primary, then
 * source as fallback on {@link NoSuchFileException}) behind one, matching {@link
 * TransferManager.StreamReader}'s signature exactly so a method reference to {@link #read} can be
 * passed anywhere a {@code StreamReader} is expected -- see {@code
 * ServerlessStorageLazyDirectoryFactory} for where this is actually wired in.
 */
public final class FallbackStreamReader implements TransferManager.StreamReader {

    private final TransferManager.StreamReader primary;
    private final TransferManager.StreamReader fallback;

    /**
     * Wraps a primary reader with a source fallback.
     *
     * @param primary the cloned shard's own reader, tried first.
     * @param fallback the clone source's reader, tried only when {@code primary} reports the blob missing.
     */
    public FallbackStreamReader(TransferManager.StreamReader primary, TransferManager.StreamReader fallback) {
        this.primary = primary;
        this.fallback = fallback;
    }

    /**
     * Folds an ordered list of readers (own shard first, then each clone-lineage hop back to the
     * original) into one nested fallback chain -- e.g. {@code chain(List.of(a, b, c))} tries
     * {@code a}, then {@code b}, then {@code c}, matching {@link ShardCloner#resolveLineageChain}'s
     * read-priority ordering. See {@link FallbackBundleFileReader#chain} for the eager-materializer
     * counterpart and the same "clone of a clone" rationale.
     *
     * @param readers at least one reader; {@code readers.get(0)} is tried first.
     */
    public static TransferManager.StreamReader chain(List<TransferManager.StreamReader> readers) {
        TransferManager.StreamReader result = readers.get(readers.size() - 1);
        for (int i = readers.size() - 2; i >= 0; i--) {
            result = new FallbackStreamReader(readers.get(i), result);
        }
        return result;
    }

    /**
     * @param name the blob to read from.
     * @param position the starting offset within the blob.
     * @param length how many bytes to read.
     */
    @Override
    public InputStream read(String name, long position, long length) throws IOException {
        try {
            return primary.read(name, position, length);
        } catch (NoSuchFileException e) {
            return fallback.read(name, position, length);
        }
    }
}
