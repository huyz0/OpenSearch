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

    public FallbackStreamReader(TransferManager.StreamReader primary, TransferManager.StreamReader fallback) {
        this.primary = primary;
        this.fallback = fallback;
    }

    @Override
    public InputStream read(String name, long position, long length) throws IOException {
        try {
            return primary.read(name, position, length);
        } catch (NoSuchFileException e) {
            return fallback.read(name, position, length);
        }
    }
}
