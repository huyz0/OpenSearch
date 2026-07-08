/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.serverless.storage.security.EncryptionKeyProvider;

import java.io.IOException;

/**
 * Wraps a {@link WalChunkService} so every record appended through it is encrypted first via
 * {@link WalRecordCrypto} (rfc-serverless-opensearch.md &sect;12 bullet 1). {@link WalChunkService}
 * is {@code final} and has no interface to implement, so this is a delegating decorator rather
 * than a subclass -- the same "wrap it at the one seam that matters" shape as {@link
 * org.opensearch.serverless.storage.security.EncryptingBlobContainer}, just one level up: that
 * class encrypts whatever bytes a {@link org.opensearch.common.blobstore.BlobContainer} is asked
 * to store, this class decides *which* bytes (the payload, not the whole chunk) get encrypted
 * before they ever reach a {@link WalChunkService}, which stays completely unaware encryption is
 * happening at all -- it already treats {@code payload} as opaque, per {@link WalRecord}'s javadoc.
 *
 * <p>Reading a chunk back out (via {@link WalChunkReader#readRecords}) yields records whose
 * payload is still ciphertext; callers must pass the result through {@link
 * WalRecordCrypto#decryptAll} with the same key provider to get plaintext back. That decrypt step
 * lives on the read side deliberately, not mirrored here as an "decrypting chunk service" -- the
 * two call sites (append, and chunk replay) don't share enough shape to make a matching read-side
 * wrapper worth it, unlike write-time encryption which every append benefits from uniformly.
 */
public final class EncryptingWalChunkService {

    private final WalChunkService delegate;
    private final EncryptionKeyProvider keyProvider;

    public EncryptingWalChunkService(WalChunkService delegate, EncryptionKeyProvider keyProvider) {
        this.delegate = delegate;
        this.keyProvider = keyProvider;
    }

    public void append(WalRecord record) throws IOException {
        delegate.append(WalRecordCrypto.encrypt(record, keyProvider));
    }

    public long flush() throws IOException {
        return delegate.flush();
    }

    public int bufferedRecordCount() {
        return delegate.bufferedRecordCount();
    }
}
