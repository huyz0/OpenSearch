/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;

/**
 * Encrypts blob content at rest, transparently to every caller that only knows {@link
 * BlobContainer} (rfc-serverless-opensearch.md's security/encryption gap): wrapping a real
 * container in this one, anywhere a {@link BlobContainer} is expected, is the entire integration
 * -- {@code BlobContainerBundleStore}, {@code BlobContainerManifestStore}, {@code
 * BlobContainerShardStateStore}, and the WAL chunk writer all work unchanged, exactly because they
 * were already written against the interface, not a concrete backend.
 *
 * <p>Ciphertext format per blob: {@code <12-byte random IV><AES/GCM ciphertext+16-byte tag>}. Each
 * write generates a fresh random IV; GCM's tag gives authenticity (a corrupted or tampered blob
 * fails to decrypt loudly) on top of confidentiality, which the plain CRC32C checksums used
 * elsewhere in this module do not provide.
 *
 * <p><b>Known, deliberate tradeoff:</b> encryption is applied to the whole blob, so a ranged read
 * ({@link #readBlob(String, long, long)}, the mechanism {@code BlobContainerBundleStore} uses to
 * fetch just one file's bytes out of a bundle without downloading the whole thing) still has to
 * fetch and decrypt the *entire* blob before slicing out the requested range in memory -- doing
 * this correctly with true partial-range decryption requires a seekable cipher mode (AES/CTR) with
 * careful block-offset bookkeeping, which is real additional work not done here. This is why
 * {@link org.opensearch.serverless.storage.format.LocalDiskCachingBundleStore} (the directory
 * tier) matters more, not less, once encryption is enabled: once a bundle's plaintext is cached
 * locally, the ranged-read cost of encryption is paid at most once per bundle per node, not once
 * per file per query.
 *
 * <p>{@code readRegister}/{@code compareAndSwapRegister} are deliberately passed through
 * unencrypted: shard-head/pin registers contain no document content, only node ids, terms, and
 * generation numbers, and encrypting them would need the same CAS-safe whole-value treatment
 * {@code BlobContainerShardStateStore} already gives their *plaintext* today; segment and WAL data
 * (this class's actual target) carry the sensitive payload.
 */
public final class EncryptingBlobContainer extends FilterBlobContainer {

    private final BlobContainer delegate;
    private final EncryptionKeyProvider keyProvider;

    public EncryptingBlobContainer(BlobContainer delegate, EncryptionKeyProvider keyProvider) {
        super(delegate);
        this.delegate = delegate;
        this.keyProvider = keyProvider;
    }

    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        return new EncryptingBlobContainer(child, keyProvider);
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        byte[] ciphertext = readAllAndClose(delegate.readBlob(blobName));
        return new ByteArrayInputStream(decrypt(ciphertext));
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        // See the class javadoc's known tradeoff: this fetches and decrypts the whole blob, then
        // slices, rather than truly reading only [position, position+length) off the wire.
        byte[] plaintext = decrypt(readAllAndClose(delegate.readBlob(blobName)));
        if (position < 0 || position > plaintext.length) {
            throw new IOException("range start " + position + " out of bounds for blob of length " + plaintext.length);
        }
        int end = (int) Math.min(plaintext.length, position + length);
        return new ByteArrayInputStream(Arrays.copyOfRange(plaintext, (int) position, end));
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        byte[] ciphertext = encrypt(inputStream.readAllBytes());
        delegate.writeBlob(blobName, new ByteArrayInputStream(ciphertext), ciphertext.length, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        byte[] ciphertext = encrypt(inputStream.readAllBytes());
        delegate.writeBlobAtomic(blobName, new ByteArrayInputStream(ciphertext), ciphertext.length, failIfAlreadyExists);
    }

    @Override
    public Optional<BlobRegister> readRegister(String blobName) throws IOException {
        return delegate.readRegister(blobName);
    }

    @Override
    public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
        throws IOException {
        return delegate.compareAndSwapRegister(blobName, expectedGeneration, newValue);
    }

    private static byte[] readAllAndClose(InputStream in) throws IOException {
        try (InputStream toClose = in) {
            return toClose.readAllBytes();
        }
    }

    private byte[] encrypt(byte[] plaintext) throws IOException {
        return AesGcmCipher.encrypt(plaintext, keyProvider.currentKey());
    }

    private byte[] decrypt(byte[] ivAndCiphertext) throws IOException {
        return AesGcmCipher.decrypt(ivAndCiphertext, keyProvider.currentKey());
    }
}
