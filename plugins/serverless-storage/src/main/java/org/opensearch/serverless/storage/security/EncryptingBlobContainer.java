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
import java.io.ByteArrayOutputStream;
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
 * <p>Ciphertext format: a fixed header ({@link BlockLayout}) followed by a sequence of
 * independently-encrypted, independently-authenticated fixed-size blocks -- see {@link
 * BlockLayout}'s javadoc for the exact byte layout. Each block is its own {@link AesGcmCipher}
 * envelope with a fresh random IV and its own GCM tag, so a corrupted or tampered block fails to
 * decrypt loudly rather than silently, exactly like the previous single-envelope-per-blob format
 * did -- the difference is granularity, not the authentication guarantee.
 *
 * <p><b>Ranged reads are now genuinely partial</b>, closing what was previously a known, deliberate
 * tradeoff: {@link #readBlob(String, long, long)} ({@code BlobContainerBundleStore}'s mechanism for
 * fetching just one file's bytes out of a bundle) fetches a small header, then computes exactly
 * which blocks overlap the requested range and reads *only* that contiguous byte span from the
 * delegate -- never the whole blob -- before decrypting just those blocks and slicing out the exact
 * requested sub-range. A read that happens to need only one block only ever decrypts that one
 * block. This is why {@link org.opensearch.serverless.storage.format.LocalDiskCachingBundleStore}
 * still matters even now: it avoids the network fetch and the header round-trip entirely on a
 * cache hit, which this class's per-request block fetch does not.
 *
 * <p>{@code readRegister}/{@code compareAndSwapRegister} are deliberately passed through
 * unencrypted: shard-head/pin registers contain no document data, only node ids, terms, and
 * generation numbers, and encrypting them would need the same CAS-safe whole-value treatment
 * {@code BlobContainerShardStateStore} already gives their *plaintext* today; segment and WAL data
 * (this class's actual target) carry the sensitive payload.
 */
public final class EncryptingBlobContainer extends FilterBlobContainer {

    /** 64 KiB: large enough that most bundle files fit in one or two blocks, small enough that a ranged read of a single file doesn't drag in unrelated data. */
    private static final int DEFAULT_BLOCK_SIZE_BYTES = 64 * 1024;

    private final BlobContainer delegate;
    private final EncryptionKeyProvider keyProvider;
    private final int blockSizeBytes;

    public EncryptingBlobContainer(BlobContainer delegate, EncryptionKeyProvider keyProvider) {
        this(delegate, keyProvider, DEFAULT_BLOCK_SIZE_BYTES);
    }

    /** @param blockSizeBytes plaintext bytes per block; package-visible so tests can use a small value to exercise multi-block logic cheaply. */
    EncryptingBlobContainer(BlobContainer delegate, EncryptionKeyProvider keyProvider, int blockSizeBytes) {
        super(delegate);
        this.delegate = delegate;
        this.keyProvider = keyProvider;
        this.blockSizeBytes = blockSizeBytes;
    }

    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        return new EncryptingBlobContainer(child, keyProvider, blockSizeBytes);
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        BlockLayout.Header header = readHeader(blobName);
        return new ByteArrayInputStream(decryptRange(blobName, header, 0, header.totalPlaintextLength()));
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        BlockLayout.Header header = readHeader(blobName);
        if (position < 0 || position > header.totalPlaintextLength()) {
            throw new IOException("range start " + position + " out of bounds for blob of length " + header.totalPlaintextLength());
        }
        long clampedLength = Math.min(length, header.totalPlaintextLength() - position);
        return new ByteArrayInputStream(decryptRange(blobName, header, position, clampedLength));
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        byte[] ciphertext = encryptBlocked(inputStream.readAllBytes());
        delegate.writeBlob(blobName, new ByteArrayInputStream(ciphertext), ciphertext.length, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        byte[] ciphertext = encryptBlocked(inputStream.readAllBytes());
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

    private byte[] encryptBlocked(byte[] plaintext) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(BlockLayout.encodeHeader(new BlockLayout.Header(blockSizeBytes, plaintext.length)));
        int blockCount = BlockLayout.blockCount(blockSizeBytes, plaintext.length);
        for (int blockIndex = 0; blockIndex < blockCount; blockIndex++) {
            int start = blockIndex * blockSizeBytes;
            int len = BlockLayout.blockPlaintextLength(blockIndex, blockSizeBytes, plaintext.length);
            byte[] blockPlaintext = Arrays.copyOfRange(plaintext, start, start + len);
            out.write(AesGcmCipher.encrypt(blockPlaintext, keyProvider.currentKey()));
        }
        return out.toByteArray();
    }

    private BlockLayout.Header readHeader(String blobName) throws IOException {
        byte[] headerBytes = readAllAndClose(delegate.readBlob(blobName, 0, BlockLayout.HEADER_SIZE_BYTES));
        return BlockLayout.decodeHeader(headerBytes);
    }

    /**
     * Fetches and decrypts exactly the blocks overlapping plaintext range {@code [position,
     * position + length)}, in one additional ranged read of the delegate (the blocks a range needs
     * are always contiguous on disk, so this never issues more than one fetch beyond the header
     * read), then slices out precisely the requested sub-range.
     */
    private byte[] decryptRange(String blobName, BlockLayout.Header header, long position, long length) throws IOException {
        if (length == 0) {
            return new byte[0];
        }
        int blockSize = header.blockSizeBytes();
        long totalLength = header.totalPlaintextLength();
        int firstBlock = BlockLayout.blockIndexFor(position, blockSize);
        int lastBlock = BlockLayout.blockIndexFor(position + length - 1, blockSize);

        long diskStart = BlockLayout.blockDiskOffset(firstBlock, blockSize);
        long diskEnd = BlockLayout.blockDiskOffset(lastBlock, blockSize) + BlockLayout.blockCiphertextLength(
            lastBlock,
            blockSize,
            totalLength
        );
        byte[] rawBlocks = readAllAndClose(delegate.readBlob(blobName, diskStart, diskEnd - diskStart));

        ByteArrayOutputStream decryptedBlocks = new ByteArrayOutputStream();
        int cursor = 0;
        for (int blockIndex = firstBlock; blockIndex <= lastBlock; blockIndex++) {
            int cipherLen = BlockLayout.blockCiphertextLength(blockIndex, blockSize, totalLength);
            byte[] blockCiphertext = Arrays.copyOfRange(rawBlocks, cursor, cursor + cipherLen);
            decryptedBlocks.write(AesGcmCipher.decrypt(blockCiphertext, keyProvider.currentKey()));
            cursor += cipherLen;
        }

        byte[] concatenated = decryptedBlocks.toByteArray();
        int sliceStart = (int) (position - (long) firstBlock * blockSize);
        return Arrays.copyOfRange(concatenated, sliceStart, sliceStart + (int) length);
    }
}
