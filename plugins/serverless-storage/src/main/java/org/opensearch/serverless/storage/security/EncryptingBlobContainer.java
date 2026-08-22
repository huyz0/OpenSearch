/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import org.opensearch.common.blobstore.BlobContainer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

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
public final class EncryptingBlobContainer extends RegisterDelegatingBlobContainer {

    /** 64 KiB: large enough that most bundle files fit in one or two blocks, small enough that a ranged read of a single file doesn't drag in unrelated data. */
    private static final int DEFAULT_BLOCK_SIZE_BYTES = 64 * 1024;

    private final EncryptionKeyProvider keyProvider;
    private final int blockSizeBytes;

    /**
     * Wraps a delegate container with block-level AES/GCM encryption at the default block size.
     *
     * @param delegate the underlying blob container to encrypt reads/writes against.
     * @param keyProvider supplies the key used for every encrypt/decrypt call.
     */
    public EncryptingBlobContainer(BlobContainer delegate, EncryptionKeyProvider keyProvider) {
        this(delegate, keyProvider, DEFAULT_BLOCK_SIZE_BYTES);
    }

    /**
     * Wraps a delegate container with block-level AES/GCM encryption at a given block size.
     *
     * @param delegate the underlying blob container to encrypt reads/writes against.
     * @param keyProvider supplies the key used for every encrypt/decrypt call.
     * @param blockSizeBytes plaintext bytes per block; package-visible so tests can use a small value to exercise multi-block logic cheaply.
     */
    EncryptingBlobContainer(BlobContainer delegate, EncryptionKeyProvider keyProvider, int blockSizeBytes) {
        super(delegate);
        this.keyProvider = keyProvider;
        this.blockSizeBytes = blockSizeBytes;
    }

    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        return new EncryptingBlobContainer(child, keyProvider, blockSizeBytes);
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        // A single unranged fetch of the whole ciphertext object, not readHeader() followed by
        // decryptRange()'s own separate ranged fetch -- those two calls together already cover
        // the entire object (the header's own bytes, immediately followed by every block's own
        // ciphertext, with no gap -- see BlockLayout#blockDiskOffset(0, ...) == HEADER_SIZE_BYTES),
        // so doing them as two requests instead of one doubled the GET cost of every full-object
        // read for no benefit. This is the manifest-read hot path whenever encryption is enabled
        // (BlobContainerManifestStore#readBlob always uses this no-range overload), so the doubled
        // cost was real, not theoretical.
        byte[] wholeObject = readAllAndClose(delegate.readBlob(blobName));
        BlockLayout.Header header = BlockLayout.decodeHeader(Arrays.copyOfRange(wholeObject, 0, BlockLayout.HEADER_SIZE_BYTES));
        byte[] blockCiphertext = Arrays.copyOfRange(wholeObject, BlockLayout.HEADER_SIZE_BYTES, wholeObject.length);
        int blockCount = BlockLayout.blockCount(header.blockSizeBytes(), header.totalPlaintextLength());
        return new ByteArrayInputStream(decryptBlockRange(blockCiphertext, header, 0, blockCount - 1));
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
        byte[] ciphertext = encryptBlocked(readAllAndClose(inputStream));
        delegate.writeBlob(blobName, new ByteArrayInputStream(ciphertext), ciphertext.length, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        byte[] ciphertext = encryptBlocked(readAllAndClose(inputStream));
        delegate.writeBlobAtomic(blobName, new ByteArrayInputStream(ciphertext), ciphertext.length, failIfAlreadyExists);
    }

    // The three writeBlob*WithMetadata overloads are not covered by writeBlob/writeBlobAtomic's own
    // overrides above -- FilterBlobContainer (this class's superclass's superclass) passes them
    // straight through to the delegate unchanged, which would write plaintext to the real object
    // store. Nothing in this plugin currently calls these (verified: zero callers under
    // plugins/serverless-storage/src/main/java), but leaving them unguarded would silently defeat
    // this class's entire purpose the moment something does -- so they encrypt exactly like
    // writeBlob/writeBlobAtomic, just forwarding the metadata/cryptoMetadata arguments unchanged.

    @Override
    public void writeBlobWithMetadata(
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists,
        java.util.Map<String, String> metadata
    ) throws IOException {
        byte[] ciphertext = encryptBlocked(readAllAndClose(inputStream));
        delegate.writeBlobWithMetadata(blobName, new ByteArrayInputStream(ciphertext), ciphertext.length, failIfAlreadyExists, metadata);
    }

    @Override
    public void writeBlobWithMetadata(
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists,
        java.util.Map<String, String> metadata,
        org.opensearch.cluster.metadata.CryptoMetadata cryptoMetadata
    ) throws IOException {
        byte[] ciphertext = encryptBlocked(readAllAndClose(inputStream));
        delegate.writeBlobWithMetadata(
            blobName,
            new ByteArrayInputStream(ciphertext),
            ciphertext.length,
            failIfAlreadyExists,
            metadata,
            cryptoMetadata
        );
    }

    @Override
    public void writeBlobAtomicWithMetadata(
        String blobName,
        InputStream inputStream,
        java.util.Map<String, String> metadata,
        long blobSize,
        boolean failIfAlreadyExists
    ) throws IOException {
        byte[] ciphertext = encryptBlocked(readAllAndClose(inputStream));
        delegate.writeBlobAtomicWithMetadata(
            blobName,
            new ByteArrayInputStream(ciphertext),
            metadata,
            ciphertext.length,
            failIfAlreadyExists
        );
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

        byte[] concatenated = decryptBlockRange(rawBlocks, header, firstBlock, lastBlock);
        int sliceStart = (int) (position - (long) firstBlock * blockSize);
        return Arrays.copyOfRange(concatenated, sliceStart, sliceStart + (int) length);
    }

    /**
     * Decrypts blocks {@code [firstBlock, lastBlock]} (inclusive) given their raw, contiguous
     * ciphertext bytes already in hand ({@code rawBlocks[0]} is the first byte of {@code
     * firstBlock}'s own ciphertext) -- shared by both {@link #readBlob(String)} (the whole object,
     * already fully fetched in one unranged call) and {@link #decryptRange} (just the blocks one
     * ranged sub-fetch needs). Neither caller does any additional I/O here.
     */
    private byte[] decryptBlockRange(byte[] rawBlocks, BlockLayout.Header header, int firstBlock, int lastBlock) throws IOException {
        int blockSize = header.blockSizeBytes();
        long totalLength = header.totalPlaintextLength();
        ByteArrayOutputStream decryptedBlocks = new ByteArrayOutputStream();
        int cursor = 0;
        for (int blockIndex = firstBlock; blockIndex <= lastBlock; blockIndex++) {
            int cipherLen = BlockLayout.blockCiphertextLength(blockIndex, blockSize, totalLength);
            byte[] blockCiphertext = Arrays.copyOfRange(rawBlocks, cursor, cursor + cipherLen);
            decryptedBlocks.write(AesGcmCipher.decrypt(blockCiphertext, keyProvider.currentKey()));
            cursor += cipherLen;
        }
        return decryptedBlocks.toByteArray();
    }
}
