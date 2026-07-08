/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

/**
 * Pure byte-offset arithmetic for {@link EncryptingBlobContainer}'s seekable block format -- no
 * I/O, no crypto, just "given this header, where on disk does block N's ciphertext live, and how
 * long is it." Kept separate from {@link EncryptingBlobContainer} so this math (the part a bug in
 * would silently corrupt or misalign reads) is independently unit-testable without needing a real
 * {@link org.opensearch.common.blobstore.BlobContainer} or any actual encryption.
 *
 * <p>Wire format (big-endian): a fixed {@value #HEADER_SIZE_BYTES}-byte header --
 * <pre>
 *   magic                 4 bytes   = 'E','B','C','1'
 *   formatVersion         4 bytes   int, currently 1
 *   blockSizeBytes        4 bytes   int, plaintext bytes per block (except possibly the last)
 *   totalPlaintextLength  8 bytes   long
 * </pre>
 * followed by however many blocks {@link #blockCount} says, each block being one independent
 * {@link AesGcmCipher} envelope ({@code <12-byte IV><ciphertext><16-byte tag>}) of that block's
 * plaintext -- see {@link #blockDiskOffset} and {@link #blockCiphertextLength} for how to find and
 * size any one of them without reading any other. Every block except the last holds exactly
 * {@code blockSizeBytes} plaintext bytes, so the disk offset of block N is a fixed stride
 * multiplication, never a scan.
 */
final class BlockLayout {

    private static final byte[] MAGIC = { 'E', 'B', 'C', '1' };
    private static final int FORMAT_VERSION = 1;
    static final int HEADER_SIZE_BYTES = 4 + 4 + 4 + 8;
    static final int GCM_IV_LENGTH_BYTES = 12;
    static final int GCM_TAG_LENGTH_BYTES = 16;
    private static final int GCM_OVERHEAD_BYTES = GCM_IV_LENGTH_BYTES + GCM_TAG_LENGTH_BYTES;

    private BlockLayout() {}

    record Header(int blockSizeBytes, long totalPlaintextLength) {
        Header {
            if (blockSizeBytes <= 0) {
                throw new IllegalArgumentException("blockSizeBytes must be > 0, got " + blockSizeBytes);
            }
            if (totalPlaintextLength < 0) {
                throw new IllegalArgumentException("totalPlaintextLength must be >= 0, got " + totalPlaintextLength);
            }
        }
    }

    static byte[] encodeHeader(Header header) {
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream(HEADER_SIZE_BYTES);
            DataOutputStream out = new DataOutputStream(buf);
            out.write(MAGIC);
            out.writeInt(FORMAT_VERSION);
            out.writeInt(header.blockSizeBytes());
            out.writeLong(header.totalPlaintextLength());
            return buf.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static Header decodeHeader(byte[] headerBytes) throws IOException {
        if (headerBytes.length < HEADER_SIZE_BYTES) {
            throw new IOException("truncated block-encrypted header: " + headerBytes.length + " bytes, need " + HEADER_SIZE_BYTES);
        }
        ByteBuffer buf = ByteBuffer.wrap(headerBytes);
        byte[] magic = new byte[MAGIC.length];
        buf.get(magic);
        for (int i = 0; i < MAGIC.length; i++) {
            if (magic[i] != MAGIC[i]) {
                throw new IOException("not a block-encrypted blob: bad magic header");
            }
        }
        int version = buf.getInt();
        if (version != FORMAT_VERSION) {
            throw new IOException("unsupported block-encrypted format version " + version);
        }
        int blockSizeBytes = buf.getInt();
        long totalPlaintextLength = buf.getLong();
        return new Header(blockSizeBytes, totalPlaintextLength);
    }

    /** How many blocks a plaintext of {@code totalPlaintextLength} bytes is split into (0 for an empty blob). */
    static int blockCount(int blockSizeBytes, long totalPlaintextLength) {
        if (totalPlaintextLength == 0) {
            return 0;
        }
        return (int) ((totalPlaintextLength + blockSizeBytes - 1) / blockSizeBytes);
    }

    /** Which block index a plaintext byte offset falls into. */
    static int blockIndexFor(long plaintextOffset, int blockSizeBytes) {
        return (int) (plaintextOffset / blockSizeBytes);
    }

    /** How many plaintext bytes block {@code blockIndex} holds -- {@code blockSizeBytes} for every block but the last. */
    static int blockPlaintextLength(int blockIndex, int blockSizeBytes, long totalPlaintextLength) {
        long blockStart = (long) blockIndex * blockSizeBytes;
        long remaining = totalPlaintextLength - blockStart;
        if (remaining <= 0) {
            throw new IllegalArgumentException("blockIndex " + blockIndex + " is out of range for length " + totalPlaintextLength);
        }
        return (int) Math.min(blockSizeBytes, remaining);
    }

    /** Ciphertext-on-disk length of block {@code blockIndex}: its plaintext length plus the fixed IV+tag overhead. */
    static int blockCiphertextLength(int blockIndex, int blockSizeBytes, long totalPlaintextLength) {
        return blockPlaintextLength(blockIndex, blockSizeBytes, totalPlaintextLength) + GCM_OVERHEAD_BYTES;
    }

    /**
     * Byte offset on disk (relative to the start of the blob, i.e. including the header) where
     * block {@code blockIndex}'s ciphertext begins. A fixed stride multiplication -- valid even for
     * a block past the end of the data, which callers must guard against separately (this class has
     * no opinion on bounds; {@link EncryptingBlobContainer} enforces those).
     */
    static long blockDiskOffset(int blockIndex, int blockSizeBytes) {
        long strideBytes = (long) blockSizeBytes + GCM_OVERHEAD_BYTES;
        return HEADER_SIZE_BYTES + (long) blockIndex * strideBytes;
    }
}
