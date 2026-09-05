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
 *   formatVersion         4 bytes   int, 1 (legacy, no AAD) or 2 (current)
 *   blockSizeBytes        4 bytes   int, plaintext bytes per block (except possibly the last)
 *   totalPlaintextLength  8 bytes   long
 * </pre>
 * followed by however many blocks {@link #blockCount} says, each block being one independent
 * {@link AesGcmCipher} envelope ({@code <12-byte IV><ciphertext><16-byte tag>}) of that block's
 * plaintext -- see {@link #blockDiskOffset} and {@link #blockCiphertextLength} for how to find and
 * size any one of them without reading any other. Every block except the last holds exactly
 * {@code blockSizeBytes} plaintext bytes, so the disk offset of block N is a fixed stride
 * multiplication, never a scan.
 *
 * <h2>Why there is a version 2, and what version 1 could not defend</h2>
 *
 * <p>This header is written in the clear -- it has to be, since a reader needs {@code
 * blockSizeBytes} and {@code totalPlaintextLength} before it can locate, let alone decrypt, any
 * block. In {@link #FORMAT_VERSION_LEGACY_NO_AAD version 1} it was also covered by <em>nothing</em>:
 * no tag anywhere in the blob authenticated it. Two consequences followed directly, neither of
 * which any amount of care inside {@link EncryptingBlobContainer} could have caught:
 *
 * <ul>
 *   <li><b>Truncation is undetectable.</b> Lower {@code totalPlaintextLength}, drop the trailing
 *       blocks, and every remaining block still decrypts and authenticates. The blob simply reads
 *       as a shorter, entirely valid file. For a commit manifest -- which is a <em>control</em>
 *       input naming which bundles a shard reads -- that is a silent rewrite of what the shard
 *       contains.</li>
 *   <li><b>Header-driven allocation.</b> {@code blockSizeBytes} and {@code totalPlaintextLength}
 *       size the reader's own buffers before a single tag has been checked, so a forged header is
 *       an allocation primitive. {@link #MAX_BLOCK_SIZE_BYTES} bounds the first half of that
 *       directly (see {@link #decodeHeader}); the second half is bounded by the AAD below, because
 *       a forged length no longer decrypts at all.</li>
 * </ul>
 *
 * <p>Version 2 fixes both by feeding the header's own fields into each block's GCM associated data
 * (see {@code EncryptingBlobContainer#associatedDataFor}), so the header is authenticated by every
 * block that was written under it, for free and without adding a byte to the blob. Reading version
 * 1 is still supported, and is still exactly as weak as it always was -- see {@link
 * EncryptingBlobContainer}'s javadoc for the migration path off it and for the node setting that
 * refuses it once migration is done.
 */
final class BlockLayout {

    private static final byte[] MAGIC = { 'E', 'B', 'C', '1' };

    /**
     * The version every blob written by this build carries: each block's GCM tag covers associated
     * data that binds it to its index, shard, blob name, block index, and this header's own
     * declared sizes.
     */
    static final int FORMAT_VERSION = 2;

    /**
     * The version written before associated data existed: bare {@code IV||ct||tag} per block, an
     * unauthenticated header, and therefore blocks that are freely interchangeable between blobs
     * under one key. Still readable (see {@link EncryptingBlobContainer}), never written.
     */
    static final int FORMAT_VERSION_LEGACY_NO_AAD = 1;

    /**
     * The largest {@code blockSizeBytes} {@link #decodeHeader} will accept from a blob.
     *
     * <p>The header arrives from the object store before anything has been authenticated, and its
     * {@code blockSizeBytes} is what sizes the reader's per-block buffers. The record's own compact
     * constructor only rejects {@code <= 0}, so {@code Integer.MAX_VALUE} passed it and became a
     * 2 GiB allocation attempt driven entirely by attacker-controlled bytes. 16 MiB is two orders
     * of magnitude above the 64 KiB this plugin actually writes, so it constrains nothing real
     * while removing the primitive. Enforced only on decode, deliberately: the encode side's block
     * size comes from configuration, not from the network, and bounding a trusted value in the same
     * place as an untrusted one hides which one was the risk.
     */
    static final int MAX_BLOCK_SIZE_BYTES = 16 * 1024 * 1024;

    static final int HEADER_SIZE_BYTES = 4 + 4 + 4 + 8;
    static final int GCM_IV_LENGTH_BYTES = 12;
    static final int GCM_TAG_LENGTH_BYTES = 16;
    private static final int GCM_OVERHEAD_BYTES = GCM_IV_LENGTH_BYTES + GCM_TAG_LENGTH_BYTES;

    private BlockLayout() {}

    record Header(int formatVersion, int blockSizeBytes, long totalPlaintextLength) {
        Header {
            if (blockSizeBytes <= 0) {
                throw new IllegalArgumentException("blockSizeBytes must be > 0, got " + blockSizeBytes);
            }
            if (totalPlaintextLength < 0) {
                throw new IllegalArgumentException("totalPlaintextLength must be >= 0, got " + totalPlaintextLength);
            }
        }

        /** A header for a blob being written now, i.e. at {@link #FORMAT_VERSION}. */
        Header(int blockSizeBytes, long totalPlaintextLength) {
            this(FORMAT_VERSION, blockSizeBytes, totalPlaintextLength);
        }

        /** Whether blocks under this header carry associated data -- false only for {@link #FORMAT_VERSION_LEGACY_NO_AAD}. */
        boolean authenticatesAssociatedData() {
            return formatVersion >= FORMAT_VERSION;
        }
    }

    static byte[] encodeHeader(Header header) {
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream(HEADER_SIZE_BYTES);
            DataOutputStream out = new DataOutputStream(buf);
            out.write(MAGIC);
            out.writeInt(header.formatVersion());
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
        if (version != FORMAT_VERSION && version != FORMAT_VERSION_LEGACY_NO_AAD) {
            throw new IOException("unsupported block-encrypted format version " + version);
        }
        int blockSizeBytes = buf.getInt();
        // Every field below this point is attacker-controlled if the object store is. Bound the one
        // that sizes an allocation before any tag has been checked; totalPlaintextLength needs no
        // separate bound at version 2 because it is inside every block's AAD, so a forged value
        // fails the very first block's tag rather than being trusted.
        if (blockSizeBytes > MAX_BLOCK_SIZE_BYTES) {
            throw new IOException(
                "block-encrypted header declares blockSizeBytes " + blockSizeBytes + ", above the maximum " + MAX_BLOCK_SIZE_BYTES
            );
        }
        long totalPlaintextLength = buf.getLong();
        return new Header(version, blockSizeBytes, totalPlaintextLength);
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
