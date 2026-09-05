/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class BlockLayoutTests extends OpenSearchTestCase {

    public void testHeaderRoundTripsExactly() throws Exception {
        BlockLayout.Header header = new BlockLayout.Header(1024, 5000);
        byte[] encoded = BlockLayout.encodeHeader(header);
        assertEquals(BlockLayout.HEADER_SIZE_BYTES, encoded.length);
        BlockLayout.Header decoded = BlockLayout.decodeHeader(encoded);
        assertEquals(header, decoded);
    }

    public void testDecodeRejectsBadMagic() {
        byte[] bad = new byte[BlockLayout.HEADER_SIZE_BYTES];
        expectThrows(IOException.class, () -> BlockLayout.decodeHeader(bad));
    }

    public void testDecodeRejectsTruncatedHeader() {
        byte[] tooShort = new byte[BlockLayout.HEADER_SIZE_BYTES - 1];
        expectThrows(IOException.class, () -> BlockLayout.decodeHeader(tooShort));
    }

    public void testHeaderConstructorRejectsInvalidArguments() {
        expectThrows(IllegalArgumentException.class, () -> new BlockLayout.Header(0, 100));
        expectThrows(IllegalArgumentException.class, () -> new BlockLayout.Header(-1, 100));
        expectThrows(IllegalArgumentException.class, () -> new BlockLayout.Header(1024, -1));
    }

    public void testBlockCountForAnEmptyBlobIsZero() {
        assertEquals(0, BlockLayout.blockCount(1024, 0));
    }

    public void testBlockCountForExactMultipleOfBlockSize() {
        assertEquals(3, BlockLayout.blockCount(100, 300));
    }

    public void testBlockCountRoundsUpForAPartialFinalBlock() {
        assertEquals(3, BlockLayout.blockCount(100, 250));
        assertEquals(1, BlockLayout.blockCount(100, 1));
    }

    public void testBlockIndexForFindsTheContainingBlock() {
        assertEquals(0, BlockLayout.blockIndexFor(0, 100));
        assertEquals(0, BlockLayout.blockIndexFor(99, 100));
        assertEquals(1, BlockLayout.blockIndexFor(100, 100));
        assertEquals(2, BlockLayout.blockIndexFor(250, 100));
    }

    public void testBlockPlaintextLengthIsFullSizeForNonFinalBlocks() {
        assertEquals(100, BlockLayout.blockPlaintextLength(0, 100, 250));
        assertEquals(100, BlockLayout.blockPlaintextLength(1, 100, 250));
    }

    public void testBlockPlaintextLengthIsTheRemainderForTheFinalBlock() {
        assertEquals(50, BlockLayout.blockPlaintextLength(2, 100, 250));
    }

    public void testBlockPlaintextLengthForAnExactMultipleFinalBlockIsFullSize() {
        assertEquals(100, BlockLayout.blockPlaintextLength(2, 100, 300));
    }

    public void testBlockPlaintextLengthRejectsAnOutOfRangeBlockIndex() {
        expectThrows(IllegalArgumentException.class, () -> BlockLayout.blockPlaintextLength(3, 100, 250));
    }

    public void testBlockCiphertextLengthAddsTheFixedGcmOverhead() {
        int overhead = BlockLayout.GCM_IV_LENGTH_BYTES + BlockLayout.GCM_TAG_LENGTH_BYTES;
        assertEquals(100 + overhead, BlockLayout.blockCiphertextLength(0, 100, 250));
        assertEquals(50 + overhead, BlockLayout.blockCiphertextLength(2, 100, 250));
    }

    public void testBlockDiskOffsetsAreContiguousFixedStrides() {
        int blockSize = 100;
        int overhead = BlockLayout.GCM_IV_LENGTH_BYTES + BlockLayout.GCM_TAG_LENGTH_BYTES;
        long stride = blockSize + overhead;

        long offset0 = BlockLayout.blockDiskOffset(0, blockSize);
        long offset1 = BlockLayout.blockDiskOffset(1, blockSize);
        long offset2 = BlockLayout.blockDiskOffset(2, blockSize);

        assertEquals(BlockLayout.HEADER_SIZE_BYTES, offset0);
        assertEquals(offset0 + stride, offset1);
        assertEquals(offset1 + stride, offset2);
    }

    // ------------------------------------------------------------------------------------------
    // Format version 2 and the header bound. Both are F-5/F-6: the header is the one part of the
    // blob a reader must trust before it has authenticated anything, so what it is allowed to say
    // is a security question, not a parsing one.
    // ------------------------------------------------------------------------------------------

    /** Everything written now is version 2, whether or not the caller mentioned a version. */
    public void testHeadersAreWrittenAtTheCurrentFormatVersion() throws Exception {
        BlockLayout.Header header = new BlockLayout.Header(1024, 5000);
        assertEquals(BlockLayout.FORMAT_VERSION, header.formatVersion());
        assertTrue(header.authenticatesAssociatedData());
        assertEquals(BlockLayout.FORMAT_VERSION, BlockLayout.decodeHeader(BlockLayout.encodeHeader(header)).formatVersion());
    }

    /**
     * Version 1 still decodes, and reports that its blocks carry no associated data -- which is what
     * {@code EncryptingBlobContainer} keys the legacy read path off. Refusing it outright would turn
     * every upgrade into a data-loss event; accepting it without <em>knowing</em> it is version 1
     * would silently read a v1 blob with v2 rules and fail every tag.
     */
    public void testLegacyVersionOneHeadersStillDecodeAndSayTheyAreUnauthenticated() throws Exception {
        BlockLayout.Header legacy = new BlockLayout.Header(BlockLayout.FORMAT_VERSION_LEGACY_NO_AAD, 1024, 5000);
        BlockLayout.Header decoded = BlockLayout.decodeHeader(BlockLayout.encodeHeader(legacy));
        assertEquals(BlockLayout.FORMAT_VERSION_LEGACY_NO_AAD, decoded.formatVersion());
        assertFalse("a version 1 block was written with no associated data", decoded.authenticatesAssociatedData());
    }

    /** A version from the future is still refused: only 1 and 2 have defined read rules. */
    public void testDecodeRejectsAnUnknownFormatVersion() throws Exception {
        byte[] encoded = BlockLayout.encodeHeader(new BlockLayout.Header(1024, 5000));
        java.nio.ByteBuffer.wrap(encoded).putInt(4, 99);
        IOException e = expectThrows(IOException.class, () -> BlockLayout.decodeHeader(encoded));
        assertTrue(e.getMessage().contains("unsupported block-encrypted format version 99"));
    }

    /**
     * F-6: the header sizes the reader's buffers before any tag has been checked, so a forged
     * {@code blockSizeBytes} was an allocation primitive. The record's own constructor only rejected
     * values {@code <= 0}, which let {@code Integer.MAX_VALUE} through as a 2 GB allocation driven
     * entirely by attacker-controlled bytes.
     */
    public void testDecodeRejectsAnAbsurdBlockSize() throws Exception {
        byte[] encoded = BlockLayout.encodeHeader(new BlockLayout.Header(1024, 5000));
        java.nio.ByteBuffer.wrap(encoded).putInt(8, Integer.MAX_VALUE);
        IOException e = expectThrows(IOException.class, () -> BlockLayout.decodeHeader(encoded));
        assertTrue("the message must name the bound: " + e.getMessage(), e.getMessage().contains("above the maximum"));
    }

    /** The bound is two orders of magnitude above the 64 KiB actually written, so it constrains nothing real. */
    public void testTheBlockSizeBoundStillAllowsEveryRealisticBlockSize() throws Exception {
        for (int blockSize : new int[] { 1, 64 * 1024, BlockLayout.MAX_BLOCK_SIZE_BYTES }) {
            byte[] encoded = BlockLayout.encodeHeader(new BlockLayout.Header(blockSize, 5000));
            assertEquals(blockSize, BlockLayout.decodeHeader(encoded).blockSizeBytes());
        }
    }
}
