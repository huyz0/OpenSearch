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
}
