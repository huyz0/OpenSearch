/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BundleWriterReaderTests extends OpenSearchTestCase {

    public void testRoundTripSingleFile() throws Exception {
        byte[] content = randomByteArrayOfLength(1024);
        SegmentBundle bundle = BundleWriter.write(List.of(new BundleFileContent("_0.si", content)));

        BundleHeader header = BundleReader.parseHeader(bundle.bytes());
        assertEquals(1, header.entries().size());
        BundleFileEntry entry = header.entries().get("_0.si");
        assertNotNull(entry);
        assertEquals(content.length, entry.length());

        byte[] extracted = BundleReader.extractFile(bundle.bytes(), entry);
        assertArrayEquals(content, extracted);
    }

    public void testRoundTripManyFilesPreservesOrderAndContent() throws Exception {
        int fileCount = randomIntBetween(2, 50);
        List<BundleFileContent> files = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            files.add(new BundleFileContent("file-" + i + randomAlphaOfLength(3), randomByteArrayOfLength(randomIntBetween(0, 4096))));
        }

        SegmentBundle bundle = BundleWriter.write(files);
        BundleHeader header = BundleReader.parseHeader(bundle.bytes());
        assertEquals(files.size(), header.entries().size());

        for (BundleFileContent file : files) {
            BundleFileEntry entry = header.entries().get(file.name());
            assertNotNull("missing entry for " + file.name(), entry);
            byte[] extracted = BundleReader.extractFile(bundle.bytes(), entry);
            assertArrayEquals(file.content(), extracted);
        }
    }

    public void testEmptyFileRoundTrips() throws Exception {
        SegmentBundle bundle = BundleWriter.write(List.of(new BundleFileContent("empty.bin", new byte[0])));
        BundleHeader header = BundleReader.parseHeader(bundle.bytes());
        BundleFileEntry entry = header.entries().get("empty.bin");
        assertEquals(0, entry.length());
        assertArrayEquals(new byte[0], BundleReader.extractFile(bundle.bytes(), entry));
    }

    public void testEntriesHaveNonOverlappingAscendingOffsets() {
        List<BundleFileContent> files = List.of(
            new BundleFileContent("a", randomByteArrayOfLength(10)),
            new BundleFileContent("b", randomByteArrayOfLength(20)),
            new BundleFileContent("c", randomByteArrayOfLength(30))
        );
        SegmentBundle bundle = BundleWriter.write(files);
        BundleFileEntry a = bundle.entries().get("a");
        BundleFileEntry b = bundle.entries().get("b");
        BundleFileEntry c = bundle.entries().get("c");

        assertEquals(a.offset() + a.length(), b.offset());
        assertEquals(b.offset() + b.length(), c.offset());
        assertEquals(bundle.length(), c.offset() + c.length());
    }

    public void testCorruptedFileBytesFailChecksumVerification() throws Exception {
        byte[] content = randomByteArrayOfLength(256);
        SegmentBundle bundle = BundleWriter.write(List.of(new BundleFileContent("_0.si", content)));
        BundleFileEntry entry = bundle.entries().get("_0.si");

        byte[] corrupted = bundle.bytes().clone();
        int flipIndex = (int) entry.offset() + randomIntBetween(0, content.length - 1);
        corrupted[flipIndex] ^= 0xFF;

        BundleFormatException e = expectThrows(BundleFormatException.class, () -> BundleReader.extractFile(corrupted, entry));
        assertTrue(e.getMessage().contains("checksum mismatch"));
    }

    public void testCorruptedHeaderIsRejected() {
        SegmentBundle bundle = BundleWriter.write(List.of(new BundleFileContent("_0.si", randomByteArrayOfLength(64))));
        byte[] corrupted = bundle.bytes().clone();
        // flip a byte inside the header (well before the body starts)
        corrupted[6] ^= 0xFF;

        expectThrows(BundleFormatException.class, () -> BundleReader.parseHeader(corrupted));
    }

    public void testTruncatedBundleIsRejected() {
        SegmentBundle bundle = BundleWriter.write(
            List.of(new BundleFileContent("a", randomByteArrayOfLength(100)), new BundleFileContent("b", randomByteArrayOfLength(100)))
        );
        byte[] truncated = new byte[10];
        System.arraycopy(bundle.bytes(), 0, truncated, 0, truncated.length);

        expectThrows(BundleFormatException.class, () -> BundleReader.parseHeader(truncated));
    }

    public void testNotABundleIsRejected() {
        byte[] junk = randomByteArrayOfLength(100);
        expectThrows(BundleFormatException.class, () -> BundleReader.parseHeader(junk));
    }

    public void testExtractFileOutOfRangeIsRejected() throws Exception {
        SegmentBundle bundle = BundleWriter.write(List.of(new BundleFileContent("a", randomByteArrayOfLength(10))));
        BundleFileEntry outOfRange = new BundleFileEntry("a", bundle.length() - 1, 1000, 0);
        expectThrows(BundleFormatException.class, () -> BundleReader.extractFile(bundle.bytes(), outOfRange));
    }

    public void testDuplicateFileNamesAreRejectedOnWrite() {
        // BundleWriter itself doesn't dedupe; verify the reader rejects a header hand-crafted with
        // duplicate names, since that would make offset reconstruction ambiguous downstream.
        List<BundleFileContent> files = List.of(
            new BundleFileContent("dup", randomByteArrayOfLength(5)),
            new BundleFileContent("dup", randomByteArrayOfLength(5))
        );
        SegmentBundle bundle = BundleWriter.write(files);
        expectThrows(BundleFormatException.class, () -> BundleReader.parseHeader(bundle.bytes()));
    }

    public void testEmptyBundleOfZeroFiles() throws Exception {
        SegmentBundle bundle = BundleWriter.write(List.of());
        BundleHeader header = BundleReader.parseHeader(bundle.bytes());
        assertEquals(Map.of(), header.entries());
    }
}
