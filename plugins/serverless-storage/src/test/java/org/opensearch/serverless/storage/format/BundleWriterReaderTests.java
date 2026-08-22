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

    public void testCorruptedFormatVersionByteIsRejectedWithSpecificMessage() {
        SegmentBundle bundle = BundleWriter.write(List.of(new BundleFileContent("_0.si", randomByteArrayOfLength(64))));
        byte[] corrupted = bundle.bytes().clone();
        corrupted[6] ^= 0xFF; // last byte of the 4-byte format-version field

        BundleFormatException e = expectThrows(BundleFormatException.class, () -> BundleReader.parseHeader(corrupted));
        assertTrue(e.getMessage(), e.getMessage().contains("unsupported bundle format version"));
    }

    // Regression test for a real bug: BundleFormatException extends IOException, so an explicit
    // `throw new BundleFormatException(...)` inside the same try block as a trailing
    // `catch (IOException e)` was re-caught by that generic clause and re-wrapped into an
    // unhelpful message, destroying the specific diagnosis (see WalChunkReader's identical fix).
    public void testCorruptedHeaderChecksumIsRejectedWithSpecificMessage() {
        SegmentBundle bundle = BundleWriter.write(List.of(new BundleFileContent("_0.si", randomByteArrayOfLength(64))));
        byte[] corrupted = bundle.bytes().clone();
        // Flip a byte within an entry's stored length/checksum fields (inside the header, well
        // after the version field) so the header parses structurally fine but its trailing
        // checksum no longer matches -- this exercises the checksum-mismatch branch specifically,
        // as opposed to the format-version or magic-header branches exercised by other tests.
        corrupted[20] ^= 0xFF;

        BundleFormatException e = expectThrows(BundleFormatException.class, () -> BundleReader.parseHeader(corrupted));
        assertTrue(e.getMessage(), e.getMessage().contains("bundle header checksum mismatch"));
    }

    // Regression test for a real bug, the exact mirror of WalChunkReader's already-fixed record
    // count: entryCount drove four allocations (a LinkedHashMap plus three arrays) before the
    // trailing header checksum was ever verified, with only a `< 0` check -- so a corrupted
    // entryCount field (magic/version intact) could demand an absurdly large allocation instead of
    // failing closed with a clean BundleFormatException, which is what this reader's own contract
    // promises.
    public void testImpossiblyLargeEntryCountIsRejectedBeforeAllocatingAnything() {
        SegmentBundle bundle = BundleWriter.write(List.of(new BundleFileContent("_0.si", randomByteArrayOfLength(64))));
        byte[] corrupted = bundle.bytes().clone();
        // entryCount is the 4-byte int immediately after the 4-byte magic + 4-byte version header,
        // i.e. bytes [8, 12). Overwrite it with a huge, clearly-impossible value.
        corrupted[8] = 0x7F;
        corrupted[9] = (byte) 0xFF;
        corrupted[10] = (byte) 0xFF;
        corrupted[11] = (byte) 0xFF;

        BundleFormatException e = expectThrows(BundleFormatException.class, () -> BundleReader.parseHeader(corrupted));
        assertTrue(e.getMessage(), e.getMessage().contains("impossibly large"));
    }

    /** A negative entryCount must still be caught by its own, more specific check. */
    public void testNegativeEntryCountIsRejectedWithSpecificMessage() {
        SegmentBundle bundle = BundleWriter.write(List.of(new BundleFileContent("_0.si", randomByteArrayOfLength(64))));
        byte[] corrupted = bundle.bytes().clone();
        corrupted[8] = (byte) 0x80;
        corrupted[9] = 0;
        corrupted[10] = 0;
        corrupted[11] = 0;

        BundleFormatException e = expectThrows(BundleFormatException.class, () -> BundleReader.parseHeader(corrupted));
        assertTrue(e.getMessage(), e.getMessage().contains("negative entry count"));
    }

    public void testTruncatedBundleIsRejectedWithSpecificMessage() {
        SegmentBundle bundle = BundleWriter.write(
            List.of(new BundleFileContent("a", randomByteArrayOfLength(100)), new BundleFileContent("b", randomByteArrayOfLength(100)))
        );
        byte[] truncated = new byte[10];
        System.arraycopy(bundle.bytes(), 0, truncated, 0, truncated.length);

        BundleFormatException e = expectThrows(BundleFormatException.class, () -> BundleReader.parseHeader(truncated));
        assertTrue(e.getMessage(), e.getMessage().contains("truncated"));
    }

    public void testNotABundleIsRejectedWithSpecificMessage() {
        byte[] junk = randomByteArrayOfLength(100);
        BundleFormatException e = expectThrows(BundleFormatException.class, () -> BundleReader.parseHeader(junk));
        assertTrue(e.getMessage(), e.getMessage().contains("bad magic header"));
    }

    public void testExtractFileOutOfRangeIsRejected() throws Exception {
        SegmentBundle bundle = BundleWriter.write(List.of(new BundleFileContent("a", randomByteArrayOfLength(10))));
        BundleFileEntry outOfRange = new BundleFileEntry("a", bundle.length() - 1, 1000, 0);
        BundleFormatException e = expectThrows(BundleFormatException.class, () -> BundleReader.extractFile(bundle.bytes(), outOfRange));
        assertTrue(e.getMessage(), e.getMessage().contains("is outside bundle of length"));
    }

    /**
     * rfc-serverless-opensearch.md &sect;17's "Format-level: property-based tests on bundle/manifest
     * round-trips; corruption injection... must fail closed" bullet. This codebase doesn't pull in
     * a dedicated property-based-testing library, but {@link OpenSearchTestCase}'s own randomized
     * base (a real seed-driven `RandomizedRunner`, reproducible via the seed the test framework
     * already prints on failure) gives the same essential property: run the same invariant across
     * many independently-random inputs, not one hand-picked example. The invariant here: for any
     * randomly-shaped bundle, corrupting exactly one random byte anywhere in its file-content region
     * must always be caught as a checksum mismatch -- extraction must never silently return
     * different bytes than were written.
     */
    public void testRandomizedSingleByteCorruptionAlwaysFailsClosedAcrossManyTrials() throws Exception {
        int trialCount = 200;
        for (int trial = 0; trial < trialCount; trial++) {
            int fileCount = randomIntBetween(1, 8);
            List<BundleFileContent> files = new ArrayList<>();
            for (int i = 0; i < fileCount; i++) {
                // At least 1 byte so there's always somewhere to flip; a genuinely empty file would
                // have nothing to corrupt and isn't the case this property is testing.
                files.add(new BundleFileContent("f" + i + "-" + randomAlphaOfLength(4), randomByteArrayOfLength(randomIntBetween(1, 512))));
            }
            SegmentBundle bundle = BundleWriter.write(files);

            // Pick a random file and a random byte within it -- not a random offset across the
            // whole bundle, which could land in header/padding regions other tests already cover
            // specifically; this property is about file-content corruption always being caught.
            BundleFileContent target = files.get(randomIntBetween(0, files.size() - 1));
            BundleFileEntry entry = bundle.entries().get(target.name());
            int flipIndex = (int) entry.offset() + randomIntBetween(0, (int) entry.length() - 1);

            byte[] corrupted = bundle.bytes().clone();
            corrupted[flipIndex] ^= 0xFF;

            BundleFormatException e = expectThrows(BundleFormatException.class, () -> BundleReader.extractFile(corrupted, entry));
            assertTrue(
                "trial " + trial + " (fileCount=" + fileCount + ", flipIndex=" + flipIndex + "): must fail closed with a checksum mismatch",
                e.getMessage().contains("checksum mismatch")
            );

            // The uncorrupted round-trip for every OTHER file in this same bundle must still be
            // exact -- one corrupted file's bytes must never leak into or invalidate a sibling's.
            for (BundleFileContent other : files) {
                if (other == target) {
                    continue;
                }
                BundleFileEntry otherEntry = bundle.entries().get(other.name());
                assertArrayEquals(
                    "trial " + trial + ": an untouched sibling file must still round-trip exactly despite another file's corruption",
                    other.content(),
                    BundleReader.extractFile(bundle.bytes(), otherEntry)
                );
            }
        }
    }

    public void testDuplicateFileNamesAreRejectedOnWrite() {
        // BundleWriter itself doesn't dedupe; verify the reader rejects a header hand-crafted with
        // duplicate names, since that would make offset reconstruction ambiguous downstream.
        List<BundleFileContent> files = List.of(
            new BundleFileContent("dup", randomByteArrayOfLength(5)),
            new BundleFileContent("dup", randomByteArrayOfLength(5))
        );
        SegmentBundle bundle = BundleWriter.write(files);
        BundleFormatException e = expectThrows(BundleFormatException.class, () -> BundleReader.parseHeader(bundle.bytes()));
        assertTrue(e.getMessage(), e.getMessage().contains("duplicate file name"));
    }

    public void testEmptyBundleOfZeroFiles() throws Exception {
        SegmentBundle bundle = BundleWriter.write(List.of());
        BundleHeader header = BundleReader.parseHeader(bundle.bytes());
        assertEquals(Map.of(), header.entries());
    }
}
