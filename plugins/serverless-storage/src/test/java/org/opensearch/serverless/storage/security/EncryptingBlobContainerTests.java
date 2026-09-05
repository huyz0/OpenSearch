/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.test.OpenSearchTestCase;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class EncryptingBlobContainerTests extends OpenSearchTestCase {

    /**
     * The index every container in this suite claims to belong to. It is a real argument now, not
     * decoration: it selects the key and it is the first field of every block's associated data, so
     * two containers built with different values here produce blocks that cannot be exchanged.
     */
    private static final String INDEX_UUID = "index-uuid-aaaaaaaaaaaa";

    private static SecretKey newAesKey() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(256);
        return keyGenerator.generateKey();
    }

    private BlobContainer newFsBlobContainer() throws IOException {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    public void testWriteThenReadWholeBlobRoundTripsExactly() throws Exception {
        BlobContainer delegate = newFsBlobContainer();
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            delegate,
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0
        );

        byte[] plaintext = "the quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8);
        encrypting.writeBlob("blob-1", new java.io.ByteArrayInputStream(plaintext), plaintext.length, false);

        try (InputStream in = encrypting.readBlob("blob-1")) {
            assertArrayEquals(plaintext, in.readAllBytes());
        }
    }

    public void testRangedReadReturnsExactlyTheRequestedPlaintextSlice() throws Exception {
        BlobContainer delegate = newFsBlobContainer();
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            delegate,
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0
        );

        byte[] plaintext = "0123456789abcdefghij".getBytes(StandardCharsets.UTF_8);
        encrypting.writeBlobAtomic("blob-1", new java.io.ByteArrayInputStream(plaintext), plaintext.length, true);

        try (InputStream in = encrypting.readBlob("blob-1", 5, 10)) {
            assertEquals("56789abcde", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    public void testContentIsActuallyEncryptedAtRestNotJustAtTheApiSurface() throws Exception {
        Path storageDir = createTempDir();
        FsBlobStore blobStore = new FsBlobStore(1024, storageDir, false);
        BlobContainer delegate = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            delegate,
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0
        );

        byte[] plaintext = "super secret document contents".getBytes(StandardCharsets.UTF_8);
        encrypting.writeBlob("blob-1", new java.io.ByteArrayInputStream(plaintext), plaintext.length, false);

        byte[] rawBytesOnDisk = Files.readAllBytes(storageDir.resolve("blob-1"));
        assertFalse(
            "the plaintext must not appear verbatim in the stored blob",
            new String(rawBytesOnDisk, StandardCharsets.UTF_8).contains("super secret")
        );
    }

    public void testDecryptingWithADifferentKeyFailsLoudlyRatherThanReturningGarbage() throws Exception {
        BlobContainer delegate = newFsBlobContainer();
        EncryptingBlobContainer writer = new EncryptingBlobContainer(delegate, new StaticEncryptionKeyProvider(newAesKey()), INDEX_UUID, 0);
        byte[] plaintext = "data only the right key should open".getBytes(StandardCharsets.UTF_8);
        writer.writeBlob("blob-1", new java.io.ByteArrayInputStream(plaintext), plaintext.length, false);

        EncryptingBlobContainer readerWithWrongKey = new EncryptingBlobContainer(
            delegate,
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0
        );
        expectThrows(IOException.class, () -> readerWithWrongKey.readBlob("blob-1").readAllBytes());
    }

    public void testCorruptedCiphertextFailsAuthenticationRatherThanReturningGarbage() throws Exception {
        Path storageDir = createTempDir();
        FsBlobStore blobStore = new FsBlobStore(1024, storageDir, false);
        BlobContainer delegate = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            delegate,
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0
        );

        byte[] plaintext = "integrity matters".getBytes(StandardCharsets.UTF_8);
        encrypting.writeBlob("blob-1", new java.io.ByteArrayInputStream(plaintext), plaintext.length, false);

        Path onDisk = storageDir.resolve("blob-1");
        byte[] bytes = Files.readAllBytes(onDisk);
        bytes[bytes.length - 1] ^= 0xFF; // flip the last byte, inside the GCM auth tag
        Files.write(onDisk, bytes);

        expectThrows(IOException.class, () -> encrypting.readBlob("blob-1").readAllBytes());
    }

    public void testRegistersPassThroughUnencryptedAndStillWork() throws Exception {
        BlobContainer delegate = newFsBlobContainer();
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            delegate,
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0
        );

        BlobRegisterCasResult first = encrypting.compareAndSwapRegister(
            "register-1",
            BlobRegister.ABSENT_GENERATION,
            new BytesArray("v1".getBytes(StandardCharsets.UTF_8))
        );
        assertTrue(first.applied());

        Optional<BlobRegister> read = encrypting.readRegister("register-1");
        assertTrue(read.isPresent());
        assertEquals("v1", read.get().value().utf8ToString());
    }

    public void testEmptyBlobRoundTrips() throws Exception {
        BlobContainer delegate = newFsBlobContainer();
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            delegate,
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0
        );

        encrypting.writeBlob("blob-1", new java.io.ByteArrayInputStream(new byte[0]), 0, false);

        try (InputStream in = encrypting.readBlob("blob-1")) {
            assertEquals(0, in.readAllBytes().length);
        }
    }

    public void testWholeBlobReadRoundTripsAcrossMultipleBlocks() throws Exception {
        BlobContainer delegate = newFsBlobContainer();
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            delegate,
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0,
            7
        );

        byte[] plaintext = "the quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8); // 44 bytes, not a multiple of 7
        encrypting.writeBlob("blob-1", new java.io.ByteArrayInputStream(plaintext), plaintext.length, false);

        try (InputStream in = encrypting.readBlob("blob-1")) {
            assertArrayEquals(plaintext, in.readAllBytes());
        }
    }

    public void testRangedReadSpanningMultipleBlockBoundariesReturnsExactBytes() throws Exception {
        BlobContainer delegate = newFsBlobContainer();
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            delegate,
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0,
            10
        );

        byte[] plaintext = new byte[55];
        for (int i = 0; i < plaintext.length; i++) {
            plaintext[i] = (byte) i;
        }
        encrypting.writeBlob("blob-1", new java.io.ByteArrayInputStream(plaintext), plaintext.length, false);

        // [8, 33) spans block boundaries at 10/20/30: the tail of block 0, all of blocks 1-2, and
        // the head of block 3.
        try (InputStream in = encrypting.readBlob("blob-1", 8, 25)) {
            byte[] expected = java.util.Arrays.copyOfRange(plaintext, 8, 33);
            assertArrayEquals(expected, in.readAllBytes());
        }
    }

    /** Wraps a delegate {@link BlobContainer}, recording every ranged-read call's (position, length) for inspection. */
    private static final class RecordingBlobContainer extends FilterBlobContainer {
        private final BlobContainer delegate;
        final List<long[]> rangedReadCalls = new ArrayList<>();
        int unrangedReadCalls = 0;

        RecordingBlobContainer(BlobContainer delegate) {
            super(delegate);
            this.delegate = delegate;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new RecordingBlobContainer(child);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            rangedReadCalls.add(new long[] { position, length });
            return delegate.readBlob(blobName, position, length);
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            unrangedReadCalls++;
            return delegate.readBlob(blobName);
        }
    }

    public void testRangedReadFetchesOnlyTheOverlappingBlocksNotTheWholeBlob() throws Exception {
        RecordingBlobContainer recording = new RecordingBlobContainer(newFsBlobContainer());
        int blockSize = 10;
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            recording,
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0,
            blockSize
        );

        byte[] plaintext = new byte[1000]; // 100 blocks
        for (int i = 0; i < plaintext.length; i++) {
            plaintext[i] = (byte) i;
        }
        encrypting.writeBlob("blob-1", new java.io.ByteArrayInputStream(plaintext), plaintext.length, false);
        recording.rangedReadCalls.clear(); // writeBlob doesn't issue ranged reads, but clear defensively either way

        try (InputStream in = encrypting.readBlob("blob-1", 505, 3)) {
            assertArrayEquals(new byte[] { (byte) 505, (byte) 506, (byte) 507 }, in.readAllBytes());
        }

        // Exactly two delegate range reads -- the header, then one contiguous fetch spanning only
        // the single block overlapping [505, 508) -- never anything close to the ~4KB whole blob.
        assertEquals(2, recording.rangedReadCalls.size());
        assertEquals(0, recording.rangedReadCalls.get(0)[0]);
        long[] blockFetch = recording.rangedReadCalls.get(1);
        assertTrue("must fetch far less than the whole blob", blockFetch[1] < 100);
    }

    /**
     * S3-efficiency regression test: readBlob(String) (no range) previously did readHeader()'s own
     * ranged fetch followed by decryptRange()'s own separate ranged fetch -- two delegate round
     * trips for what should be a single full-object read, doubling the GET cost of every full read
     * (the manifest-read hot path whenever encryption is enabled).
     */
    public void testWholeBlobReadIssuesExactlyOneDelegateReadNotTwo() throws Exception {
        RecordingBlobContainer recording = new RecordingBlobContainer(newFsBlobContainer());
        int blockSize = 10;
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            recording,
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0,
            blockSize
        );

        byte[] plaintext = new byte[1000]; // 100 blocks
        for (int i = 0; i < plaintext.length; i++) {
            plaintext[i] = (byte) i;
        }
        encrypting.writeBlob("blob-1", new java.io.ByteArrayInputStream(plaintext), plaintext.length, false);
        recording.rangedReadCalls.clear();
        recording.unrangedReadCalls = 0;

        try (InputStream in = encrypting.readBlob("blob-1")) {
            assertArrayEquals(plaintext, in.readAllBytes());
        }

        assertEquals(
            "a full-object read must issue exactly one delegate call, not a header fetch plus a separate body fetch",
            1,
            recording.unrangedReadCalls
        );
        assertEquals("must not fall back to any ranged delegate read either", 0, recording.rangedReadCalls.size());
    }

    public void testRangedReadOfOneBlockSucceedsEvenIfADifferentBlockIsCorrupted() throws Exception {
        Path storageDir = createTempDir();
        FsBlobStore blobStore = new FsBlobStore(1024, storageDir, false);
        BlobContainer delegate = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            delegate,
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0,
            10
        );

        byte[] plaintext = "0123456789ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8); // 20 bytes -> 2 blocks of 10
        encrypting.writeBlob("blob-1", new java.io.ByteArrayInputStream(plaintext), plaintext.length, false);

        // Corrupt the very last byte of the file, which lives inside the second block's GCM tag.
        Path onDisk = storageDir.resolve("blob-1");
        byte[] bytes = Files.readAllBytes(onDisk);
        bytes[bytes.length - 1] ^= 0xFF;
        Files.write(onDisk, bytes);

        // A ranged read entirely within the first (uncorrupted) block must still succeed.
        try (InputStream in = encrypting.readBlob("blob-1", 0, 10)) {
            assertEquals("0123456789", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        // A read touching the corrupted second block must fail loudly, not return garbage.
        expectThrows(IOException.class, () -> encrypting.readBlob("blob-1", 10, 10).readAllBytes());
    }

    /**
     * {@code FsBlobContainer} (the real delegate every other test in this class uses) doesn't itself
     * implement the {@code writeBlob*WithMetadata} overloads -- {@link BlobContainer}'s own defaults
     * throw {@link UnsupportedOperationException} for them. This tiny wrapper implements them by
     * dropping straight to the plain (non-metadata) write, purely so the two regression tests below
     * can exercise {@link EncryptingBlobContainer}'s own metadata-write overrides against something
     * that accepts the call at all -- the metadata itself is not what's under test.
     */
    private static final class MetadataCapableBlobContainer extends FilterBlobContainer {
        MetadataCapableBlobContainer(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new MetadataCapableBlobContainer(child);
        }

        @Override
        public void writeBlobWithMetadata(
            String blobName,
            InputStream inputStream,
            long blobSize,
            boolean failIfAlreadyExists,
            java.util.Map<String, String> metadata
        ) throws IOException {
            writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void writeBlobAtomicWithMetadata(
            String blobName,
            InputStream inputStream,
            java.util.Map<String, String> metadata,
            long blobSize,
            boolean failIfAlreadyExists
        ) throws IOException {
            writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }
    }

    /**
     * Regression test: {@code writeBlobWithMetadata} previously fell through {@code
     * FilterBlobContainer}'s own passthrough default straight to the delegate, writing plaintext to
     * the real object store instead of going through this class's block-cipher path -- see this
     * class's own writeBlobWithMetadata overrides. Proves the delegate never sees plaintext and the
     * write round-trips correctly through the normal (non-metadata) read path.
     */
    public void testWriteBlobWithMetadataEncryptsInsteadOfBypassing() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        Path storageDir = blobStore.path();
        BlobContainer delegate = new MetadataCapableBlobContainer(new FsBlobContainer(blobStore, BlobPath.cleanPath(), storageDir));
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            delegate,
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0
        );

        byte[] plaintext = "the quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8);
        encrypting.writeBlobWithMetadata(
            "blob-metadata",
            new java.io.ByteArrayInputStream(plaintext),
            plaintext.length,
            false,
            java.util.Map.of("k", "v")
        );

        byte[] onDisk = Files.readAllBytes(storageDir.resolve("blob-metadata"));
        assertFalse(
            "writeBlobWithMetadata must not have written plaintext straight to the delegate",
            new String(onDisk, StandardCharsets.UTF_8).contains("quick brown fox")
        );

        try (InputStream in = encrypting.readBlob("blob-metadata")) {
            assertArrayEquals(plaintext, in.readAllBytes());
        }
    }

    /** Same regression, for the atomic metadata-write overload. */
    public void testWriteBlobAtomicWithMetadataEncryptsInsteadOfBypassing() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        Path storageDir = blobStore.path();
        BlobContainer delegate = new MetadataCapableBlobContainer(new FsBlobContainer(blobStore, BlobPath.cleanPath(), storageDir));
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            delegate,
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0
        );

        byte[] plaintext = "the quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8);
        encrypting.writeBlobAtomicWithMetadata(
            "blob-atomic-metadata",
            new java.io.ByteArrayInputStream(plaintext),
            java.util.Map.of("k", "v"),
            plaintext.length,
            false
        );

        byte[] onDisk = Files.readAllBytes(storageDir.resolve("blob-atomic-metadata"));
        assertFalse(
            "writeBlobAtomicWithMetadata must not have written plaintext straight to the delegate",
            new String(onDisk, StandardCharsets.UTF_8).contains("quick brown fox")
        );

        try (InputStream in = encrypting.readBlob("blob-atomic-metadata")) {
            assertArrayEquals(plaintext, in.readAllBytes());
        }
    }

    // ------------------------------------------------------------------------------------------
    // F-5: associated data. Every test below fails on the pre-AAD format, because on that format
    // the attack it describes succeeds silently -- the blob decrypts, authenticates, and returns
    // the wrong bytes. They are written as attacks rather than as assertions about a version
    // number on purpose: what changed is not that an integer moved, it is that these stopped
    // working.
    //
    // All of them assume one key shared across indices and shards, which is not a contrived
    // setup -- it is the only setup any node configuration of this plugin can produce, since
    // StaticEncryptionKeyProvider is the only provider anything ever builds.
    // ------------------------------------------------------------------------------------------

    /** BlockLayout.HEADER_SIZE_BYTES, restated here so these tests read as byte surgery on a wire format. */
    private static final int HEADER_SIZE = 4 + 4 + 4 + 8;

    /** IV + GCM tag per block. */
    private static final int BLOCK_OVERHEAD = 12 + 16;

    /** An {@link FsBlobContainer} together with the directory behind it, so a test can tamper with a blob directly. */
    private record FsContainer(BlobContainer container, Path dir) {
    }

    private FsContainer newFsContainerWithDir() throws IOException {
        Path dir = createTempDir();
        FsBlobStore blobStore = new FsBlobStore(1024, dir, false);
        return new FsContainer(new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path()), blobStore.path());
    }

    /**
     * Block substitution across two indices -- the headline consequence of having no associated
     * data, and the one that turns "someone can write to the bucket" into "one tenant serves
     * another tenant's documents".
     *
     * <p>Before AAD, splicing index B's ciphertext block into index A's blob produced a blob that
     * decrypted cleanly and authenticated: nothing anywhere recorded which index a block was
     * written for, so there was nothing to check. Now the tag covers the index UUID.
     */
    public void testABlockFromAnotherIndexCannotBeSplicedIn() throws Exception {
        SecretKey sharedKey = newAesKey();
        FsContainer a = newFsContainerWithDir();
        FsContainer b = newFsContainerWithDir();
        int blockSize = 16;
        EncryptingBlobContainer indexA = new EncryptingBlobContainer(
            a.container(),
            new StaticEncryptionKeyProvider(sharedKey),
            "index-uuid-AAAAAAAAAAAA",
            0,
            blockSize
        );
        EncryptingBlobContainer indexB = new EncryptingBlobContainer(
            b.container(),
            new StaticEncryptionKeyProvider(sharedKey),
            "index-uuid-BBBBBBBBBBBB",
            0,
            blockSize
        );

        // Identical length and block count, so the two blobs are interchangeable in every respect
        // except what the tags cover. That is what makes this a substitution and not a corruption.
        byte[] plaintextA = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".getBytes(StandardCharsets.UTF_8);
        byte[] plaintextB = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".getBytes(StandardCharsets.UTF_8);
        indexA.writeBlob("bundle-1", new java.io.ByteArrayInputStream(plaintextA), plaintextA.length, false);
        indexB.writeBlob("bundle-1", new java.io.ByteArrayInputStream(plaintextB), plaintextB.length, false);

        byte[] rawA = Files.readAllBytes(a.dir().resolve("bundle-1"));
        byte[] rawB = Files.readAllBytes(b.dir().resolve("bundle-1"));
        assertEquals("the two blobs must be the same shape for this to be a real substitution", rawA.length, rawB.length);

        byte[] spliced = rawA.clone();
        System.arraycopy(rawB, HEADER_SIZE, spliced, HEADER_SIZE, blockSize + BLOCK_OVERHEAD);
        Files.write(a.dir().resolve("bundle-1"), spliced);

        IOException e = expectThrows(IOException.class, () -> {
            try (InputStream in = indexA.readBlob("bundle-1")) {
                in.readAllBytes();
            }
        });
        assertTrue(
            "a block written for another index must fail authentication, not decrypt: " + e.getMessage(),
            e.getMessage().contains("failed authentication")
        );
    }

    /**
     * The same blob name in two shards of one index. The index UUID alone does not separate these,
     * and a shard is the granularity at which containers are actually built, so the shard is its own
     * field in the associated data.
     */
    public void testABlockFromAnotherShardOfTheSameIndexCannotBeSplicedIn() throws Exception {
        SecretKey sharedKey = newAesKey();
        FsContainer zero = newFsContainerWithDir();
        FsContainer one = newFsContainerWithDir();
        int blockSize = 16;
        EncryptingBlobContainer shard0 = new EncryptingBlobContainer(
            zero.container(),
            new StaticEncryptionKeyProvider(sharedKey),
            INDEX_UUID,
            0,
            blockSize
        );
        EncryptingBlobContainer shard1 = new EncryptingBlobContainer(
            one.container(),
            new StaticEncryptionKeyProvider(sharedKey),
            INDEX_UUID,
            1,
            blockSize
        );

        byte[] plaintext0 = "0000000000000000".getBytes(StandardCharsets.UTF_8);
        byte[] plaintext1 = "1111111111111111".getBytes(StandardCharsets.UTF_8);
        shard0.writeBlob("bundle-1", new java.io.ByteArrayInputStream(plaintext0), plaintext0.length, false);
        shard1.writeBlob("bundle-1", new java.io.ByteArrayInputStream(plaintext1), plaintext1.length, false);

        Files.write(zero.dir().resolve("bundle-1"), Files.readAllBytes(one.dir().resolve("bundle-1")));

        expectThrows(IOException.class, () -> {
            try (InputStream in = shard0.readBlob("bundle-1")) {
                in.readAllBytes();
            }
        });
    }

    /**
     * Reordering two blocks within one blob. Same key, same index, same shard, same blob -- the only
     * thing that differs between block 0 and block 1 is the block index, which is why it is in the
     * associated data. Before AAD this swap was undetectable, and the blob came back with its two
     * halves transposed.
     */
    public void testBlocksCannotBeReorderedWithinOneBlob() throws Exception {
        FsContainer fs = newFsContainerWithDir();
        int blockSize = 16;
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            fs.container(),
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0,
            blockSize
        );

        // Two full blocks, so swapping them changes nothing an arithmetic check could notice: the
        // total length, the block count and every block's length are all unchanged.
        byte[] plaintext = "00000000000000001111111111111111".getBytes(StandardCharsets.UTF_8);
        encrypting.writeBlob("bundle-1", new java.io.ByteArrayInputStream(plaintext), plaintext.length, false);

        byte[] raw = Files.readAllBytes(fs.dir().resolve("bundle-1"));
        int blockLength = blockSize + BLOCK_OVERHEAD;
        byte[] swapped = raw.clone();
        System.arraycopy(raw, HEADER_SIZE + blockLength, swapped, HEADER_SIZE, blockLength);
        System.arraycopy(raw, HEADER_SIZE, swapped, HEADER_SIZE + blockLength, blockLength);
        Files.write(fs.dir().resolve("bundle-1"), swapped);

        IOException e = expectThrows(IOException.class, () -> {
            try (InputStream in = encrypting.readBlob("bundle-1")) {
                in.readAllBytes();
            }
        });
        assertTrue("a reordered block must fail authentication: " + e.getMessage(), e.getMessage().contains("failed authentication"));
    }

    /**
     * Truncation by header rewrite. The header is still written in the clear -- it has to be, since
     * a reader needs it before it can locate any block -- so an attacker can still lower
     * {@code totalPlaintextLength} and drop the trailing blocks. What changed is that the header's
     * fields are inside every block's associated data, so the surviving blocks no longer
     * authenticate under the rewritten header. Before AAD this produced a shorter blob that read as
     * entirely valid.
     */
    public void testHeaderRewrittenToTruncateTheBlobIsRejected() throws Exception {
        FsContainer fs = newFsContainerWithDir();
        int blockSize = 16;
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            fs.container(),
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0,
            blockSize
        );

        byte[] plaintext = "00000000000000001111111111111111".getBytes(StandardCharsets.UTF_8);
        encrypting.writeBlob("bundle-1", new java.io.ByteArrayInputStream(plaintext), plaintext.length, false);

        byte[] raw = Files.readAllBytes(fs.dir().resolve("bundle-1"));
        byte[] truncated = new byte[HEADER_SIZE + blockSize + BLOCK_OVERHEAD];
        System.arraycopy(raw, 0, truncated, 0, truncated.length);
        // totalPlaintextLength is the header's last 8 bytes: claim 16 where 32 was written.
        java.nio.ByteBuffer.wrap(truncated).putLong(HEADER_SIZE - 8, 16L);
        Files.write(fs.dir().resolve("bundle-1"), truncated);

        IOException e = expectThrows(IOException.class, () -> {
            try (InputStream in = encrypting.readBlob("bundle-1")) {
                in.readAllBytes();
            }
        });
        assertTrue(
            "a rewritten header must invalidate the blocks written under the original: " + e.getMessage(),
            e.getMessage().contains("failed authentication")
        );
    }

    /**
     * A whole blob moved to a different name within one shard. Manifests are named
     * {@code <term>-<generation>}, so this is the "replay an older generation over a newer one"
     * primitive -- the one that matters most, because a manifest is a control input naming which
     * bundles a shard reads, not merely data.
     */
    public void testAWholeBlobCannotBeRenamedOverAnother() throws Exception {
        FsContainer fs = newFsContainerWithDir();
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            fs.container(),
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0
        );

        byte[] generationOne = "manifest for generation 1".getBytes(StandardCharsets.UTF_8);
        byte[] generationTwo = "manifest for generation 2".getBytes(StandardCharsets.UTF_8);
        encrypting.writeBlob("1-1", new java.io.ByteArrayInputStream(generationOne), generationOne.length, false);
        encrypting.writeBlob("1-2", new java.io.ByteArrayInputStream(generationTwo), generationTwo.length, false);

        // Roll generation 2 back to generation 1's content, keeping generation 2's name.
        Files.write(fs.dir().resolve("1-2"), Files.readAllBytes(fs.dir().resolve("1-1")));

        expectThrows(IOException.class, () -> {
            try (InputStream in = encrypting.readBlob("1-2")) {
                in.readAllBytes();
            }
        });
    }

    /**
     * A child container must not share its parent's binding, or {@code children()} would hand back
     * containers whose blocks are interchangeable with the parent's -- the same substitution, one
     * directory down.
     */
    public void testAChildContainersBlocksAreNotInterchangeableWithItsParents() throws Exception {
        FsContainer fs = newFsContainerWithDir();
        Path childDir = Files.createDirectories(fs.dir().resolve("child"));

        EncryptingBlobContainer parent = new EncryptingBlobContainer(
            fs.container(),
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0
        );
        BlobContainer child = parent.children().get("child");
        assertNotNull("the child container must be wrapped, not returned raw", child);

        byte[] parentPlaintext = "parent contents!".getBytes(StandardCharsets.UTF_8);
        byte[] childPlaintext = "child contents!!".getBytes(StandardCharsets.UTF_8);
        parent.writeBlob("blob-1", new java.io.ByteArrayInputStream(parentPlaintext), parentPlaintext.length, false);
        child.writeBlob("blob-1", new java.io.ByteArrayInputStream(childPlaintext), childPlaintext.length, false);

        Files.write(fs.dir().resolve("blob-1"), Files.readAllBytes(childDir.resolve("blob-1")));

        expectThrows(IOException.class, () -> {
            try (InputStream in = parent.readBlob("blob-1")) {
                in.readAllBytes();
            }
        });
    }

    /**
     * The compatibility half of F-5: a blob written by a build from before this change still reads.
     * Constructed by hand rather than by an old binary -- a version 1 header followed by bare
     * {@link AesGcmCipher} envelopes with no associated data, which is exactly what the previous
     * format was.
     */
    public void testLegacyVersionOneBlobsAreStillReadable() throws Exception {
        FsContainer fs = newFsContainerWithDir();
        SecretKey key = newAesKey();
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            fs.container(),
            new StaticEncryptionKeyProvider(key),
            INDEX_UUID,
            0,
            16
        );

        byte[] plaintext = "00000000000000001111111111111111".getBytes(StandardCharsets.UTF_8);
        Files.write(fs.dir().resolve("legacy-1"), legacyVersionOneBlob(plaintext, 16, key));

        try (InputStream in = encrypting.readBlob("legacy-1")) {
            assertArrayEquals("a version 1 blob must still read after the upgrade", plaintext, in.readAllBytes());
        }
        try (InputStream in = encrypting.readBlob("legacy-1", 16, 16)) {
            assertArrayEquals(
                "ranged reads of a version 1 blob must work too",
                "1111111111111111".getBytes(StandardCharsets.UTF_8),
                in.readAllBytes()
            );
        }
    }

    /**
     * And the operator's way to end the compatibility window: once every version 1 object has been
     * rewritten, {@code serverless_storage.encryption.require_authenticated_blocks} makes reading
     * one an error instead of a silent acceptance of an unauthenticated format.
     */
    public void testLegacyBlobsAreRefusedWhenAuthenticatedBlocksAreRequired() throws Exception {
        FsContainer fs = newFsContainerWithDir();
        SecretKey key = newAesKey();
        EncryptingBlobContainer strict = new EncryptingBlobContainer(
            fs.container(),
            new StaticEncryptionKeyProvider(key),
            INDEX_UUID,
            0,
            true
        );

        byte[] plaintext = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);
        Files.write(fs.dir().resolve("legacy-1"), legacyVersionOneBlob(plaintext, 64 * 1024, key));

        IOException e = expectThrows(IOException.class, () -> {
            try (InputStream in = strict.readBlob("legacy-1")) {
                in.readAllBytes();
            }
        });
        assertTrue(
            "the refusal must name the setting that caused it: " + e.getMessage(),
            e.getMessage().contains("require_authenticated_blocks")
        );
    }

    /**
     * A negative range length used to survive {@code Math.min} (which has no floor at zero), reach
     * {@code decryptRange}, and produce a negative block index and a delegate read at a negative
     * offset.
     */
    public void testNegativeRangeLengthIsRejected() throws Exception {
        FsContainer fs = newFsContainerWithDir();
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            fs.container(),
            new StaticEncryptionKeyProvider(newAesKey()),
            INDEX_UUID,
            0
        );
        byte[] plaintext = "0123456789".getBytes(StandardCharsets.UTF_8);
        encrypting.writeBlob("blob-1", new java.io.ByteArrayInputStream(plaintext), plaintext.length, false);

        expectThrows(IOException.class, () -> encrypting.readBlob("blob-1", 0, -1));
    }

    /**
     * Builds the pre-AAD on-disk format by hand: a version 1 header, then one bare
     * {@link AesGcmCipher} envelope per block, with no associated data.
     */
    private static byte[] legacyVersionOneBlob(byte[] plaintext, int blockSizeBytes, SecretKey key) throws IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        java.nio.ByteBuffer header = java.nio.ByteBuffer.allocate(HEADER_SIZE);
        header.put(new byte[] { 'E', 'B', 'C', '1' });
        header.putInt(1);
        header.putInt(blockSizeBytes);
        header.putLong(plaintext.length);
        out.write(header.array());
        for (int offset = 0; offset < plaintext.length; offset += blockSizeBytes) {
            int len = Math.min(blockSizeBytes, plaintext.length - offset);
            out.write(AesGcmCipher.encrypt(java.util.Arrays.copyOfRange(plaintext, offset, offset + len), key));
        }
        return out.toByteArray();
    }
}
