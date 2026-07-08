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
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(delegate, new StaticEncryptionKeyProvider(newAesKey()));

        byte[] plaintext = "the quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8);
        encrypting.writeBlob("blob-1", new java.io.ByteArrayInputStream(plaintext), plaintext.length, false);

        try (InputStream in = encrypting.readBlob("blob-1")) {
            assertArrayEquals(plaintext, in.readAllBytes());
        }
    }

    public void testRangedReadReturnsExactlyTheRequestedPlaintextSlice() throws Exception {
        BlobContainer delegate = newFsBlobContainer();
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(delegate, new StaticEncryptionKeyProvider(newAesKey()));

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
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(delegate, new StaticEncryptionKeyProvider(newAesKey()));

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
        EncryptingBlobContainer writer = new EncryptingBlobContainer(delegate, new StaticEncryptionKeyProvider(newAesKey()));
        byte[] plaintext = "data only the right key should open".getBytes(StandardCharsets.UTF_8);
        writer.writeBlob("blob-1", new java.io.ByteArrayInputStream(plaintext), plaintext.length, false);

        EncryptingBlobContainer readerWithWrongKey = new EncryptingBlobContainer(delegate, new StaticEncryptionKeyProvider(newAesKey()));
        expectThrows(IOException.class, () -> readerWithWrongKey.readBlob("blob-1").readAllBytes());
    }

    public void testCorruptedCiphertextFailsAuthenticationRatherThanReturningGarbage() throws Exception {
        Path storageDir = createTempDir();
        FsBlobStore blobStore = new FsBlobStore(1024, storageDir, false);
        BlobContainer delegate = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(delegate, new StaticEncryptionKeyProvider(newAesKey()));

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
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(delegate, new StaticEncryptionKeyProvider(newAesKey()));

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
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(delegate, new StaticEncryptionKeyProvider(newAesKey()));

        encrypting.writeBlob("blob-1", new java.io.ByteArrayInputStream(new byte[0]), 0, false);

        try (InputStream in = encrypting.readBlob("blob-1")) {
            assertEquals(0, in.readAllBytes().length);
        }
    }

    public void testWholeBlobReadRoundTripsAcrossMultipleBlocks() throws Exception {
        BlobContainer delegate = newFsBlobContainer();
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(delegate, new StaticEncryptionKeyProvider(newAesKey()), 7);

        byte[] plaintext = "the quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8); // 44 bytes, not a multiple of 7
        encrypting.writeBlob("blob-1", new java.io.ByteArrayInputStream(plaintext), plaintext.length, false);

        try (InputStream in = encrypting.readBlob("blob-1")) {
            assertArrayEquals(plaintext, in.readAllBytes());
        }
    }

    public void testRangedReadSpanningMultipleBlockBoundariesReturnsExactBytes() throws Exception {
        BlobContainer delegate = newFsBlobContainer();
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(delegate, new StaticEncryptionKeyProvider(newAesKey()), 10);

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
    }

    public void testRangedReadFetchesOnlyTheOverlappingBlocksNotTheWholeBlob() throws Exception {
        RecordingBlobContainer recording = new RecordingBlobContainer(newFsBlobContainer());
        int blockSize = 10;
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(
            recording,
            new StaticEncryptionKeyProvider(newAesKey()),
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

    public void testRangedReadOfOneBlockSucceedsEvenIfADifferentBlockIsCorrupted() throws Exception {
        Path storageDir = createTempDir();
        FsBlobStore blobStore = new FsBlobStore(1024, storageDir, false);
        BlobContainer delegate = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        EncryptingBlobContainer encrypting = new EncryptingBlobContainer(delegate, new StaticEncryptionKeyProvider(newAesKey()), 10);

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
}
