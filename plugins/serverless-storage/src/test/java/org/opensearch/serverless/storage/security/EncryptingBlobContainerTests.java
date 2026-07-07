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
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.test.OpenSearchTestCase;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
}
