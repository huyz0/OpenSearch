/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.security.StaticEncryptionKeyProvider;
import org.opensearch.test.OpenSearchTestCase;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class EncryptingWalChunkServiceTests extends OpenSearchTestCase {

    private static SecretKey newAesKey() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(256);
        return keyGenerator.generateKey();
    }

    private BlobContainer newBlobContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    public void testAppendedRecordsAreStoredAsCiphertextNotPlaintext() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService delegate = new WalChunkService(blobContainer, "epoch-0");
        StaticEncryptionKeyProvider keyProvider = new StaticEncryptionKeyProvider(newAesKey());
        EncryptingWalChunkService encryptingService = new EncryptingWalChunkService(delegate, keyProvider);

        byte[] plaintext = "sensitive document body".getBytes(StandardCharsets.UTF_8);
        encryptingService.append(new WalRecord("idx", 0, 0, plaintext));
        assertEquals(0, encryptingService.flush());

        byte[] chunkBytes;
        try (InputStream in = blobContainer.readBlob(WalChunkNaming.blobName("epoch-0", 0))) {
            chunkBytes = in.readAllBytes();
        }
        List<WalRecord> records = WalChunkReader.readRecords(chunkBytes);
        assertEquals(1, records.size());
        assertFalse("the raw chunk must not contain the plaintext payload", java.util.Arrays.equals(plaintext, records.get(0).payload()));
    }

    public void testDecryptingTheReplayedChunkRecoversTheOriginalPayload() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService delegate = new WalChunkService(blobContainer, "epoch-0");
        StaticEncryptionKeyProvider keyProvider = new StaticEncryptionKeyProvider(newAesKey());
        EncryptingWalChunkService encryptingService = new EncryptingWalChunkService(delegate, keyProvider);

        byte[] plaintextA = "operation A".getBytes(StandardCharsets.UTF_8);
        byte[] plaintextB = "operation B".getBytes(StandardCharsets.UTF_8);
        encryptingService.append(new WalRecord("idx", 0, 0, plaintextA));
        encryptingService.append(new WalRecord("idx", 1, 0, plaintextB));
        encryptingService.flush();

        byte[] chunkBytes;
        try (InputStream in = blobContainer.readBlob(WalChunkNaming.blobName("epoch-0", 0))) {
            chunkBytes = in.readAllBytes();
        }
        List<WalRecord> ciphertextRecords = WalChunkReader.readRecords(chunkBytes);
        List<WalRecord> decrypted = WalRecordCrypto.decryptAll(ciphertextRecords, keyProvider);

        assertEquals(2, decrypted.size());
        assertArrayEquals(plaintextA, decrypted.get(0).payload());
        assertArrayEquals(plaintextB, decrypted.get(1).payload());
        // Routing metadata must have round-tripped through the real chunk format untouched.
        assertEquals("idx", decrypted.get(0).indexUuid());
        assertEquals(1, decrypted.get(1).shardId());
    }

    public void testBufferedRecordCountAndFlushDelegateToTheUnderlyingService() throws Exception {
        BlobContainer blobContainer = newBlobContainer();
        WalChunkService delegate = new WalChunkService(blobContainer, "epoch-0");
        EncryptingWalChunkService encryptingService = new EncryptingWalChunkService(delegate, new StaticEncryptionKeyProvider(newAesKey()));

        assertEquals(0, encryptingService.bufferedRecordCount());
        encryptingService.append(new WalRecord("idx", 0, 0, "x".getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, encryptingService.bufferedRecordCount());

        assertEquals(0, encryptingService.flush());
        assertEquals(0, encryptingService.bufferedRecordCount());
        assertEquals(-1, encryptingService.flush());
    }
}
