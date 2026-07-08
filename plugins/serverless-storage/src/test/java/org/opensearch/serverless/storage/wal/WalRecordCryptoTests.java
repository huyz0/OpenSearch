/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.serverless.storage.security.StaticEncryptionKeyProvider;
import org.opensearch.test.OpenSearchTestCase;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class WalRecordCryptoTests extends OpenSearchTestCase {

    private static SecretKey newAesKey() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(256);
        return keyGenerator.generateKey();
    }

    public void testEncryptThenDecryptRoundTripsThePayloadExactly() throws Exception {
        StaticEncryptionKeyProvider keyProvider = new StaticEncryptionKeyProvider(newAesKey());
        WalRecord original = new WalRecord("idx", 3, 42, "the operation payload".getBytes(StandardCharsets.UTF_8));

        WalRecord encrypted = WalRecordCrypto.encrypt(original, keyProvider);
        WalRecord decrypted = WalRecordCrypto.decrypt(encrypted, keyProvider);

        assertArrayEquals(original.payload(), decrypted.payload());
    }

    public void testEncryptionLeavesIndexShardAndSeqNoAsPlaintext() throws Exception {
        // Routing metadata (WalChunkReader#filterByShard) must survive untouched -- only the
        // payload is meant to become opaque.
        StaticEncryptionKeyProvider keyProvider = new StaticEncryptionKeyProvider(newAesKey());
        WalRecord original = new WalRecord("idx-a", 7, 99, "payload".getBytes(StandardCharsets.UTF_8));

        WalRecord encrypted = WalRecordCrypto.encrypt(original, keyProvider);

        assertEquals("idx-a", encrypted.indexUuid());
        assertEquals(7, encrypted.shardId());
        assertEquals(99, encrypted.seqNo());
    }

    public void testEncryptedPayloadDiffersFromThePlaintext() throws Exception {
        StaticEncryptionKeyProvider keyProvider = new StaticEncryptionKeyProvider(newAesKey());
        byte[] plaintext = "not secret unless encrypted".getBytes(StandardCharsets.UTF_8);
        WalRecord original = new WalRecord("idx", 0, 0, plaintext);

        WalRecord encrypted = WalRecordCrypto.encrypt(original, keyProvider);

        assertFalse(java.util.Arrays.equals(plaintext, encrypted.payload()));
    }

    public void testDecryptingWithADifferentKeyFailsLoudly() throws Exception {
        WalRecord original = new WalRecord("idx", 0, 0, "payload".getBytes(StandardCharsets.UTF_8));
        WalRecord encrypted = WalRecordCrypto.encrypt(original, new StaticEncryptionKeyProvider(newAesKey()));

        StaticEncryptionKeyProvider wrongKey = new StaticEncryptionKeyProvider(newAesKey());
        expectThrows(IOException.class, () -> WalRecordCrypto.decrypt(encrypted, wrongKey));
    }

    public void testDecryptAllAppliesToEveryRecordInOrder() throws Exception {
        StaticEncryptionKeyProvider keyProvider = new StaticEncryptionKeyProvider(newAesKey());
        List<WalRecord> originals = List.of(
            new WalRecord("idx", 0, 0, "a".getBytes(StandardCharsets.UTF_8)),
            new WalRecord("idx", 0, 1, "b".getBytes(StandardCharsets.UTF_8)),
            new WalRecord("idx", 1, 0, "c".getBytes(StandardCharsets.UTF_8))
        );
        List<WalRecord> encrypted = originals.stream().map(r -> {
            try {
                return WalRecordCrypto.encrypt(r, keyProvider);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).toList();

        List<WalRecord> decrypted = WalRecordCrypto.decryptAll(encrypted, keyProvider);

        assertEquals(originals.size(), decrypted.size());
        for (int i = 0; i < originals.size(); i++) {
            assertArrayEquals(originals.get(i).payload(), decrypted.get(i).payload());
            assertEquals(originals.get(i).seqNo(), decrypted.get(i).seqNo());
        }
    }
}
