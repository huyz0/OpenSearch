/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.serverless.storage.security.PerIndexEncryptionKeyProvider;
import org.opensearch.serverless.storage.security.StaticEncryptionKeyProvider;
import org.opensearch.test.OpenSearchTestCase;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class WalRecordCryptoTests extends OpenSearchTestCase {

    private static SecretKey newAesKey() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(256);
        return keyGenerator.generateKey();
    }

    public void testEncryptThenDecryptRoundTripsThePayloadExactly() throws Exception {
        StaticEncryptionKeyProvider keyProvider = new StaticEncryptionKeyProvider(newAesKey());
        WalRecord original = new WalRecord("idx", 3, 1, 42, "the operation payload".getBytes(StandardCharsets.UTF_8));

        WalRecord encrypted = WalRecordCrypto.encrypt(original, keyProvider);
        WalRecord decrypted = WalRecordCrypto.decrypt(encrypted, keyProvider);

        assertArrayEquals(original.payload(), decrypted.payload());
    }

    public void testEncryptionLeavesIndexShardAndSeqNoAsPlaintext() throws Exception {
        // Routing metadata (WalChunkReader#filterByShard) must survive untouched -- only the
        // payload is meant to become opaque.
        StaticEncryptionKeyProvider keyProvider = new StaticEncryptionKeyProvider(newAesKey());
        WalRecord original = new WalRecord("idx-a", 7, 1, 99, "payload".getBytes(StandardCharsets.UTF_8));

        WalRecord encrypted = WalRecordCrypto.encrypt(original, keyProvider);

        assertEquals("idx-a", encrypted.indexUuid());
        assertEquals(7, encrypted.shardId());
        assertEquals(99, encrypted.seqNo());
    }

    public void testEncryptedPayloadDiffersFromThePlaintext() throws Exception {
        StaticEncryptionKeyProvider keyProvider = new StaticEncryptionKeyProvider(newAesKey());
        byte[] plaintext = "not secret unless encrypted".getBytes(StandardCharsets.UTF_8);
        WalRecord original = new WalRecord("idx", 0, 1, 0, plaintext);

        WalRecord encrypted = WalRecordCrypto.encrypt(original, keyProvider);

        assertFalse(java.util.Arrays.equals(plaintext, encrypted.payload()));
    }

    public void testDecryptingWithADifferentKeyFailsLoudly() throws Exception {
        WalRecord original = new WalRecord("idx", 0, 1, 0, "payload".getBytes(StandardCharsets.UTF_8));
        WalRecord encrypted = WalRecordCrypto.encrypt(original, new StaticEncryptionKeyProvider(newAesKey()));

        StaticEncryptionKeyProvider wrongKey = new StaticEncryptionKeyProvider(newAesKey());
        expectThrows(IOException.class, () -> WalRecordCrypto.decrypt(encrypted, wrongKey));
    }

    public void testPerIndexKeyProviderGivesGenuineIndexIsolation() throws Exception {
        // rfc-serverless-opensearch.md §12's own previously-stated caveat -- record-level
        // encryption "doesn't yet buy per-index key isolation in practice" against a provider that
        // only ever has one key -- is what a real per-index-aware provider closes: two indices
        // sharing one WAL chunk get genuinely different ciphertext under genuinely different keys,
        // and cross-decrypting one index's record with another index's own provider view fails.
        SecretKey keyA = newAesKey();
        SecretKey keyB = newAesKey();
        PerIndexEncryptionKeyProvider keyProvider = new PerIndexEncryptionKeyProvider(Map.of("index-a", keyA, "index-b", keyB), null);
        byte[] plaintext = "same payload bytes, different index".getBytes(StandardCharsets.UTF_8);

        WalRecord recordA = WalRecordCrypto.encrypt(new WalRecord("index-a", 0, 1, 0, plaintext), keyProvider);
        WalRecord recordB = WalRecordCrypto.encrypt(new WalRecord("index-b", 0, 1, 0, plaintext), keyProvider);

        assertFalse(
            "the same plaintext under two indices' own keys must not produce identical ciphertext",
            java.util.Arrays.equals(recordA.payload(), recordB.payload())
        );
        assertArrayEquals(plaintext, WalRecordCrypto.decrypt(recordA, keyProvider).payload());
        assertArrayEquals(plaintext, WalRecordCrypto.decrypt(recordB, keyProvider).payload());

        // Decrypting index A's record against a provider whose own "index-a" entry actually holds
        // index B's key fails loudly (AEAD tag mismatch), the same as any other wrong-key decrypt --
        // proving the lookup is genuinely keyed by index, not merely present/absent.
        PerIndexEncryptionKeyProvider misconfigured = new PerIndexEncryptionKeyProvider(Map.of("index-a", keyB), null);
        expectThrows(IOException.class, () -> WalRecordCrypto.decrypt(recordA, misconfigured));
    }

    public void testDecryptAllAppliesToEveryRecordInOrder() throws Exception {
        StaticEncryptionKeyProvider keyProvider = new StaticEncryptionKeyProvider(newAesKey());
        List<WalRecord> originals = List.of(
            new WalRecord("idx", 0, 1, 0, "a".getBytes(StandardCharsets.UTF_8)),
            new WalRecord("idx", 0, 1, 1, "b".getBytes(StandardCharsets.UTF_8)),
            new WalRecord("idx", 1, 1, 0, "c".getBytes(StandardCharsets.UTF_8))
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
