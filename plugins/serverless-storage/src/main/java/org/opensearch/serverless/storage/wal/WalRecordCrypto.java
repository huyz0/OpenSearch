/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.serverless.storage.security.AesGcmCipher;
import org.opensearch.serverless.storage.security.EncryptionKeyProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Per-record envelope encryption for a {@link WalRecord}'s {@code payload}
 * (rfc-serverless-opensearch.md &sect;12 bullet 1, previously "not implemented"): {@code
 * indexUuid}, {@code shardId}, and {@code seqNo} stay plaintext (a chunk reader needs them to
 * route/filter records without decrypting anything, per {@link WalChunkReader#filterByShard}),
 * only {@code payload} is encrypted. This is deliberately record-level, not
 * whole-chunk-blob-level like {@link org.opensearch.serverless.storage.security.EncryptingBlobContainer}:
 * {@link WalChunkService} group-commits records from many shards (potentially many indices) into
 * one chunk blob, so a single whole-blob key wouldn't respect per-index key boundaries the way
 * whole-blob encryption does for bundles/manifests, which are already single-index by
 * construction. Today's {@link EncryptionKeyProvider} only ever supplies one key regardless of
 * index, so this doesn't yet buy per-index key isolation in practice -- but the record-level
 * design means it will, the moment a real per-index-aware key provider exists, without any change
 * to the WAL wire format or this class.
 */
public final class WalRecordCrypto {

    private WalRecordCrypto() {}

    /**
     * Returns a copy of {@code record} with its payload replaced by ciphertext.
     *
     * @param record the record whose plaintext payload should be encrypted
     * @param keyProvider supplies the key used to encrypt the payload
     * @return a copy of {@code record} with its payload replaced by ciphertext
     */
    public static WalRecord encrypt(WalRecord record, EncryptionKeyProvider keyProvider) throws IOException {
        byte[] ciphertext = AesGcmCipher.encrypt(record.payload(), keyProvider.currentKey());
        return new WalRecord(record.indexUuid(), record.shardId(), record.primaryTerm(), record.seqNo(), ciphertext);
    }

    /**
     * Returns a copy of {@code record} with its payload replaced by the decrypted plaintext.
     *
     * @param record the record whose ciphertext payload should be decrypted
     * @param keyProvider supplies the key used to decrypt the payload
     * @return a copy of {@code record} with its payload replaced by plaintext
     */
    public static WalRecord decrypt(WalRecord record, EncryptionKeyProvider keyProvider) throws IOException {
        byte[] plaintext = AesGcmCipher.decrypt(record.payload(), keyProvider.currentKey());
        return new WalRecord(record.indexUuid(), record.shardId(), record.primaryTerm(), record.seqNo(), plaintext);
    }

    /**
     * {@link #decrypt} applied to every record in {@code records}, in order.
     *
     * @param records the records whose ciphertext payloads should be decrypted
     * @param keyProvider supplies the key used to decrypt each payload
     * @return the decrypted records, in the same order as {@code records}
     */
    public static List<WalRecord> decryptAll(List<WalRecord> records, EncryptionKeyProvider keyProvider) throws IOException {
        List<WalRecord> decrypted = new ArrayList<>(records.size());
        for (WalRecord record : records) {
            decrypted.add(decrypt(record, keyProvider));
        }
        return decrypted;
    }
}
