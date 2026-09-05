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
import java.nio.charset.StandardCharsets;
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
 * construction. Each record's own {@code indexUuid} is looked up via {@link
 * EncryptionKeyProvider#currentKey(String)} rather than the index-agnostic {@link
 * EncryptionKeyProvider#currentKey()}, so this genuinely buys per-index key isolation the moment
 * {@code keyProvider} is a real per-index-aware implementation (e.g. {@link
 * org.opensearch.serverless.storage.security.PerIndexEncryptionKeyProvider}) -- a provider that
 * only ever has one key regardless of index ({@link
 * org.opensearch.serverless.storage.security.StaticEncryptionKeyProvider}) still works unchanged,
 * since {@link EncryptionKeyProvider#currentKey(String)} defaults to {@link
 * EncryptionKeyProvider#currentKey()} for those.
 */
public final class WalRecordCrypto {

    private WalRecordCrypto() {}

    /**
     * The first {@link WalChunkWriter#FORMAT_VERSION} whose records are encrypted with their
     * identity as AES-GCM associated data. Chunks written at an earlier version have no associated
     * data and must be decrypted without it, or every record written before the upgrade fails to
     * replay -- which is data loss on the first crash after a rolling restart, not a compatibility
     * inconvenience. See {@link #associatedDataFor} for what the binding buys and
     * {@link #decryptAll(List, EncryptionKeyProvider, int)} for how the version reaches this class.
     */
    public static final int FIRST_IDENTITY_BOUND_FORMAT_VERSION = 3;

    /**
     * Binds this record's ciphertext to the identity that travels beside it in the clear. Without it,
     * the envelope is portable: under the single node-wide key that is the only key any deployable
     * configuration has, index A's payload can be relabelled as index B's and replayed. AES-GCM
     * authenticates the payload; only associated data authenticates the label.
     *
     * <p>All four identity fields, not just the index: a chunk is a node-level group commit mixing
     * many shards, so relabelling within one index (shard 3's record presented as shard 0's) is just
     * as reachable as relabelling across indices, and {@code primaryTerm}/{@code seqNo} are what
     * replay's own fencing filter and the local checkpoint bookkeeping key off.
     */
    private static byte[] associatedDataFor(WalRecord record) {
        return ("WALv1|" + record.indexUuid() + "|" + record.shardId() + "|" + record.primaryTerm() + "|" + record.seqNo()).getBytes(
            StandardCharsets.UTF_8
        );
    }

    /**
     * Returns a copy of {@code record} with its payload replaced by ciphertext, bound to the
     * record's own identity as associated data.
     *
     * @param record the record whose plaintext payload should be encrypted
     * @param keyProvider supplies the key used to encrypt the payload
     * @return a copy of {@code record} with its payload replaced by ciphertext
     */
    public static WalRecord encrypt(WalRecord record, EncryptionKeyProvider keyProvider) throws IOException {
        byte[] ciphertext = AesGcmCipher.encrypt(record.payload(), keyProvider.currentKey(record.indexUuid()), associatedDataFor(record));
        return new WalRecord(record.indexUuid(), record.shardId(), record.primaryTerm(), record.seqNo(), ciphertext);
    }

    /**
     * Returns a copy of {@code record} with its payload replaced by the decrypted plaintext, checking
     * the identity binding {@link #encrypt} applied.
     *
     * @param record the record whose ciphertext payload should be decrypted
     * @param keyProvider supplies the key used to decrypt the payload
     * @return a copy of {@code record} with its payload replaced by plaintext
     */
    public static WalRecord decrypt(WalRecord record, EncryptionKeyProvider keyProvider) throws IOException {
        return decrypt(record, keyProvider, WalChunkWriter.FORMAT_VERSION);
    }

    /**
     * As {@link #decrypt(WalRecord, EncryptionKeyProvider)}, but for a record read out of a chunk
     * written at {@code chunkFormatVersion}: identity is only supplied as associated data when that
     * version actually wrote it that way (see {@link #FIRST_IDENTITY_BOUND_FORMAT_VERSION}).
     *
     * @param record the record whose ciphertext payload should be decrypted
     * @param keyProvider supplies the key used to decrypt the payload
     * @param chunkFormatVersion the {@code formatVersion} of the chunk this record was read from
     * @return a copy of {@code record} with its payload replaced by plaintext
     */
    public static WalRecord decrypt(WalRecord record, EncryptionKeyProvider keyProvider, int chunkFormatVersion) throws IOException {
        byte[] plaintext = chunkFormatVersion >= FIRST_IDENTITY_BOUND_FORMAT_VERSION
            ? AesGcmCipher.decrypt(record.payload(), keyProvider.currentKey(record.indexUuid()), associatedDataFor(record))
            : AesGcmCipher.decrypt(record.payload(), keyProvider.currentKey(record.indexUuid()));
        return new WalRecord(record.indexUuid(), record.shardId(), record.primaryTerm(), record.seqNo(), plaintext);
    }

    /**
     * {@link #decrypt} applied to every record in {@code records}, in order, assuming they came from
     * a chunk written at the current {@link WalChunkWriter#FORMAT_VERSION}.
     *
     * @param records the records whose ciphertext payloads should be decrypted
     * @param keyProvider supplies the key used to decrypt each payload
     * @return the decrypted records, in the same order as {@code records}
     */
    public static List<WalRecord> decryptAll(List<WalRecord> records, EncryptionKeyProvider keyProvider) throws IOException {
        return decryptAll(records, keyProvider, WalChunkWriter.FORMAT_VERSION);
    }

    /**
     * {@link #decrypt(WalRecord, EncryptionKeyProvider, int)} applied to every record in {@code
     * records}, in order. This is the overload replay uses, since only the chunk parser knows which
     * format version the bytes on disk were written at.
     *
     * @param records the records whose ciphertext payloads should be decrypted
     * @param keyProvider supplies the key used to decrypt each payload
     * @param chunkFormatVersion the {@code formatVersion} of the chunk these records were read from
     * @return the decrypted records, in the same order as {@code records}
     */
    public static List<WalRecord> decryptAll(List<WalRecord> records, EncryptionKeyProvider keyProvider, int chunkFormatVersion)
        throws IOException {
        List<WalRecord> decrypted = new ArrayList<>(records.size());
        for (WalRecord record : records) {
            decrypted.add(decrypt(record, keyProvider, chunkFormatVersion));
        }
        return decrypted;
    }
}
