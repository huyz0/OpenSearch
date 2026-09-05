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
import org.opensearch.serverless.storage.security.StaticEncryptionKeyProvider;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Tests for the security review's finding 11.3: a WAL record's {@code indexUuid}, {@code shardId},
 * {@code primaryTerm} and {@code seqNo} travel beside its ciphertext in the clear and used to be
 * authenticated by nothing, so under the single node-wide key that is the only key any deployable
 * configuration has, one index's payload could be relabelled as another's and replayed. They are now
 * AES-GCM associated data.
 *
 * <p>The second test is the other half of that change and matters just as much: bumping the chunk
 * format version silently would mean every record written before the upgrade fails to replay -- data
 * loss on the first crash after a rolling restart, not a compatibility inconvenience.
 */
public class WalRecordCryptoIdentityBindingTests extends OpenSearchTestCase {

    private static final String INDEX_A = "index-aaaaaaaaaaaaaaaaaaaa";
    private static final String INDEX_B = "index-bbbbbbbbbbbbbbbbbbbb";

    private final EncryptionKeyProvider keyProvider = StaticEncryptionKeyProvider.fromRawKeyBytes(new byte[32]);

    private static WalRecord plaintextRecord(String indexUuid, int shardId, long primaryTerm, long seqNo) {
        byte[] payload = ("payload-" + seqNo).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return new WalRecord(indexUuid, shardId, primaryTerm, seqNo, payload);
    }

    /** The round trip still works -- proof the binding is checked, not merely applied on the way in. */
    public void testAnIdentityBoundRecordRoundTrips() throws Exception {
        WalRecord original = plaintextRecord(INDEX_A, 3, 7L, 42L);
        WalRecord encrypted = WalRecordCrypto.encrypt(original, keyProvider);
        assertFalse("the payload must actually be ciphertext", java.util.Arrays.equals(original.payload(), encrypted.payload()));
        WalRecord decrypted = WalRecordCrypto.decrypt(encrypted, keyProvider);
        assertArrayEquals(original.payload(), decrypted.payload());
    }

    /**
     * The finding at its narrowest: take a record's ciphertext and present it under a different
     * index's label. Before the binding this decrypted cleanly and replayed into the wrong index.
     */
    public void testARecordRelabelledIntoAnotherIndexFailsToDecrypt() throws Exception {
        WalRecord encrypted = WalRecordCrypto.encrypt(plaintextRecord(INDEX_A, 0, 1L, 1L), keyProvider);
        WalRecord relabelled = new WalRecord(INDEX_B, 0, 1L, 1L, encrypted.payload());

        expectThrows(IOException.class, () -> WalRecordCrypto.decrypt(relabelled, keyProvider));
    }

    /**
     * Relabelling <em>within</em> one index is just as reachable, because a chunk is a node-level
     * group commit mixing many shards -- so the shard, term and sequence number are bound too, not
     * only the index.
     */
    public void testARecordRelabelledIntoAnotherShardTermOrSeqNoFailsToDecrypt() throws Exception {
        WalRecord encrypted = WalRecordCrypto.encrypt(plaintextRecord(INDEX_A, 0, 1L, 1L), keyProvider);

        expectThrows(IOException.class, () -> WalRecordCrypto.decrypt(new WalRecord(INDEX_A, 1, 1L, 1L, encrypted.payload()), keyProvider));
        expectThrows(IOException.class, () -> WalRecordCrypto.decrypt(new WalRecord(INDEX_A, 0, 2L, 1L, encrypted.payload()), keyProvider));
        expectThrows(IOException.class, () -> WalRecordCrypto.decrypt(new WalRecord(INDEX_A, 0, 1L, 2L, encrypted.payload()), keyProvider));
    }

    /**
     * A chunk written by a node running the previous format -- records encrypted with no associated
     * data, header claiming version 2 -- must still parse and still decrypt. This is the case that
     * exists in every real upgrade: the chunks already sitting in the shared container when a node
     * restarts are exactly the ones a crash immediately afterwards has to replay.
     */
    public void testAChunkWrittenAtTheOlderFormatVersionStillDecrypts() throws Exception {
        WalRecord plaintext = plaintextRecord(INDEX_A, 0, 1L, 5L);
        // Encrypted the way the old code did it: no associated data at all.
        WalRecord legacyEncrypted = new WalRecord(
            plaintext.indexUuid(),
            plaintext.shardId(),
            plaintext.primaryTerm(),
            plaintext.seqNo(),
            AesGcmCipher.encrypt(plaintext.payload(), keyProvider.currentKey(plaintext.indexUuid()))
        );

        byte[] chunk = downgradeChunkToVersion(WalChunkWriter.write(List.of(legacyEncrypted)), 2);

        WalChunkReader.ParsedChunk parsed = WalChunkReader.readChunk(chunk);
        assertEquals("the older version must be accepted, and reported", 2, parsed.formatVersion());

        List<WalRecord> decrypted = WalRecordCrypto.decryptAll(parsed.records(), keyProvider, parsed.formatVersion());
        assertEquals(1, decrypted.size());
        assertArrayEquals(plaintext.payload(), decrypted.get(0).payload());

        // And the version genuinely is load-bearing: decrypting the same bytes as if they were the
        // current format (i.e. expecting associated data) must fail rather than silently succeed.
        expectThrows(IOException.class, () -> WalRecordCrypto.decryptAll(parsed.records(), keyProvider));
    }

    /** A version this build has never heard of is still rejected -- the range check has an upper bound too. */
    public void testAFutureFormatVersionIsStillRejected() throws Exception {
        byte[] chunk = downgradeChunkToVersion(WalChunkWriter.write(List.of(plaintextRecord(INDEX_A, 0, 1L, 1L))), 99);
        WalFormatException e = expectThrows(WalFormatException.class, () -> WalChunkReader.readChunk(chunk));
        assertTrue(e.getMessage(), e.getMessage().contains("unsupported WAL chunk format version 99"));
    }

    /**
     * Rewrites a chunk's {@code formatVersion} header field and recomputes the trailing CRC32C, so a
     * test can produce bytes that a previous (or future) build would have written without keeping a
     * copy of that build's writer around.
     */
    private static byte[] downgradeChunkToVersion(byte[] chunk, int version) {
        byte[] copy = chunk.clone();
        // Layout: 4-byte magic, then the 4-byte big-endian version, ..., then an 8-byte trailing CRC.
        copy[4] = (byte) (version >>> 24);
        copy[5] = (byte) (version >>> 16);
        copy[6] = (byte) (version >>> 8);
        copy[7] = (byte) version;
        CRC32C crc = new CRC32C();
        crc.update(copy, 0, copy.length - 8);
        long value = crc.getValue();
        for (int i = 0; i < 8; i++) {
            copy[copy.length - 8 + i] = (byte) (value >>> (56 - 8 * i));
        }
        return copy;
    }
}
