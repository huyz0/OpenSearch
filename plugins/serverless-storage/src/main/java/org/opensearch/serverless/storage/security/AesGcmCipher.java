/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * The AES/GCM envelope format shared by every at-rest encryption use in this plugin: {@code
 * <12-byte random IV><AES/GCM ciphertext+16-byte tag>}, one fresh random IV per call. Extracted out
 * of {@link EncryptingBlobContainer} so a second caller ({@code WalRecordCrypto}, for per-record
 * WAL payload encryption -- see rfc-serverless-opensearch.md &sect;12 bullet 1) doesn't have to
 * duplicate the same cipher setup and error handling; both callers only ever differ in *what* bytes
 * they hand this class, never in how those bytes get protected.
 *
 * <h2>Associated data, and why the no-AAD overloads are the dangerous ones</h2>
 *
 * <p>AES/GCM authenticates its ciphertext, so a bit flipped inside an envelope is caught. What it
 * does <em>not</em> authenticate, unless it is told to, is <em>where that envelope was supposed to
 * live</em>. Every envelope this class produced before AAD existed was a self-contained, portable
 * blob of ciphertext under one key: valid wherever it was pasted. Concretely, with an attacker who
 * can write to the object store (the trust boundary &sect;12 explicitly says is shared with the
 * storage provider), and with the single node-wide key that is the only key any deployable
 * configuration of this plugin actually has:
 *
 * <ul>
 *   <li><b>Substitution.</b> Block 7 of index A's bundle and block 7 of index B's bundle are two
 *       64 KiB envelopes under the same key. Swap them and both blobs still decrypt <em>and still
 *       authenticate</em>. A reader serves B's documents believing they are A's.</li>
 *   <li><b>Reordering.</b> Blocks within one blob are equally interchangeable, so a blob's contents
 *       can be permuted without any tag ever failing.</li>
 *   <li><b>Truncation.</b> {@link BlockLayout}'s header was written in the clear and covered by no
 *       tag at all, so lowering {@code totalPlaintextLength} and dropping the trailing blocks
 *       yielded a shorter file that decrypts perfectly.</li>
 *   <li><b>Relabelling.</b> A WAL record's {@code indexUuid}/{@code shardId}/{@code seqNo} travel
 *       beside the ciphertext in plaintext, so record payloads can be moved between indices.</li>
 * </ul>
 *
 * <p>Binding an envelope to its position -- which blob, which index, which shard, which block index,
 * under which declared length -- is exactly what GCM's associated-data input is for, and it costs
 * nothing: AAD is authenticated, not encrypted, so it adds no bytes to the ciphertext and no
 * measurable time. That is why {@link #encrypt(byte[], SecretKey, byte[])} and {@link
 * #decrypt(byte[], SecretKey, byte[])} are the overloads a new caller should use, and why they take
 * the AAD as a required argument rather than defaulting it: an empty AAD is a decision, and it
 * should have to be typed out.
 *
 * <p>The two-argument overloads remain <b>only</b> because two existing callers write formats whose
 * on-disk layout has no version field to negotiate an upgrade through
 * ({@code format/LocalDiskCachingBundleStore}, which encrypts a node-local cache file, and {@code
 * wal/WalRecordCrypto}, which encrypts a WAL record payload) and because {@link
 * EncryptingBlobContainer} must still read v1 blobs written before this change. They are a
 * compatibility surface, not the default. See each caller's own javadoc for what it is exposed to
 * while it still uses them.
 */
public final class AesGcmCipher {

    private static final int GCM_IV_LENGTH_BYTES = 12;
    private static final int GCM_TAG_LENGTH_BITS = 128;
    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * The AAD every {@code encrypt(byte[], SecretKey)} / {@code decrypt(byte[], SecretKey)} call
     * uses: none at all. Named rather than inlined so the two-argument overloads read as "AAD =
     * NO_ASSOCIATED_DATA" at their one call site each, instead of quietly not mentioning AAD --
     * the omission is the whole vulnerability this class's javadoc describes, and it should be
     * visible in the code, not only in prose.
     */
    private static final byte[] NO_ASSOCIATED_DATA = new byte[0];

    private AesGcmCipher() {}

    /**
     * Encrypts with a fresh random IV prepended to the ciphertext, and <b>no associated data</b>.
     *
     * <p>The resulting envelope is portable: it decrypts and authenticates anywhere the same key is
     * in use, so it carries no evidence of which blob, index, shard or offset it was written for.
     * Prefer {@link #encrypt(byte[], SecretKey, byte[])}. See this class's javadoc for what
     * "portable" costs.
     *
     * @param plaintext the bytes to encrypt.
     * @param key the AES key to encrypt with.
     * @return {@code <12-byte IV><AES/GCM ciphertext+16-byte tag>}.
     */
    public static byte[] encrypt(byte[] plaintext, SecretKey key) throws IOException {
        return encrypt(plaintext, key, NO_ASSOCIATED_DATA);
    }

    /**
     * Encrypts with a fresh random IV prepended to the ciphertext, binding the envelope to {@code
     * associatedData}.
     *
     * <p>{@code associatedData} is authenticated but not encrypted and does not appear in the
     * output: the reader must reconstruct exactly the same bytes from its own context (the blob
     * name it asked for, the index/shard it believes it is reading, the block index it is at) and
     * pass them to {@link #decrypt(byte[], SecretKey, byte[])}. Any mismatch -- including an
     * envelope that is byte-identical but was written for a different position -- fails the tag
     * check, which is the entire point.
     *
     * @param plaintext the bytes to encrypt.
     * @param key the AES key to encrypt with.
     * @param associatedData the context to bind this envelope to; never {@code null}, and an empty
     *                       array means the caller has deliberately chosen a portable envelope.
     * @return {@code <12-byte IV><AES/GCM ciphertext+16-byte tag>}.
     */
    public static byte[] encrypt(byte[] plaintext, SecretKey key, byte[] associatedData) throws IOException {
        byte[] iv = new byte[GCM_IV_LENGTH_BYTES];
        RANDOM.nextBytes(iv);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
            // Must precede doFinal: updateAAD after any update/doFinal call is an
            // IllegalStateException, and a Cipher that was never told about AAD produces a tag over
            // the ciphertext alone -- which is exactly the envelope this change exists to stop
            // producing. Calling it unconditionally (even for a zero-length array) keeps the two
            // code paths identical, since GCM over empty AAD is defined and equals the no-AAD tag.
            cipher.updateAAD(associatedData);
            byte[] ciphertext = cipher.doFinal(plaintext);
            byte[] result = new byte[iv.length + ciphertext.length];
            System.arraycopy(iv, 0, result, 0, iv.length);
            System.arraycopy(ciphertext, 0, result, iv.length, ciphertext.length);
            return result;
        } catch (GeneralSecurityException e) {
            throw new IOException("failed to encrypt", e);
        }
    }

    /**
     * Decrypts and authenticates the output of {@link #encrypt(byte[], SecretKey)} -- i.e. with no
     * associated data. Prefer {@link #decrypt(byte[], SecretKey, byte[])}.
     *
     * @param ivAndCiphertext {@code <12-byte IV><AES/GCM ciphertext+16-byte tag>}.
     * @param key the AES key to decrypt with; must match the key {@code encrypt} used.
     * @return the original plaintext.
     */
    public static byte[] decrypt(byte[] ivAndCiphertext, SecretKey key) throws IOException {
        return decrypt(ivAndCiphertext, key, NO_ASSOCIATED_DATA);
    }

    /**
     * Decrypts and authenticates the output of {@link #encrypt(byte[], SecretKey, byte[])}, checking
     * that it was written for exactly this {@code associatedData}.
     *
     * <p>A wrong key, a corrupted envelope, and an envelope written for a <em>different</em>
     * position all surface identically here, as the same {@link IOException}: GCM cannot tell them
     * apart, and the caller must not treat "authentication failed" as anything other than "these
     * bytes are not the bytes that belong here."
     *
     * @param ivAndCiphertext {@code <12-byte IV><AES/GCM ciphertext+16-byte tag>}.
     * @param key the AES key to decrypt with; must match the key {@code encrypt} used.
     * @param associatedData the same context bytes the writer passed; never {@code null}.
     * @return the original plaintext.
     */
    public static byte[] decrypt(byte[] ivAndCiphertext, SecretKey key, byte[] associatedData) throws IOException {
        if (ivAndCiphertext.length < GCM_IV_LENGTH_BYTES) {
            throw new IOException("ciphertext too short to contain an IV: " + ivAndCiphertext.length + " bytes");
        }
        byte[] iv = Arrays.copyOfRange(ivAndCiphertext, 0, GCM_IV_LENGTH_BYTES);
        byte[] ciphertext = Arrays.copyOfRange(ivAndCiphertext, GCM_IV_LENGTH_BYTES, ivAndCiphertext.length);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
            cipher.updateAAD(associatedData);
            return cipher.doFinal(ciphertext);
        } catch (BadPaddingException | IllegalBlockSizeException e) {
            throw new IOException(
                "failed authentication -- wrong key, the data was corrupted/tampered with, or this envelope "
                    + "was written for a different blob/index/shard/block than the one being read",
                e
            );
        } catch (GeneralSecurityException e) {
            throw new IOException("failed to decrypt", e);
        }
    }
}
