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
 */
public final class AesGcmCipher {

    private static final int GCM_IV_LENGTH_BYTES = 12;
    private static final int GCM_TAG_LENGTH_BITS = 128;
    private static final SecureRandom RANDOM = new SecureRandom();

    private AesGcmCipher() {}

    /**
     * Encrypts with a fresh random IV prepended to the ciphertext.
     *
     * @param plaintext the bytes to encrypt.
     * @param key the AES key to encrypt with.
     * @return {@code <12-byte IV><AES/GCM ciphertext+16-byte tag>}.
     */
    public static byte[] encrypt(byte[] plaintext, SecretKey key) throws IOException {
        byte[] iv = new byte[GCM_IV_LENGTH_BYTES];
        RANDOM.nextBytes(iv);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
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
     * Decrypts and authenticates the output of {@link #encrypt}.
     *
     * @param ivAndCiphertext {@code <12-byte IV><AES/GCM ciphertext+16-byte tag>}.
     * @param key the AES key to decrypt with; must match the key {@code encrypt} used.
     * @return the original plaintext.
     */
    public static byte[] decrypt(byte[] ivAndCiphertext, SecretKey key) throws IOException {
        if (ivAndCiphertext.length < GCM_IV_LENGTH_BYTES) {
            throw new IOException("ciphertext too short to contain an IV: " + ivAndCiphertext.length + " bytes");
        }
        byte[] iv = Arrays.copyOfRange(ivAndCiphertext, 0, GCM_IV_LENGTH_BYTES);
        byte[] ciphertext = Arrays.copyOfRange(ivAndCiphertext, GCM_IV_LENGTH_BYTES, ivAndCiphertext.length);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
            return cipher.doFinal(ciphertext);
        } catch (BadPaddingException | IllegalBlockSizeException e) {
            throw new IOException("failed authentication -- wrong key, or the data was corrupted/tampered with", e);
        } catch (GeneralSecurityException e) {
            throw new IOException("failed to decrypt", e);
        }
    }
}
